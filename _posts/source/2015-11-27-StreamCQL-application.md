---
layout: post
title: StreamCQL源码分析之application
category: Source
tags: BigData
keywords: 
description: 
---

上篇在解析Schema的时候顺便分析了一些常用的Statement syntax和对应的语法语义解析器/结果,  
现在继续ApplicationBuilder.buildApplication中parseSchemas的下一步splitOperators.  
```java
    private void buildApplication() {
        app = new Application(applicationName);
        parseSchemas();
        List<SplitContext> splitContexts = splitOperators();            //拆分算子
        SplitContext splitContext = combineOperators(splitContexts);    //组合算子
        changeUnionOperators(splitContext);
        changeSchemaAfterAggregate(splitContext);
        app.setOperators(splitContext.getOperators());          //拆分结果包含了operatots和transitions
        app.setOpTransition(splitContext.getTransitions());
    }
```

# Application

## Split and Combine Operators

combineOperators会创建OperatorCombiner, 并调用combine方法将splitContexts合并起来,终于打印了日志中看到的:`combine all split contexts`(解析submit之后).  
构建Application的主要工作就是Split和Combine,最后将SplitContext的operators和transitions设置到Application对象中,完成应用程序的构建,在这基础上再进行物理优化.    

第一步就是SplitOperators拆分算子(parseContexts是语义解析器结果列表): 创建对应的Spliter,调用其split方法.     
```java
    private List<SplitContext> splitOperators() {
        List<SplitContext> splitContexts = Lists.newArrayList();
        for (AnalyzeContext pContext : parseContexts) {
            parseAutoCreatePipeStream(splitContexts, pContext);
            parseSubQueryOperators(splitContexts, pContext);
            SplitContext context = OperatorSplitter.split(buildUtils, pContext);
            splitContexts.add(context);
        }
        return splitContexts;
    }
```

OperatorSplitter的splitters采用static块提前添加了系统中也有的算子拆分类. 结合上面的splitOperators就是一个双层循环了:  
针对Application的每一个AnalyzeContext, 判断哪个Splitter可以解析这个AnalyzeContext. 那么会有可能一个AnalyzeContext  
有多个Splitter吗? 不会的,因为只要一个Splitter验证通过就返回当前Splitter拆分后的SplitContext了(return结束下面的for循环).  
```java
    public static SplitContext split(BuilderUtils buildUtils, AnalyzeContext parseContext) {
        for (Splitter splitter : splitters) {
            if (splitter.validate(parseContext)) {
                return createSplitter(splitter.getClass(), buildUtils).split(parseContext);
            }
        }
        return null;
    }
```

都有哪些Splitter(SelectSplitter抽象类是DataSource,Aggregate,Join的父类,所以不能被实例化):  
每个具体的Splitter都实现了validate方法根据传入的AnalyzeContext实现类(pContext)用来验证能否进行解析  
```
Splitter                                AnalyzeContext
    |-- SelectSplitter                      |-- SelectAnalyzeContext     
            |-- DataSourceSplitter 
            |-- AggregateSplitter           
            |-- JoinSplitter 
    |-- InsertSplitter                      |-- InsertAnalyzeContext >> InsertOnlyAnalyzeContext
    |-- SourceOperatorSplitter              |-- CreateStreamAnalyzeContext
    |-- MultiInsertSplitter                 |-- MultiInsertStatementAnalyzeContext
    |-- UserOperatorSplitter                |-- InsertUserOperatorStatementAnalyzeContext
```

比如AggregateSplitter的validate方法会验证是不是SelectAnalyzeContext.  
```java
    public boolean validate(AnalyzeContext parseContext) {
        if (!(parseContext instanceof SelectAnalyzeContext))    return false;
        SelectAnalyzeContext selectAnalyzeContext = (SelectAnalyzeContext)parseContext;
        FromClauseAnalyzeContext clauseContext = selectAnalyzeContext.getFromClauseContext();
        if (clauseContext.getJoinexpression() != null)          return false;
        if (clauseContext.getCombineConditions().size() != 0)   return false;
        return true;  //属于select,然后既不是combine，又不是join，那么就是aggregate
    }
```

>普通的select可以看做是aggregate,比如`select count(id) from a where`就是一种聚合. 因为select子句是count(id)    
但是如果是`select id from a join b on a.id=b.id`因为有join操作就不是aggregate了.  
所以可以看到SelectSplitter针对这两种语句分成了AggregateSplitter和JoinSplitter.  

### Input/Output Operator

Input和Output算子的拆分由SourceOperatorSplitter实现: 根据AnalyzeContext创建Operator算子, 即根据语义分析结果拆分内容   
```java
public class SourceOperatorSplitter implements Splitter {   //源算子拆分,包括输入算子和输出算子
    private SplitContext result = new SplitContext();
    private CreateStreamAnalyzeContext context; 

    public SplitContext split(AnalyzeContext parseContext) {
        context = (CreateStreamAnalyzeContext)parseContext;  //语义解析结果
        setParallelNumber();
        addToInput();   //创建输入算子并加入到result中: InputStreamOperator
        addToOutput();  //创建输出算子并加入到result中: OutputStreamOperator
        addToPipe();    //连接算子: FilterOperator
        result.setParseContext(context);
        return result;
    }
}
```
 
pipe stream算子属于中间算子，本来是不会对应任何算子，只要解析出schema即可的. 但是为了和CQL的整体规则一致，便于后面创建算子之间的连线，  
所以这里创建一个空的filter算子，不带任何过滤。 这样，就可以在优化器阶段将这个filter算子优化掉

算子要从CreateStreamAnalyzeContext context获取出运行时的数据, context的设值是在语义解析器的analyze时.   
AnalyzeContext语义解析结果对CQL语法进行解析, 解析出来的数据最终会被用在算子上. 而算子才是构建Application的基础.  
```java
    private InputStreamOperator createInputSourceOperator() {
        String operatorName = getOperatorName(context.getRecordReaderClassName(),"Input");
        //创建输入流算子
        InputStreamOperator op = new InputStreamOperator(buildUtils.getNextOperatorName(operatorName), parallelNumber);
        //设置输入算子的属性: 反序列化类, 读取记录类
        op.setName(context.getStreamAlias());
        op.setDeserializerClassName(context.getDeserializerClassName());
        op.setRecordReaderClassName(context.getRecordReaderClassName());
        //反序列化类的属性和读取记录的属性, 对应CQL最原始的properties. 
        op.setArgs(new TreeMap<String, String>());
        op.getArgs().putAll(context.getReadWriterProperties());
        op.getArgs().putAll(context.getSerDeProperties());
        return op;
    }
```

#### Insert Operator

insert into语句的拆分.  
```java
public class InsertSplitter implements Splitter {
    private InsertAnalyzeContext context;

    public SplitContext split(AnalyzeContext parseContext) {
        context = (InsertAnalyzeContext)parseContext;
        result.setOutputStreamName(context.getOutputStreamName());
        //insert中包含了select, 所以要先创建select算子, 调用SelectSplitter.split
        SplitContext selectResult = OperatorSplitter.split(buildUtils, context.getSelectContext());
        //将select的结果算子和连接都加入到insert算子中
        result.getOperators().addAll(selectResult.getOperators());
        result.getTransitions().addAll(selectResult.getTransitions());
        result.setParseContext(context);
        return result;
    }
}
```

#### Select Operator

select语句的拆分以及Schema

1、最一般的select子句。  
只有一个schema, 输入和输出都是(同)一个schema, 不论有没有窗口，都必须放在聚合算子(AggregateSplitter)中。  
只要有where，就都放在functor算子(表达式)中，在优化器中，再进行调整，可以改为filter或者继承再聚合算子中。  

>单单一个select为什么要添加聚合算子?  
答: 聚合不一定就是group by, 可能是filter过滤,limit限制条数等.  
而select后面是可以跟上filter或者limit等. 聚合还可以是count,sum等.  

2、Join  
多个Join的schema，一个outputschema  
先查询出多个表所有的列，再在functor算子中进行列过滤。  

3、Groupby: 聚合算子  
4、orderby: 同一般select子句  
5、Join语句中不支持聚合和groupby，至少目前不支持  

6、三种过滤  
窗口之后的过滤：where，放在聚合算子中  
窗口之前的过滤：filter，前面加一个filter算子, 但是这样就牵扯到schema的变化，这个就麻烦一些了。先解析出所有的列，再进行过滤。  
聚合之后的过滤：having，放在聚合算子中  

总结下：  
1、聚合算子是必须有的。  
2、Orderby必须放在独立sort算子中  
3、limit放在output中作为限制，但是目前还不支持。  
4、一个select语句，如论如何拆分，都只有一个输出schema. 至少目前是这样，后面在优化器中会进行调整，将where中的一些列加入到select中，进行一些列变换。  
5、Join时候，先查询该流所有列的Join结果，之后再进行列过滤。  
6、Sort、Join、Aggregate算子都是按照字段进行分发，其他都是随机分发。  

SelectSplitter有三个实现类: AggregateSplitter, JoinSplitter, DataSourceSplitter.  

#### AggregateOperator

![stream-operators](http://img.blog.csdn.net/20151126173430754)

AggregateOperator聚合算子: 包含了window，以及window前后的一些filter操作. 当然还少不了count，sum之类的UDAF函数计算和UDF函数计算(BasicAggFunctionOperator)    

过滤条件是一个字符串形式的逻辑表达式, 允许有and,or以及大括号和udf函数, 但是绝对不允许出现udaf函数，因为这没有聚合操作  
**过滤发生在数据进入窗口之后，聚合之前**. 比如 (a>1 and a <100) or ( b is not null) 就是where的过滤，事件进入窗口之后，聚合之前的过滤。  

InnerFunctionOperator功能性算子，主要为系统提供window，join，order by，group by等聚合操作。  
这里的名称和operator包中的不一样. 这里定义的这些operator，主要是进行执行计划的序列化和反序列化的。  
所有的数据类型全部是字符串类型，之后还要经过语法的解析，物理执行计划的优化之后，才会在application中提交。  

```java
public class AggregateOperator extends BasicAggFunctionOperator {
    private Window window;                  //窗口的名称    
    private String filterBeforeAggregate;   //filter的过滤条件
}

public class BasicAggFunctionOperator extends InnerFunctionOperator {
    private String filterAfterAggregate;    //聚合类的过滤条件, 这里都是udaf函数, 过滤一定发生在数据聚合之后, 这里的表达式一定使用的是outputSchema中的列名称
    private String groupbyExpression;       //分组的表达式
    private String orderBy;                 //排序: 允许有多个字段，之间按照逗号分割, 允许出现udf和udaf函数
    private Integer limit;                  //窗口的输出限制
}

public class InnerFunctionOperator extends Operator {
    private String outputExpression;        //输出的列定义,不光有单纯的列，还有udf以及udaf函数
}
```

#### AggregateSplitter 

AggregateSplitter的父类是SelectSplitter, 而Select包含From子句.  
```java
    protected void splitFromClause() {
        //获取Select中From子句的语义解析结果
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        String streamName = clauseContext.getInputStreams().get(0);
        //在Window操作前可以有Filter操作, 拆分出Filter算子, 在父类SelectSplitter中实现
        FilterOperator fop = splitFiterBeforeWindow(streamName);
        //聚合算子
        AggregateOperator aggregateOperator = splitAggregateOperator(clauseContext, streamName);
        //创建算子之间的连接
        OperatorTransition transition = createTransition(fop, aggregateOperator, streamName);
        
        getResult().addOperators(fop);
        getResult().addOperators(aggregateOperator);
        getResult().addTransitions(transition);
    }
```

filter before window 语句解析. 根据From子句的语法,流前的过滤: `FROM transform (evnetid>10)[range UNBOUNDED]`. 其中[]表示window, 而[]前面的()则是filter过滤.  
```java
    protected FilterOperator splitFiterBeforeWindow(String streamName) {
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        //新创建一个Filter过滤算子
        FilterOperator fop = new FilterOperator(buildUtils.getNextOperatorName("Filter"), parallelNumber);
        //从From语义解析结果中获取filterBeforeWindow对应当前stream的filter表达式
        ExpressionDescribe expression = clauseContext.getFilterBeForeWindow().get(streamName);
        if (expression == null) {
            fop.setFilterExpression(null);
        }else{
            fop.setFilterExpression(expression.toString());
        }

        fop.setOutputExpression(createFilterOutputExpression(streamName));
        return fop;
    }
```

拆分AggregateOperator, 因为聚合算子可能包括多种聚合操作, 如果存在则都设置到AggregateOperator对应的字段中.  
```java
    private AggregateOperator splitAggregateOperator(FromClauseAnalyzeContext clauseContext, String streamName) {
        //创建新的聚合算子
        AggregateOperator aggop = new AggregateOperator(getBuildUtils().getNextOperatorName("Aggregator"), getParallelNumber());
        parseWindow(clauseContext, streamName, aggop);  //解析窗口
        parseWhere(aggop);                              //解析过滤
        
        aggop.setFilterAfterAggregate(parseHaving());   //解析having
        aggop.setGroupbyExpression(parseGroupby());     //解析分组
        aggop.setOrderBy(parseOrderBy());               //解析排序
        aggop.setLimit(parseLimit());                   //解析限制
        aggop.setOutputExpression(getSelectClauseContext().toString());
        return aggop;
    }
```

|aggregation|setXXX|AnalyzerContext|
|-----------|------|---------------|
|window|setWindow|FromClauseAnalyzeContext.windows.get(streamName)
|where|setFilterBeforeAggregate|FilterClauseAnalzyeContext whereClauseContext  
|having|setFilterAfterAggregate|FilterClauseAnalzyeContext havingClauseContext
|group by|setGroupbyExpression|SelectClauseAnalyzeContext groupbyClauseContext
|order by|setOrderBy|OrderByClauseAnalyzeContext
|limit|setLimit|LimitClauseAnalzyeContext
|output exp|setOutputExpression|SelectClauseAnalyzeContext selectClauseContext

#### OperatorCombiner

将多个算子组合起来, 组建算子之间的上下级关系. 算子之间的连线，有两种来源：  
1、算子是由一条CQL语句拆分出多个算子组成，这样，连线就可以在拆分的时候确定。  
2、算子是由多条CQL语句组合而来，通过使用`insert into select from`这样的语句，就可以实现多个算子之间的级联。  
甚至可以改变算子之间的连接关系。比如在aggregate算子之前加入union算子, 在aggregate算子之后加入split算子。  

为每个insert into select语句解析出来的结果加入上下文连线。  
CQL语句之间的连线，必然从inputStream或者PipeStream发起，连接到outputStream或者PipeStream.  
首先找到insert into语句中计算出来的连线的起点。找到对应的算子. 然后根据起点的schema名称，找到对应的流名称，创建连线  

```java
    private void createFromTransition(SplitContext context, InsertAnalyzeContext insertContext) {
        List<OperatorTransition> startTransitions = context.getFirstTransitons();
        for (OperatorTransition transition : startTransitions) {
            Operator op = context.getOperatorById(transition.getFromOperatorId());
            String startStreamName = transition.getSchemaName();
            SplitContext fromContext = getFromSplitContext(startStreamName);
            Schema schema = getInputSchema(startStreamName, insertContext);
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator fromOp = fromContext.getLastOperator();
            OperatorTransition fromtransition = new OperatorTransition(nextStreamName, fromOp, op, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(fromtransition);
        }
    }
    private void createToTransition(SplitContext context, InsertAnalyzeContext insertContext) {
        Set<Operator> ops = getLastOperator(context);
        for (Operator op : ops) {
            String startStreamName = insertContext.getOutputStreamName();
            SplitContext toContext = getToSplitContext(startStreamName);
            Schema schema = insertContext.getOutputSchema();
            String nextStreamName = buildUtils.getNextStreamName();
            
            Operator toOp = toContext.getFirstOperator();
            OperatorTransition totransition = new OperatorTransition(nextStreamName, op, toOp, DistributeType.SHUFFLE, null, schema);
            result.addTransitions(totransition);
        }
    }
```

---

### submitApplication 

历经千辛万苦, 终于回到SubmitTask的submitApplication, 创建物理计划Executor,并执行Application.  
```java
    private void submitApplication() {
        new PhysicalPlanExecutor().execute(context.getApp());
    }
```

#### api.Application -> application.Application

api的Application是流处理执行计划应用程序, 封装的是CQL语句构建而成的应用程序:  
```java
public class Application {                      
    private String applicationId = null;                    //应用id
    private String applicationName = null;                  //应用名称
    private TreeMap<String, String> confs;                  //整个应用程序中用到的配置属性,也包含用户自定义的配置属性
    private String[] userFiles;                             //用户自定义添加的一些文件
    private List<UserFunction> userFunctions;               //用户自定义的函数,udf和udaf都在这个里面
    private List<Schema> schemas = new ArrayList<Schema>(); //执行计划中的所有的schema
    private List<Operator> operators = null;                //执行计划中所有的操作,包含输入、输出和计算操作      
    private List<OperatorTransition> opTransition = null;   //整个执行计划中所有的连接线，定义了operator之间的连接关系
}
```

application.Application针对Schema和算子采用Manager管理类(实际上底层的存储结构都是由Map构成的)来操作:
```java
public abstract class Application {
    private String appName;                 //应用程序名称
    private EventTypeMng streamSchema;      //所有Schema集合
    private OperatorMng operatorManager;    //算子集合
    private StreamingConfig conf;           //系统级别的配置属性
}
```

OperatorMng管理的算子包括输入算子(addInputStream),输出算子(addOutputStream),功能算子(addFunctionStream).  
```java
IRichOperator (com.huawei.streaming.operator)
    AbsOperator (com.huawei.streaming.operator)
        FunctionOperator (com.huawei.streaming.operator)    //功能算子
            JoinFunctionOp (com.huawei.streaming.operator.functionstream)
                DataSourceFunctionOp (com.huawei.streaming.operator.functionstream)
            AggFunctionOp (com.huawei.streaming.operator.functionstream)
            SplitOp (com.huawei.streaming.operator.functionstream)
            UnionFunctionOp (com.huawei.streaming.operator.functionstream)
            SelfJoinFunctionOp (com.huawei.streaming.operator.functionstream)
            FunctorOp (com.huawei.streaming.operator.functionstream)
            FilterFunctionOp (com.huawei.streaming.operator.functionstream)
        OutputOperator (com.huawei.streaming.operator)      //输出算子
        FunctionStreamOperator (com.huawei.streaming.operator)
        InputOperator (com.huawei.streaming.operator)       //输入算子
```

IRichOperator流处理算子基本接口: 所有的流处理相关的算子实现，都来源于这个算子, 所有的外部Storm实现，均依赖于这个接口  
```java
public interface IRichOperator extends IOperator, Configurable{
    String getOperatorId();                     //获取算子id
    int getParallelNumber();                    //获取算子并发度
    List<String> getInputStream();              //获取输入流名称, 多个输入流
    String getOutputStream();                   //获取输出流名称
    Map<String, IEventType> getInputSchema();   //获取输入schema, <key是输入流名称,IEventType是输入流的Schema>
    IEventType getOutputSchema();               //获取输出schema
    Map<String, GroupInfo> getGroupInfo();      //获取分组信息
}
```

通过ExecutorPlanGenerator生成的application.Application则是生成可执行的执行计划. 可执行指的是可以运行在Storm引擎.  
```java
    public void execute(Application apiApplication) {
        parseUserDefineds(apiApplication, isStartFromDriver);
        com.huawei.streaming.application.Application app = generatorPlan(apiApplication);
        submit(app);                    //③ 提交Application
    }
    //有物理执行计划api.Application生成可执行计划application.Application(最终生成的，可以提交的应用程序)
    private com.huawei.streaming.application.Application generatorPlan(Application apiApplication) {
        preExecute(apiApplication);     //执行器执行之前的钩子
        new PhysicPlanChecker().check(apiApplication);
        //① 用户自定义的处理: 执行计划的组装, 构建application, 表达式的解析被延迟到这里来实现
        com.huawei.streaming.application.Application app = generator.generate(apiApplication);  
        preSubmit(app);                 //提交执行计划之前的钩子
        executorChecker.check(app);     //② 执行计划检查
        return app;
    }
```

日志中`start to execute application`, 在生成器工作之前会`parseUserDefineds`设置一些用户自定义的准备工作:比如注册jar包,注册函数,打包等发生在parseUserDefineds.    
```
2015-11-25 02:32:24 | INFO  | [main] | start to execute application example | com.huawei.streaming.cql.executor.PhysicalPlanExecutor (PhysicalPlanExecutor.java:127)
2015-11-25 02:32:25 | INFO  | [main] | start to unzip jar stream-storm-1.0-jar-with-dependencies.jar | com.huawei.streaming.cql.executor.mergeuserdefinds.JarExpander (JarExpander.java:79)
2015-11-25 02:32:25 | INFO  | [main] | unzip jar /private/var/folders/xc/x0b8crk9667ddh1zhfs29_zr0000gn/T/da6d53114b1f49458c0e6329553b1ff9/stream-storm-1.0-jar-with-dependencies.jar to /private/var/folders/xc/x0b8crk9667ddh1zhfs29_zr0000gn/T/da6d53114b1f49458c0e6329553b1ff9/jartmp | com.huawei.streaming.cql.executor.mergeuserdefinds.JarExpander (JarExpander.java:91)
2015-11-25 02:32:30 | INFO  | [main] | finished to unzip jar to dir | com.huawei.streaming.cql.executor.mergeuserdefinds.JarExpander (JarExpander.java:84)
2015-11-25 02:32:30 | INFO  | [main] | start to copy ch | com.huawei.streaming.cql.executor.mergeuserdefinds.JarFilesMerger (JarFilesMerger.java:82)
...
2015-11-25 02:32:38 | INFO  | [main] | finished to package jar | com.huawei.streaming.cql.executor.mergeuserdefinds.JarPacker (JarPacker.java:68)
```

#### Schema -> TupleEventType

前面的第一个Topology的CQL语句:  
```
CREATE INPUT STREAM s(id INT, name STRING, type INT) SOURCE randomgen PROPERTIES ( timeUnit = "SECONDS", period = "1", eventNumPerperiod = "1", isSchedule = "true" );
CREATE OUTPUT STREAM rs(type INT, cc INT) SINK consoleOutput;

INSERT INTO STREAM rs SELECT type, COUNT(id) as cc
FROM s[RANGE 20 SECONDS BATCH]
WHERE id > 5 GROUP BY type;
```

生成可执行计划对应的日志, 会解析schema和算子.   
```
2015-11-25 02:32:39 | INFO  | [main] | start to generator executor application for app example | com.huawei.streaming.cql.executor.ExecutorPlanGenerator (ExecutorPlanGenerator.java:102)

解析schemas时, 添加输入和输出的Schema, 转换成EventType.  
2015-11-25 02:32:39 | INFO  | [main] | AddEventType enter, the eventtypeName is:s. | com.huawei.streaming.event.EventTypeMng (EventTypeMng.java:73)
2015-11-25 02:32:39 | INFO  | [main] | AddEventType enter, the eventtypeName is:rs. | com.huawei.streaming.event.EventTypeMng (EventTypeMng.java:73)

解析算子: 1)解析二元表达式(属性值表达式) id > 5
2015-11-25 02:32:39 | INFO  | [main] | start to parse cql : (s.id > 5) | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:44)
2015-11-25 02:32:39 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:69)
2015-11-25 02:32:39 | INFO  | [main] | start to create binary Expressions. | com.huawei.streaming.cql.executor.expressioncreater.PropertyValueExpressionCreator (BinaryExpressionCreator.java:54)
2015-11-25 02:32:39 | INFO  | [main] | Parse Completed, cql : s.type,  count( s.id )  | com.huawei.streaming.cql.semanticanalyzer.parser.SelectClauseParser (SelectClauseParser.java:68)

2)解析 group by
2015-11-25 02:32:39 | INFO  | [main] | start to parse cql : s.type | com.huawei.streaming.cql.semanticanalyzer.parser.GroupbyClauseParser (GroupbyClauseParser.java:45)
2015-11-25 02:32:39 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.GroupbyClauseParser (GroupbyClauseParser.java:68)
2015-11-25 02:32:39 | INFO  | [main] | start to parse cql : s.type | com.huawei.streaming.cql.semanticanalyzer.parser.GroupbyClauseParser (GroupbyClauseParser.java:45)
2015-11-25 02:32:39 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.GroupbyClauseParser (GroupbyClauseParser.java:68)

3)聚合算子 count(id)
2015-11-25 02:32:39 | INFO  | [main] | start to create aggregate service | com.huawei.streaming.cql.executor.operatorviewscreater.AggregateServiceViewCreator (AggregateServiceViewCreator.java:89)
```

>前面在submit之前已经start to parse cql过一次了,这里为什么还会再次parse?  
答: 前面只是LazyTask懒解析,其实还是没有开始的. 那为什么要在这里才开始? 因为解析完schema后, 就该轮到operator的解析了.   

生成的可执行计划会解析Application中的Schema和Operators,经过重新组装,设置到可执行的Application中.  
```java
    public com.huawei.streaming.application.Application generate(Application vap) {
        LOG.info("start to generator executor application for app " + vap.getApplicationId());
        apiApplication = vap;
        createEmptyApplication(vap.getApplicationId());
        parseUserDefineds(vap);     //用户自定义的处理
        parseSchemas();             //解析所有的Schema，构建schema信息
        parseOperators();           //解析所有的Operator,构建OperatorInfo. 整理Operator中的上下级关系
        return executorApp;         
    }
```

解析Schema会将Schema转换为IEvent事件: TupleEventType. Schema中的Column会转换为TupleEventType的Attribute.而schemaName仍然不变. 
```java
public class TupleEventType implements IEventType {
    private String name;            //schemaName,表名
    private Attribute[] schema;     //schemas, 所有列
    private String[] attNames;      //所有列的列名
    private Class< ? >[] attTypes;  //所有列的列类型
    private HashMap<String, Integer> attid;
}
```

Schema的管理类用Map结构保存schemaName/eventTypeName和对应的Schema/TupleEventType: 表名->表结构.   
```java
public class EventTypeMng implements Serializable {
    private Map<String, IEventType> schemas;            //MAP: 数据类型名称 => 具体数据类型

    public void addEventType(IEventType schema) {
        schemas.put(schema.getEventTypeName(), schema); //数据类型|事件类型|表名schemaName|streamName流名称
    }
}
```
同样算子管理OperatorMng则用三个Map分别管理输入,输出,功能算子. Map的key是operatorId,value是Operator算子本身.   
```java
public class OperatorMng{    
    private List<IRichOperator> sortedFunctions;    //DFG排序后的功能算子列表，作为创建Storm拓扑顺序的基础(输出和功能算子组成-->Bolt)
    private Map<String, IRichOperator> inputs;      //输入算子 --> Spout
    private Map<String, IRichOperator> functions;   //功能算子
    private Map<String, IRichOperator> outputs;     //输出算子
}
```

#### 算子解析

算子解析: 这里的解析是为了使得输入和输出算子统一，避免用户自定义和系统内置的算子对外表现不一致处理起来的麻烦  
由于输入和输出算子中存在特例，即针对文件，tcp，kafka等编写了特例, 所以需要首先将他们抽象化，之后再来处理  
```java
    private void parseOperators(){
        Map<String, Operator> opts = formatOperators();                     //① 输入输出算子抽象化
        Map<String, AbsOperator> opMappings = createOperatorInfos(opts);    //② 算子 解析
        combineOperators(opMappings);                                       //③ 整理算子顺序
        for (Entry<String, AbsOperator> et : opMappings.entrySet()){        //④ 添加算子到Application
            IRichOperator operator = et.getValue();
            //如果没有输入，也算是input
            if (operator instanceof InputOperator){
                executorApp.addInputStream(operator);
                continue;
            }
            //如果没有输出，也算是output
            if (operator instanceof OutputOperator){
                executorApp.addOutputStream(operator);
                continue;
            }
            //不是输入输出,就是功能算子
            executorApp.addFunctionStream(operator);
        }
    }
```

Operator是算子, AbsOperator则是流处理算子(继承IRichOperator). 它们的转换由OperatorInfoCreatorFactory.buildStreamOperator完成.   
![stream-aboperators](http://img.blog.csdn.net/20151127114702874)

combineOperators会将算子用OperatorTransition进行连接: 梳理operatorInfo之间的上下级关系
```java
    private void combineOperators(Map<String, AbsOperator> operatorInfos) {
        //算子之间的连接
        for (OperatorTransition ot : apiApplication.getOpTransition()) {
            //获取连接的入口和出口算子
            String fromOpId = ot.getFromOperatorId();
            String toOpId = ot.getToOperatorId();
            String streamName = ot.getStreamName();
            DistributeType distributedType = ot.getDistributedType();
            String distributedFields = ot.getDistributedFields();
            //连接的Schema, 对于From和To都是使用相同的Schema
            String outputSchemaName = ot.getSchemaName();
            distributedFields = ExecutorUtils.removeStreamName(distributedFields);            
            TupleEventType outputSchema = (TupleEventType)(executorApp.getEventType(outputSchemaName));
            
            //operatorInfos是所有的算子集合, 根据传入的fromOpId或者toOpId,从集合中找出对应的算子
            combineFromTransition(operatorInfos, fromOpId, streamName, outputSchema);
            combineToTransition(operatorInfos, toOpId, streamName, distributedType, distributedFields, outputSchema);
        }
    }
```

FromTransition: 连线的from算子的输出是outputSchema
```java
    private void combineFromTransition(Map<String, AbsOperator> operatorInfos, String fromOpId, String streamName, TupleEventType outputSchema){
        sConfig.put(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA, outputSchema);
        sConfig.put(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME, streamName);
    }
```
ToTransition: 连线的to算子的输入是outputSchema. `这里outputSchema命名为schema似乎更好`
```java
    private void combineToTransition(Map<String, AbsOperator> operatorInfos, String toOpId, String streamName,
        DistributeType distributedType, String distributedFields, TupleEventType outputSchema) {
        sConfig.put(StreamingConfig.STREAMING_INNER_INPUT_STREAM_NAME, streamName);
        sConfig.put(StreamingConfig.STREAMING_INNER_INPUT_SCHEMA, outputSchema);
    }
```

类似于Graph中的顶点A -> 边 -> 顶点B. Transition就类似于边, 连接着左右两边的算子, 分别是From算子和To算子.  

### SubmitApplication -> launch Application

SubmitTask.submitApplication -> PhysicalPlanExecutor.execute -> PhysicalPlanExecutor.submit(application.Application) ->  
StormApplication.launch -> createTopology 创建拓扑, 对于Storm的程序而言, 构成拓扑的组件包括Spouts和Bolts.  
这些数据都来自于Application的输入,输出和功能算子. 由于Storm只有两种组件Spout和Bolt, 所以输入算子归于Spout,输出和功能算子都属于Bolt.  
```java
    private void createSpouts() {
        List< ? extends IRichOperator> sources = getInputStreams(); //获得所有源算子信息: OperatorMng.inputs
        checkInputStreams(sources);
        for (IRichOperator input : sources) {
            StormSpout spout = new StormSpout();
            spout.setOperator(input);
            builder.setSpout(input.getOperatorId(), spout, input.getParallelNumber());
        }
    }
    private void createBolts() {
        List<IRichOperator> orderedFunOp = genFunctionOpsOrder();   //获取已经排好序的功能算子，这个功能算子包含output算子
        for (IRichOperator operator : orderedFunOp) {
            setOperatorGrouping(operator);
        }
    }
```

>哪些Operator算子会作为Bolt, OutpoutBolt, Spout都是由OperatorMng管理的比如getInputStreams,genFunctionOpsOrder  
这样创建的Bolt会直接依赖于对应的Operator, 在处理Bolt时,就不需要再判断是哪一种类型的Operator了.  
所以正是由于对算子的种类进行了分离(输入,输出,功能)才使得处理Storm的component时变得容易.  

#### Bolt Grouping

在开发Storm应用程序时, 一般是在Storm的Topology代码中创建Bolt并直接设置Bolt的分组策略.  
假设有这样的Topology, Bolt1输出到Bolt3和Bolt4, Bolt2输出到Bolt3(一个Bolt可以有多个输出,也可以由多个输入).    
```java
             |------ |Bolt4|
|Bolt1| -----|
             |------ |Bolt3|
|Bolt2| -----|

//Bolt1有两个输出流, 输出字段都是一样的, 两个输出流的名称stream-id不一样
builder.setBolt("bolt1", new Bolt1(), 2)        
            declarer.declareStream("streamA", new Fields("f1","f2"))
            declarer.declareStream("streamB", new Fields("f1","f2"))
//Bolt2只有一个输出流
builder.setBolt("bolt2", new Bolt2(), 2)
            declarer.declareStream("streamC", new Fields("f1"))

//Bolt3接收Bolt1的streamA流使用字段分组, 接收Bolt2的streamC流使用shuffle分组
builder.setBolt("bolt3", new Bolt3(), 4)
        .fieldsGrouping("bolt1", "streamA", new Field("f1"))
        .shuffleGrouping("bolt2", "streamC")

//Bolt4接收Bolt1的streamB流使用字段分组, 分组字段是Bolt1产生的f2字段.  
builder.setBolt("bolt4", new Bolt4(), 3)
        .fieldsGrouping("bolt1", "streamB", , new Field("f2"))
```

>setBolt时设置的componentId/operatorId都是自己Bolt, 分组时的componentId则是输入的componentId/operatorId.  

通过解析CQL的分组以及算子/组件之间的连接, 现在就不需要在Topology写死了. 因此需要框架能够动态地构建Topology.  

>为什么IRichOperator的getInputStream()和getOutputStream()表示的是输入流和输出流的名称, 而不是输入流对象和输出流对象(比如算子本身).  
这是因为Operator算子会用于Topology的Spout/Bolt, 创建完Spout/Bolt之后, 用于构建Topology其他必要的信息除了分组外,   
还有Storm的component-id对应算子的id 和 Storm的stream-id对应算子的输入/输出流名称  

```java
    private void setOperatorGrouping(IRichOperator operator) {
        BoltDeclarer bolt = createBoltDeclarer(operator);
        //一个Bolt可能有多个输入即多个InputStream, 同时输出也可能有多个: 设置不同的Grouping策略
        //注意: Bolt设置分组时的componentId是其输入源的ComponentId,而不是自己的componentId, 自己是在builder.setBolt时设置的
        for (String strname : operator.getInputStream()) {              //strname是当前算子的输入流名称
            GroupInfo groupInfo = operator.getGroupInfo().get(strname); //算子的分组信息
            setBoltGrouping(bolt, strname, groupInfo);
        }
    }
    
    private void setBoltGrouping(BoltDeclarer bolt, String strname, GroupInfo groupInfo) {        
        DistributeType distribute = groupInfo.getDitributeType();
        switch (distribute) {
            case FIELDS:
                Fields fields = new Fields(groupInfo.getFields());
                //根据输入流的名称, 获取这个输入流是个什么算子, 为的是获得这个输入算子的operatorId,作为分组策略的第一个参数
                IRichOperator operator = getOperatorByOutputStreamName(strname);
                //字段分组三个参数分别表示: componentId, streamId, fields. 这里的componentId表示从哪个数据源接入数据,而不是当前算子的operatorId
                bolt.fieldsGrouping(operator.getOperatorId(), strname, fields);
                break;
            //... 其他分组类型 ...   
            default:               
                setDefaultBoltGrouping(bolt, strname);
        }
    }
    private void setDefaultBoltGrouping(BoltDeclarer bolt, String strname) {
        IRichOperator operator = getOperatorByOutputStreamName(strname);
        //shuffle分组两个参数分别表示: 输入流的operatorId/componentId, streamId
        bolt.shuffleGrouping(operator.getOperatorId(), strname);
    }
```

>Operator算子的id会作为Storm中Spout/Bolt的component-id, 而Operator的输入流/输出流名称是作为Spout/Bolt的stream-id.  
component-id只是用于区别不同的组件,或者用于从哪个输入组件获取数据. 而stream-id则可以作为分流/多流/合并流等.  

#### Bolt Creation

createBolts设置Operator的分组策略, 首先创建IRichBolt,并返回Bolt的声明BoltDeclarer,以便后续操作可以在BoltDeclarer继续进行(比如上面的分组策略).  
```java
    private BoltDeclarer createBoltDeclarer(IRichOperator operator){
        IRichBolt bolt;
        if ((operator instanceof FunctionOperator) || (operator instanceof FunctionStreamOperator)) {
            bolt = createStormBolt(operator);
        }else{
            bolt = createOutputStormBolt(operator);
        }
        return builder.setBolt(operator.getOperatorId(), bolt, operator.getParallelNumber());
    }
    private IRichBolt createOutputStormBolt(IRichOperator f){
        StormOutputBolt outputbolt = new StormOutputBolt();
        outputbolt.setOperator(f);
        return outputbolt;
    }
    private IRichBolt createStormBolt(IRichOperator f){
        StormBolt stormbolt = new StormBolt();
        stormbolt.setOperator(f);
        return stormbolt;
    }
```

StormSpout,StormBolt,StormOutputBolt都是对Storm的组件的封装. 除了继承各自的IRichSpout和IRichBolt外,还要实现StreamAdapter接口的setOperator方法.  
流处理算子适配接口: 依靠这个接口，将流处理的算子注入到具体的Storm的Spout/Bolt中. `创建Bolt为啥不用构造函数一句话的事儿: new StormBolt(operator)` 
```java
public class StormSpout implements IRichSpout, StreamAdapter {
    private IRichOperator input;
    public void setOperator(IRichOperator operator){
        input = operator;
    }
}
public class StormOutputBolt implements IRichBolt, StreamAdapter {
    private OutputCollector outputCollector;
    private OutputOperator output;
    public void setOperator(IRichOperator operator) {
        this.output = (OutputOperator)operator;
    }
}
```

StormBolt的execute方法
```java
    public void execute(Tuple input) {
        String sourceStreamName = input.getSourceStreamId();        //获取Tuple的输入流stream-id
        List<String> inStreams = functionStream.getInputStream();   //输入流名称列表,因为一个Bolt可以有多个输入流
        for (String streamName : inStreams) {
            if (!sourceStreamName.equals(streamName)) continue;     //只有Tuple的输入流stream-id和IRichOperator的输入流名称相同时,才处理这个Tuple
            TupleEvent event = TupleTransform.tupeToEvent(input, functionStream.getInputSchema().get(streamName));
            functionStream.execute(streamName, event);
        }
    }
```


```
2015-11-25 02:32:39 | INFO  | [main] | start to submit application example | com.huawei.streaming.cql.executor.PhysicalPlanExecutor (PhysicalPlanExecutor.java:201)
2015-11-25 02:32:39 | INFO  | [main] | reset submit jar to /private/var/folders/xc/x0b8crk9667ddh1zhfs29_zr0000gn/T/example.5f8ed0baaeb243a49308fd75144cf715.jar | com.huawei.streaming.storm.StormApplication (StormApplication.java:314)
2015-11-25 02:32:39 | INFO  | [main] | Using defaults.yaml from resources | backtype.storm.utils.Utils (Utils.java:253)
2015-11-25 02:32:39 | INFO  | [main] | The baseSleepTimeMs [2000] the maxSleepTimeMs [60000] the maxRetries [5] | backtype.storm.utils.StormBoundedExponentialBackoffRetry (StormBoundedExponentialBackoffRetry.java:47)
2015-11-25 02:32:39 | INFO  | [main] | Using defaults.yaml from resources | backtype.storm.utils.Utils (Utils.java:253)
2015-11-25 02:32:39 | INFO  | [main] | GenFunctionOpsOrder enter. | com.huawei.streaming.application.OperatorMng (OperatorMng.java:205)
2015-11-25 02:32:39 | INFO  | [main] | Using defaults.yaml from resources | backtype.storm.utils.Utils (Utils.java:253)
2015-11-25 02:32:39 | INFO  | [main] | Generated ZooKeeper secret payload for MD5-digest: -5668598407594625313:-5703359794945963404 | backtype.storm.StormSubmitter (StormSubmitter.java:82)
2015-11-25 02:32:39 | INFO  | [main] | Uploading topology jar /private/var/folders/xc/x0b8crk9667ddh1zhfs29_zr0000gn/T/example.5f8ed0baaeb243a49308fd75144cf715.jar to assigned location: storm-local/nimbus/inbox/stormjar-a5b95134-e3f2-431d-b675-924d8c468cf3.jar | backtype.storm.StormSubmitter (StormSubmitter.java:371)
2015-11-25 02:32:40 | INFO  | [main] | Successfully uploaded topology jar to assigned location: storm-local/nimbus/inbox/stormjar-a5b95134-e3f2-431d-b675-924d8c468cf3.jar | backtype.storm.StormSubmitter (StormSubmitter.java:396)
2015-11-25 02:32:40 | INFO  | [main] | Finished submitting topology: example | backtype.storm.StormSubmitter (StormSubmitter.java:248)
2015-11-25 02:32:40 | INFO  | [main] | delete user packed jar after submit | com.huawei.streaming.cql.executor.PhysicalPlanExecutor (PhysicalPlanExecutor.java:156)
2015-11-25 02:32:40 | INFO  | [main] | unRegister jars from class loader. | com.huawei.streaming.cql.DriverContext (DriverContext.java:427)
```