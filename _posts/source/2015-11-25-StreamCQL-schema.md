---
layout: post
title: StreamCQL源码分析之schema
category: Source
tags: BigData
keywords: 
description: 
---

# Introduce

CQL（Continuous Query Language），持续查询语言，用于数据流上的查询语言。相对于传统的SQL，CQL加入了窗口的概念，使得数据可以一直保存在内存中，由此可以快速进行大量内存计算，CQL的输出结果为数据流在某一时刻的计算结果。  

CQL是建立在Storm基础上的类SQL查询语言，它解决了Storm原生API使用复杂，上手难度高，很多基本功能缺失的问题，提升了流处理产品的易用性。  

在CQL设计语法之初，通过参考市面上现有的CEP产品的语法，发现这些产品都不算是全部的SQL语句，即仅仅使用SQL语句还不能运行，还必须依靠一些客户端的代码。 这样就给使用带来了一些不便， 用户必须学习客户端API，比较繁琐，上手难度也比较大。  

所以，CQL设计目标就是，用纯粹的SQL语句再加上一些命令，就可以完成所有的任务发布以及执行，这样，就可以通过SQL接口，直接进行任务的下发，统一了客户端接口。对于有一定SQL基础的用户，只需要掌握一些CQL比较特殊的语法，比如窗口或者流定义的语法，就可以写出可运行的CQL程序，大大降低了上手难度。  

**关键概念**

**Stream(流)**：流是一组（无穷）元素的集合，流上的每个元素都属于同一个schema；每个元素都和逻辑时间有关；即流包含了元组和时间的双重属性。留上的任何一个元素，都可以用Element<tuple,Time>的方式来表示，tuple是元组，包含了数据结构和数据内容，Time就是该数据的逻辑时间。

**Window(窗口)**：窗口是流处理中解决事件的无边界（unbounded）及流动性的一种重要手段，把事件流在某一时刻变成静态的视图，以便进行类似数据库表的各种查询操作。在stream上可以定义window，窗口有两种类型，时间窗口（time-based）和记录窗口（row-based）。两种窗口都支持两种模式，滑动（slide）和跳动（tumble）。

**Expression(表达式)**：符号和运算符的一种组合，CQL解析引擎处理该组合以获取单个值。简单表达式可以是常量、变量或者函数，可以用运算符将两个或者多个简单表达式联合起来构成更复杂的表达式。

# QuickStart

1.startup zk and storm local  

```
zkServer.sh start

nohup bin/storm nimbus &
nohup bin/storm ui &
nohup bin/storm supervisor &
```

2.build and run cql client

```
cd StreamCQL
mvn clean install
cd cql-binary/target
tar xvf stream-cql-bianry-1.0.tar.gz
cd stream-cql-bianry-1.0
bin/cql
```

3.create first topology:

```
CREATE INPUT STREAM s(id INT, name STRING, type INT)
SOURCE randomgen PROPERTIES ( timeUnit = "SECONDS", period = "1", eventNumPerperiod = "1", isSchedule = "true" );

CREATE OUTPUT STREAM rs(type INT, cc INT)
SINK consoleOutput;

INSERT INTO STREAM rs SELECT type, COUNT(id) as cc
FROM s[RANGE 20 SECONDS BATCH]
WHERE id > 5 GROUP BY type;

SUBMIT APPLICATION example;
```

>输入流: 随机数,每秒生成一个事件  
>输出流: 控制台, 每隔20秒输出一次, 只统计id>5,根据type分组,求和  
>提交应用程序, 相当于创建了一个Storm的Topology.  

4.A complicate topology:  

```
CREATE INPUT STREAM s1(name STRING)
SOURCE RANDOMGEN PROPERTIES ( timeUnit = "SECONDS", period = "1", eventNumPerperiod = "1", isSchedule = "true" );

CREATE OUTPUT STREAM s2(c1 STRING)
SINK kafakOutput PROPERTIES ( topic = "cqlOut", zookeepers = "127.0.0.1:2181", brokers = "127.0.0.1:9092" );

CREATE INPUT STREAM s3( c1 STRING)
SOURCE KafkaInput PROPERTIES (groupid = "cqlClient", topic = "cqlInput", zookeepers = "127.0.0.1:2181", brokers = "127.0.0.1:9092" )

CREATE OUTPUT STREAM s4(c1 STRING)
SINK consoleOutput;

INSERT INTO STREAM s2 SELECT * FROM s1;
INSERT INTO STREAM s4 SELECT * FROM s3;

SUBMIT APPLICATION cql_kafka_example;
```

>输入流s1 发射数据 到kafka输出流s2  
>kafka输入流 发射数据 到控制台输出流s4

5.查看拓扑: <http://localhost:8080> 

# Architecture

StreamCQL的代码由三部分组成: cql,streaming,adapter分别对应下面的三个组件.  

![stream-arch](http://img.blog.csdn.net/20151125162710289)

>客户端提交的`CQL语句`会由执行计划生成器`ExecutorPlanGenerator`生成`可运行的任务`,最终由`Storm适配器`组装Topology提交执行.

StreamCQL对应的Storm拓扑:  

![stream-storm](http://img.blog.csdn.net/20151125162722310)

>至少有一个输入和输出. Component之间可以组合比如Select,Join等.  

Window example:  

```sql
--按照type对窗口内数据进行分组,每组容量为10
SELECT * FROM transformEvent[ROWS 10 SLIDE PARTITION BY TYPE]; 

--时间排序窗,一般用来解决数据乱序问题
SELECT * FROM transformEvent[RANGE 1000 MILLISECONDS SORT BY dte];

--事件驱动时间滑动窗
INSERT INTO STREAM rs sum(OrderPrice),avg(OrderPrice),count(OrderPrice)
FROM transformEvent[RANGE 10 SECONDS SLIDE TRIGGER by TS EXCLUDE now];

--保存周期为一个自然天的分组窗
INSERT INTO STREAM rs select id,name,count(id)
FROM transformEvent[RANGE TODAY ts PARTITION BY TYPE] 
WHERE id > 5 GROUP BY TYPE HAVING id > 10;
```

Split example:

```sql
FROM teststream
  INSERT INTO STREAM s1 SELECT *
  INSERT INTO STREAM s2 SELECT a
  INSERT INTO STREAM s3 SELECT id, name WHERE id > 10
PRARLLEL 4;
```

![stream-split](http://img.blog.csdn.net/20151126091936179)

>原始的spout输入流会分成三个输出流. 所以中间用一个SplitBolt来作为中间介质.  

# From log to see StreamCQL

>熟悉一个开源框架的流程, 可以先跑一个测试例子, 查看打印的日志信息, 通过日志的顺序, 可以大致熟悉整体的流程.  
当然要求框架本身的日志信息足够明了, StreamCQL做的不错. 这种方式的优点是不至于不知道要从哪里看起来. 


## CQLClient to Driver

`bin/cql`会开启一个CQLClient客户端, 当输入`;`表示一个语句的终结时,就会触发一次CQL语句的编译执行等.

Driver.run是CQL的运行起点

+ 1、编译
+ 2、语义分析
+ 3、命令执行
+ 4、返回结果

```java
    public void run(String cql) {
        ParseContext parseContext = parser.parse(cql);  //CQL解析
        saveAllChangeableCQLs(cql, parseContext);
        preDriverRun(context, parseContext);
        executeTask(parseContext);                      //执行任务
        postDriverRun(context, parseContext);
    }

    private void executeTask(ParseContext parseContext) {
        mergeConfs();
        Task task = TaskFactory.createTask(context, parseContext, config, analyzeHooks);
        task.execute(parseContext);                     //执行任务
        context.setQueryResult(task.getResult());       //返回结果(查询性的命令比如show,get会有结果,其他没有结果)
    }
```

初始的语法解析类是ApplicationParser. parse方法采用visitor访问者模式遍历CQL语句.   

```
IParser (com.huawei.streaming.cql.semanticanalyzer.parser)
    |-- OrderbyClauseParser 
    |-- GroupbyClauseParser 
    |-- ApplicationParser 
    |-- SelectClauseParser 
    |-- DataSourceArgumentsParser 
```

ParseContext的实现类很多,基本上CQL语法的每一部分都会对应一个语法解析器.    

```
ParseContext (com.huawei.streaming.cql.semanticanalyzer.parser.context)
    |-- CreateStreamStatementContext        语句
            |-- CreatePipeStatementContext 
            |-- CreateInputStatementContext 
            |-- CreateOutputStatementContext 
    |-- FromClauseContext                   子句
    |-- RangeWindowContext                  窗口
    |-- ...
```

## Input,Output,Insert

ApplicationParser.parse返回的ParseContext具体针对特定的CQL语句返回的是什么类型? 这个类型对于创建什么类型的任务非常重要.   
因为这个是创建一个新的Stream, 所以ParseContext是`CreateInputStatementContext`.  

```
2015-11-25 02:32:19 | INFO  | [main] | start to parse cql : CREATE INPUT STREAM s
    (id INT, name STRING, type INT)
SOURCE randomgen
    PROPERTIES ( timeUnit = "SECONDS", period = "1",
        eventNumPerperiod = "1", isSchedule = "true" ) | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:44)
2015-11-25 02:32:19 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:69)
2015-11-25 02:32:19 | INFO  | [main] | start to execute CREATE INPUT STREAM s (id INT, name STRING, type INT) SOURCE randomgen PROPERTIES ( 'timeUnit' = 'SECONDS', 'period' = '1', 'eventNumPerperiod' = '1', 'isSchedule' = 'true' ) | com.huawei.streaming.cql.tasks.LazyTask (LazyTask.java:62)
```

CreateStreamStatementContext.createTask创建的是LazyTask. 它的execute方法只是把当前ParseContext加入到DriverContext的parseContexts中.  

```java
    public void execute(ParseContext parseContext) {
        context.addParseContext(parseContext);
    }
```

同样输出流经过ApplicationParser.parse返回的是`CreateOutputStatementContext`,它也继承了CreateStreamStatementContext.  

```
2015-11-25 02:32:20 | INFO  | [main] | start to parse cql : CREATE OUTPUT STREAM rs
    (type INT, cc INT)
SINK consoleOutput | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:44)
2015-11-25 02:32:20 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:69)
2015-11-25 02:32:20 | INFO  | [main] | start to execute CREATE OUTPUT STREAM rs (type INT, cc INT) SINK consoleOutput | com.huawei.streaming.cql.tasks.LazyTask (LazyTask.java:62)
```

insert语句是`InsertStatementContext`, 输入输出插入这些都是延迟执行的任务,并不需要立即执行,因为需要根据上下文构造一个完整的DAG拓扑图.   

```
2015-11-25 02:32:20 | INFO  | [main] | start to parse cql : INSERT INTO STREAM rs SELECT type, COUNT(id) as cc
    FROM s[RANGE 20 SECONDS BATCH]
    WHERE id > 5 GROUP BY type | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:44)
2015-11-25 02:32:20 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:69)
2015-11-25 02:32:20 | INFO  | [main] | start to execute INSERT INTO STREAM rs SELECT type, count(id) AS cc FROM s[RANGE 20 SECONDS BATCH] WHERE id > 5 GROUP BY type | com.huawei.streaming.cql.tasks.LazyTask (LazyTask.java:62)
```

---

## Submit

提交应用程序,经过parse返回的是`SubmitApplicationContext`,创建的Task是SubmitTask.  

```
2015-11-25 02:32:23 | INFO  | [main] | start to parse cql : SUBMIT APPLICATION example | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:44)
2015-11-25 02:32:23 | INFO  | [main] | Parse Completed | com.huawei.streaming.cql.semanticanalyzer.parser.ApplicationParser (ApplicationParser.java:69)
2015-11-25 02:32:24 | INFO  | [main] | combine all split contexts | com.huawei.streaming.cql.builder.operatorsplitter.OperatorSplitter (OperatorCombiner.java:101)
```

### Task & SemanticAnalyzer

![stream-createTask](http://img.blog.csdn.net/20151125203206771)

>可以看到对于前面的输入流,输出流,insert语句,并没有对应的Task实现类,所以它们都使用LazyTask.  

Driver.executeTask会根据ParseContext具体的实现类由TaskFactory创建对应的Task.  ParseContext抽象类除了创建Task,还会创建SemanticAnalyzer  

```java
    //创建对应语句的执行task
    public abstract Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks) throws CQLException;
    
    //创建语义分析执行解析器
    public abstract SemanticAnalyzer createAnalyzer() throws SemanticAnalyzerException;
```

比如SubmitApplicationContext创建的分析器是SubmitApplicationAnalyzer. CreateStreamStatementContext也是个抽象类,  
有三个子类CreateInputStatementContext,CreateOutputStatementContext,CreatePipeStatementContext,它们创建的分析器分别是:  
CreateInputStreamAnalyzer, CreateOutputStreamAnalyzer, CreatePipeStreamAnalyzer 它们都继承了CreateStreamAnalyzer.  

```
ParseContext ------|--SubmitApplicationContext
                   |--CreateStreamStatementContext--|
                                                    |---CreateInputStatementContext
                                                    |---CreateOutputStatementContext
                                                    |---CreatePipeStatementContext
                                                            
SemanticAnalyzer --|--SubmitApplicationAnalyzer
                   |--CreateStreamAnalyzer ---------|
                                                    |---CreateInputStreamAnalyzer
                                                    |---CreateOutputStreamAnalyzer
                                                    |---CreatePipeStreamAnalyzer                               
```

SemanticAnalyzer的创建方式和创建Task一样都是使用工厂类`SemanticAnalyzerFactory`. 在创建完之后都调用了init初始化.   

```java
    public static Task createTask(DriverContext driverContext, ParseContext parseContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks) {
        Task task = parseContext.createTask(driverContext, analyzeHooks);
        task.init(driverContext, config, analyzeHooks);
        return task;
    }
    public static SemanticAnalyzer createAnalyzer(ParseContext parseContext, List<Schema> schemas) {
        SemanticAnalyzer analyzer = parseContext.createAnalyzer();
        analyzer.init(schemas);
        return analyzer;
    }
```

### SubmitTask

#### parseSubmit  

SubmitTask执行应用程序提交的execute方法和前面的LazyTask有点复杂, 因为它要把前面创建的LazyTask都组合起来,组成一个完整的应用程序.  

```java
    public void execute(ParseContext parseContext) {   //这里的parseContext是SubmitApplicationContext
        parseSubmit(parseContext);  //解析
        createApplication();        //创建应用程序
        dropApplicationIfAllow();   //如果允许的话先删除应用程序
        submitApplication();        //提交应用程序
    }
    //解析Submit,创建对应的语义解析器:SubmitApplicationAnalyzer
    private void parseSubmit(ParseContext parseContext) {
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, EMPTY_SCHEMAS);
        submitContext = (SubmitApplicationAnalyzeContext)analyzer.analyze();
    }
```

这里面几个对象的创建关系是(Parser语法解析器->Context->Analyzer语义分析器->AnalyzerContext):  

```
ApplicationParser.parse() --> SubmitApplicationContext : ParseContext
                                    |--createTask: SubmitTask
                                    |--createAnalyzer: SubmitApplicationAnalyzer
                                                               |--createAnalyzeContext: SubmitApplicationAnalyzeContext
```

#### createApplication  

创建Application,如果有路径的话,直接加载物理执行计划,否则创建一个API用的Application并设置到DriverContext中.  

```java
    private Application createAPIApplication(String appName) {
        Application app = null;
        if (context.getApp() == null) {             //创建Application
            semanticAnalyzerLazyContexts();         //准备analyzeContexts
            app = new ApplicationBuilder().build(appName, analyzeContexts, context);
        } else {
            app = context.getApp();
        }
        app.setApplicationId(appName);
        return app;
    }
```

创建APIApplication, 记得前面的那些LazyTask吗, 都要用语义分析分析一遍,对应的AnalyzeContext会被ApplicationBuilder用到.  
因为LazyTask的execute方法只是简单地把当前的ParseContext实现类加入到DriverContext中,所以下面的for循环能从DriverContext获取出所有ParseContext.  

```java
    private void semanticAnalyzerLazyContexts() {
        for (ParseContext parseContext : context.getParseContexts()) {
            preAnalyze(context, parseContext);
            SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(parseContext, context.getSchemas());
            AnalyzeContext analyzeContext = analyzer.analyze();
            postAnalyze(context, analyzeContext, parseContext);
            analyzeContexts.add(analyzeContext);    //将各自的分析器分析的结果AnalyzeContext实现类加入到analyzeContexts
        }
    }
```

像SubmitTask一样都要先创建SemanticAnalyzer,然后调用analyze方法, 这些没有调用的方法都要调用. 验证了那句话:人在江湖漂,哪有不挨刀.该来的总是会来的. 

```
ApplicationParser.parse() --> CreateInputStatementContext|CreateOutputStatementContext : ParseContext
                                                         |--createTask: LazyTask
                                                         |--createAnalyzer: CreateInputStreamAnalyzer|CreateOutputStreamAnalyzer
                                                                                                     |--createAnalyzeContext: CreateStreamAnalyzeContext
```

>注意Input,Output的StatementContext,SemanticAnalyzer都有各自的实现类,并都继承了Stream的相关父类,但是AnalyzeContext没有各自的实现类,都是一样的了.   

### ApplicationBuilder  

ApplicationBuilder的构建需要每个分析器分析的结果AnalyzeContext(parseContexts): 专门用来完成从多个解析内容到应用程序的转换  
buildApplication()将整个应用程序的构建分成如下几步：  
1、各个算子的构建  
2、将完成拆分的应用程序解析成为Application  

```java
    public Application build(String appName, List<AnalyzeContext> parContexts, DriverContext driverContext) {
        this.applicationName = appName;     //应用程序名称
        this.parseContexts = parContexts;   //一系列CQL语句的解析结果
        
        executeLogicOptimizer();            //在构建应用程序之前，要先执行逻辑优化器. 目前貌似还没实现.
        buildApplication();                 //构建应用程序
        executePhysicOptimizer();           //在构建应用程序之后，要先执行物理优化器
        parseDriverContext(driverContext);  //将DriverContext的的值设置到Application中,比如UserConf,UserFile,UDF
        return app;                         //构建完成的应用程序
    }
```

逻辑计划包含的功能:   
1、SQL语句的重写，比如将where中的聚合filter调整到having中等等  
2、count(a+b),count(*),count(a) 的优化，全部改成count(1)  
3、Join的调整，将不等值Join改为Innerjoin  
4、将where条件中的等值表达式提升到On上面去。  

物理优化器的优化内容：  
1、OrderBy优化，实现sorted-merge排序。  
2、limit优化，上一个算子中加入limit。  
3、算子替换，将功能比较简单的算子，替换为Filter算子或者functor算子  
4、移除无意义的filter算子

逻辑计划和物理计划中间的步骤是构建Application. 在这里才开始new一个Application.  

```java
    private void buildApplication() {
        app = new Application(applicationName);                         //创建Application
        parseSchemas();                                                 //设置所有AnalyzeContext的CreatedSchemas到app的schemas
        List<SplitContext> splitContexts = splitOperators();            //拆分算子
        SplitContext splitContext = combineOperators(splitContexts);    //合并算子
        changeUnionOperators(splitContext);
        changeSchemaAfterAggregate(splitContext);
        app.setOperators(splitContext.getOperators());                  //将拆分|合并算子的operators和transitions设置到Application里
        app.setOpTransition(splitContext.getTransitions());
    }
```

---

## Schema & InputAnalyzer

parseSchemas解析Schema: 循环parseContexts的每个AnalyzeContext,获取AnalyzeContext对应的schemas.  
这个schemas是在什么时候设置进来的? 以我们熟悉的AnalyzeContext的一个实现类CreateStreamAnalyzeContext为例:  

```
CreateStreamAnalyzeContext.setSchema(Schema)  (com.huawei.streaming.cql.semanticanalyzer.analyzecontext)
    |--CreatePipeStreamAnalyzer.analyze()  (com.huawei.streaming.cql.semanticanalyzer)
    |--ApplicationBuilder.createPipeStreamSplitContext(Schema)  (com.huawei.streaming.cql.builder)
    |--CreateOutputStreamAnalyzer.analyze()  (com.huawei.streaming.cql.semanticanalyzer)
    |--FromClauseAnalyzer.createNewStreamContext(String, Schema)  (com.huawei.streaming.cql.semanticanalyzer)
    |--CreateInputStreamAnalyzer.analyze()  (com.huawei.streaming.cql.semanticanalyzer)
```

AnalyzeContext的setSchema方法会被对应的SemanticAnalyzer实现类的analyze调用,比如CreateInputStreamAnalyzer:  

```java
    public AnalyzeContext analyze() {
        //createInputStreamParseContext这个是ParseContext语法解析上下文对应的是CreateInputStatementContext
        String streamName = createInputStreamParseContext.getStreamName();
        ColumnNameTypeListContext columns = createInputStreamParseContext.getColumns();  //列的信息在语法内容中

        //下面的AnalyzeContext就是CreateStreamAnalyzeContext
        getAnalyzeContext().setStreamName(streamName);
        getAnalyzeContext().setSchema(createSchema(streamName, columns));  //根据streamName和columns创建Schema
        
        setSerDeDefine();               //序列化反序列化定义, 包括设置类setSerDeClass和属性setSerDeProperties
        setSourceDefine();              //数据源定义
        setParallelNumber();            //并行度
        return getAnalyzeContext();     //返回语义分析的结果,所以上面几个set动作其实都是往AnalyzeContext设置内容的.
    }
```

从DBMS过来的应该很熟悉Schema的含义,如果把streamName看做table, schema就表示表元数据: 表是由列组成的.  
streamName和columns信息最初都是存放在语法内容的上下文中ParseContext, 而这里的语义分析上下文AnalyzeContext需要根据语法创建对应的表结构.  
创建表元数据createSchema很简单了,就是new一个Schema,并将columns的每一列创建一个Column对象,加入到Schema中.  
现在我们知道了Schema的来龙去脉了. 其实语义分析的基础是语法内容, 根据语法产生的数据,构建语义需要的数据结构.  

>能不能把语法和语义的这两个上下文结果数据合并在一起? 作者在注释中也提到了这个问题. 如果修改了语法,对应的语义也要一起修改.   

## CQL Statement syntax

### Create Input Stream Statement syntax

```sql
CREATE INPUT STREAM streamName 
columnList [streamComment]      ①数据列
[serdeClause]                   ②反序列化
sourceClause                    ③数据读取/数据源
[parallelClause];               ④可选的算子并行度
```

Create Input Stream 语句定义了输入流的`①数据列名称,③数据读取方式和②数据反序列化方式`。  
SERDE 定义了数据的反序列化方式,即如何将从inputStream中读取的数据解析成对应的完整的流的Schema。  
系统配置了默认的序列化和反序列化类,当使用系统默认反序列化类时,SERDE子句可以省略,同时,SERDE 的相关配置属性也可以省略。  
SOURCE 定义了数据从什么地方读取,比如从MQ消息队列中读取或者文件等其他方式读取。SOURCE 语句一定不能省略。  
ParallelClause 语句指定了该输入算子的并发数量。

使用系统内置反序列化类读取算子示例  

```sql
CREATE INPUT STREAM example
(eventId INT, eventDesc STRING)
SERDE simpleSerDe PROPERTIES (separator = "|")
SOURCE TCPClientInput PROPERTIES (server = "127.0.0.1", port = "9999")
Parallel 2;
```

输入流语句的Context即输入流的语法解析内容对应了语法结构的组成部分.  

```java
public class CreateInputStatementContext extends CreateStreamStatementContext {
    private ClassNameContext deserClassName;                //② 反序列化类: SERDE  
    private StreamPropertiesContext deserProperties;        //   反序列化属性
    private ClassNameContext sourceClassName;               //③ 数据源类: SOURCE  
    private StreamPropertiesContext sourceProperties;       //   数据源属性
    private ParallelClauseContext parallelNumber;           //④ 并行度: PARALLEL  
}
```

>streamName和columns是在父类CreateStreamStatementContext中定义的.  
CreateInputStreamAnalyzer语义分析的结果已经在上面分析过了. 接下来我们看看insert和select语法,   
分别按照A.语法结构定义(statement syntax), B.语法解析结果(statement context), C.语义解析(analyze) D.语义解析结果(analyze context).    
最终结果都是为了得到语义解析结果. 一般都是获取语法解析的结果(StatementContext)经过计算最后设置到语义解析结果中(AnalyzeContext).    

### Insert Statement syntax

`A`.Insert包含三种**语法结构**:  

```sql
--1.Single Insert
INSERT INTO STREAM transformTemp SELECT * FROM transform;

--2.不包含子查询的 MultiInsert
FROM teststream
INSERT INTO STREAM s1 SELECT *
INSERT INTO STREAM s2 SELECT a
INSERT INTO STREAM s3 SELECT id, name WHERE id > 10 
Parallel 4;

--3.包含子查询的 MultiInsert
FROM
(
SELECT count(id) as id, 'sss' as name
FROM testStream(id >5 )[RANGE 1 SECONDS SLIDE] GROUP BY ss
)
INSERT INTO STREAM s1 SELECT *
INSERT INTO STREAM s2 SELECT a
INSERT INTO STREAM s3 SELECT id,name WHERE id > 10;
```

可以将数据导入一个未定义的流中,但是如果有要将多个流的数据导入一个新流,这么几个导入语句生成的schema列名称和列类型必须相同。  
允许将select结果导入到一个不存在的流中,只要这个流不是输入和输出流,系统就会自动创建该流。  

MultiInsert 语句一般用来进行单流数据的分割,它可以将一个流的数据,按照不同的处理规则,在处理完毕之后,发送至指定流。从而达到单流变多流的目的。  
MultiInsert 语句只有一个From子句,该子句中,只允许进行简单流的定义,不允许出现窗口等复杂语法。

`B`.InsertStatementContext对应第一种的**语法解析结果**(没有并行度), MultiInsertStatementContext对应2.3两种的多级插入语法解析结果(有并行度).  

```java
public class InsertStatementContext extends ParseContext {
    private String streamName;                  //insert into stream s1的streamName是s1
    private SelectStatementContext select;      //select * from ...
}

public class MultiInsertStatementContext extends ParseContext {
    private FromClauseContext from;             //FROM ...
    private List<MultiInsertContext> inserts;   //insert into ... insert into ...
    private ParallelClauseContext parallel;
}
```

`C`.insert语句的**语义解析**(multi insert的类似:解析from以及多条insert语句):  

```java
public class InsertStatementAnalyzer extends BaseAnalyzer {
    private InsertAnalyzeContext context = null;
    private InsertStatementContext insertContext;

    public AnalyzeContext analyze() {
        String streamName = insertContext.getStreamName();  //insertContext: InsertStatementContext
        context.setOutputStreamName(streamName);
        if (checkSchemaExists(streamName)) {
            context.setOutputSchema(getSchemaByName(streamName));
        } else {
            context.setPipeStreamNotCreated(true);
        }
        analyzeSelectStatement();   //分析Select语句
        createOutputStream();       //创建输出流
        return context;             //返回的是语义分析的结果AnalyzeContext: InsertAnalyzeContext
    }
}
```

`D`.语义解析结果AnalyzeContext: 对于第一种的insert语句(单条insert),包含了select子句.   
InsertStatementContext 语法解析结果中含有 SelectStatementContext select.  
InsertAnalyzeContext 语义解析结果中也含有 SelectAnalyzeContext selectContext.  

```java
    private void analyzeSelectStatement() {
        //insertContext.getSelect()返回的是InsertStatementContext中的SelectStatementContext, 对应分析器就是SelectStatementAnalyzer
        SemanticAnalyzer selectAnalzyer = SemanticAnalyzerFactory.createAnalyzer(insertContext.getSelect(), getAllSchemas());
        if (selectAnalzyer instanceof SelectStatementAnalyzer) {
            //创建查询语句的语义分析器, 设置查询的输出流名称为当前insert语句的输出流名称. 
            //比如insert into stream s1 select ... 表示查询语句的输出是s1.  
            ((SelectStatementAnalyzer)selectAnalzyer).setOutputStreamName(context.getOutputStreamName());
        }
        //InsertAnalyzeContext context是insert语句的语义解析结果. 
        //还是要对查询分析器SelectStatementAnalyzer经过分析analyze得到查询语义解析结果SelectAnalyzeContext
        context.setSelectContext((SelectAnalyzeContext)selectAnalzyer.analyze());
    }
```

insert中包含了select语句, 所以在语义解析insert语句时, 要首先对其包含的select进行语义解析.  

```
    SelectStatementContext select -----------------------------
          |                                                   ⬇️
    InsertStatementContext insertContext                      ⬇️  insertContext.getSelect() = SelectStatementContext
          |                                                   ⬇️  根据ParaseContext创建对应的SemanticAnalyzer  
InsertStatementAnalyzer.analyze()                             ⬇️  再调用语义解析器的analyze方法进行语义解析,返回值为语义解析结果AnalyzeContext
                       |--analyzeSelectStatement() -- SelectStatementAnalyzer.analyze()
                       |--createOutputStream()                                |
                       |--InsertAnalyzeContext  <-----------------------------|--SelectAnalyzeContext
                               context              context.setSelectContext
```

创建输出流, select的schema也就是insert的schema.  

```java
    private void createOutputStream() {
        //select的Schema中应该已经包含了完整的元数据,比如columns, 这里创建的输出流只是更改了输出流的名称.  
        Schema outputSchema = context.getSelectContext().getSelectClauseContext().getOutputSchema();
        outputSchema.setId(context.getOutputStreamName());          
        outputSchema.setName(context.getOutputStreamName());        
        outputSchema.setStreamName(context.getOutputStreamName());
        context.setOutputSchema(outputSchema);
    }
```

### Select Statement syntax

`A`.查询语句的语法结构:  

```
SelectClause 
FromClause 
[WhereClause] 
[GroupByClause] 
[HavingClause] 
[OrderbyClause] 
[LimitClause] 
[ParallelClause];
```

>Select语句(Statement)中的每个子句(Clause)都有自己的语法结构,因此都有自己的语法解析和语义解析.  

`B`.Select语句包含了多个子句, 比如select * from table. 则`select *`对应select子句, `from table`对应from子句.  

```java
public class SelectStatementContext extends ParseContext {  //②
    private SelectClauseContext select;
    private FromClauseContext from;                         //③
    private WhereClauseContext where;
    private GroupbyClauseContext groupby;
    private HavingClauseContext having;
    private OrderbyClauseContext orderby;
    private LimitClauseContext limit;
    private ParallelClauseContext parallel;
}
```

`C`.查询语句的语义分析(from子句的解析要优先于select子句).  

```java
public class SelectStatementAnalyzer extends BaseAnalyzer {
    private SelectAnalyzeContext context;                   //① StatementAnalyzer的analyze结果为AnalyzerContext
    private SelectStatementContext selectContext;           //② 语义解析StatementAnalyzer依赖于语法解析StatementContext

    public SelectAnalyzeContext analyze() {
        fromAnalyzer();                                     //③
        selectAnalyzer();
        resetOutputColumnTypes();
        whereAnalyzer();
        groupbyAnalyzer();
        havingAnalyzer();
        orderByAnalyzer();
        limitAnalyzer();
        parallelAnalyzer();
        dataSourceQueryArgumentsAnalyzer();
        return context;                                     //①
    }
}
```

`D`.查询的语义解析结果SelectAnalyzeContext(和SelectWithOutFromAnalyzeContext)    

```
一次查询: 带有from的比如select * from table
SelectAnalyzeContext ---------------|
      |                             |--FromClauseAnalyzeContext fromClauseContext
      |                             |--ParallelClauseAnalyzeContext parallelClauseContext
      |                             |++SelectStatementContext context   //②
      |
多次查询: 不带from的,比如前面multi insert,对同一个流查询多次
SelectWithOutFromAnalyzeContext ----|
                                    |--SelectClauseAnalyzeContext selectClauseContext
                                    |--FilterClauseAnalzyeContext whereClauseContext
                                    |--SelectClauseAnalyzeContext groupbyClauseContext
                                    |--OrderByClauseAnalyzeContext orderbyClauseContext
                                    |--FilterClauseAnalzyeContext havingClauseContext
                                    |--LimitClauseAnalzyeContext limitClauseContext
                                    |++MultiSelectContext context
```

下面的context是SelectAnalyzeContext, 不同解析器解析的结果也是AnalyzeContext,分别设置到SelectAnalyzeContext对应的字段中.  

```java
    private void fromAnalyzer() {                                         //    ②         ③
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(selectContext.getFrom(), getAllSchemas());
        context.setFromClauseContext((FromClauseAnalyzeContext)analyzer.analyze());
    }   //①            ③
    private void groupbyAnalyzer() {   
        SemanticAnalyzer analyzer = SemanticAnalyzerFactory.createAnalyzer(selectContext.getGroupby(), getInputSchemas());
        context.setGroupbyClauseContext((SelectClauseAnalyzeContext)analyzer.analyze());
    }
```

`语法解析Parser` - `语法解析结果ParseContext` -- `语义解析SemanticAnalyzer` -- `语义解析结果AnalyzeContext`

![stream-select](http://img.blog.csdn.net/20151127193205608)

### DataSource Statement syntax

数据源和 From 子句一起结合使用,dataSourceBody 语法在 from 字句中也进行了定义  

数据源的查询参数分为查询Schema定义(SCHEMA)和数据源查询(QUERY)两个部分。  
Schema 的定义同 Create Input Stream 中的 schema 定义,主要用来指定数据源查询结果 的列数量、名称、类型,便于进行下一步处理。

RDB 数据读取,支持多行数据读取,同时支持 CQL UDF 以及窗口和聚合运算  
QUERY 内部的参数顺序固定,不同的数据源,有不同的参数。  
RDB 的 SQL 中,如果不包含 Where,就会一次查询出多行记录。和原始流做了 Join 之后,最终输出多条结果。  

```sql
--数据源定义
CREATE DATASOURCE rdbdatasource         #dataSourceName
SOURCE RDBDataSource                    #className
PROPERTIES (                            #datasourceProperties     
url = "jdbc:postgresql://127.0.0.1:1521/streaming", 
username = "55B5B07CF57318642D38F0CEE0666D26", 
password = "55B5B07CF57318642D38F0CEE0666D26" );

--数据源查询
insert into rs select rdb.id,s.id,count(rdb.id),sum(s.id) 
from S[rows 10 slide],                  #普通的流可以和DataSource进行join 
DATASOURCE rdbdatasource                #dataSourceName, 前面定义好的数据源
[                                       #dataSourceBody
SCHEMA (id int,name String,type int),                                       #dataSourceSchema
QUERY ("select rid as id,rname,rtype from rdbtable where id = ? ", s.id)    #dataSourceQuery
] rdb                                   #sourceAlias
where rdb.name like '%hdd%'             #dataSourceArguments
group by rdb.id,s.id;
```

>原始流s的id会作为查询条件,传入数据源的SQL查询语句中, 同时数据源本身也有自己的查询条件.  

CreateDataSource相关的语法解析,语义解析和CreateStreamStatement类似,就不分析了.  

