---
layout: post
title: Apache Drill源码分析之物理计划
category: Source
tags: BigData
keywords: 
description: 
---

## Drill源码阅读(3) : 分析DrillBit各个角色

UserServer处理RUN_QUERY_VALUE客户端的查询请求,会将任务分派给UserWorker处理, 由worker提交工作:  
显然worker要在构造UserServer的时候也一起构造出来, 这样在收到任务的时候, 确保立即有工人接手这份工作.  
UserServer的构造在ServiceEngine,而服务引擎是由DrillBit创建的.  
UserWorker是由WorkerManager管理的, 而WorkerManager也是由DrillBit创建的.  
所以启动DrillBit服务后,参与计算的角色都已经准备好了.  
   

|Role|Explain|
|----|-------|
|WorkerBee|工蜂, 真正干活的|
|UserWorker|用户操作的(工人), 通过WorkerBee构成|
|WorkerManager|工人管理员,负责选择一个工人来工作|
|UserServer|用户操作的服务端,会将工作交给UserWorker,它需要一个UserWorker|
|Foreman|包工头,监工.由UserWorker创建出来. 因为UserWorker底层是WorkerBee,所以会将WorkerBee和Foreman关联起来|
|ServiceEngine|服务引擎,管理UserServer,Controller|
|DrillBit|Drill的服务端控制进程,管理ServiceEngine,WorkerManager|
|BootStrapContext|启动DrillBit的上下文,包括配置信息,度量注册|
|DrillbitContext|DrillBit工作时候的上下文|
|Controller|不同DrillBit节点的通信|
|ControllServer|不同节点间消息传输,连接等的RPC服务端|
|DataServer|负责数据交互的RPC服务端|

### 工人和监工的那些事

首先看下UserWorker是怎么提交一个任务的:  

```
public class UserWorker{
  private final WorkerBee bee;

  public QueryId submitWork(UserClientConnection connection, RunQuery query) {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    // create a new queryid where the first four bytes are a growing time (each new value comes earlier in sequence).  Last 12 bytes are random.
    long time = (int) (System.currentTimeMillis()/1000);
    long p1 = ((Integer.MAX_VALUE - time) << 32) + r.nextInt();
    long p2 = r.nextLong();
    QueryId id = QueryId.newBuilder().setPart1(p1).setPart2(p2).build();
    incrementer.increment(connection.getSession());
    Foreman foreman = new Foreman(bee, bee.getContext(), connection, id, query);
    bee.addNewForeman(foreman);
    return id;
  }
```

返回的QueryId会由UserServer通过RPC发送给客户端, 表示客户端这一次的查询标识. 服务端已经接受了这次查询.  
但是服务端还没有开始执行这个查询任务, 后续如果客户端需要查询结果, 可以凭这个QueryId, 就可以向服务端要数据结果.  

WorkerBee从名字上看是工作的蜜蜂, 工蜂一直默默无闻地工作. 它为母蜂Foreman服务.   
现在我们由UserWorker创建了一个Foreman. 工蜂把它加进来.    

>问题:    
>1.为什么不是由Foreman管理WorkerBee,而是让WorkerBee(工蜂)主动把Foreman(监工)加进来?  
>2.为什么Foreman作为一个进程,不是自己启动,而是要由工人来启动?  


Foreman负责管理一次查询的所有fragments, Foreman会作为根节点/驱动节点  

```
/**
 * Foreman manages all the fragments (local and remote) for a single query where this is the driving/root node.
 * The flow is as follows:
 * - Foreman is submitted as a runnable.  被提交为可执行的
 * - Runnable does query planning. 做什么: 查询计划
 * - state changes from PENDING to RUNNING 状态改变
 * - Runnable sends out starting fragments 发射起始fragments
 * - Status listener are activated 监听器被激活
 * - The Runnable's run() completes, but the Foreman stays around 线程的run方法结束,而Foreman还停留...做什么, 看下面的
 * - Foreman listens for state change messages. 监听状态改变的消息
 * - state change messages can drive the state to FAILED or CANCELED, in which case 状态消息会驱动/更新Foreman的状态
 *   messages are sent to running fragments to terminate 消息会使得正在运行的fragments终结
 * - when all fragments complete, state change messages drive the state to COMPLETED 当所有的fragments完成后, 状态改变的消息更新Formeman的状态为已完成
 */
public class Foreman implements Runnable {
  private final QueryId queryId; //the id for the query
  private final RunQuery queryRequest; //the query to execute
  private final QueryContext queryContext;
  private final QueryManager queryManager; // handles lower-level details of query execution
  private final WorkerBee bee; // provides an interface to submit tasks, used to submit additional work
  private final DrillbitContext drillbitContext;
  private final UserClientConnection initiatingClient; // used to send responses

  // Sets up the Foreman, but does not initiate any execution. 设置Foreman, 但是并没有初始化任何的执行
  public Foreman(final WorkerBee bee, final DrillbitContext drillbitContext, final UserClientConnection connection, final QueryId queryId, final RunQuery queryRequest) {
    this.bee = bee;
    this.queryId = queryId;
    this.queryRequest = queryRequest;
    this.drillbitContext = drillbitContext;
    this.initiatingClient = connection;
    this.closeFuture = initiatingClient.getChannel().closeFuture();
    closeFuture.addListener(closeListener);
    queryContext = new QueryContext(connection.getSession(), drillbitContext);
    queryManager = new QueryManager(queryId, queryRequest, drillbitContext.getPersistentStoreProvider(), stateListener, this);
    recordNewState(QueryState.PENDING);
  }
```

Foreman的run方法根据RunQuery的类型执行不同的方法,比如SQL类型,则要负责将SQL语句通过Calcite解析成逻辑计划,生成物理计划,最后运行物理计划.  

```
  private void runSQL(final String sql) throws ExecutionSetupException {
    final DrillSqlWorker sqlWorker = new DrillSqlWorker(queryContext);
    final Pointer<String> textPlan = new Pointer<>();
    final PhysicalPlan plan = sqlWorker.getPlan(sql, textPlan);
    queryManager.setPlanText(textPlan.value);
    runPhysicalPlan(plan);
  }
```

---

### SQL Parser

Calcite的planner对SQL进行parse解析, 生成SqlNode节点,  对于不同的SqlNode类型, 由不同的Handler进行进行解析.   

```
public class DrillSqlWorker {
  private final Planner planner;  //这两个Planner都是Calcite的,负责解析成逻辑计划
  private final HepPlanner hepPlanner;
  private final QueryContext context;

  public PhysicalPlan getPlan(String sql, Pointer<String> textPlan) throws ForemanSetupException {
    SqlNode sqlNode = planner.parse(sql);  //将SQL语句解析成SqlNode解析树①
    AbstractSqlHandler handler;
    SqlHandlerConfig config = new SqlHandlerConfig(hepPlanner, planner, context);
    switch(sqlNode.getKind()){
    case EXPLAIN:
      handler = new ExplainHandler(config);
      break;
    case SET_OPTION:
      handler = new SetOptionHandler(context);
      break;
    case OTHER:
      if(sqlNode instanceof SqlCreateTable) {
        handler = ((DrillSqlCall)sqlNode).getSqlHandler(config, textPlan);
        break;
      }
      if (sqlNode instanceof DrillSqlCall) {
        handler = ((DrillSqlCall)sqlNode).getSqlHandler(config);
        break;
      }
    default:
      handler = new DefaultSqlHandler(config, textPlan);
    }
    return handler.getPlan(sqlNode);
  }
```

>The Drillbit that receives the query from a client or application becomes the Foreman for the query and drives the entire query. 
>A parser in the Foreman parses the SQL[①], applying custom rules[②] to convert specific SQL operators into a specific logical operator syntax that Drill understands. 
>This collection of logical operators forms a logical plan. The logical plan describes the work required to generate the query results and defines what data sources and operations to apply.  
>
>Foreman中的parser解析SQL, 并运用定制的规则, 将SQL操作符(Calcite的节点)转换成Drill认识的逻辑操作符(Drill的节点DrillRel).  
>转换后的逻辑操作符集合会组成一个逻辑计划.  注意上面的sqlNode=planner.parse(sql)对应的是SQL操作符, 转换成DrillRelNode在Handler的getPlan中完成.  

### SqlNode(Calcite SQL操作符)

Calcite的编程API主要包括了: Operator, Rule, RelationExpression, SqlNode.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/calcite1.png)

### What's Rule?

Calcite的planner对SQL进行parse解析, 除了用到Calcite自身的一些规则外, Drill也会附加一些规则getRules给它. 定义在DrillSqlWorker的构造函数中.   
规则包括物理计划, 逻辑计划, 转换规则.  其中逻辑计划包括基本规则,用户自定义规则. 物理计划包括物理规则,存储插件的规则. 比如hive插件有自己的SQL执行转换规则.  

```
  public DrillSqlWorker(QueryContext context) {
    FrameworkConfig config = Frameworks.newConfigBuilder() ...
        .ruleSets(getRules(context))...  //Drill附加的规则②
        .build();
    this.planner = Frameworks.getPlanner(config);    
  }

  private RuleSet[] getRules(QueryContext context) {
    StoragePluginRegistry storagePluginRegistry = context.getStorage();
    RuleSet drillLogicalRules = DrillRuleSets.mergedRuleSets(DrillRuleSets.getDrillBasicRules(context), DrillRuleSets.getJoinPermRules(context), DrillRuleSets.getDrillUserConfigurableLogicalRules(context));
    RuleSet drillPhysicalMem = DrillRuleSets.mergedRuleSets(DrillRuleSets.getPhysicalRules(context), storagePluginRegistry.getStoragePluginRuleSet());
    // Following is used in LOPT join OPT.
    RuleSet logicalConvertRules = DrillRuleSets.mergedRuleSets(DrillRuleSets.getDrillBasicRules(context), DrillRuleSets.getDrillUserConfigurableLogicalRules(context));
    RuleSet[] allRules = new RuleSet[] {drillLogicalRules, drillPhysicalMem, logicalConvertRules};
    return allRules;
  }
```

逻辑计划的基本规则, 这些规则是通用的, 不需要在物理计划阶段完成, 通用的规则尽早做.  

```
  // Get an immutable list of rules that will always be used when running logical planning.
  public static RuleSet getDrillBasicRules(QueryContext context) {
      DRILL_BASIC_RULES = new DrillRuleSet(ImmutableSet.<RelOptRule> builder().add( //
      // Add support for Distinct Union (by using Union-All followed by Distinct)
      UnionToDistinctRule.INSTANCE,

      // Add support for WHERE style joins. 添加支持where类型的join
      DrillFilterJoinRules.DRILL_FILTER_ON_JOIN,
      DrillFilterJoinRules.DRILL_JOIN,
```

举个where类型的join规则转换: <http://blog.aliyun.com/733>  
SELECT * FROM A JOIN B ON A.ID=B.ID WHERE A.AGE>10 AND B.AGE>5   
`Predict Push Down`: 在遇有JOIN运算时,用户很有可能还要在JOIN之后做WHERE运算,此时就要从代数逻辑上分析,  
WHERE中计算的条件是否可以被提前到JOIN之前运算,以此来减少JOIN运算的数据量,提升效率  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill17.png)

那么Drill的FilterJoin规则是怎么样的呢?  

```
public class DrillFilterJoinRules {
  /** Predicate that always returns true for any filter in OUTER join, and only true for EQUAL or IS_DISTINCT_FROM over RexInputRef in INNER join.
   * With this predicate, the filter expression that return true will be kept in the JOIN OP.
   * Example:  INNER JOIN,   L.C1 = R.C2 and L.C3 + 100 = R.C4 + 100 will be kepted in JOIN.
   *                         L.C5 < R.C6 will be pulled up into Filter above JOIN. 
   *           OUTER JOIN,   Keep any filter in JOIN.
  */
  public static final FilterJoinRule.Predicate EQUAL_IS_DISTINCT_FROM =
      new FilterJoinRule.Predicate() {
        public boolean apply(Join join, JoinRelType joinType, RexNode exp) {
          // In OUTER join, we could not pull-up the filter. All we can do is keep the filter with JOIN, and then decide whether the filter could be pushed down into LEFT/RIGHT.
          if (joinType != JoinRelType.INNER) return true;
          List<RexNode> tmpLeftKeys = Lists.newArrayList();
          List<RexNode> tmpRightKeys = Lists.newArrayList();
          List<RelDataTypeField> sysFields = Lists.newArrayList();
          RexNode remaining = RelOptUtil.splitJoinCondition(sysFields, join.getLeft(), join.getRight(), exp, tmpLeftKeys, tmpRightKeys, null, null);
          if (remaining.isAlwaysTrue())  return true;
          return false;
        }
      };

  /** Rule that pushes predicates from a Filter into the Join below them. */
  public static final FilterJoinRule DRILL_FILTER_ON_JOIN =
      new FilterJoinRule.FilterIntoJoinRule(true, RelFactories.DEFAULT_FILTER_FACTORY,
          RelFactories.DEFAULT_PROJECT_FACTORY, EQUAL_IS_DISTINCT_FROM);
```

>这里最好要理解下Calcite的一些概念, 要不然理解起来有一定困难.  
>参考<http://blog.csdn.net/yunlong34574/article/details/46375733>了解下optiq-javaben这个项目的源码.  
>然后参考这里了解下查询下推优化:<https://datapsyche.wordpress.com/2014/08/06/optiq-query-push-down-concept>  

### Calcite FilterJoinRule

下面引用了Optiq作者的Apache Calcite Overview的一个示例:  

两张表进行join后有一个where过滤条件, 没有使用规则的话, 则要join完后才进行过滤:  

![](http://7xjs7x.com1.z0.glb.clouddn.com/calcite2.png)

使用FilterJoinRule后, 把Filter提前到Join之前, 扫描之后立刻进行, 这样减少了join的数据量:  

![](http://7xjs7x.com1.z0.glb.clouddn.com/calcite3.png)

那么怎么定义一个规则呢?  cal.rels是一个RelationExpression数组, 调用onMatch时, rels=[Join,Filter,Scan]  
因此我们要获得call.rels中的Join和Filter. 使用数组索引rel(0)表示Join, rel(1)表示Filter.  
最后调用call.transform(newJoin)将原始的RelationExpression转换成新的RelExp.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/calcite4.png)

> 注意转换后的右图Join',Filter'上面的引号表示new. 和原来的Join,Filter是不一样的变量了.  

这里我们进入Calcite的源码看看它是怎么做的. 内部类FilterIntoJoinRule的构造函数参数:  

filterFactory和projectFactory分别是FilterFactoryImpl,ProjectFactoryImpl. 
作为工厂类,它们的create方法会调用LogicalFilter,LogicalProject的create方法返回RelNode.  

那么问题是这里传入的为什么是Filter和Project呢? Filter显然需要,因为我们的规则就是针对Filter和Join进行优化的.  
Project呢? Filter肯定是针对某个字段进行过滤的, 这里的过滤字段就可以认为是先Project把结果查出来,才有机会进行过滤.  

```
  /** Rule that tries to push filter expressions into a join condition and into the inputs of the join. */
  public static class FilterIntoJoinRule extends FilterJoinRule {
    public FilterIntoJoinRule(boolean smart, RelFactories.FilterFactory filterFactory, RelFactories.ProjectFactory projectFactory, Predicate predicate) {
      super(
          operand(Filter.class,
              operand(Join.class, RelOptRule.any())),
          "FilterJoinRule:filter", smart, filterFactory, projectFactory, predicate);
    }

    @Override public void onMatch(RelOptRuleCall call) {
      Filter filter = call.rel(0);
      Join join = call.rel(1);
      perform(call, filter, join);
    }
  }
```

onMatch方法和上面的图是一样的. 而具体的call.transform则在FilterJoinRule的perform中完成.  我们先看下FilterIntoJoinRule类上面的注释:  
尝试着把filter表达式push到一个join条件里面, 并且push到join的输入. 假设join的输入是Scan,则filer会push到Scan后面.  

再来看看FilterJoinRule类上面的注释: `Planner rule that pushes filters above and within a join node into the join node and/or its children nodes.` 

向上提升filters(为什么是向上, 向上向下的方向是什么? Scan是输入源,则Scan在上, Scan-Join-Filter转换为Scan-Filter-Join,则Filter向上提升了一个等级),  
within表示在一个join节点内部, 原先是Scan-Join-Filter, 第一步是把Filter合并到Join里面: Scan-Join(Filter)  
或者join节点的子节点, 从Tree的角度来看, Join下面是两张数据源表,数据源就是Join的children节点.  
我们可以把Filterwithin到Join的孩子节点即Scan中. 即第二步就是: Scan(Filter)-Join. DAG图就是: Scan-Filter-Join. Wow!!!

1.把left,right表解析出来即join.left,join.right. 以及leftFitlers,rightFilters.  
2.根据leftFilters和left, rightFilters和right创建新的leftRel,rightRel节点  
3.创建新的join节点,并且引用新的孩子节点(即上面的leftRel,rightRel)  
4.调用call的transformTo,参数是最新的join节点.   

```
    // create FilterRels on top of the children if any filters were pushed to them
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
    RelNode leftRel = RelOptUtil.createFilter(join.getLeft(), leftFilters, filterFactory);
    RelNode rightRel = RelOptUtil.createFilter(join.getRight(), rightFilters, filterFactory);

    // create the new join node referencing the new children and containing its new join filters (if there are any)
    final RexNode joinFilter = RexUtil.composeConjunction(rexBuilder, joinFilters, false);

    RelNode newJoinRel = join.copy(join.getTraitSet(), joinFilter, leftRel, rightRel, joinType, join.isSemiJoinDone());
    call.getPlanner().onCopy(join, newJoinRel);
    if (!leftFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, leftRel);
    }
    if (!rightFilters.isEmpty()) {
      call.getPlanner().onCopy(filter, rightRel);
    }        

    // create a FilterRel on top of the join if needed
    RelNode newRel = RelOptUtil.createFilter(newJoinRel,
            RexUtil.fixUp(rexBuilder, aboveFilters, newJoinRel.getRowType()),
            filterFactory);

    call.transformTo(newRel);
  }
```

上面我们实现了自定义规则的onMatch方法, 那么谁来调用它呢:   

RelOptPlanner实现类VolcanoPlanner.fireRules->RelOptRule的实现类VolcanoRuleCall.match->matchRecurse->onMatch->getRule().onMatch(this);  

> Volcano的意思是火山似的,猛烈的. 由此说明规则很多的话, match调用会是很凶猛的. 


### Drill FilterJoin Example

执行下面的SQL语句, 第一次不加where,第二次添加where过滤条件, 第三次where是字段比较

```
select * FROM dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/nation.parquet` nations
join dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/region.parquet` regions
on nations.N_REGIONKEY = regions.R_REGIONKEY

select * FROM dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/nation.parquet` nations
join dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/region.parquet` regions
on nations.N_REGIONKEY = regions.R_REGIONKEY
where nations.N_NATIONKEY>10 and regions.R_NAME='AMERICA'

select * FROM dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/nation.parquet` nations
join dfs.`/home/hadoop/soft/apache-drill-1.0.0/sample-data/region.parquet` regions
on nations.N_REGIONKEY = regions.R_REGIONKEY
where nations.N_NAME<regions.R_NAME
```

下面是对应物理计划可视化图, 图1在Scan和JOIN之间有Project:  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill18.png)   

图2虽然where过滤在join之后, 但是经过优化后, 会先于join执行的: 即filter之后才进行join  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill19.png)   

图3就没这么幸运了,要在join之后才能filter.

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill20.png)   


---

### DrillRel(Drill逻辑操作符)

getPlan的参数SqlNode在前面通过Calcite的解析, 结果是一颗SQL parse tree(不要以为Node就只有一个节点),  
但它还只是Calcite认识的SQL操作符, 我们要将它转换为Drill能够认识的逻辑操作符即DrillRel.  

```
  public PhysicalPlan getPlan(SqlNode sqlNode) throws ValidationException, RelConversionException, IOException, ForemanSetupException {
    SqlNode rewrittenSqlNode = rewrite(sqlNode);
    TypedSqlNode validatedTypedSqlNode = validateNode(rewrittenSqlNode);
    SqlNode validated = validatedTypedSqlNode.getSqlNode();
    RelDataType validatedRowType = validatedTypedSqlNode.getType();
    RelNode rel = convertToRel(validated);
    rel = preprocessNode(rel);
    log("Optiq Logical", rel);

    DrillRel drel = convertToDrel(rel, validatedRowType);
    log("Drill Logical", drel);

    Prel prel = convertToPrel(drel);
    log("Drill Physical", prel);

    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);
    log("Drill Plan", plan);
    return plan;
  }
```

**Relational Expression(Rel)**

在查询过程中也说了: `执行计划总是包含一个Screen Operator,用来阻塞并且等待返回的数据. 返回的DrillRel就是逻辑计划.`  
SqlNode,RelNode是Calcite的节点, DrillRel是Drill的关系表达式节点,在最外层包装了一个Screen用于屏幕输出.    

```
  protected DrillRel convertToDrel(RelNode relNode, RelDataType validatedRowType) {
        // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
        DrillRel topPreservedNameProj = addRenamedProject((DrillRel) convertedRelNode, validatedRowType);
        return new DrillScreenRel(topPreservedNameProj.getCluster(), topPreservedNameProj.getTraitSet(), topPreservedNameProj);
      }
```

Screen Node和其他一些DrillRel的构造函数, 其中input指定了Screen的输入,表示用Screen节点包装上原先的节点, 使其成为一个新的节点.  

```
public class DrillScreenRel extends DrillScreenRelBase implements DrillRel {
  public DrillScreenRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(DRILL_LOGICAL, cluster, traitSet, input);
  }
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Store.builder().setInput(childOp).storageEngine("--SCREEN--").build();
  }  
```

类继承关系: DrillScreenRel >> DrillRel >> DrillRelNode >> RelNode  
其中DrillRel是逻辑计划的关系表达式. 子类要实现implement方法, 返回逻辑操作符.  

```
// Relational expression that is implemented in Drill.
public interface DrillRel extends DrillRelNode {
  // Calling convention for relational expressions that are "implemented" by generating Drill logical plans
  public static final Convention DRILL_LOGICAL = new Convention.Impl("LOGICAL", DrillRel.class);

  LogicalOperator implement(DrillImplementor implementor);
}
```

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill13-2.png)  ![](http://7xjs7x.com1.z0.glb.clouddn.com/drill13-1.png)

### DrillRel Nodes Tree → Drill LogicalPlan

DrillImplementor: `Context for converting a tree of DrillRel nodes into a Drill logical plan`  


### 物理计划Prel

然后将逻辑计划转换为物理计划, 将DrillRel转换为Prel. 最后才是Drill的Plan. 注意Drill的物理计划和最终的Plan是有点差别的.  

```
  protected Prel convertToPrel(RelNode drel) {
    Prel phyRelNode = (Prel) planner.transform(DrillSqlWorker.PHYSICAL_MEM_RULES, traits, drel);

    /*  The order of the following transformation is important */

    /*
     * 0.) For select * from join query, we need insert project on top of scan and a top project just
     * under screen operator. The project on top of scan will rename from * to T1*, while the top project
     * will rename T1* to *, before it output the final result. Only the top project will allow
     * duplicate columns, since user could "explicitly" ask for duplicate columns ( select *, col, *).
     * The rest of projects will remove the duplicate column when we generate POP in json format.
     */
    phyRelNode = StarColumnConverter.insertRenameProject(phyRelNode);  //* is star, and this column should convert
  }
```

转换的过程比较复杂, 而且转换的顺序也很重要. 先看第一个, 在select * from join这种情况下, 要插入两个Project.  
一个是scan(bottom)之上, 一个是screen(top)之下. 比如下面的SQL语句:    


```
select * from dfs.`/usr/install/apache-drill-1.1.0/sample-data/nation.parquet` nations
join dfs.`/usr/install/apache-drill-1.1.0/sample-data/region.parquet` regions
on nations.N_REGIONKEY = regions.R_REGIONKEY;
+--------------+-----------------+--------------+-----------------------+--------------+--------------+-----------------------+
| N_NATIONKEY  |     N_NAME      | N_REGIONKEY  |       N_COMMENT       | R_REGIONKEY  |    R_NAME    |       R_COMMENT       |
+--------------+-----------------+--------------+-----------------------+--------------+--------------+-----------------------+
| 0            | ALGERIA         | 0            |  haggle. carefully f  | 0            | AFRICA       | lar deposits. blithe  |
| 1            | ARGENTINA       | 1            | al foxes promise sly  | 1            | AMERICA      | hs use ironic, even   |
```

物理计划: 

```
00-00    Screen : rowType = RecordType(ANY *, ANY *0): rowcount = 25.0, cumulative cost = {62.5 rows, 402.5 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 2432
00-01   ⑤ ProjectAllowDup(*=[$0], *0=[$1]) : rowType = RecordType(ANY *, ANY *0): rowcount = 25.0, cumulative cost = {60.0 rows, 400.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 2431
00-02   ④   Project(T0¦¦*=[$0], T1¦¦*=[$2]) : rowType = RecordType(ANY T0¦¦*, ANY T1¦¦*): rowcount = 25.0, cumulative cost = {60.0 rows, 400.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 2430
00-03   ③     HashJoin(condition=[=($1, $3)], joinType=[inner]) : rowType = RecordType(ANY T0¦¦*, ANY N_REGIONKEY, ANY T1¦¦*, ANY R_REGIONKEY): rowcount = 25.0, cumulative cost = {60.0 rows, 400.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 2429
00-05   ①       Project(T0¦¦*=[$0], N_REGIONKEY=[$1]) : rowType = RecordType(ANY T0¦¦*, ANY N_REGIONKEY): rowcount = 25.0, cumulative cost = {25.0 rows, 50.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 2426
00-07              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/usr/install/apache-drill-1.1.0/sample-data/nation.parquet]], selectionRoot=file:/usr/install/apache-drill-1.1.0/sample-data/nation.parquet, numFiles=1, columns=[`*`]]]) : rowType = (DrillRecordRow[*, N_REGIONKEY]): rowcount = 25.0, cumulative cost = {25.0 rows, 50.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 2425
00-04   ②       Project(T1¦¦*=[$0], R_REGIONKEY=[$1]) : rowType = RecordType(ANY T1¦¦*, ANY R_REGIONKEY): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 2428
00-06              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet]], selectionRoot=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet, numFiles=1, columns=[`*`]]]) : rowType = (DrillRecordRow[*, R_REGIONKEY]): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 2427
```

对应的可视化图:  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill14.png)


物理计划中的$0, $1...这些数字代表的是as后的变量,如果是join有可能列名相同,所以也要添加project重命名防止名称冲突:    

```
① select T0.* as $0, T0.N_REGIONKEY as $1 from nations T0 
② select T1.* as $0, T1.R_REGIONKEY as $1 from regions T1
③ select T0.$0 as $0, T0.$1 as $1, T1.$0 as $2, T1.$1 as $3 
   from (select T0.$0 as $0, T0.$1 as $1 from nations) T0
   join (select T1.$0 as $2, T1.$1 as $3 from regions) T1
   on T0.$1 = T1.$3 
④ select $0 as $0,$2 as $1 from ( 
     select T0.$0 as $0, T0.$1 as $1, T1.$0 as $2, T1.$1 as $3 
     from (select T0.$0 as $0, T0.$1 as $1 from nations) T0
     join (select T1.$0 as $2, T1.$1 as $3 from regions) T1
     on T0.$1 = T1.$3
   )
⑤ select $0 as *, $1 as *0 from(  
     select $0 as $0,$2 as $1 from ( 
       select T0.$0 as $0, T0.$1 as $1, T1.$0 as $2, T1.$1 as $3 
       from (select T0.$0 as $0, T0.$1 as $1 from nations) T0
       join (select T1.$0 as $2, T1.$1 as $3 from regions) T1
       on T0.$1 = T1.$3
     )
   )
   select *,*0 from ...     
```

上面的StarColumn规则有点复杂, 我们看下Join列冲突的规则. 对应上面的③JOIN. 将所有的列都重命名了($0,$1,$2,$3, 然后以$1,$3进行join).  

```
    /*
     * 1.)
     * Join might cause naming conflicts from its left and right child.
     * In such case, we have to insert Project to rename the conflicting names.
     */
    phyRelNode = JoinPrelRenameVisitor.insertRenameProject(phyRelNode);
```

根据注释中说的join有left或者right child. 注意child这个词的含义. join作为根, 而left和right表分别是根的左右子节点.    

>为了防止名称冲突, 添加project, 这样就和上面我们看到的可视化Plan图是一一对应的了.  
>那么思考下: 这里的join插入的Project是在①和②,还是④??  
>我觉得是在④这里, 因为①和②已经在上面第一个转换规则StarColumnConverter中运用过了.  

insert操作让传入的phyRelNode节点调用它的accept方法, 并接收JoinPrelRenameVisitor实例对象.  

```
public class JoinPrelRenameVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{
  private static JoinPrelRenameVisitor INSTANCE = new JoinPrelRenameVisitor();

  public static Prel insertRenameProject(Prel prel){
    return prel.accept(INSTANCE, null);
  }

```

这里的Prel通过层层的规则嵌套, 最终返回的还是一个Prel, 也就是说,每次运用一个规则,都要把当前最新值传进来. Prel也实现了DrillRelNode接口.  
DrillRelNode再结合上Visitor, 有种层层嵌套的感觉.首先注册操作符的规则,从而构成一张图,最后根据DAG图访问每个操作符的时候,再运用上规则. 

假设上面JoinPrelRenameVisitor的insertRenameProject的Prel是JoinPrel

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill15.png)

```
public abstract class JoinPrel extends DrillJoinRelBase implements Prel{
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitJoin(this, value);
  }

  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getLeft(), getRight());
  }
```

accept()的参数logicalVisitor显然就是JoinPrelRenameVisitor了. this是当前对象即JoinPrel. 
那么就要调用JoinPrelRenameVisitor的visitJoin方法. 你看又回到Visitor来了.  

```
public class JoinPrelRenameVisitor extends BasePrelVisitor<Prel, Void, RuntimeException>{

  public Prel visitJoin(JoinPrel prel, Void value) throws RuntimeException {
    List<RelNode> children = Lists.newArrayList();
    for(Prel child : prel){
      child = child.accept(this, null);
      children.add(child);
    }

    final int leftCount = children.get(0).getRowType().getFieldCount();
    List<RelNode> reNamedChildren = Lists.newArrayList();

    RelNode left = prel.getJoinInput(0, children.get(0));
    RelNode right = prel.getJoinInput(leftCount, children.get(1));
    reNamedChildren.add(left);
    reNamedChildren.add(right);
    return (Prel) prel.copy(prel.getTraitSet(), reNamedChildren);
  }
}  
```

JoinPrel是个迭代器, 因此用for-loop方式可以遍历它的节点: 即参与join的left和right表(实现了iterator方法).    
JoinPrel的getJoinInput方法参数是offset和RelNode. offset表示join之后列的索引(两张表join后的所有列). 

假设我们用两张一样的表进行join,可以看到相同的列, 右边的表会被重命名:  

```
select * from dfs.`/usr/install/apache-drill-1.1.0/sample-data/region.parquet` region1
join dfs.`/usr/install/apache-drill-1.1.0/sample-data/region.parquet` regions
on region1.R_REGIONKEY = regions.R_REGIONKEY;
+--------------+--------------+-----------------------+---------------+--------------+-----------------------+
| R_REGIONKEY  |    R_NAME    |       R_COMMENT       | R_REGIONKEY0  |   R_NAME0    |      R_COMMENT0       |
+--------------+--------------+-----------------------+---------------+--------------+-----------------------+
| 0            | AFRICA       | lar deposits. blithe  | 0             | AFRICA       | lar deposits. blithe  |
```

分别调用两次getJoinInput,传入不同的offset和input, 这两个结果一定是不同的.   

```
  // Check to make sure that the fields of the inputs are the same as the output field names.  If not, insert a project renaming them.
  public RelNode getJoinInput(int offset, RelNode input) {
    final List<String> fields = getRowType().getFieldNames();
    final List<String> inputFields = input.getRowType().getFieldNames();
    final List<String> outputFields = fields.subList(offset, offset + inputFields.size());
    if (!outputFields.equals(inputFields)) {
      // Ensure that input field names are the same as output field names.
      // If there are duplicate field names on left and right, fields will get lost.
      // In such case, we need insert a rename Project on top of the input.
      return rename(input, input.getRowType().getFieldList(), outputFields);
    } else {
      return input;
    }
  }
```

>上面的处理不知道什么情况下会进入if部分.  假设有两张表都是A,B,C三列.   
>left表不可能有重复的列名, right表相对于left而言,三个列都是重复的. 调用getJoinInput(3, rightNode){} 
>inputFields=[A,B,C], fields=[A,B,C,A,B,C]. outputFields=[A,B,C],不是相等的吗??

看下相同表的join的可视化树, 对比一下就知道了, 在00-04中加了Project:  

```
00-00    Screen : rowType = RecordType(ANY *, ANY *0): rowcount = 5.0, cumulative cost = {20.5 rows, 120.5 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 1299
00-01      ProjectAllowDup(*=[$0], *0=[$1]) : rowType = RecordType(ANY *, ANY *0): rowcount = 5.0, cumulative cost = {20.0 rows, 120.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 1298
00-02        Project(T0¦¦*=[$0], T1¦¦*=[$2]) : rowType = RecordType(ANY T0¦¦*, ANY T1¦¦*): rowcount = 5.0, cumulative cost = {20.0 rows, 120.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 1297
00-03          HashJoin(condition=[=($1, $3)], joinType=[inner]) : rowType = RecordType(ANY T0¦¦*, ANY R_REGIONKEY, ANY T1¦¦*, ANY R_REGIONKEY0): rowcount = 5.0, cumulative cost = {20.0 rows, 120.0 cpu, 0.0 io, 0.0 network, 88.0 memory}, id = 1296
00-04            Project(T1¦¦*=[$0], R_REGIONKEY0=[$1]) : rowType = RecordType(ANY T1¦¦*, ANY R_REGIONKEY0): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 1295
00-06              Project(T1¦¦*=[$0], R_REGIONKEY=[$1]) : rowType = RecordType(ANY T1¦¦*, ANY R_REGIONKEY): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 1294
00-08                Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet]], selectionRoot=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet, numFiles=1, columns=[`*`]]]) : rowType = (DrillRecordRow[*, R_REGIONKEY]): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 1293
00-05            Project(T0¦¦*=[$0], R_REGIONKEY=[$1]) : rowType = RecordType(ANY T0¦¦*, ANY R_REGIONKEY): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 1292
00-07              Scan(groupscan=[ParquetGroupScan [entries=[ReadEntryWithPath [path=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet]], selectionRoot=file:/usr/install/apache-drill-1.1.0/sample-data/region.parquet, numFiles=1, columns=[`*`]]]) : rowType = (DrillRecordRow[*, R_REGIONKEY]): rowcount = 5.0, cumulative cost = {5.0 rows, 10.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 1291
```

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill16.png)