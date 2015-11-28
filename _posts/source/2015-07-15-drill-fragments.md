---
layout: post
title: Apache Drill源码分析之fragment
category: Source
tags: BigData
keywords: 
description: 
---

## Drill源码阅读(4) : Fragment

### Foreman.runPhysicalPlan运行物理计划

在查询一节中说过: `有了物理计划,所有的统计信息,最优端点,Foreman中的Parallellizer会将物理计划转换为多个fragments`  
将物理计划转换为fragments是在Foreman中, 就是在runPhysicalPlan的第一步getQueryWorkUnit中  

```
  private QueryWorkUnit getQueryWorkUnit(final PhysicalPlan plan) throws ExecutionSetupException {
    final PhysicalOperator rootOperator = plan.getSortedOperators(false).iterator().next();   // 物理计划的根节点物理操作符为Screen
    final Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);   // ①递归调用树入口,从上到下调用每个操作符的accept方法

    final SimpleParallelizer parallelizer = new SimpleParallelizer(queryContext);   // 并行, 用来设置fragments的并行度

    final QueryWorkUnit queryWorkUnit = parallelizer.getFragments(
        queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(),
        queryId, queryContext.getActiveEndpoints(), drillbitContext.getPlanReader(), rootFragment,
        initiatingClient.getSession(), queryContext.getQueryContextInfo());
    return queryWorkUnit;
  }        
```

在debug一节,我们知道DrillRel drel, Prel prel, PhysicalOperator pop, PhysicalPlan plan各个变量的值.  
上面通过PhysicalPlan获得的rootOperator就是PhysicalOperator pop根节点, 即`rootOperator=Screen`.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-pop.png)

rootOperator.accept后返回的是rootFragment.  然后通过root又开始递归遍历了(跟debug一节rootLOP.accept一样).   
下面是MakeFragmentsVisitor访问器当访问到的是一个操作符时, 首先将当前操作符加入到Fragment中, 然后遍历其孩子节点.  

```
  public Fragment visitOp(PhysicalOperator op, Fragment value)  throws ForemanSetupException{
    value = ensureBuilder(value);
    value.addOperator(op);
    for (PhysicalOperator child : op) {
      child.accept(this, value);
    }
    return value;
  }
```

下面是Screen->..->Scan的递归调用示例树:  

```
rootOperator.accept(v,null)-->Screen.accept(visitor, null)
                          |--MakeFragmentsVisitor.visitOp
                                        |--value=new Fragment 
                                        |--value.addOperator(Screen)
                                        |--for child : [EasyGroupScan]--child.accept(visitor, value)    我们省略了中间的一些节点,假设Screen的下一个节点是Scan
                                                     |--EasyGroupScan.accept(visitor, value)
                                                                    |--MakeFragmentsVisitor.visitOp
                                                                    |--value.addOperator(EasyGroupScan)
                                                                    |--EasyGroupScan has no child
                                                                    |--return value
```

> 递归调用树有点类似于设计模式中的访问者模式(Visitor Pattern).  

返回值`Fragment rootFragment = rootOperator.accept(MakeFragmentsVisitor.INSTANCE, null);`  
因为参数是null, 所以第一次调用rootOperator.accept的时候就创建了新的Fragment. 接下来child.accept,   
因为把value : Fragment传入, 所以不会再构造Fragment了(除非出现Exchange的时候才会new一个新的Fragment).   
注意在每次递归调用child.accept之前, 把当前的物理操作符加入到Fragment中.   
也就是说物理计划组成的DAG图的每个物理操作符都会加入到Fragment中, 这个Fragment并不代表DAG图中的某个节点(比如根节点), 而是包含了所有的操作符.    
这和前面的DrillRel, Prel, PhysicalOperator, PhysicalPlan不一样:它们的值是DAG图的第一个节点,然后通过input或者child嵌套包含其他节点.  

当然Fragment中要有根物理操作符, 这样把根拎出来, 其他所有的操作符也都能找到了.  

```
public class Fragment implements Iterable<Fragment.ExchangeFragmentPair> {
  private PhysicalOperator root;
  private ExchangeFragmentPair sendingExchange;
  private final List<ExchangeFragmentPair> receivingExchangePairs = Lists.newLinkedList();

  // Set the given operator as root operator of this fragment. If root operator is already set, then this method call is a no-op.
  public void addOperator(PhysicalOperator o) {
    if (root == null) {
      root = o;
    }
  }  

   public void addSendExchange(Exchange e, Fragment sendingToFragment) throws ForemanSetupException{
      if (sendingExchange != null) {
         throw new ForemanSetupException("Fragment was trying to add a second SendExchange.  ");
      }
      addOperator(e);
      sendingExchange = new ExchangeFragmentPair(e, sendingToFragment);
   }

   public void addReceiveExchange(Exchange e, Fragment fragment) {
       this.receivingExchangePairs.add(new ExchangeFragmentPair(e, fragment));
   }  
}
```

MakeFragmentsVisitor如果访问到的是一个Exchange操作符, Exchange会和Fragment组成一个ExchangeFragmentPair.  

```
  public Fragment visitExchange(Exchange exchange, Fragment value) throws ForemanSetupException {
    Fragment next = getNextBuilder();           // 总是会新建一个新的Fragment:next
    value.addReceiveExchange(exchange, next);   // 将Exchange操作符和新的Fragment组成一个ExchangeFragmentPair, 添加到原来Fragment的list中
    next.addSendExchange(exchange, value);      // Exchange操作符和原来的Fragment也会组成一个ExchangeFragmentPair,不过用于发送
    exchange.getChild().accept(this, next);     // Exchange下面的孩子节点, 用的Fragment是新的那一个
    return value;                               // 但是我们最后返回的, 仍然是第一次新建的那一个Fragment
  }
```

调用这个方法时, value一定不为空: The simple fragmenter was called without a FragmentBuilder value.    
This will only happen if the initial call to SimpleFragmenter  is by a Exchange node.    
This should never happen since an Exchange node should never be the root node of a plan
一个Exchange节点永远不能是一个计划的根节点.  Exchange前是一个Major Fragment, Exchange后也是一个Major Fragment.  

> 至于为什么先是Receiver,然后是Sender, 我们先看下官网中的概念,以及举个带有Exchange的例子: 

### fragments theory

> <http://drill.apache.org/docs/drill-query-execution/>  
> A parallelizer in the Foreman transforms the physical plan into multiple phases, called major and minor fragments.  
> These fragments create a multi-level execution tree that rewrites the query and executes it in 
> parallel against the configured data sources, sending the results back to the client or application.
>
> 并行化会将物理计划分成多个阶段. 什么时候需要并行? 任务是可以分解的时候, 任务之间没有关联, 比如Hadoop的MapReduce就是可并行化的.  
> 这些阶段叫做major或者minor fragmens. 它们组成了一个多层的执行树, 重写查询, 并且能够并行地在数据源上执行.  
> 
> 以传统DAG图的方式, 只有前面的节点处理完后,后面的节点才会继续运行. 而用并行化的方式,每个节点运行完一部分数据,后面的节点就可以接着这些数据进行计算.

![](http://drill.apache.org/docs/img/execution-tree.PNG)

### Major Fragments  

> Drill separates major fragments by an exchange operator. An exchange is a change in data location and/or parallelization of the physical plan.   
> An exchange is composed of a sender and a receiver to allow data to move between nodes  
>
> Drill用交换操作符来分隔major fragments. 一个交换操作符是数据位置的交换, 或者物理计划并行度的变更.  
> 一个交换操作符由一个发送器和一个接收器组成, 以运行数据在不同节点之间进行移动. 
>
> Major fragments do not actually perform any query tasks. Each major fragment is divided into one or multiple minor fragments  
> that actually execute the operations required to complete the query and return results back to the client. 
>
> Major Fragments不执行任何的查询任务, 每个major fragments会分成一个或多个minor fragments. 
> Minor Fragments会执行完成这个查询需要的操作, 并且返回结果给客户端.  

![](http://drill.apache.org/docs/img/ex-operator.png)

那么什么时候会产生Major Fragments: 读取的是HDFS上的文件时(count(*)无条件查询即使是hdfs文件也没有exchange).  

```
- select count(*) from hdfs.`/user/hive/warehouse/test.db/koudai`
+ select count(*) from hdfs.`/user/hive/warehouse/test.db/koudai` where sequence_id like '%12%'
+ select t.event_result_map.map from hdfs.`/user/hive/warehouse/test.db/koudai` t where t.sequence_id='1433300095954-25887486'
+ select t.sequence_id from hdfs.`/user/hive/warehouse/test.db/koudai` t limit 1
+ select * from hdfs.`/user/hive/warehouse/test.db/koudai` limit 1

- select * from cp.`employee.json` limit 1
- select * from dfs.`/Users/zhengqh/data/hive_alltypes.parquet` limit 1
```

> 为什么无条件的count(*)查询没有exchange. 观察Operator,发现count查询底层的scan是DIRECT_SUB_SCAN,
> 而parquet的其他查询(带条件的count,where,limit,*)用的是PARQUET_ROW_GROUP_SCAN. 后面的cp和本地查询则没有Exchange.    

下图中白色的UnionExchange分隔了两个Major Fragments. totalFragments的个数指的是所有的minor framgnet. 
对比DAG图和Operator Profiles. 可以看到Exchange对应的Operator是Receiver和Sender.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill24.png) ![](http://7xjs7x.com1.z0.glb.clouddn.com/drill26.png)

> 注意左侧的UnionExchange在右侧中被分成了Receiver和Sender.  

第一个Major framgnet在UnionExchange的上方, 即Screen, 只有一个mior Fragment.  
第二个Major framgnet包括了多个操作符, 有2个minor Fragments. 

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill25.png)

### Receiver+Sender

以上图中的Screen->UnionExchange->Project->...->Scan的顺序分析下UnionExchange:  

在访问根操作符visitOp(Screen,null)时, MakeFragmentsVisitor会新建一个Fragment, 设置Fragment的root=Screen.  
接着因为Screen的Child是UninonExchange,调用的是MakeFragmentsVisitor的visitExchange(UnionExchange,Fragment value).  
第二个参数Fragment value是访问Screen时创建的第一个Fragment, 第一个Fragment value一定不为空, 因为不允许根节点是Exchange.  

因为Exchange是用来分隔Major Fragment的, 所以在Exchange之前和之后都要有一个Major Fragment,之前就是第一个Fragment了.  
重点看下visitExchange的下面的逻辑, 理清到底第一个Fragment value和下一个Fragment next分别添加的是什么组件.  

```
    value.addReceiveExchange(exchange, next);   // first add Receiver from next
    next.addSendExchange(exchange, value);      // next add Sender to first
```

注意我们的DAG图从上到下,第一个节点是Screen,最下面的节点是Scan, 所以上面的是作为接收数据的一方,下面的是发送数据的一方.  
而first Fragment即上面的value, 是在上方的,那么就是作为接收方Receiver的. 
而且最后返回给客户端的也是上层的,客户端只需要知道和Screen相关的那个Fragment,即返回值是value.   

第一个Fragment value添加一个Receive Exchange, 只是把新建的ExchangeFragmentPair加入到value的List<ExchangeFragmentPair> receivingExchangePairs中.  
而第二个Fragment next我们已经知道了在visitExchange时创建了一个新的Fragment. 对于每一个全新的Fragment, 都要设置root节点操作符.    

实际上观察前面的DAG图和Operator Profiles,你会发现UnionExchange的UNORDERED_RECEIVER的编号是00-xx-01,因此是属于第一个Fragment的.  
而SINGLE_SENDER的编号是01-xx-00, 则是属于第二个Fragment. 因此第二个Fragment的root就是SINGLE_SENDER. 

上面两个value和next互相添加对方, 实际上是为了在上下文中都能找到对方. 否则如果只是value添加了next. 则在next时就无法找到value的.  

|Fragment|root|sendingExchange|receivingExchangePairs|Explain|Role|
|--------|----|---------------|----------------------|-------|----|
|value|Screen|×|ExchangeFragmentPair(e,next)|value的接收者是next|Reciever|
|next|SINGLE_SENDER|ExchangeFragmentPair(e,value)|×|next要发送给value|Sender|

看看UnionExchange的几个相关方法:

```
public class UnionExchange extends AbstractExchange{
  // Ephemeral info for generating execution fragments. 这几个变量是AbstractExchange中的,为了阅读的方便放在这里
  protected int senderMajorFragmentId;
  protected int receiverMajorFragmentId;
  protected List<DrillbitEndpoint> senderLocations;
  protected List<DrillbitEndpoint> receiverLocations;

  public void setupSenders(List<DrillbitEndpoint> senderLocations) {
    this.senderLocations = senderLocations;
  }

  protected void setupReceivers(List<DrillbitEndpoint> receiverLocations) throws PhysicalOperatorSetupException {
    Preconditions.checkArgument(receiverLocations.size() == 1, "Union Exchange only supports a single receiver endpoint.");
    super.setupReceivers(receiverLocations);
  }

  public Sender getSender(int minorFragmentId, PhysicalOperator child) {
    return new SingleSender(receiverMajorFragmentId, child, receiverLocations.get(0));
  }

  public Receiver getReceiver(int minorFragmentId) {
    return new UnorderedReceiver(senderMajorFragmentId, PhysicalOperatorUtil.getIndexOrderedEndpoints(senderLocations), false);
  }
}
```

上面getSender和getReceiver的第一个参数是minorFragmentId. new一个Sender或者Receiver都要知道对方的MajorFragmentId.  
比如SingleSender要知道Receiver的MajorFragmentId,以及接收者的一个Location. UnorderReceiver要知道Sender的MajorId,以及所有发送者的Locations.     

SingleSender: `Sender that pushes all data to a single destination node.` 发送者会发送所有的数据到一个目标节点,那么当然要指定这个目标节点了.  
这个目标节点应该是跟上表中的sendingExchange变量相关的, 可以看到这一行的root=SINGLE_SENDER. 当然目标节点指的应该是Drillbit级别,而不是Operator了.  

```
public class SingleSender extends AbstractSender {
  /**
   * Create a SingleSender which sends data to fragment identified by given MajorFragmentId and MinorFragmentId, and running at given endpoint
   *
   * @param oppositeMajorFragmentId MajorFragmentId of the receiver fragment.
   * @param oppositeMinorFragmentId MinorFragmentId of the receiver fragment.
   * @param child Child operator
   * @param destination Drillbit endpoint where the receiver fragment is running.
   */
  @JsonCreator
  public SingleSender(@JsonProperty("receiver-major-fragment") int oppositeMajorFragmentId,
                      @JsonProperty("receiver-minor-fragment") int oppositeMinorFragmentId,
                      @JsonProperty("child") PhysicalOperator child,
                      @JsonProperty("destination") DrillbitEndpoint destination) {
    super(oppositeMajorFragmentId, child,
        Collections.singletonList(new MinorFragmentEndpoint(oppositeMinorFragmentId, destination)));
  }
```

`MinorFragmentEndpoint represents fragment's MinorFragmentId and Drillbit endpoint to which the fragment is assigned for execution.`  
DrillbitEndpoint是运行'Drillbit'服务的节点(集群的计算节点). MinorFragmentEndpoint是fragment要执行在哪个Drillbit节点,更细粒度(Container?).  

对于Reciever而言, 它可以有多个Sender. 

```
public class UnorderedReceiver extends AbstractReceiver{
  @JsonCreator
  public UnorderedReceiver(@JsonProperty("sender-major-fragment") int oppositeMajorFragmentId,
                           @JsonProperty("senders") List<MinorFragmentEndpoint> senders,
                           @JsonProperty("spooling") boolean spooling) {
    super(oppositeMajorFragmentId, senders, spooling);
  }
```

下面解释下FragmentLeaf这个接口下都有哪些实现类. 

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill27.png)

FragmentLeaf是一个Fragment的叶子节点, Fragment和DAG图的叶子节点是有点差别呢的. 因为一个DAG图会包括多个Fragment.   
1.接收者是一个Fragment的叶子, 因为Exchange会分隔Fragment. Fragment的上方是接收者,是上面一个Fragment的叶子节点.   
2.整个DAG图的叶子节点通常是Scan,是组成DAG最下面的那个Fragment的叶子节点.   

Fragment的Root是一个Fragment的根节点  
1.发送者是一个Fragment的根节点, 即Exchange分隔的下面一个Fragment的根节点,而Fragment下发是一个Sender.    
2.整个DAG图的根节点通常是Screen.  

### QueryWorkUnit

在运行物理计划的第一句是根据物理计划得到QueryWorkUnit:    

```
    final QueryWorkUnit work = getQueryWorkUnit(plan);
    final List<PlanFragment> planFragments = work.getFragments();
    final PlanFragment rootPlanFragment = work.getRootFragment();
```

查询的工作单元包含了三个组件, 对于本地而言的根Fragment和根操作符.  这里的本地指的是Foreman.  

```
public class QueryWorkUnit {
  private final PlanFragment rootFragment; // for local
  private final FragmentRoot rootOperator; // for local
  private final List<PlanFragment> fragments;  // Major+Minor Fragments
```

而PlanFragment既是Plan又是Fragment.  前面我们知道Fragment由Exchange分成了多个Major Fragment.  
在遍历物理操作符时, 会将物理操作符加入到对应的Fragment中.  

#### **Protobuf**

必须上Protobuf这道菜了. 对于理解不同组件之间的关系是有作用的.  其实前面RPC部分也是用到了protobuf.  
PlanFragment的protobuf定义在BitControl.proto中. FragmentHandle在ExecutionProtos.proto中  

```
message PlanFragment {
  optional FragmentHandle handle = 1;
  optional float network_cost = 4;
  optional float cpu_cost = 5;
  optional float disk_cost = 6;
  optional float memory_cost = 7;
  optional string fragment_json = 8;
  optional bool leaf_fragment = 9;
  optional DrillbitEndpoint assignment = 10;
  optional DrillbitEndpoint foreman = 11;
  optional int64 mem_initial = 12 [default = 20000000]; // 20 megs
  optional int64 mem_max = 13 [default = 2000000000]; // 20 gigs
  optional exec.shared.UserCredentials credentials = 14;
  optional string options_json = 15;
  optional QueryContextInformation context = 16;
  repeated Collector collector = 17;
}

message FragmentHandle {
	optional exec.shared.QueryId query_id = 1;
	optional int32 major_fragment_id = 2;
	optional int32 minor_fragment_id = 3;
}
```

在日志一节, 其中Root Fragment(rootFragment对象)打印的信息如下, 可以看到正好对应了上面的PlanFragment的协议格式:  

```
handle {				→ FragmentHandle
  query_id {
    part1: 3053657859282349058
    part2: -8863752500417580646
  }
  major_fragment_id: 0
  minor_fragment_id: 0
}
fragment_json: "{		→ fragment_json
  ...
}"
leaf_fragment: true
assignment {			→ DrillbitEndpoint
  address: "localhost"
  user_port: 31010
  control_port: 31011
  data_port: 31012
}
foreman {				→ DrillbitEndpoint
  address: "localhost"
  user_port: 31010
  control_port: 31011
  data_port: 31012
}
context {
  query_start_time: 1436498522273
  time_zone: 299
  default_schema_name: ""
}
```

#### **QueryContext & DrillbitContext**

计算fragments要根据查询的上下文QueryContext,以及DrillbitContext.   
queryContext.getCurrentEndpoint()表示Foreman节点, queryContext.getActiveEndpoints()表示参与计算的其他节点.  
我们重点看下获取活动的Endpoints是怎么做得, 因为Drill是分布式的计算引擎,添加计算节点能够让计算能力提高.  
那么它是怎么实现的, 通过ZK的Watcher机制, 如果有节点增加进来,获取可用的计算节点时就是动态实时的.   

```
queryContext.getActiveEndpoints()
            |
            |-->drillbitContext.getBits()
                      |     
                      |-->ClusterCoordinator.getAvailableEndpoints()
                                 |
                                 |<--ZKClusterCoordinator.endpoints
                                               |
                                               |<--updateEndpoints()

```

>我们知道了通过ZK实时获取动态的计算节点, 但是任务是怎么分配到计算节点上的. 我们能不能自定义转发规则??  


#### SimpleParallelizer

由SimpleParallelizer获得Fragments, 参数activeEndpoints就是上面从上下文中得到的集群中可用的Drillbit计算节点.  
rootFragment是rootOperator返回的Fragment, 物理计划的rootOperator一般是Screen. 这里的Fragment指的是由Exchange分割的Major Fragment.  

该方法根据提供的Fragment树生成分配好的Fragments集合, 就是PlanFragment Protobuf对象集合, 会被分配到单独的节点.   
返回的Fragments(注意是复数形式), 则是Major+Minor级别的Fragment了.  而Minor Fragments可以有多个, 是可以并行处理的.  

>什么是Fragment树?  就是文档中提到的将物理计划转成多个Fragments,这些Fragments组成了一棵树.  

```
    final QueryWorkUnit queryWorkUnit = parallelizer.getFragments(
        queryContext.getOptions().getOptionList(), queryContext.getCurrentEndpoint(),
        queryId, queryContext.getActiveEndpoints(), drillbitContext.getPlanReader(), rootFragment,
        initiatingClient.getSession(), queryContext.getQueryContextInfo());

  /**
   * Generate a set of assigned fragments based on the provided fragment tree. Do not allow parallelization stages to go beyond the global max width.
   * @param foremanNode       The driving/foreman node for this query.  (this node) 本次查询的驱动节点/Foreman节点.
   * @param activeEndpoints   The list of endpoints to consider for inclusion in planning this query. 要计划本次查询, 需要考虑包括在内的计算节点
   * @param reader                  Tool used to read JSON plans 读取JSON格式的物理计划
   * @param rootFragment      The root node of the PhysicalPlan that we will be parallelizing. 物理计划的根节点(对应的Fragment), 会并行处理. 
   * @return The list of generated PlanFragment protobuf objects to be assigned out to the individual nodes.
   */
  public QueryWorkUnit getFragments(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId, Collection<DrillbitEndpoint> activeEndpoints, 
      PhysicalPlanReader reader, Fragment rootFragment, UserSession session, QueryContextInformation queryContextInfo)  {
    final PlanningSet planningSet = new PlanningSet();
    initFragmentWrappers(rootFragment, planningSet);

    final Set<Wrapper> leafFragments = constructFragmentDependencyGraph(planningSet);
    // Start parallelizing from leaf fragments
    for (Wrapper wrapper : leafFragments) {
      parallelizeFragment(wrapper, planningSet, activeEndpoints);
    }
    return generateWorkUnit(options, foremanNode, queryId, reader, rootFragment, planningSet, session, queryContextInfo);
  }
```

我们知道rootFragment只是代表了DAG图最顶上的那个Major Fragment, 在下面的迭代中,要给DAG图中的每个Major Fragment都添加到planningSet中.  

```
  // For every fragment, create a Wrapper in PlanningSet.
  public void initFragmentWrappers(Fragment rootFragment, PlanningSet planningSet) {
    planningSet.get(rootFragment);
    for(ExchangeFragmentPair fragmentPair : rootFragment) {
      initFragmentWrappers(fragmentPair.getNode(), planningSet);
    }
  }
```

下面我们再给出Fragment的迭代方法iterator.  for循环迭代的是receivingExchangePairs.  
前面分析过上一个Fragment作为接收者接收下一个Fragment发送的数据: `value.addReceiveExchange(exchange, next);`   
那么ExchangeFragmentPair的Node就是next, 即下一个Major Fragment, 然后继续递归下去.  

```
public class Fragment implements Iterable<Fragment.ExchangeFragmentPair> {
  private final List<ExchangeFragmentPair> receivingExchangePairs = Lists.newLinkedList();

  public void addReceiveExchange(Exchange e, Fragment fragment) {
    this.receivingExchangePairs.add(new ExchangeFragmentPair(e, fragment));
  }
  public Iterator<ExchangeFragmentPair> iterator() {
    return this.receivingExchangePairs.iterator();
  }
```

#### PlanningSet+Wrapper

既然用到了ReceiveExchange, 下面马上就用到了SendingExchange.  添加是在: `next.addSendExchange(exchange, value);`  
下面用的不是ExchangeFragmentPair的Fragment了, 而是Fragment的Exchange. 这里要做到的是设置MajorFragmentId.  
因为由Exchange分割的Major Fragment, 它们的ID分别是00,01,02等等.  

```
public class PlanningSet implements Iterable<Wrapper> {
  private final Map<Fragment, Wrapper> fragmentMap = Maps.newHashMap();
  private int majorFragmentIdIndex = 0;

  public Wrapper get(Fragment node) {
    Wrapper wrapper = fragmentMap.get(node);
    if (wrapper == null) {
      int majorFragmentId = 0;
      // If there is a sending exchange, we need to number other than zero.
      if (node.getSendingExchange() != null) {
        // assign the upper 16 bits as the major fragment id.
        majorFragmentId = node.getSendingExchange().getChild().getOperatorId() >> 16;
        // if they are not assigned, that means we mostly likely have an externally generated plan.  in this case, come up with a major fragmentid.
        if (majorFragmentId == 0)   majorFragmentId = majorFragmentIdIndex;
      }
      wrapper = new Wrapper(node, majorFragmentId);  // Wrapper由Fragment和major编号组成
      fragmentMap.put(node, wrapper);
      majorFragmentIdIndex++;  // 只有调用Major Fragment时, 每遇到新的Major, 索引编号+1
    }
    return wrapper;  // planningSet.get并没有用返回值做什么事情. 其实主要是放到Map中, 由迭代器访问所有的Wrapper.  
  }

  public Iterator<Wrapper> iterator() {
    return this.fragmentMap.values().iterator();
  }
```

Wrapper: `A wrapping class that allows us to add additional information to each fragment node for planning purposes`  
它的构造函数创建对象是由PlanningSet指定MajorFragment和MajorFragmentId. 它的其余属性需要在下面中设置进来.  

先来看下Exchange的并行依赖:  发送者和接收者是否相互依赖.  

```
  /**
   * Exchanges are fragment boundaries in physical operator tree. It is divided into two parts. First part is Sender
   * which becomes part of the sending fragment. Second part is Receiver which becomes part of the fragment that receives the data.
   * Exchange是物理操作符树的Fragment边界. 第一部分Sender,它是发送者Fragment的一部分, 第二部分Reciever是接收者Fragment的一部分. 
   * Assignment dependency describes whether sender fragments depend on receiver fragment's endpoint assignment for
   * determining its parallelization and endpoint assignment and vice versa.
   * 分配依赖描述了发送者Fragment是否依赖于接收者Fragment的节点分配任务, 以便于决定并行度和如何分配工作到节点上. 反过来一样.   
   */
  public enum ParallelizationDependency {
    SENDER_DEPENDS_ON_RECEIVER, // Sending fragment depends on receiving fragment for parallelization
    RECEIVER_DEPENDS_ON_SENDER, // Receiving fragment depends on sending fragment for parallelization (default value).
  }
```

根据PlanningSet构造依赖图:  

```
    final Set<Wrapper> leafFragments = constructFragmentDependencyGraph(planningSet);

  /** 根据Exchange的亲密程序分割两个fragments, 并且设置fragment的依赖关系.  
   * Based on the affinity of the Exchange that separates two fragments, setup fragment dependencies.
   * @return Returns a list of leaf fragments in fragment dependency graph. */
  private static Set<Wrapper> constructFragmentDependencyGraph(PlanningSet planningSet) {
    // Set up dependency of fragments based on the affinity of exchange that separates the fragments.
    for(Wrapper currentFragmentWrapper : planningSet) {  // PlanningSet包含了所有的Major Fragment组成的Wrapper,循环每一个Wrapper
      ExchangeFragmentPair sendingExchange = currentFragmentWrapper.getNode().getSendingExchangePair();  //每个MajorFragment要发送的目标
      if (sendingExchange != null) {  // SendingExchange不为空的, 比如next, 而不是DAG图的第一个Fragment. 因为只有next才是发送者
        ParallelizationDependency dependency = sendingExchange.getExchange().getParallelizationDependency();  // 依赖关系记录在Exchange中, 而不是Fragment中
        Wrapper receivingFragmentWrapper = planningSet.get(sendingExchange.getNode());  // 目标节点, 实际上就是接收者了
        // 根据依赖关系, 判断要加到哪个Wrapper中, 实际上是哪个Fragment中. 因为Wrapper由MajorFragment组成.  
        if (dependency == ParallelizationDependency.RECEIVER_DEPENDS_ON_SENDER) {     // Receiver依赖Sender
          receivingFragmentWrapper.addFragmentDependency(currentFragmentWrapper);   // Receiver的依赖关系图中有当前Major Fragment
        } else if (dependency == ParallelizationDependency.SENDER_DEPENDS_ON_RECEIVER) {  // Sender依赖Reciever
          currentFragmentWrapper.addFragmentDependency(receivingFragmentWrapper);   // 当前节点刚好是Sender, 所以它依赖了接收者
        }
      }
    }
    // 上面的添加Fragment依赖图, 下面的Wrapper才可以获得依赖图, 来判断是否是叶子节点.  
    // Identify leaf fragments. Leaf fragments are fragments that have no other fragments depending on them for parallelization info. 
    // First assume all fragments are leaf fragments. Go through the fragments one by one and  remove the fragment on which the current fragment depends on.
    final Set<Wrapper> roots = Sets.newHashSet();
    for(Wrapper w : planningSet) {
      roots.add(w);  // 所有的Major Fragment
    }
    for(Wrapper wrapper : planningSet) {
      final List<Wrapper> fragmentDependencies = wrapper.getFragmentDependencies();  // 每个Major Fragment的依赖图
      if (fragmentDependencies != null && fragmentDependencies.size() > 0) 
        for(Wrapper dependency : fragmentDependencies)   // 它的所有依赖者
          if (roots.contains(dependency)) 
            roots.remove(dependency);  // 从roots中移除
    } 
    return roots;  // 返回值是leaf fragments. 
  }    
```

上面的方法roots返回的是leaf fragments. 在这之前首先对每个Major Fragments都设置了依赖图. 然后把非叶子节点从所有的Major中删除.  
叶子节点的定义是: 没有依赖其他任何一个节点. 一旦一个节点有依赖某一个节点, 它就不是叶子节点了.  


获得叶子Fragment后, 对每一个叶子节点进行并行处理. 处理的时候先处理依赖的,然后才处理自己.所以也是递归的过程    

```
    // Start parallelizing from leaf fragments 从叶子节点开始并行处理
    for (Wrapper wrapper : leafFragments) {
      parallelizeFragment(wrapper, planningSet, activeEndpoints);
    }

  // Helper method for parallelizing a given fragment. Dependent fragments are parallelized first before  parallelizing the given fragment.
  private void parallelizeFragment(Wrapper fragmentWrapper, PlanningSet planningSet, Collection<DrillbitEndpoint> activeEndpoints)  {
    // First parallelize fragments on which this fragment depends on.
    final List<Wrapper> fragmentDependencies = fragmentWrapper.getFragmentDependencies();
    if (fragmentDependencies != null && fragmentDependencies.size() > 0) {
      for(Wrapper dependency : fragmentDependencies) {
        parallelizeFragment(dependency, planningSet, activeEndpoints);
      }
    }
    Fragment fragment = fragmentWrapper.getNode();

    // Step 1: Find stats. Stats include various factors including cost of physical operators, parallelizability of work in physical operator and affinity of physical operator to certain nodes.
    fragment.getRoot().accept(new StatsCollector(planningSet), fragmentWrapper);

    // Step 2: Find the parallelization width of fragment
    
    List<DrillbitEndpoint> assignedEndpoints = findEndpoints(activeEndpoints, parallelizationInfo.getEndpointAffinityMap(), fragmentWrapper.getWidth());
    fragmentWrapper.assignEndpoints(assignedEndpoints);
  }
```
找到要分配的DrillBit后,就为Fragment分配计算节点 . 一个Fragment的Sending只有最多一个,可以有多个Receiver.   

```
  public void assignEndpoints(List<DrillbitEndpoint> assignedEndpoints)  {
    endpoints.addAll(assignedEndpoints);

    // Set scan and store endpoints.
    AssignEndpointsToScanAndStore visitor = new AssignEndpointsToScanAndStore();
    node.getRoot().accept(visitor, endpoints);

    // Set the endpoints for this (one at most) sending exchange.
    if (node.getSendingExchange() != null) {
      node.getSendingExchange().setupSenders(majorFragmentId, endpoints);
    }

    // Set the endpoints for each incoming exchange within this fragment.
    for (ExchangeFragmentPair e : node.getReceivingExchangePairs()) {
      e.getExchange().setupReceivers(majorFragmentId, endpoints);
    }
  }
```

最后基于上面的工作, 生成WorkUnit, QueryWorkUnit只是封装了rootOperator,rootFragment,fragments的对象.  注意下面是个双层循环,  
外层的是对每个MajorFragment,内层则对每个MinorFragment. 如果不是根节点,则把创建的PlanFragment加入到fragments中.  
PlanFragment一个重要的对象是FragmentHandle,顾名思义是Fragment的处理类, 它只封装了Major,Minor的FragmentID,以及查询ID.  

```
  private QueryWorkUnit generateWorkUnit(OptionList options, DrillbitEndpoint foremanNode, QueryId queryId,
      PhysicalPlanReader reader, Fragment rootNode, PlanningSet planningSet, UserSession session, QueryContextInformation queryContextInfo) {
    List<PlanFragment> fragments = Lists.newArrayList();
    PlanFragment rootFragment = null;
    FragmentRoot rootOperator = null;

    // now we generate all the individual plan fragments and associated assignments. Note, we need all endpoints
    // assigned before we can materialize, so we start a new loop here rather than utilizing the previous one.
    for (Wrapper wrapper : planningSet) {
      Fragment node = wrapper.getNode();
      final PhysicalOperator physicalOperatorRoot = node.getRoot();
      boolean isRootNode = rootNode == node;

      // a fragment is self driven if it doesn't rely on any other exchanges.
      boolean isLeafFragment = node.getReceivingExchangePairs().size() == 0;

      // Create a minorFragment for each major fragment.
      for (int minorFragmentId = 0; minorFragmentId < wrapper.getWidth(); minorFragmentId++) {
        IndexedFragmentNode iNode = new IndexedFragmentNode(minorFragmentId, wrapper);
        wrapper.resetAllocation();
        PhysicalOperator op = physicalOperatorRoot.accept(Materializer.INSTANCE, iNode);
        FragmentRoot root = (FragmentRoot) op;
        FragmentHandle handle = FragmentHandle.newBuilder() //
            .setMajorFragmentId(wrapper.getMajorFragmentId()) //
            .setMinorFragmentId(minorFragmentId) //
            .setQueryId(queryId) //
            .build();
        PlanFragment fragment = PlanFragment.newBuilder() //
            .setForeman(foremanNode) //
            .setFragmentJson(reader.writeJson(root)) //
            .setHandle(handle) //
            .setAssignment(wrapper.getAssignedEndpoint(minorFragmentId)) //
            .setLeafFragment(isLeafFragment) //
            .setContext(queryContextInfo)
            .setMemInitial(wrapper.getInitialAllocation())//
            .setMemMax(wrapper.getMaxAllocation())
            .setOptionsJson(reader.writeJson(options))
            .setCredentials(session.getCredentials())
            .addAllCollector(CountRequiredFragments.getCollectors(root))
            .build();

        if (isRootNode) {
          logger.debug("Root fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          rootFragment = fragment;
          rootOperator = root;
        } else {
          logger.debug("Remote fragment:\n {}", DrillStringUtils.unescapeJava(fragment.toString()));
          fragments.add(fragment);
        }
      }
    }
    return new QueryWorkUnit(rootOperator, rootFragment, fragments);
  }
```

### 提交并执行Fragments

现在主线回到Foreman的runPhysicalPlan, 在提交Fragments执行前, 先添加了两个监听器到DrillbitContext对应的WorkBus和集群协调器.    
然后设置RootFragment和非RootFragment. 设置根节点需要QueryWorkUnit的rootFragment和rootOperator.  非根节点只需要planFragments.  

```
  private void runPhysicalPlan(final PhysicalPlan plan) throws ExecutionSetupException {
    final QueryWorkUnit work = getQueryWorkUnit(plan);
    final List<PlanFragment> planFragments = work.getFragments();
    final PlanFragment rootPlanFragment = work.getRootFragment();

    drillbitContext.getWorkBus().addFragmentStatusListener(queryId, queryManager.getFragmentStatusListener());
    drillbitContext.getClusterCoordinator().addDrillbitStatusListener(queryManager.getDrillbitStatusListener());
    logger.debug("Submitting fragments to run.");

    // set up the root fragment first so we'll have incoming buffers available.
    setupRootFragment(rootPlanFragment, work.getRootOperator());
    setupNonRootFragments(planFragments);
    drillbitContext.getAllocator().resetFragmentLimits(); // TODO a global effect for this query?!?

    moveToState(QueryState.RUNNING, null);
    logger.debug("Fragments running.");
  }
```









