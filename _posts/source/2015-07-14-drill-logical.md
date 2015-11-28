---
layout: post
title: Apache Drill源码分析之逻辑计划
category: Source
tags: BigData
keywords: 
description: 
---

## Drill源码阅读(5) : 逻辑计划是如何产生的

在前面说过, Calcite的SQL节点转换为Drill的DrillRel节点,在DefaultSqlHandler.convertToDrel会包装上一个Screen: DrillScreenRel  

```
  protected DrillRel convertToDrel(RelNode relNode, RelDataType validatedRowType) {
        // Put a non-trivial topProject to ensure the final output field name is preserved, when necessary.
        DrillRel topPreservedNameProj = addRenamedProject((DrillRel) convertedRelNode, validatedRowType);
        return new DrillScreenRel(topPreservedNameProj.getCluster(), topPreservedNameProj.getTraitSet(), topPreservedNameProj);
      }
```

那么convertToDrel中的RelNode以及更早之前的SqlNode到底是个什么样的数据结构? 为了解开这个谜题,我们要debug下drill的源码才能知晓. 

### sqlline

SQL语句: 

```
select department_id,count(*) cnt  	→ Project
from cp.`employee.json` 			→ Scan 		
group by department_id 				→ HashAgg
order by count(*) desc 				→ Sort
```

生成的物理计划:

```
00-00    Screen : rowType = RecordType(ANY department_id, BIGINT cnt): rowcount = 46.3, cumulative cost = {1023.2299999999999 rows, 10798.630541406654 cpu, 0.0 io, 0.0 network, 8889.6 memory}, id = 361
00-01      Project(department_id=[$0], cnt=[$1]) : rowType = RecordType(ANY department_id, BIGINT cnt): rowcount = 46.3, cumulative cost = {1018.5999999999999 rows, 10794.000541406655 cpu, 0.0 io, 0.0 network, 8889.6 memory}, id = 360
00-02        SelectionVectorRemover : rowType = RecordType(ANY department_id, BIGINT cnt): rowcount = 46.3, cumulative cost = {1018.5999999999999 rows, 10794.000541406655 cpu, 0.0 io, 0.0 network, 8889.6 memory}, id = 359
00-03          Sort(sort0=[$1], dir0=[DESC]) : rowType = RecordType(ANY department_id, BIGINT cnt): rowcount = 46.3, cumulative cost = {972.3 rows, 10747.700541406655 cpu, 0.0 io, 0.0 network, 8889.6 memory}, id = 358
00-04            HashAgg(group=[{0}], cnt=[COUNT()]) : rowType = RecordType(ANY department_id, BIGINT cnt): rowcount = 46.3, cumulative cost = {926.0 rows, 9723.0 cpu, 0.0 io, 0.0 network, 8148.800000000001 memory}, id = 357
00-05              Scan(groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`department_id`], files=[classpath:/employee.json]]]) : rowType = RecordType(ANY department_id): rowcount = 463.0, cumulative cost = {463.0 rows, 463.0 cpu, 0.0 io, 0.0 network, 0.0 memory}, id = 356
```

可视化的计划图:

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill21.png)

### Debug 

使用`mvn clean install -DskipTests`编译源码工程后, 在drill-jdbc的test工程下的DrillResultSetTest测试类下新建一个测试方法: 

```
  @Test
  public void testQueryCP() throws Exception{
    Connection connection = new Driver().connect( "jdbc:drill:zk=local", JdbcAssert.getDefaultProperties() );
    Statement statement = connection.createStatement();

    //SQL
    ResultSet resultSet = statement.executeQuery( "select department_id,count(*) cnt from cp.`employee.json` group by department_id order by count(*) desc" );

    //物理计划
    //ResultSet resultSet = statement.executeQuery( "explain plan for select department_id,count(*) cnt from cp.`employee.json` group by department_id order by count(*) desc" );

    //逻辑计划
    //ResultSet resultSet = statement.executeQuery( "explain plan without implementation for select department_id,count(*) cnt from cp.`employee.json` group by department_id order by count(*) desc" );
  }
```

然后在DrillSqlWorker.getPlan的`sqlNode = planner.parse(sql)`打上断点: 

**SqlNode:**

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-sqlnode.png)

这里的sqlNode是一个SqlOrderBy解析树节点.  

```
// Parse tree node that represents an "ORDER BY" on a query other than a "SELECT" (e.g. "VALUES" or "UNION").
public class SqlOrderBy extends SqlCall {
  public final SqlNode query;
  public final SqlNodeList orderList;
  public final SqlNode offset;
  public final SqlNode fetch;
```

其中query节点是SqlSelect. 一个完整的select语句有多个部分组成:  

```
// A SqlSelect is a node of a parse tree which represents a select statement.
public class SqlSelect extends SqlCall {
  SqlNodeList keywordList;
  SqlNodeList selectList;
  SqlNode from;
  SqlNode where;
  SqlNodeList groupBy;
  SqlNode having;
  SqlNodeList windowDecls;
  SqlNodeList orderBy;
  SqlNode offset;
  SqlNode fetch;
```

注意上面的sqlNode有自己的orderList即count(*) desc, 所以SqlSelect中的order by为null.  

> 注意到类名字后面跟的@数字吗, 这是一个对象的字符串值表示形式:Class@1234,  
> 其中数字还代表了创建对象的顺序. 数字越大, 则对象创建的时间越晚.  

**①RelNode:**  

RelNode是Calcite的关系表达式节点, 它比SqlNode还要更上层. 下面是rel在debug时候的value  

```
rel#13:LogicalSort.NONE.ANY([]).[1 DESC]
  (input=rel#11:LogicalAggregate.NONE.ANY([]).[]
  	(input=rel#9:LogicalProject.NONE.ANY([]).[]
  		(input=rel#4:EnumerableTableScan.ENUMERABLE.ANY([]).[]
  			(table=[cp, employee.json]),department_id=$1),group={0},cnt=COUNT()
  	),
  	sort0=$1,
  	dir0=DESC
  )
```

下图中rel的初始值是LogicalSort, 然后通过input指针, 不断地嵌套. 和下图的input嵌套一一对应.    
`LogicalSort > LogicalAggregate > LogicalProject > EnumerableTableScan`  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-RelNode.png)

> 从上面几个对象后面@代表的数字可以看出, 这几个对象, 依次创建的时间越来越晚.  

**②DrillRel drel:**

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-DrillRel.png)

CalCite的RelNode转换为Drill的DrillRel, drel的值是DrillScreenRel. 它的input也是嵌套的:  
`DrillScreenRel > DrillSortRel > DrillAggregateRel > DrillScanRel`  

虽然DrillRel drel的值是DrillScreenRel, 以及接下来的DrillRel, 它们都代表的是逻辑计划的表达式树.  

**③Prel:**

物理计划的input嵌套: `ScreenPrel >> ProjectPrel >> SelectionVectorRemoverPrel > SortPrel > HashAggPrel > ScanPrel`  
可以看到物理计划会比逻辑计划多一些节点, 并且上面的嵌套和WEBUI上的物理计划和图是能够对应上来的.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-Prel.png)

**PhysicalOperator pop:**

pop不再是input嵌套, 而是child嵌套了, 因为pop从plan过来,所以它和Prel类似  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-pop.png)

**④PhysicalPlan plan:**

最后的物理计划是一张DAG图, 即Drill Plan(注意和Drill PhysicalPlan不一样). 

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-plan.png)

图中根节点roots和叶子节点leaves. 这里的根是Screen, 即DAG图的最上面一个屏幕输出节点, 叶子只有一个, 即DAG图的最底下扫描节点.  

```
    RelNode rel = convertToRel(validated);
    rel = preprocessNode(rel);
    log("Optiq Logical", rel);					→ ①

    DrillRel drel = convertToDrel(rel, validatedRowType);
    log("Drill Logical", drel);					→ ②

    Prel prel = convertToPrel(drel);			→ ③
    log("Drill Physical", prel);				

    PhysicalOperator pop = convertToPop(prel);
    PhysicalPlan plan = convertToPlan(pop);    	→ ④
    log("Drill Plan", plan);
```

其实WebUI上的可视化Plan图就是这里的graph对象.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill23.png)

### 从DrillScreenRel入手

在 new DrillScreenRel添加前后打上断点, 验证添加Screen节点的变化:   

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-noscreen.png)

可以看到在还没有添加Screen时, convertedRelNode和topPreservedNameProj两个对象是一样的, 因为它们的对象编号都是: DrillSortRel@7614  
添加Screen后, 即DrillRel dre变成了DrillScreenRel@7665, 而其input域仍然是DrillSortRel@7614. 说明确实封装了上面的节点对象.  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-screen.png)

DrillRel drel是逻辑计划, 可以通过explain plan withou implementation for <query>得到查询的逻辑计划(和上图是对应的): 

```
+------+------+
| text | json |
+------+------+
| DrillScreenRel
  DrillSortRel(sort0=[$1], dir0=[DESC])
    DrillAggregateRel(group=[{0}], cnt=[COUNT()])
      DrillScanRel(table=[[cp, employee.json]], groupscan=[EasyGroupScan [selectionRoot=classpath:/employee.json, numFiles=1, columns=[`department_id`], files=[classpath:/employee.json]]])

  "query" : [ {
    "op" : "scan",
    "@id" : 1,
    "storageengine" : "cp",
    "selection" : {
      "format" : {
        "type" : "named",
        "name" : "json"
      },
      "files" : [ "classpath:/employee.json" ]
    }
  }, {
    "op" : "groupingaggregate",
    "@id" : 2,
    "input" : 1,
    "keys" : [ {
      "ref" : "`department_id`",
      "expr" : "`department_id`"
    } ],
    "exprs" : [ {
      "ref" : "`cnt`",
      "expr" : "count(1) "
    } ]
  }, {
    "op" : "order",
    "@id" : 3,
    "input" : 2,
    "within" : null,
    "orderings" : [ {
      "expr" : "`cnt`",
      "order" : "DESC",
      "nullDirection" : "UNSPECIFIED"
    } ]
  }, {
    "op" : "store",
    "@id" : 4,
    "input" : 3,
    "target" : null,
    "storageEngine" : "--SCREEN--"
  } ]
```

上面我们debug出来RelNode rel是LogicalSort, 其input嵌套依次是: LogicalAggregate > LogicalProject > EnumerableTableScan 
经过logicalPlanningVolcano()处理, 会将CalCite的LogicalSort操作符节点转换为Drill认识的DrillSortRel节点. 
而且topPreservedNameProj=DrillSortRel@7614, 所以创建DrillScreenRel我们可以这么看:  

```
  return new DrillScreenRel(cluster, trait, DrillSortRel@7614)

  public DrillScreenRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(DRILL_LOGICAL, cluster, traitSet, input);
  }    
```

对于DrillScreenRel构造函数而言, 输入RelNode input=DrillSortRel对象. 其实从我们debug出来的变量也是这么一回事的.  
即DrillScreenRel的input是DrillSortRel, DrillSortRel的input是DrillAggregateRel, DrillAggregateRel的input是DrillScanRel.   
`DrillScreenRel[4] > DrillSortRel[1] > DrillAggregateRel[2] > DrillScanRel[3]`  序号表示创建时间顺序.  

这里只是创建对象, 由于DrillXXXRel都实现了DrillRel, 而DrillRel定义了implement接口方法, 我们找到了DrillImplementor.  
比较重要的是入口方法是go, 它由ExplainHandler, 只有在逻辑计划的时候才会调用到(所以在debug时要用逻辑计划SQL语句才能进入DrillImplementor).  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-go.png)

```
  public static class LogicalExplain{
    public String text;
    public String json;

    public LogicalExplain(RelNode node, SqlExplainLevel level, QueryContext context) {
      this.text = RelOptUtil.toString(node, level);
      DrillImplementor implementor = new DrillImplementor(new DrillParseContext(context.getPlannerSettings()), ResultMode.LOGICAL);
      implementor.go( (DrillRel) node);
      LogicalPlan plan = implementor.getPlan();
      this.json = plan.unparse(context.getConfig());
    }
  }

  public void go(DrillRel root) {
    LogicalOperator rootLOP = root.implement(this);
    System.out.println("ROOTLOP:" + rootLOP);
    rootLOP.accept(new AddOpsVisitor(), null);
  }
```

参数root是逻辑计划节点=DrillScreenRel, 调用implement后返回的是逻辑操作符. 那么这个rootLOP是什么东东?  

![](http://7xjs7x.com1.z0.glb.clouddn.com/drill-rootLOP.png)


调用root的implement, this为DrillImplementor. 看下DrillScreenRel的implement实现:

```
DrillScreenRel:
  public LogicalOperator implement(DrillImplementor implementor) {
    LogicalOperator childOp = implementor.visitChild(this, 0, getInput());
    return Store.builder().setInput(childOp).storageEngine("--SCREEN--").build();
  }
```

其中getInput()就是创建DrillScreenRel时的最后一个参数input, 那么这里就是DrillSortRel.  

```
DrillImplementor:
  public LogicalOperator visitChild(DrillRel parent, int ordinal, RelNode child) {
    return ((DrillRel) child).implement(this);
  }
```

child就是DrillScreenRel的getInput()即DrillSortRel. 然后调用DrillSortRel的implement. 以此类推, 所以这是一个递归的过程. 

```
DrillImplementor.go
   |--root.implement-->DrillScreenRel.implement
                           |--implementor.visitChild(..,DrillSortRel)
                                  |--child.implement-->DrillSortRel.implement
                                                            |--implementor.visitChild(..,DrillAggregateRel)
                                                                   |--child.implement-->DrillAggregateRel.implement
                                                                                            |--implementor.visitChild(..,DrillScanRel)
                                                                                                  |--child.implement-->DrillScanRel.implement
                                                                                                                         |--不再visitChild了,因为Scan是叶子节点,NO-MORE-CHILD
                                                                                                                         |--implementor.registerSource(table)
                                                                                                                         |--返回Scan LogicalOperator
                                                                                            |--GroupingAggregate.setInput(implementor.visitChild(..))=GroupingAggregate.setInput(Scan) 
                                                                                            |--返回GroupingAggregate LogicalOperator 
                                                            |--Order.setInput(implementor.visitChild(..))=Order.setInput(GroupingAggregate)
                                                            |--返回Order LogicalOperator 
                            |--Store.setInput(implementor.visitChild(..))=Store.setInput(Order) [①]
    |--rootLOP.accept-->Store.accept
    						|--planBuilder.addLogicalOperator(Store)
                            |--for o : Store.getInput=Order
                                |--o : = Order.accept
                                              |--planBuilder.addLogicalOperator(Order)
                                              |--for o : Order.getInput=GroupingAggregate 
                                                  |--o : GroupingAggregate.accept
                                                             |--planBuilder.addLogicalOperator(GroupingAggregate)
                                                             |--for o : GroupingAggregate.getInput=Scan
                                                                 |--o : Scan.accept
                                                                            |--planBuilder.addLogicalOperator(Scan)
                                                                            |--Scan has not inputs. END

  	|--END OF GO
```

DONE with planBuilder which is builder of logical plan.  
调用完DrillImplementor.go, 就把planBuilder构造完毕, 接着调用getPlan就可以获得planBuilder的输出了.  







