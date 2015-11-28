---
layout: post
---

<http://cascading.io/>   
<http://www.cascading.org/documentation/>  
<http://docs.cascading.org/impatient/>  

## 1. 安装Driven

<http://docs.cascading.io/driven/1.1/getting-started/index.html> 
登陆driver web页面: <https://driven.cascading.io/index.html#/apps> 注册username/password: z@gmail.com/z

```
$ wget -i http://eap.concurrentinc.com/driven/1.1/driven-plugin/latest-shaded-jar.txt 
```

登陆 http://driven.cascading.io/index.html#/myTeams 复制ApiKey

```
$ vi $HADOOP_HOME/etc/hadoop/cascading-service.properties
cascading.management.document.service.apikey=1C756DAA814A40228B0142BDA3FBD4BE
cascading.management.service.jar=/home/hadoop/bin/driven-plugin-1.1.1-eap-13-shaded.jar
cascading.management.document.service.hosts=https://driven.cascading.io
```

注意: jar的路径必须是绝对路径, 如果写成~/bin则在运行时告警: invalid URL value, 导致无法加载driven-plugin

### Cascading Impatient Demo

```
hadoop@hadoop:~/github-example$ git clone https://github.com/Cascading/Impatient 
hadoop@hadoop:~/github-example/Impatient$ cd Impatient/part1
hadoop@hadoop:~/github-example/Impatient/part1$ gradle clean jar
:part1:clean
:part1:compileJava
:part1:processResources UP-TO-DATE
:part1:classes
:part1:jar
BUILD SUCCESSFUL
Total time: 9.155 secs

hadoop@hadoop:~/github-example/Impatient/part1$ hadoop jar ./build/libs/impatient.jar data/rain.txt output/rain
```

1) 使用jdk1.8会报错:Exception in thread "main" java.lang.UnsupportedClassVersionError: impatient/Main : Unsupported major.minor version 52.0
2) 输入输出参数必须是hdfs上的, 否则会报错:  

```
Caused by: cascading.tap.TapException: unable to read fields from tap: 
Hfs["TextLine[['line']->[ALL]]"]["hdfs://localhost:9000/user/hadoop/data/rain.txt"], does not exist
```

3) hdfs文件, 以及在hdfs上查看输出结果

```
$ hadoop fs -mkdir /input/cascading/impatient
$ hadoop fs -mkdir /output/cascading
hadoop@hadoop:~/github-example/Impatient/part1$ hadoop fs -put data/rain.txt /input/cascading/impatient
hadoop@hadoop:~/github-example/Impatient/part1$ hadoop jar ./build/libs/impatient.jar \
/input/cascading/impatient/rain.txt /output/cascading/rain
hadoop@hadoop:~$ hadoop fs -ls /output/cascading/rain
Found 3 items
-rw-r--r--   3 hadoop supergroup          0 2015-01-22 17:44 /output/cascading/rain/_SUCCESS
-rw-r--r--   3 hadoop supergroup        308 2015-01-22 17:44 /output/cascading/rain/part-00000
-rw-r--r--   3 hadoop supergroup        214 2015-01-22 17:44 /output/cascading/rain/part-00001
hadoop@hadoop:~$ hadoop fs -cat /output/cascading/rain/part-*
doc_id	text
doc01	A rain shadow is a dry area on the lee back side of a mountainous area.
doc02	This sinking, dry air produces a rain shadow, or area in the lee of a mountain with less rain and cloudcover.
doc03	A rain shadow is an area of dry land that lies on the leeward (or downwind) side of a mountain.
doc_id	text
doc04	This is known as the rain shadow effect and is the primary cause of leeward deserts of mountain ranges, such as California's Death Valley.
doc05	Two Women. Secrets. A Broken Land. [DVD Australia]
```

如果没有成功配置cascading-service.properties, 则只会在本地运行cascading: 

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-1.png)

如果成功加载cascading-service.properties运行日志如下:

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-2.png)

本机Hadoop集群的Web页面查看作业: <http://localhost:8088/cluster>　

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-3.png)

### Cascading Driven Web UI

在Cascading Driven页面查看作业: <http://driven.cascading.io/index.html#/apps?name=impatient>

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-4.png)

作业详细信息: <https://driven.cascading.io/index.html#/apps/97703E817FE74A69BC98634F923CB1D8>

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-5.png)

点击绿色的显示某个任务: 

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-6.png)

### Question : Cascading Dependency

IDEA运行Impatient.part1的Main类报错：

```
WARN - server version: '1.2-eap-5' does not match plugin version: '1.1.1-eap-13'
Exception in thread "main" cascading.flow.FlowException: unhandled exception
	at cascading.flow.BaseFlow.complete(BaseFlow.java:918)
	at com.zqh.cascading.impatient.Copy.main(Copy.java:66)
	at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
	at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.lang.reflect.Method.invoke(Method.java:606)
	at com.intellij.rt.execution.application.AppMain.main(AppMain.java:134)
Caused by: java.lang.NoSuchMethodError: org.apache.hadoop.mapred.LocalJobRunner.<init>(Lorg/apache/hadoop/conf/Configuration;)V
	at org.apache.hadoop.mapred.LocalClientProtocolProvider.create(LocalClientProtocolProvider.java:42)
	at org.apache.hadoop.mapreduce.Cluster.initialize(Cluster.java:95)
	at org.apache.hadoop.mapreduce.Cluster.<init>(Cluster.java:82)
	at org.apache.hadoop.mapreduce.Cluster.<init>(Cluster.java:75)
	at org.apache.hadoop.mapred.JobClient.init(JobClient.java:472)
	at org.apache.hadoop.mapred.JobClient.<init>(JobClient.java:450)
	at cascading.flow.hadoop.planner.HadoopFlowStepJob.internalNonBlockingStart(HadoopFlowStepJob.java:107)
	at cascading.flow.planner.FlowStepJob.blockOnJob(FlowStepJob.java:207)
	at cascading.flow.planner.FlowStepJob.start(FlowStepJob.java:150)
	at cascading.flow.planner.FlowStepJob.call(FlowStepJob.java:124)
	at cascading.flow.planner.FlowStepJob.call(FlowStepJob.java:43)
```

查看<http://www.cascading.org/support/compatibility/> 实际上是支持2.5.0的. 应该也是支持2.5.0-cdh5.2.0  
解决办法是: 单独新建一个Maven项目就可以了. hadoop的依赖只需要2个就可以:

```
<dependency>    
  <groupId>org.apache.hadoop</groupId>      
  <artifactId>hadoop-client</artifactId>    
  <version>2.5.0</version>
</dependency>
<dependency>    
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-common</artifactId>    
  <version>2.5.0</version>
</dependency>
```


### WordCount

<http://docs.cascading.org/impatient/impatient2.html> 

流程图: 

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-7.png)

下图是Driven和Hadoop UI的对比. Cascading的Slices对应了Hadoop的Tasks

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-8.png)

运行完成:

![](http://7xjs7x.com1.z0.glb.clouddn.com/cascading-9.png)

查看Cascading版本的WordCount的结果

```
hadoop@hadoop:~/github-example/Impatient/part1$ hadoop fs -cat /output/cascading/rain/part*
token	count
	9
A	3
Australia	1
Broken	1
California's	1
DVD	1
Death	1
Land	1
Secrets	1
This	2
Two	1
Valley	1
....
```


## TODO

Scading
