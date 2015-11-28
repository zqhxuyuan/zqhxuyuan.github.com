---
layout: post
---

## Build Tez

1) 编译protobuffer

```
$ tar zxf protobuf-2.5.0.tar.gz
$ cd protobuf-2.5.0
$ ./configure && make && sudo make install
$ protoc --version
libprotoc 2.5.0
```

2) 修改nodejs和npm的版本

```
hadoop@hadoop:~$ node --version
v0.10.33
hadoop@hadoop:~$ npm --version
1.4.28
```

由于本机已经安装了node.js和npm, 为了和版本一致, 修改tez-ui的pom.xml. 试过注释掉<!--NPM Install-->那段代码, 但是会报错. 

```
  <properties>
    <webappDir>src/main/webapp</webappDir>
    <!-- /usr/local/bin/node -->
    <node.executable>${basedir}/src/main/webapp/node/node</node.executable>
    <fileName>${artifactId}-${parent.version}</fileName>
    <nodeVersion>v0.10.33</nodeVersion> <!--v0.10.18  1.3.8-->
    <npmVersion>1.4.28</npmVersion>
  </properties>
```

3) 编译tez

```
hadoop@hadoop:~/github/apache/tez$ mvn clean package -DskipTests=true -Dmaven.javadoc.skip=true
```

![c-1](http://7xjs7x.com1.z0.glb.clouddn.com/tez-1.png)

hadoop@hadoop:~/github/apache/tez/tez-ui/src/main/webapp$ **vi package.json**  
添加"zlib-browserify": "0.0.3",

4) 最后编译成功

```
[INFO] Reactor Summary:
[INFO] tez ............................................... SUCCESS [0.704s]
[INFO] tez-api ........................................... SUCCESS [5.669s]
[INFO] tez-common ........................................ SUCCESS [0.069s]
[INFO] tez-runtime-internals ............................. SUCCESS [0.569s]
[INFO] tez-runtime-library ............................... SUCCESS [1.520s]
[INFO] tez-mapreduce ..................................... SUCCESS [0.812s]
[INFO] tez-examples ...................................... SUCCESS [0.069s]
[INFO] tez-dag ........................................... SUCCESS [3.070s]
[INFO] tez-tests ......................................... SUCCESS [0.130s]
[INFO] tez-ui ............................................ SUCCESS [21.313s]
[INFO] tez-plugins ....................................... SUCCESS [0.016s]
[INFO] tez-yarn-timeline-history-with-acls ............... SUCCESS [0.374s]
[INFO] tez-mbeans-resource-calculator .................... SUCCESS [0.169s]
[INFO] tez-dist .......................................... SUCCESS [12.529s]
[INFO] Tez ............................................... SUCCESS [0.012s]
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 47.316s
[INFO] Finished at: Thu Jan 15 08:41:58 CST 2015
[INFO] Final Memory: 65M/948M
[INFO] ------------------------------------------------------------------------
```

## Tez Intro

![c-1](http://7xjs7x.com1.z0.glb.clouddn.com/tez-2.png)

TODO

## Tez on YARN

<http://blog.woopi.org/wordpress/?p=96>   
<http://hadooptutorial.info/apache-tez-successor-mapreduce-framework/>  
<http://blog.csdn.net/teddeyang/article/details/19564603>   

```
$ cd /home/hadoop/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT
$ mkdir conf 
$ hadoop fs -mkdir /apps
$ hadoop fs -mkdir /apps/tez
$ hadoop fs -put /home/hadoop/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT /apps/tez
```

**$ vi conf/tez-site.xml**

```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>tez.version</name>
    <value>tez-0.7.0-SNAPSHOT</value>
  </property>
  <property>
    <name>tez.lib.uris</name>
    <value>${fs.default.name}/apps/tez/${tez.version},${fs.default.name}/apps/tez/${tez.version}/lib/</value>
  </property>
</configuration>
```

**$ vi ~/.bashrc**

```
export TEZ_HOME=/home/hadoop/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:${TEZ_HOME}/conf:${TEZ_HOME}/*:${TEZ_HOME}/lib/*
```

另一种方案是复制tez-site.xml到$HADOOP_HOME/etc/hadoop下, 并修改hadoop-env.sh文件.   
PS: 这种侵入性大, 不建议. 建议使用上面环境变量的方式.     

**vi ${HADOOP_INSTALL}/etc/hadoop/hadoop-env.sh**

```
export HADOOP_CLASSPATH=$HADOOP_HOME:$HADOOP_HOME/etc/hadoop
for f in /home/hadoop/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done
for f in /home/hadoop/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT/lib/*.jar; do
  if [ "$HADOOP_CLASSPATH" ]; then
    export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$f
  else
    export HADOOP_CLASSPATH=$f
  fi
done
```

可选步骤:

```
$ vi ${HADOOP_HOME}/etc/hadoop/mapred-site.xml
  <property>
    <name>mapreduce.framework.name</name>
    <value>yarn-tez</value>
  </property>
```

## Tez WorldCount

```
$ stop-yarn.sh
$ start-yarn.sh
$ cd ~/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT
$ hadoop fs -put ~/data/helloworld.txt /input/tez
$ hadoop jar tez-examples-0.7.0-SNAPSHOT.jar orderedwordcount \
/input/tez/helloworld.txt /output/tez/helloworld
```

可以看到控制台中Tez会将作业分成多个DAG

![c-1](http://7xjs7x.com1.z0.glb.clouddn.com/tez-3.png)

```
hadoop@hadoop:~/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT$ hadoop fs -ls /output/tez/helloworld
-rw-r--r--   3 hadoop supergroup          0 2015-01-20 19:33 /output/tez/helloworld/_SUCCESS
-rw-r--r--   3 hadoop supergroup        130 2015-01-20 19:33 /output/tez/helloworld/part-v002-o000-00000
hadoop@hadoop:~/github/apache/tez/tez-dist/target/tez-0.7.0-SNAPSHOT$ hadoop fs -cat /output/tez/helloworld/part-v002-o000-00000
again	1
system.	1
Spark	1
system,	1
process	1
Now	1
hot	1
today	1
batch	1
also	1
..	2
bigdata	2
a	2
Hadoop	2
Hello	3
world	3
is	3
```

可以看到结果安装word的count进行升序排列, 因为作业的名称是orderwordcount.  
在<http://localhost:8088/cluster>看到作业的类型是TEZ,不再是MAPREDUCE了 

![c-1](http://7xjs7x.com1.z0.glb.clouddn.com/tez-4.png)

Tez还可以指定多个输入路径和多个输出路径. 这如果要用MapReduce来做得分成两次执行了.  
**-DUSE_TEZ_SESSION=true**表示重用Tez会话

```
$ hadoop jar tez-tests-0.7.0-SNAPSHOT.jar testorderedwordcount -DUSE_TEZ_SESSION=true \
/input/tez/helloworld.txt /output/tez/helloworld2 \
/input/tez/helloworld2.txt /output/tez/helloworld3
```

```
15/01/20 20:01:04 INFO examples.TestOrderedWordCount: Creating Tez Session
15/01/20 20:01:04 INFO client.TezClient: Tez Client Version: [ component=tez-api, version=0.7.0-SNAPSHOT, revision=83261659809f7904b786c9c81def4451dca27078, SCM-URL=scm:git:https://git-wip-us.apache.org/repos/asf/tez.git, buildTime=20150120-1554 ]
15/01/20 20:01:04 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
15/01/20 20:01:04 INFO client.TezClient: Session mode. Starting session.
15/01/20 20:01:04 INFO Configuration.deprecation: fs.default.name is deprecated. Instead, use fs.defaultFS
15/01/20 20:01:04 INFO client.TezClientUtils: Using tez.lib.uris value from configuration: 
hdfs://localhost:9000/apps/tez/tez-0.7.0-SNAPSHOT,hdfs://localhost:9000/apps/tez/tez-0.7.0-SNAPSHOT/lib/
15/01/20 20:01:04 INFO client.TezClient: Tez system stage directory 
hdfs://localhost:9000/tmp/hadoop/tez/staging/1421755263939/.tez/application_1421753603786_0002 doesn't exist and is created
15/01/20 20:01:04 INFO impl.YarnClientImpl: Submitted application application_1421753603786_0002
15/01/20 20:01:04 INFO client.TezClient: The url to track the Tez Session: http://localhost:8088/proxy/application_1421753603786_0002/
15/01/20 20:01:04 INFO examples.TestOrderedWordCount: Running OrderedWordCount DAG, 
dagIndex=1, inputPath=/input/tez/helloworld.txt, outputPath=/output/tez/helloworld2
15/01/20 20:01:05 INFO examples.TestOrderedWordCount: Checking DAG specific ACLS
15/01/20 20:01:05 INFO examples.TestOrderedWordCount: Waiting for TezSession to get into ready state
15/01/20 20:01:08 INFO examples.TestOrderedWordCount: Submitting DAG to Tez Session, dagIndex=1
15/01/20 20:01:08 INFO client.TezClient: Submitting dag to TezSession, 
sessionName=OrderedWordCountSession, applicationId=application_1421753603786_0002, dagName=OrderedWordCount1
15/01/20 20:01:08 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
15/01/20 20:01:08 INFO examples.TestOrderedWordCount: Submitted DAG to Tez Session, dagIndex=1	
15/01/20 20:01:15 INFO examples.TestOrderedWordCount: DAG 1 completed. FinalState=SUCCEEDED
examples.TestOrderedWordCount: Running OrderedWordCount DAG, dagIndex=2, inputPath=/input/tez/helloworld2.txt, outputPath=/output/tez/helloworld3
15/01/20 20:01:15 INFO examples.TestOrderedWordCount: Checking DAG specific ACLS
15/01/20 20:01:15 INFO examples.TestOrderedWordCount: Waiting for TezSession to get into ready state
15/01/20 20:01:15 INFO examples.TestOrderedWordCount: Submitting DAG to Tez Session, dagIndex=2
15/01/20 20:01:15 INFO client.TezClient: Submitting dag to TezSession, 
sessionName=OrderedWordCountSession, applicationId=application_1421753603786_0002, dagName=OrderedWordCount2
15/01/20 20:01:15 INFO client.RMProxy: Connecting to ResourceManager at localhost/127.0.0.1:8032
15/01/20 20:01:15 INFO examples.TestOrderedWordCount: Submitted DAG to Tez Session, dagIndex=2
15/01/20 20:01:16 INFO examples.TestOrderedWordCount: DAG 2 completed. FinalState=SUCCEEDED
15/01/20 20:01:16 INFO examples.TestOrderedWordCount: Shutting down session
client.TezClient: Shutting down Tez Session, sessionName=OrderedWordCountSession, applicationId=application_1421753603786_0002
```

查看yarn web

![c-1](http://7xjs7x.com1.z0.glb.clouddn.com/tez-5.png)


## Reference


## Example
<https://github.com/t3rmin4t0r/tez-broadcast-example>  
<https://github.com/olegz/tez-samples> 

