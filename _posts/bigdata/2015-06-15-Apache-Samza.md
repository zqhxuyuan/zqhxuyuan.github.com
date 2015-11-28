---
layout: post
---

## Hello Samza

由于下面第二行启动一个测试环境会下载hadoop,所以第一步手动指定软链接

```
$ ln -s /home/hadoop/install/bigdata/hadoop/hadoop-2.4.0.tar.gz /home/hadoop/.samza/download
$ bin/grid bootstrap
```

下图中可以看到还是会去下载kafka,注意到测试环境的EXECUTING: start ...帮我们启动了zookeeper,yarn,kafka

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-1.png)

把hello-samza项目部署到samza环境中

```
$ mvn clean package
$ mkdir -p deploy/samza
$ tar -xvf ./target/hello-samza-0.8.0-dist.tar.gz -C deploy/samza
```
### Run a Samza Job

After you've built your Samza package, you can start a job on the grid using the run-job.sh script.  
运行一个Samza作业类似于运行一个hadoop jar WordCount

```
$ deploy/samza/bin/run-job.sh \
--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
--config-path=file://$PWD/deploy/samza/config/wikipedia-feed.properties
```

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-2.png)

在Hadoop的web ui上可以看到此时作业的类型是Samza,但此次作业的状态是Accepted,还没开始运行

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-3.png)

查看进程, Samza会启动ZooKeeper,Kafka,以及基于Yarn的SamzaAppMaster和SamzaContainer    
注意在还没有提交作业时是没有两个Samza进程的.    
类似于提交一个Hadoop YARN JOB之后才有ApplicationMaster和Container进程

```
$ jps -m
11596 ResourceManager
26019 SamzaAppMaster
11533 QuorumPeerMain /home/hadoop/github-example/apache_hello-samza/deploy/zookeeper/bin/../conf/zoo.cfg
26148 SamzaContainer
11710 Kafka config/server.properties
11662 NodeManager
```

The job will consume a feed of real-time edits from Wikipedia, and produce them to a Kafka topic called “wikipedia-raw”.   
Give the job a minute to startup, and then tail the Kafka topic:  
上面的作业会读取wikipedia的实时数据流, 将它们喂给kafka的topic. 接着你就可以从kafka中读取这些实时消息流  

启动一个Kafka消费进程,从指定的topic中消费消息

```
$ deploy/kafka/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic wikipedia-raw
```

在kafka消费者的控制台上可以看到不停地有新数据进来

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-4.png)

此时Hadoop web ui上samza作业的状态为Running

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-5.png)


为了验证控制台打印出的是实时数据, 你可以打开控制台中的某个链接. 并和系统的时间对比(时区相差8小时) → Revision as of时间

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-6.png)

再启动两个作业,分别是解析wiki和统计wiki状态,指定了不同的配置文件路径

```
$ deploy/samza/bin/run-job.sh \
--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
--config-path=file://$PWD/deploy/samza/config/wikipedia-parser.properties
$ deploy/samza/bin/run-job.sh \
--config-factory=org.apache.samza.config.factories.PropertiesConfigFactory \
--config-path=file://$PWD/deploy/samza/config/wikipedia-stats.properties
```
下面是三个consumer同时运行的截图:

```
$ deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-raw
$ deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-edits
$ deploy/kafka/bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wikipedia-stats
```

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-7.png)


### KafkaOffsetMonitor

```
java -cp KafkaOffsetMonitor-assembly-0.2.0.jar com.quantifind.kafka.offsetapp.OffsetGetterWeb \
--zk localhost:2181 --port 8081 --refresh 10.seconds --retain 2.days
```
<http://localhost:8081/#/activetopicsviz> 可以在TopicList中查看所有的topic

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-8.png)

点击wikipedia-edits topic, 可以查看wikipedia-edits的实时数据图, 其他topic类似

![s](http://7xjs7x.com1.z0.glb.clouddn.com/samza-9.png)


## TODO



