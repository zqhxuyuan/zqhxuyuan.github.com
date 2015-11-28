---
layout: post
---

> Apache Crunch是FlumeJava的实现,为不太方便直接开发和使用的MapReduce程序,开发一套MR流水线,具备数据表示模型,提供基础和高级原语,根据底层执行引擎对MR Job的执行进行优化.从分布式计算角度看,Crunch提供的许多计算原语,可以在Spark,Hive,Pig等地方找到很多相似之处,而本身的数据读写,序列化处理,分组,排序,聚合的实现,类似MapReduce各阶段的拆分都可以在Hadoop里找到影子.

![c-1](https://raw.githubusercontent.com/zqhxuyuan/zqhxuyuan.github.com/master/_posts/img/crunch-1.png)

### maven编译安装

```
git clone https://github.com/apache/crunch.git 
cd crunch
mvn clean install -DskipTests -Dcrunch.platform=2
```

### 运行测试例子

```
hadoop jar crunch-examples/target/crunch-examples-*-job.jar org.apache.crunch.examples.WordCount \
hdfs://localhost:9000/input_helloworld output/crunch/wordcount
```

或者使用cdh编译好的包:

```
hadoop@hadoop:~/soft/cdh5.2.0/crunch-0.11.0-cdh5.2.0$ hadoop jar crunch-examples-0.11.0-cdh5.2.0-job.jar \
org.apache.crunch.examples.WordCount hdfs://localhost:9000/input_helloworld  output/crunch/wordcount2
```

![c-2](https://raw.githubusercontent.com/zqhxuyuan/zqhxuyuan.github.com/master/_posts/img/crunch-2.png)

查看wordcount的输出结果: hadoop fs -cat output/crunch/wordcount/part-r-0000

![c-3](https://raw.githubusercontent.com/zqhxuyuan/zqhxuyuan.github.com/master/_posts/img/crunch-3.png)

### IDEA运行WordCount示例

修改args参数的input, output都为hdfs的绝对路径. 

![c-1](https://raw.githubusercontent.com/zqhxuyuan/zqhxuyuan.github.com/master/_posts/img/crunch-4.png)

<http://blog.csdn.net/pelick/article/details/38359361>  
<http://puffsun.iteye.com/category/287172>  
<http://blog.cloudera.com/blog/2011/10/introducing-crunch/>  
<http://blog.cloudera.com/blog/2014/05/how-to-process-time-series-data-using-apache-crunch/>  
<http://blog.cloudera.com/blog/2014/07/how-to-build-advanced-time-series-pipelines-in-apache-crunch/>   

### How-to: tradesequence

![c-1](https://raw.githubusercontent.com/zqhxuyuan/zqhxuyuan.github.com/master/_posts/img/crunch-5.png)

1. 使用avro-tools生成avro文件testdata.avro

```
java -jar ~/soft/cdh5.2.0/avro-1.7.6-cdh5.2.0/dist/java/avro-tools-1.7.6-cdh5.2.0.jar fromjson --schema-file trade_raw.avsc testdata.json > testdata.avro
```

数据源是json文件, 通过trade_raw.avsc映射, 序列化为testdata.avro文件

```
hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ vi testdata.json
{"stock_symbol":"GOOG", "trade_time":40, "trade_price":100.0}
{"stock_symbol":"MSFT", "trade_time":20, "trade_price":34.1}
{"stock_symbol":"AAPL", "trade_time":90, "trade_price":534.97}
{"stock_symbol":"AAPL", "trade_time":30, "trade_price":440.40}
{"stock_symbol":"MSFT", "trade_time":70, "trade_price":34.2}
{"stock_symbol":"GOOG", "trade_time":10, "trade_price":95.0}
{"stock_symbol":"MSFT", "trade_time":60, "trade_price":34.3}
{"stock_symbol":"AAPL", "trade_time":50, "trade_price":480.08}
{"stock_symbol":"GOOG", "trade_time":80, "trade_price":105.0}

hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ vi trade_raw.avsc 
{
  "name": "trade_raw , 
  "namespace": "com.zqh.crunch", 
  "type": "record", 
  "fields": [
    {"name": "stock_symbol", "type": "string"}, 
    {"name": "trade_time", "type": "long"}, 
    {"name": "trade_price", "type": "double"}
  ]
}

hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ strings testdata.avro 
avro.schema
{"type":"record","name":"trade_raw","namespace":"com.zqh.crunch","fields":[{"name":"stock_symbol","type":"string"},{"name":"trade_time","type":"long"},{"name":"trade_price","type":"double"}]}
avro.codec
null
l4~ni
GOOGP
MSFT(
AAPL
AAPL<fffff
MSFT
GOOG
MSFTxfffff&A@
AAPLd
GOOG
@Z@f
l4~ni
```

2. 上传testdata.avro到hdfs上

```
hadoop fs -mkdir /input/
hadoop fs -mkdir /input/avro
hadoop fs -put testdata.avro /input/avro
```

3. input参数指定hdfs上的testdata.avro的路径: hdfs://localhost:9000/input/avro/testdata.avro
4. 运行SequenceDeriver

5. 下载output生成的avro文件, 在本地查看

```
$ hadoop fs -get /output/avro/tradeseq/part-r-00000.avro
hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ strings part-r-00000.avro 
avro.schema
{"type":"record","name":"trade","namespace":"com.zqh.crunch","fields":[{"name":"stock_symbol","type":"string"},{"name":"trade_time","type":"long"},{"name":"trade_price","type":"double"},{"name":"sequence_num","type":["null","int"],"default":null}]}
AAPL<fffff
AAPLd
AAPL
GOOG
GOOGP
GOOG
MSFT(
MSFTxfffff&A@
MSFT

hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ ll
-rw-rw-r-- 1 hadoop hadoop  109  1月 14 14:45 create_test_avro.sh
-rw-r--r-- 1 hadoop hadoop  450  1月 14 16:21 part-r-00000.avro
-rw-rw-r-- 1 hadoop hadoop  391  1月 14 15:30 testdata.avro
-rw-rw-r-- 1 hadoop hadoop  557  1月 14 14:45 testdata.json
-rw-rw-r-- 1 hadoop hadoop  323  1月 14 14:48 trade.avsc
-rw-rw-r-- 1 hadoop hadoop  253  1月 14 15:30 trade_raw.avsc
```

6. 使用avro-tools的tojson反序列化avro文件

```
hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ cp \
~/soft/cdh5.2.0/avro-1.7.6-cdh5.2.0/dist/java/avro-tools-1.7.6-cdh5.2.0.jar ~/bin/
hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ java -jar ~/bin/avro-tools-1.7.6-cdh5.2.0.jar \
tojson part-r-00000.avro > testdata2.json
hadoop@hadoop:~/IdeaProjects/go-bigdata/helloworld/data/tradeseq$ cat testdata2.json 
{"stock_symbol":"AAPL","trade_time":30,"trade_price":440.4,"sequence_num":{"int":1}}
{"stock_symbol":"AAPL","trade_time":50,"trade_price":480.08,"sequence_num":{"int":2}}
{"stock_symbol":"AAPL","trade_time":90,"trade_price":534.97,"sequence_num":{"int":3}}
{"stock_symbol":"GOOG","trade_time":10,"trade_price":95.0,"sequence_num":{"int":1}}
{"stock_symbol":"GOOG","trade_time":40,"trade_price":100.0,"sequence_num":{"int":2}}
{"stock_symbol":"GOOG","trade_time":80,"trade_price":105.0,"sequence_num":{"int":3}}
{"stock_symbol":"MSFT","trade_time":20,"trade_price":34.1,"sequence_num":{"int":1}}
{"stock_symbol":"MSFT","trade_time":60,"trade_price":34.3,"sequence_num":{"int":2}}
{"stock_symbol":"MSFT","trade_time":70,"trade_price":34.2,"sequence_num":{"int":3}}
```

可以看出新json文件按照stock_symbol进行分组, 并以trade_time进行排序: 这样就完成了时间序列分析. 
