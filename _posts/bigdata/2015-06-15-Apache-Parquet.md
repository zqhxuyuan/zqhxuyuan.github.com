---
layout: post
---

### Create Hive Table with Parquet format

创建hive表, 包括复杂类型. 存储格式为parquet

```
hive> CREATE TABLE alltypesparquet (
c1 int,
c2 boolean,
c3 double,
c4 string,
c5 array<int>,
c6 map<int,string>,
c7 map<string,string>,
c8 struct<r:string,s:int,t:double>,
c9 tinyint,
c10 smallint,
c11 float,
c12 bigint,
c13 array<array<string>> ,
c15 struct<r:int,s:struct<a:int,b:string>> ,
c16 array<struct<m:map<string,string>,n:int>>
) STORED AS PARQUET
```

查看表结构

```
hive> desc alltypesparquet;
OK
c1                  	int
c2                  	boolean
c3                  	double
c4                  	string
c5                  	array<int>
c6                  	map<int,string>
c7                  	map<string,string>
c8                  	struct<r:string,s:int,t:double>
c9                  	tinyint
c10                 	smallint
c11                 	float
c12                 	bigint
c13                 	array<array<string>>
c15                 	struct<r:int,s:struct<a:int,b:string>>
c16                 	array<struct<m:map<string,string>,n:int>>
```

加载本地数据到表中. 数据来源于: hive_alltypes.parquet: <https://issues.apache.org/jira/browse/DRILL-2005>

```
hive> LOAD DATA LOCAL INPATH '/home/qihuang.zheng/hive_alltypes.parquet' OVERWRITE INTO TABLE alltypesparquet;
hive> select * from alltypesparquet;
1 true  1.1 1 [1,2] {1:"x",2:"y"} {"k":"v"} {"r":"a","s":9,"t":2.2} 1 1 1.0 1 [["a","b"],["c","d"]] {"r":1,"s":{"a":2,"b":"x"}} [{"m":null,"n":1},{"m":{"a":"b","c":"d"},"n":2}]
hive> select c6 from alltypesparquet;
{1:"x",2:"y"}
```

### Drill query Parquet

注意上面c6,c7作为Map的输出. 在hive中是$key:$value. $表示结果.  但是在Drill中的输出:  

```
0: jdbc:drill:zk=local> select * from  dfs.`/home/qihuang.zheng/hive_alltypes.parquet`;
+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+
| c1 | c2 | c3 | c4 | c5 | c6 | c7 | c8 | c9 | c10 | c11 | c12 | c13 | c15 | c16 |
+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+
| 1 | true | 1.1 | [B@282fd12a | {"bag":[{"array_element":1},{"array_element":2}]} | {"map":[{"key":1,"value":"eA=="},{"key":2,"value":"eQ=="}]} | {"map":[{"key":"aw==","value":"dg=="}]} | {"r":"YQ==","s":9,"t":2.2} | 1 | 1 | 1.0 | 1 | {"bag":[{"array_element":{"bag":[{"array_element":"YQ=="},{"array_element":"Yg=="}]}},{"array_element":{"bag":[{"array_element":"Yw=="},{"array_element":"ZA=="}]}}]} | {"r":1,"s":{"a":2,"b":"eA=="}} | {"bag":[{"array_element":{"m":{"map":[]},"n":1}},{"array_element":{"m":{"map":[{"key":"YQ==","value":"Yg=="},{"key":"Yw==","value":"ZA=="}]},"n":2}}]} |
+----+----+----+----+----+----+----+----+----+-----+-----+-----+-----+-----+-----+
```

它的Map输出: `{"map":[{"key":1,"value":"eA=="},{"key":2,"value":"eQ=="}]}` 而不是hive的: `{1:"x",2:"y"}`.  
不仅仅结构发生了变化, 连输出的x,y都变成了eA==,eQ==. 显然Drill在处理Parquet文件时有点问题.  
1.map的值是一个数组了,因此不能使用t.c6.key,而应该使用t.c6[0].key. 但是如果要所有的列呢?  
2.用flatten结构,但是最终会将一行变成多行.  

### Parquet-Tools

使用parquet自带的工具的schema命令, 可以直接查看parquet文件的结构:  

```
[qihuang.zheng@dp0653 ~] tar zxf parquet-1.5.0-cdh5.4.2.tar.gz && cd arquet-1.5.0-cdh5.4.2/parquet-tools/target && tar zxf parquet-tools-1.5.0-cdh5.4.2.tar.gz && cd parquet-tools-1.5.0-cdh5.4.2
[qihuang.zheng@dp0653 parquet-tools-1.5.0-cdh5.4.2]$ ./parquet-tools schema /home/qihuang.zheng/hive_alltypes.parquet
message hive_schema {
  optional int32 c1;
  optional boolean c2;
  optional double c3;
  optional binary c4;
  optional group c5 (LIST) {
    repeated group bag {
      optional int32 array_element;
    }
  }
  optional group c6 (MAP) {
    repeated group map (MAP_KEY_VALUE) {
      required int32 key;
      optional binary value;
    }
  }
  optional group c7 (MAP) {
    repeated group map (MAP_KEY_VALUE) {
      required binary key;
      optional binary value;
    }
  }
  optional group c8 {
    optional binary r;
    optional int32 s;
    optional double t;
  }
  optional int32 c9;
  optional int32 c10;
  optional float c11;
  optional int64 c12;
  optional group c13 (LIST) {
    repeated group bag {
      optional group array_element (LIST) {
        repeated group bag {
          optional binary array_element;
        }
      }
    }
  }
  optional group c15 {
    optional int32 r;
    optional group s {
      optional int32 a;
      optional binary b;
    }
  }
  optional group c16 (LIST) {
    repeated group bag {
      optional group array_element {
        optional group m (MAP) {
          repeated group map (MAP_KEY_VALUE) {
            required binary key;
            optional binary value;
          }
        }
        optional int32 n;
      }
    }
  }
}
```

### How-to: Use Parquet with Impala, Hive, Pig, and MapReduce
<http://blog.cloudera.com/blog/2014/03/how-to-use-parquet-with-impala-hive-pig-mapreduce/>  
<https://github.com/cloudera/parquet-examples/tree/master/MapReduce>

运行```TestReadWriteParquet```(输入路径指定为hive_alltypes.parquet文件.输出可以随便指定一个事先不存在的文件夹.)输出Parquet文件的内部格式如下:  


```
message hive_schema {
  optional int32 c1;
  optional boolean c2;
  optional double c3;
  optional binary c4;
  optional group c5 (LIST) {
    repeated group bag {
      optional int32 array_element;
    }
  }
  optional group c6 (MAP) {
    repeated group map (MAP_KEY_VALUE) {
      required int32 key;
      optional binary value;
    }
  }
  ...
}
```

可以看到和parquet-tools schema的输出是一样的结果.  

输出文件夹包括了:

```
-rw-r--r--  1 zhengqh  staff     0B  6 23 09:53 _SUCCESS
-rw-r--r--  1 zhengqh  staff   531B  6 23 09:53 _common_metadata
-rw-r--r--  1 zhengqh  staff   2.2K  6 23 09:53 _metadata
-rw-r--r--  1 zhengqh  staff   2.8K  6 23 09:53 part-m-00000.parquet
```

**问题1: 运行```TestReadParquet.java```时报错空指针异常:**

```
java.lang.Exception: java.lang.NullPointerException
at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
Caused by: java.lang.NullPointerException
at com.zqh.parquet.TestReadParquet$RecordSchema.<init>(TestReadParquet.java:46)
at com.zqh.parquet.TestReadParquet$ReadRequestMap.map(TestReadParquet.java:80)
at com.zqh.parquet.TestReadParquet$ReadRequestMap.map(TestReadParquet.java:70)
at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:145)
at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:787)
at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
at java.util.concurrent.FutureTask.run(FutureTask.java:266)
at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
at java.lang.Thread.run(Thread.java:745)
 INFO - Job job_local1516732386_0001 failed with state FAILED due to: NA
 INFO - Counters: 0
```
原因分析: 因为使用的是hive内嵌的parquet(在hive-exec下)

```
import parquet.Log;
import parquet.example.data.Group;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.example.ExampleInputFormat;
```

而ParquetInputSplit.getFileSchame()中的实现直接返回null,导致上面的空指针


```
    /** @deprecated */
    @Deprecated
    public String getFileSchema() {
        return null;
    }

```

**问题2: 换成apache版本的parquet**

```
import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetInputSplit;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
```

原因仍然是apache版本的实现中也是直接抛出了一个异常:  

```
java.lang.Exception: java.lang.UnsupportedOperationException: Splits no longer have the file schema, see PARQUET-234
	at org.apache.hadoop.mapred.LocalJobRunner$Job.runTasks(LocalJobRunner.java:462)
	at org.apache.hadoop.mapred.LocalJobRunner$Job.run(LocalJobRunner.java:522)
Caused by: java.lang.UnsupportedOperationException: Splits no longer have the file schema, see PARQUET-234
	at org.apache.parquet.hadoop.ParquetInputSplit.getFileSchema(ParquetInputSplit.java:187)
	at com.zqh.parquet.TestReadParquet$ReadRequestMap.map(TestReadParquet.java:82)
	at com.zqh.parquet.TestReadParquet$ReadRequestMap.map(TestReadParquet.java:74)
	at org.apache.hadoop.mapreduce.Mapper.run(Mapper.java:145)
	at org.apache.hadoop.mapred.MapTask.runNewMapper(MapTask.java:787)
	at org.apache.hadoop.mapred.MapTask.run(MapTask.java:341)
	at org.apache.hadoop.mapred.LocalJobRunner$Job$MapTaskRunnable.run(LocalJobRunner.java:243)
	at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)
	at java.util.concurrent.FutureTask.run(FutureTask.java:266)
	at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142)
	at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617)
	at java.lang.Thread.run(Thread.java:745)
 INFO - Job job_local1407158255_0001 failed with state FAILED due to: NA
 INFO - Counters: 0
```

查看ParquetInputSlit的getFileSchema()源码:里面根本就没有实现方法.

```
  /**
   * @return the file schema
   * @deprecated the file footer is no longer read before creating input splits
   */
  @Deprecated
  public String getFileSchema() {
    throw new UnsupportedOperationException(
        "Splits no longer have the file schema, see PARQUET-234");
  }
```

So, Don't Trust the Example! Run yourself!

### Parquet and Thrift dependency
按照文章中的说法: 需要thrift和parquet-format的jar包放在classpath中:  
MapReduce needs thrift in its CLASSPATH and in libjars to access Parquet files. It also needs parquet-format in libjars. Perform the following setup before running MapReduce jobs that access Parquet data files:

```
if [ -e /opt/cloudera/parcels/CDH ] ; then
    CDH_BASE=/opt/cloudera/parcels/CDH
else
    CDH_BASE=/usr
fi
THRIFTJAR=`ls -l $CDH_BASE/lib/hive/lib/libthrift*jar | awk '{print $9}' | head -1`
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$THRIFTJAR
export LIBJARS=`echo "$CLASSPATH" | awk 'BEGIN { RS = ":" } { print }' | grep parquet-format | tail -1`
export LIBJARS=$LIBJARS,$THRIFTJAR

hadoop jar my-parquet-mr.jar -libjars $LIBJARS
```

由于我们不是按照CDH parcels的方式安装, 所以jar包的路径要手动指定: 比如

```
/Users/zhengqh/Soft/cdh542/hive-1.1.0-cdh5.4.2/lib/libthrift-0.9.2.jar,
/Users/zhengqh/Soft/parquet-format-2.1.0-cdh5.4.2/target/parquet-format-2.1.0-cdh5.4.2.jar
```

### Install Thrift
安装thrift: <https://github.com/apache/parquet-mr>  
官方文档: <http://thrift.apache.org/docs/BuildingFromSource>

```
wget -nv http://archive.apache.org/dist/thrift/0.7.0/thrift-0.7.0.tar.gz
tar xzf thrift-0.7.0.tar.gz
cd thrift-0.7.0
chmod +x ./configure
chmod +x ./install-sh
./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang
sudo make install
```

安装0.7.0版本的thrift会因为php版本不匹配导致安装失败: <https://issues.apache.org/jira/browse/THRIFT-1602>  
解决方式是在configure时直接添加: **--without-php**

```
/Users/zhengqh/Soft/thrift-0.7.0/lib/php/src/ext/thrift_protocol/php_thrift_protocol.cpp:95:8: error: unknown type name 'function_entry'
static function_entry thrift_protocol_functions[] = {
```

thrift依赖了boost和libevent: <http://thrift.apache.org/docs/install/os_x>

```
./configure --prefix=/usr/local/ --with-boost=/usr/local --with-libevent=/usr/local
```


#### mac下安装thrift
在mac下安装thrift其实一句话的事儿,但是如果之前用源码装过了,会报错:  

```
➜ brew install thrift

Error: The `brew link` step did not complete successfully
The formula built, but is not symlinked into /usr/local
Could not symlink bin/thrift
Target /usr/local/bin/thrift
already exists. You may want to remove it:
  rm '/usr/local/bin/thrift'

To force the link and overwrite all conflicting files:
  brew link --overwrite thrift

To list all files that would be deleted:
  brew link --overwrite --dry-run thrift

Possible conflicting files are:
/usr/local/bin/thrift
==> Summary
🍺  /usr/local/Cellar/thrift/0.9.2: 90 files, 5.4M
```

设置新的软链接:

```
➜ rm /usr/local/bin/thrift
➜ ln -s /usr/local/Cellar/thrift/0.9.2/bin/thrift /usr/local/bin/
➜ thrift -version
```

安装成功后, 查看thrift版本:  

```
thrift -version
Thrift version 0.9.2
```


### 参考文档  
[How-to: Use Parquet with Impala, Hive, Pig, and MapReduce][1]  
[Understanding how Parquet integrates with Avro, Thrift and Protocol Buffers][2]

[1]: http://blog.cloudera.com/blog/2014/03/how-to-use-parquet-with-impala-hive-pig-mapreduce/
[2]: http://grepalex.com/2014/05/13/parquet-file-format-and-object-model/
