---
layout: post
---

### hive使用文本文件
```
$ cat hello.txt
a	b	c
b	c	d
a	d	f
c	f	g
a	c	r
$ hadoop fs -mkdir /user/qihuang.zheng/test
$ hadoop fs -put hello.txt /user/qihuang.zheng/test

hive> create table abc(
  a STRING,
  b STRING,
  c STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/user/qihuang.zheng/test';

hive> select * from abc;
OK
a	b	c
b	c	d
a	d	f
c	f	g
a	c	r
Time taken: 0.079 seconds, Fetched: 5 row(s)
```

### hive使用自定义的SerDe--json
<https://github.com/rcongiu/Hive-JSON-Serde>

json数据:  

```
➜  data  cat nesteddata.txt
{ "country":"Switzerland", "languages":["German","French","Italian"], "religions":{ "catholic":[10,20], "protestant":[40,50] } }
```

编译json-serde包:  

```
git clone https://github.com/rcongiu/Hive-JSON-Serde.git
mvn -Pcdh5 clean package -DskipTests
```

hive查询时使用自定义json:  

```
hive> add jar /Users/zhengqh/Github/Hive-JSON-Serde/json-serde/target/json-serde-1.3.1-SNAPSHOT-jar-with-dependencies.jar;
Added [/Users/zhengqh/Github/Hive-JSON-Serde/json-serde/target/json-serde-1.3.1-SNAPSHOT-jar-with-dependencies.jar] to class path
Added resources: [/Users/zhengqh/Github/Hive-JSON-Serde/json-serde/target/json-serde-1.3.1-SNAPSHOT-jar-with-dependencies.jar]

hive> CREATE TABLE json_nested_test (
         country string,
         languages array<string>,
         religions map<string,array<int>>)
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     STORED AS TEXTFILE;

hive> LOAD DATA LOCAL INPATH '/Users/zhengqh/data/nesteddata.txt' OVERWRITE INTO TABLE  json_nested_test ;
Loading data to table default.json_nested_test
Table default.json_nested_test stats: [numFiles=1, numRows=0, totalSize=122, rawDataSize=0]

hive> select * from json_nested_test;
OK
Switzerland	["German","French","Italian"]	{"catholic":[10,20],"protestant":[40,50]}
Time taken: 1.089 seconds, Fetched: 1 row(s)

hive> select languages[0] from json_nested_test;
OK
German
Time taken: 0.146 seconds, Fetched: 1 row(s)

hive> select religions['catholic'][0] from json_nested_test;
OK
10
Time taken: 0.08 seconds, Fetched: 1 row(s)
```

注意: json文件改成下面换行的形式

```
{
  "country":"Switzerland",
  "languages":["German","French","Italian"],
  "religions":{
     "catholic":[10,20],
     "protestant":[40,50]
  }
}
```

则会报错:

```
hive> select * from json_nested_test;
OK
Failed with exception java.io.IOException:org.apache.hadoop.hive.serde2.SerDeException: Row is not a valid JSON Object - JSONException: A JSONObject text must end with '}' at 2 [character 3 line 1]
Time taken: 0.067 seconds
```

### Use a SerDe in Apache Hive
<http://blog.cloudera.com/blog/2012/12/how-to-use-a-serde-in-apache-hive/>

示例数据:  

```
vi tweets.json
{"retweeted_status": {"contributors": null,"text": "#Crowdsourcing – drivers already generate traffic data for your smartphone to suggest alternative routes when a road is clogged. #bigdata","geo": null,"retweeted": false,"in_reply_to_screen_name": null,"truncated": false,"entities": {"urls": [],"hashtags": [{"text": "Crowdsourcing","indices": [ 0, 14]},{"text": "bigdata","indices": [ 129, 137]}],"user_mentions": []},"in_reply_to_status_id_str": null,"id": 245255511388336128,"in_reply_to_user_id_str": null,"source": "SocialOomph","favorited": false,"in_reply_to_status_id": null,"in_reply_to_user_id": null,"retweet_count": 0,"created_at": "Mon Sep 10 20:20:45 +0000 2012","id_str": "245255511388336128","place": null,"usr": {"location": "Oregon, ","default_profile": false,"statuses_count": 5289,"profile_background_tile": false,"lang": "en","profile_link_color": "627E91","id": 347471575,"following": null,"protected": false,"favourites_count": 17,"profile_text_color": "D4B020","verified": false,"description": "Dad, Innovator, Sales Professional. Project Management Professional (PMP).  Soccer Coach,  Little League Coach  #Agile #PMOT - views are my own -","contributors_enabled": false,"name": "Scott Ostby","profile_sidebar_border_color": "404040","profile_background_color": "0F0F0F","created_at": "Tue Aug 02 21:10:39 +0000 2011","default_profile_image": false,"followers_count": 19005,"profile_image_url_https": "https://si0.twimg.com/profile_images/1928022765/scott_normal.jpg","geo_enabled": true,"profile_background_image_url": "http://a0.twimg.com/profile_background_images/327807929/xce5b8c5dfff3dc3bbfbdef5ca2a62b4.jpg","profile_background_image_url_https": "https://si0.twimg.com/profile_background_images/327807929/xce5b8c5dfff3dc3bbfbdef5ca2a62b4.jpg","follow_request_sent": null,"url": "http://facebook.com/ostby","utc_offset": -28800,"time_zone": "Pacific Time (US &amp; Canada)","notifications": null,"friends_count": 13172,"profile_use_background_image": true,"profile_sidebar_fill_color": "1C1C1C","screen_name": "ScottOstby","id_str": "347471575","profile_image_url": "http://a0.twimg.com/profile_images/1928022765/scott_normal.jpg","show_all_inline_media": true,"is_translator": false,"listed_count": 45},"coordinates": null},"contributors": null,"text": "RT @ScottOstby: #Crowdsourcing – drivers already generate traffic data for your smartphone to suggest alternative routes when a road is  ...","geo": null,"retweeted": false,"in_reply_to_screen_name": null,"truncated": false,"entities": {"urls": [],"hashtags": [{"text": "Crowdsourcing","indices": [16,30]}],"user_mentions": [{"id": 347471575,"name": "Scott Ostby","indices": [3,14],"screen_name": "ScottOstby","id_str": "347471575"}]},"in_reply_to_status_id_str": null,"id": 245270269525123072,"in_reply_to_user_id_str": null,"source": "web","favorited": false,"in_reply_to_status_id": null,"in_reply_to_user_id": null,"retweet_count": 0,"created_at": "Mon Sep 10 21:19:23 +0000 2012","id_str": "245270269525123072","place": null,"usr": {"location": "","default_profile": true,"statuses_count": 1294,"profile_background_tile": false,"lang": "en","profile_link_color": "0084B4","id": 21804678,"following": null,"protected": false,"favourites_count": 11,"profile_text_color": "333333","verified": false,"description": "","contributors_enabled": false,"name": "Parvez Jugon","profile_sidebar_border_color": "C0DEED","profile_background_color": "C0DEED","created_at": "Tue Feb 24 22:10:43 +0000 2009","default_profile_image": false,"followers_count": 70,"profile_image_url_https": "https://si0.twimg.com/profile_images/2280737846/ni91dkogtgwp1or5rwp4_normal.gif","geo_enabled": false,"profile_background_image_url": "http://a0.twimg.com/images/themes/theme1/bg.png","profile_background_image_url_https": "https://si0.twimg.com/images/themes/theme1/bg.png","follow_request_sent": null,"url": null,"utc_offset": null,"time_zone": null,"notifications": null,"friends_count": 299,"profile_use_background_image": true,"profile_sidebar_fill_color": "DDEEF6","screen_name": "ParvezJugon","id_str": "21804678","profile_image_url": "http://a0.twimg.com/profile_images/2280737846/ni91dkogtgwp1or5rwp4_normal.gif","show_all_inline_media": false,"is_translator": false,"listed_count": 7},"coordinates": null}
```

使用JSON插件加载数据:  

```
hive> add jar /home/qihuang.zheng/hive-plugin-1.0.0.jar;

hive> CREATE TABLE tweets (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    usr:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  usr STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
)
PARTITIONED BY (datehour INT)
ROW FORMAT SERDE 'cn.tongdun.bigdata.hive.JSONSerDe'
STORED AS TEXTFILE;

hive> LOAD DATA LOCAL INPATH '/user/qihuang.zheng/tweets/tweets.json' OVERWRITE INTO TABLE tweets partition(datehour=12);

hive> select * from tweets where datehour=12;
OK
245270269525123072	Mon Sep 10 21:19:23 +0000 2012	web	false	{"text":"#Crowdsourcing – drivers already generate traffic data for your smartphone to suggest alternative routes when a road is clogged. #bigdata","usr":{"screen_name":"ScottOstby","name":"Scott Ostby"},"retweet_count":0}	{"urls":[],"user_mentions":[{"screen_name":"ScottOstby","name":"Scott Ostby"}],"hashtags":[{"text":"Crowdsourcing"}]}	RT @ScottOstby: #Crowdsourcing – drivers already generate traffic data for your smartphone to suggest alternative routes when a road is  ...	{"screen_name":"ParvezJugon","name":"Parvez Jugon","friends_count":299,"followers_count":70,"statuses_count":1294,"verified":false,"utc_offset":null,"time_zone":null}	NULL	12
```

因为建立的是内部表,在hive表中会添加一个以datehour=12为目录的分区:

```
$ /usr/install/hadoop/bin/hadoop fs -ls /user/hive/warehouse/test.db/tweets
Found 1 items
drwxr-xr-x   - qihuang.zheng supergroup          0 2015-06-23 17:28 /user/hive/warehouse/test.db/tweets/datehour=12
```

查看下表结构:

```
hive> desc tweets;
OK
id                  	bigint              	from deserializer
created_at          	string              	from deserializer
source              	string              	from deserializer
favorited           	boolean             	from deserializer
retweeted_status    	struct<text:string,usr:struct<screen_name:string,name:string>,retweet_count:int>	from deserializer
entities            	struct<urls:array<struct<expanded_url:string>>,user_mentions:array<struct<screen_name:string,name:string>>,hashtags:array<struct<text:string>>>	from deserializer
text                	string              	from deserializer
usr                 	struct<screen_name:string,name:string,friends_count:int,followers_count:int,statuses_count:int,verified:boolean,utc_offset:int,time_zone:string>	from deserializer
in_reply_to_screen_name	string              	from deserializer
datehour            	int

# Partition Information
# col_name            	data_type           	comment

datehour            	int
Time taken: 0.435 seconds, Fetched: 15 row(s)
```

**TODO: 如果在建表的时候直接指定location,则无法查询到数据.  
location=/user/qihuang.zheng/tweets也不行.**  

```
CREATE external TABLE tweets2 (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    usr:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  usr STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
)
PARTITIONED BY (datehour INT)
ROW FORMAT SERDE 'cn.tongdun.bigdata.hive.JSONSerDe'
LOCATION '/user/qihuang.zheng/tweets/datehour=11';
```
