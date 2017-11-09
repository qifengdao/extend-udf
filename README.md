# extend-udf

#1 Bulid project and put jar to hdfs 
mvn package

hadoop fs -put extend-udf-1.0.jar hdfs://namenode:9000/hive/

hadoop fs -put ip.txt hdfs://namenode:9000/hive/

ip.txt为纯真ip库，需要自己下载整理，本项目中只提供部分例子

hadoop fs -put channel.txt hdfs://namenode:9000/hive/

#2 Load function

add jar hdfs://namenode:9000/hive/extend-udf-1.0.jar;

CREATE TEMPORARY FUNCTION parseip AS 'extend.udf.ParseIp';

解析ip地址为地域函数

CREATE TEMPORARY FUNCTION getid4url AS 'extend.udf.GetIdByURL';

将url映射到具体的id的函数（从左模糊匹配，最长匹配）

CREATE TEMPORARY FUNCTION countdist AS 'extend.udf.CountDistinct';

去重求数（类似count(distinct column),传入字符串以逗号分隔，空和NULL不计数）

describe function getid4url;

查看array2中元素是否全部在array1中存在
CREATE TEMPORARY FUNCTION array_contains_all as 'extend.udf.GenericUDFArrayContainsAll';

查看array2中元素是否有至少有一个在array1中
CREATE TEMPORARY FUNCTION array_contains_either as 'com.hylinkad.dmp.udf.GenericUDFArrayContainsEither';

#3 Use function

select parseip('61.178.58.10');

select getid4url('http://www.baidu.com?v=1');

select countdist('192.168.1.1,192.168.2.1');

select array_contains_all(array1,array2);

select array_contains_either(array1,array2);


