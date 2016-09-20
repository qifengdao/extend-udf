# extend-udf

#1 Bulid project and put jar to hdfs 
mvn package

hadoop fs -put extend-udf-1.0.jar hdfs://namenode:9000/hive/

hadoop fs -put ip.txt hdfs://namenode:9000/hive/

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

#3 Use function

select parseip('61.178.58.10');

select getid4url('http://www.baidu.com?v=1');

select countdist('192.168.1.1,192.168.2.1');


