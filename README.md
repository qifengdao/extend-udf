# extend-udf

#1 Bulid project and put jar to hdfs 
mvn package

hadoop fs -put extend-udf-1.0.jar hdfs://namenode:9000/hive/
#2 Load function

add jar hdfs://namenode:9000/hive/extend-udf-1.0.jar;

CREATE TEMPORARY FUNCTION parseip AS 'extend.udf.ParseIp';

CREATE TEMPORARY FUNCTION getid4url AS 'extend.udf.GetIdByURL';

CREATE TEMPORARY FUNCTION countdist AS 'extend.udf.CountDistinct';

describe function getid4url;

#3 Use function

select parseip('61.178.58.10');

select getid4url('http://www.baidu.com?v=1');

select countdist('192.168.1.1,192.168.2.1');


