# extend-udf
add jar hdfs://namenode:9000/hive/extend-udf-1.0.jar;

CREATE TEMPORARY FUNCTION parseip AS 'extend.udf.ParseIp';
select parseip('61.178.58.10');

CREATE TEMPORARY FUNCTION getid4url AS 'extend.udf.GetIdByURL';
select getid4url('http://www.baidu.com?v=1');
