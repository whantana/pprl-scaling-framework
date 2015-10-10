DROP TABLE IF EXISTS ${hiveconf:tableName};
CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:tableName}
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
WITH SERDEPROPERTIES ('avro.schema.url'='${hiveconf:schemaLocation}')
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:avroLocation}';
