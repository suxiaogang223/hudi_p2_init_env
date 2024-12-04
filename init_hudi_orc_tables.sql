use regression_hudi;
drop table if exists orc_hudi_table_mor;
CREATE TABLE orc_hudi_table_mor (
  id STRING,
  name STRING
)
USING hudi
OPTIONS (
  'hoodie.datasource.write.table.type' = 'mor',
  'hoodie.table.base.file.format' = 'ORC'
);
insert into orc_hudi_table_mor values ('1', 'A') , ('2', 'B') , ('3', 'C') , ('4', 'D') , ('5', 'E');

drop table if exists orc_hudi_table_cow;
CREATE TABLE orc_hudi_table_cow (
  id STRING,
  name STRING
)
USING hudi
OPTIONS (
  'hoodie.datasource.write.table.type' = 'cow',
  'hoodie.table.base.file.format' = 'ORC'
);
insert into orc_hudi_table_cow values ('1', 'A') , ('2', 'B') , ('3', 'C') , ('4', 'D') , ('5', 'E');
