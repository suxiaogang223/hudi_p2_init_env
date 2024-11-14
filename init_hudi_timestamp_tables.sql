use regression_hudi;

DROP TABLE IF EXISTS hudi_table_with_timestamp;

-- 创建Hudi表
CREATE TABLE hudi_table_with_timestamp (
  id STRING,
  name STRING,
  event_time TIMESTAMP
) USING HUDI
OPTIONS (
  type = 'cow',  -- cow: Copy On Write 类型；可以改为 'mor' 表示 Merge On Read
  primaryKey = 'id',
  preCombineField = 'event_time'
);

-- 使用Spark SQL插入数据，确保数据的时间戳为 'America/Los_Angeles' 时区
-- 设置Spark会话的时区
SET TIME ZONE 'America/Los_Angeles';

-- 插入数据
INSERT OVERWRITE hudi_table_with_timestamp VALUES
('1', 'Alice', timestamp('2024-10-25 08:00:00')),
('2', 'Bob', timestamp('2024-10-25 09:30:00')),
('3', 'Charlie', timestamp('2024-10-25 11:00:00'));

-- 查询数据，确保时区正确
SELECT * FROM hudi_table_with_timestamp;