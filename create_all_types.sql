-- 表设计说明

-- 	1.	基础字段：包括 user_id、event_id、age、event_time 等基础数据类型，用于存储最基本的信息。
-- 	2.	日期和时间字段：包括 event_time 和 signup_date，方便基于时间进行查询和分析。
-- 	3.	复杂类型：
-- 	    •	数组 (tags)：用于存储用户活动的标签，可以有多个标签来描述用户的兴趣或活动类型。
-- 	    •	Map (preferences)：用于存储用户的偏好配置，键值对的形式非常适合这类配置项。
-- 	4.	嵌套结构体：
-- 	    •	address：嵌套结构体包含了用户的地址和地理坐标，示例中加入了多重嵌套，像 coordinates，便于存储地理信息。
-- 	5.	多重嵌套字段 (purchases)：
-- 	    •	purchases 是一个数组，每个元素都是一个结构体 item，代表一笔购买记录。
-- 	    •	进一步嵌套：item 下还包含嵌套的结构体和数组，如 category 和 price，这使得它适合复杂的业务逻辑。

CREATE TABLE user_activity_log (
  user_id STRING,
  event_id BIGINT,
  age INT,
  event_time TIMESTAMP,
  is_active BOOLEAN,
  rating DECIMAL(10, 2),
  profile_picture BINARY,
  signup_date DATE,
  tags ARRAY<STRING>,
  preferences MAP<STRING, STRING>,
  address STRUCT<
    city: STRING,
    state: STRING,
    postal_code: STRING,
    coordinates: STRUCT<
      latitude: DOUBLE,
      longitude: DOUBLE
    >
  >,
  purchases ARRAY<STRUCT<
    item: STRUCT<
      item_id: STRING,
      item_name: STRING,
      category: ARRAY<STRING>,
      price: DECIMAL(8, 2),
      purchase_date: DATE
    >,
    shipping_details: STRUCT<
      address: STRUCT<
        street: STRING,
        city: STRING,
        postal_code: STRING
      >,
      expected_delivery: DATE
    >
  >>
) USING hudi
OPTIONS (
  type = 'MERGE_ON_READ',
  primaryKey = 'user_id',
  preCombineField = 'event_time'
);