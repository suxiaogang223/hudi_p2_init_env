# hudi_p2_init_env
shells to init the emr environment for hudi p2 case

## quick start
```bash
python3 -m pip install -r requirements.txt
python3 init_hudi_tables.py
```

## cerate tables
The tables are base on user_activity_log with all data types supported by hudi.
```sql
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
```

## test type read
