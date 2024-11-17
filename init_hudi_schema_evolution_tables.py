""" 使用 PySpark 验证 Hudi 的 Schema Evolution 功能"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    StructType,
    ArrayType,
)

spark = (
    SparkSession.builder.appName("Hudi P2 Data Generator")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config(
        "spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    )
    .enableHiveSupport()
    .getOrCreate()
)

hudi_options_base = {
    "hoodie.table.name": "test_table",
    "hoodie.datasource.write.recordkey.field": "id",
    "hoodie.datasource.write.precombine.field": "id",
    "hoodie.datasource.write.operation": "upsert",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.schema.on.read.enable": "true",
}

database = "regression_hudi"

table_path_base = f"hdfs://hdfs-cluster/user/hive/warehouse/{database}.db/"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
spark.sql(f"USE {database}")

# 1. 添加新列（Adding Columns）
initial_data = [("1", "Alice"), ("2", "Bob"), ("3", "Cathy")]
initial_schema = StructType(
    [StructField("id", StringType(), True), StructField("name", StringType(), True)]
)

df = spark.createDataFrame(initial_data, initial_schema)
hudi_options = hudi_options_base.copy()
hudi_options["hoodie.table.name"] = "adding_simple_columns_table"
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "adding_simple_columns_table"
)

# 添加新列 age
data_with_new_column = [("4", "David", 25), ("5", "Eva", 30), ("6", "Frank", 28)]
schema_with_new_column = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)

df_with_new_column = spark.createDataFrame(data_with_new_column, schema_with_new_column)
df_with_new_column.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "adding_simple_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS adding_simple_columns_table (
        id STRING,
        name STRING,
        age INT
    ) USING HUDI
    LOCATION '{table_path_base + 'adding_simple_columns_table'}'
"""
)

# 2. 修改列类型（Altering Column Type）
initial_data_with_age = [("1", "Alice", 25), ("2", "Bob", 30), ("3", "Cathy", 28)]
initial_schema_with_age = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)

hudi_options["hoodie.table.name"] = "altering_simple_columns_table"
df_initial_with_age = spark.createDataFrame(
    initial_data_with_age, initial_schema_with_age
)
df_initial_with_age.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "altering_simple_columns_table"
)

# 修改 age 列的类型为 DOUBLE
data_with_altered_column = [
    ("4", "David", 26.0),
    ("5", "Eva", 31.5),
    ("6", "Frank", 29.2),
]
schema_with_altered_column = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", DoubleType(), True),
    ]
)

df_with_altered_column = spark.createDataFrame(
    data_with_altered_column, schema_with_altered_column
)
df_with_altered_column.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "altering_simple_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS altering_simple_columns_table (
        id STRING,
        name STRING,
        age DOUBLE
    ) USING HUDI
    LOCATION '{table_path_base + 'altering_simple_columns_table'}'
"""
)

# 3. 删除列（Deleting Columns）
initial_data_with_age = [("1", "Alice", 25), ("2", "Bob", 30), ("3", "Cathy", 28)]
initial_schema_with_age = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
    ]
)

hudi_options["hoodie.table.name"] = "deleting_simple_columns_table"
df_initial_with_age = spark.createDataFrame(
    initial_data_with_age, initial_schema_with_age
)
df_initial_with_age.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "deleting_simple_columns_table"
)

# 删除 age 列
data_without_age = [("4", "David"), ("5", "Eva"), ("6", "Frank")]
schema_without_age = StructType(
    [StructField("id", StringType(), True), StructField("name", StringType(), True)]
)

df_without_age = spark.createDataFrame(data_without_age, schema_without_age)
df_without_age.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "deleting_simple_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS deleting_simple_columns_table (
        id STRING,
        name STRING
    ) USING HUDI
    LOCATION '{table_path_base + 'deleting_simple_columns_table'}'
"""
)

# 4. 重命名列 (Renaming Columns)
initial_data = [("1", "Alice"), ("2", "Bob"), ("3", "Cathy")]
initial_schema = StructType(
    [StructField("id", StringType(), True), StructField("name", StringType(), True)]
)

hudi_options["hoodie.table.name"] = "renaming_simple_columns_table"
df = spark.createDataFrame(initial_data, initial_schema)
df.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "renaming_simple_columns_table"
)

# 重命名 name 列为 full_name
data_with_renamed_column = [("4", "David"), ("5", "Eva"), ("6", "Frank")]
schema_with_renamed_column = StructType(
    [
        StructField("id", StringType(), True),
        StructField("full_name", StringType(), True),
    ]
)

df_with_renamed_column = spark.createDataFrame(
    data_with_renamed_column, schema_with_renamed_column
)
df_with_renamed_column.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "renaming_simple_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS renaming_simple_columns_table (
        id STRING,
        full_name STRING
    ) USING HUDI
    LOCATION '{table_path_base + 'renaming_simple_columns_table'}'
"""
)

# 复杂类型列的测试

# 1. 添加复杂类型列（Adding Columns）
initial_complex_data = [
    ("1", "Alice", (25, "Guangzhou")),
    ("2", "Bob", (30, "Shanghai")),
    ("3", "Cathy", (28, "Beijing")),
]
initial_complex_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

df_complex = spark.createDataFrame(initial_complex_data, initial_complex_schema)
hudi_options["hoodie.table.name"] = "adding_complex_columns_table"
df_complex.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "adding_complex_columns_table"
)

# 添加新列 email 到 info 结构体中
data_with_email = [
    ("4", "David", (25, "Shenzhen", "david@example.com")),
    ("5", "Eva", (30, "Chengdu", "eva@example.com")),
    ("6", "Frank", (28, "Wuhan", "frank@example.com")),
]
schema_with_email = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("address", StringType(), True),
                    StructField("email", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

df_with_email = spark.createDataFrame(data_with_email, schema_with_email)
df_with_email.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "adding_complex_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS adding_complex_columns_table (
        id STRING,
        name STRING,
        info STRUCT<age: INT, address: STRING, email: STRING>
    ) USING HUDI
    LOCATION '{table_path_base + 'adding_complex_columns_table'}'
"""
)

# 2. 修改复杂类型结构体中的列类型（Altering Column Type）
data_with_altered_field = [
    ("1", "Alice", (25.5, "Guangzhou")),
    ("2", "Bob", (30.8, "Shanghai")),
    ("3", "Cathy", (28.3, "Beijing")),
]
schema_with_altered_field = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", DoubleType(), True),
                    StructField("address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

hudi_options["hoodie.table.name"] = "altering_complex_columns_table"
df_with_altered_field = spark.createDataFrame(
    data_with_altered_field, schema_with_altered_field
)
df_with_altered_field.write.format("hudi").options(**hudi_options).mode(
    "overwrite"
).save(table_path_base + "altering_complex_columns_table")

# 修改 age 列的类型为 DOUBLE
data_with_altered_field = [
    ("4", "David", (26.0, "Shenzhen ")),
    ("5", "Eva", (31.5, "Chengdu")),
    ("6", "Frank", (29.2, "Wuhan")),
]
schema_with_altered_field = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", DoubleType(), True),
                    StructField("address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

df_with_altered_field = spark.createDataFrame(
    data_with_altered_field, schema_with_altered_field
)
df_with_altered_field.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "altering_complex_columns_table"
)


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS altering_complex_columns_table (
        id STRING,
        name STRING,
        info STRUCT<age: DOUBLE, address: STRING>
    ) USING HUDI
    LOCATION '{table_path_base + 'altering_complex_columns_table'}'
"""
)

# 3. 删除复杂类型结构体中的列（Deleting Columns）
data_without_email = [
    ("4", "David", (25, "Shenzhen")),
    ("5", "Eva", (30, "Chengdu")),
    ("6", "Frank", (28, "Wuhan")),
]
schema_without_email = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

hudi_options["hoodie.table.name"] = "deleting_complex_columns_table"
df_without_email = spark.createDataFrame(data_without_email, schema_without_email)
df_without_email.write.format("hudi").options(**hudi_options).mode("overwrite").save(
    table_path_base + "deleting_complex_columns_table"
)
# 删除 address 列
data_without_address = [
    ("4", "David", (25,)),
    ("5", "Eva", (30,)),
    ("6", "Frank", (28,)),
]
schema_without_address = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType([StructField("age", IntegerType(), True)]),
            True,
        ),
    ]
)
df_without_address = spark.createDataFrame(data_without_address, schema_without_address)
df_without_address.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "deleting_complex_columns_table"
)


spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS deleting_complex_columns_table (
        id STRING,
        name STRING,
        info STRUCT<age: INT>
    ) USING HUDI
    LOCATION '{table_path_base + 'deleting_complex_columns_table'}'
"""
)

# 4. 重命名复杂类型结构体中的列（Renaming Columns）
data_with_renamed_field = [
    ("1", "Alice", (25, "Guangzhou")),
    ("2", "Bob", (30, "Shanghai")),
    ("3", "Cathy", (28, "Beijing")),
]
schema_with_renamed_field = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("location", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

hudi_options["hoodie.table.name"] = "renaming_complex_columns_table"
df_with_renamed_field = spark.createDataFrame(
    data_with_renamed_field, schema_with_renamed_field
)
df_with_renamed_field.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "renaming_complex_columns_table"
)
# 重命名 location 列为 address
data_with_renamed_field = [
    ("4", "David", (25, "Shenzhen")),
    ("5", "Eva", (30, "Chengdu")),
    ("6", "Frank", (28, "Wuhan")),
]
schema_with_renamed_field = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField(
            "info",
            StructType(
                [
                    StructField("age", IntegerType(), True),
                    StructField("address", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

df_with_renamed_field = spark.createDataFrame(
    data_with_renamed_field, schema_with_renamed_field
)
df_with_renamed_field.write.format("hudi").options(**hudi_options).mode("append").save(
    table_path_base + "renaming_complex_columns_table"
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS renaming_complex_columns_table (
        id STRING,
        name STRING,
        info STRUCT<age: INT, address: STRING>
    ) USING HUDI
    LOCATION '{table_path_base + 'renaming_complex_columns_table'}'
"""
)
