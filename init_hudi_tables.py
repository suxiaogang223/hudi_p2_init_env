"""
    This script generates random user activity log data 
and writes it to Hudi tables in various configurations.
    It uses Faker to generate synthetic data and PySpark 
to create DataFrames and write to Hudi tables.
"""

import argparse
import datetime
from decimal import Decimal

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Generate random user activity log data and write to Hudi tables."
)
parser.add_argument(
    "--batch_size",
    type=int,
    default=10000,
    help="Number of records to generate in each batch",
)
parser.add_argument(
    "--batch_num",
    type=int,
    default=10,
    help="Number of batches to write data in",
)
args = parser.parse_args()

# set seed for reproducibility
fake = Faker()
fake.seed_instance(42)
now = datetime.datetime(2024, 11, 14, 14, 52, 58, 147061)
today = now.date()


def generate_user_activity():
    """Generate random user activity log data"""
    return {
        "user_id": fake.uuid4(),
        "event_id": fake.random_int(min=1, max=1000000),
        "age": fake.random_int(min=18, max=70),
        "event_time": fake.date_time_between_dates(
            now - datetime.timedelta(days=365), now
        ),
        "is_active": fake.boolean(),
        "rating": Decimal(
            round(fake.pyfloat(min_value=1, max_value=5, right_digits=2), 2)
        ),  # Convert to float to match DecimalType requirement
        "profile_picture": fake.binary(length=fake.random_int(min=1, max=100)),
        "signup_date": fake.date_between_dates(
            today - datetime.timedelta(days=365), today
        ),
        "tags": [fake.word() for _ in range(fake.random_int(min=1, max=5))],
        "preferences": {
            fake.word(): fake.word() for _ in range(fake.random_int(min=1, max=3))
        },
        "address": {
            "city": fake.city(),
            "state": fake.state(),
            "postal_code": fake.postcode(),
            "coordinates": {
                "latitude": float(
                    fake.latitude()
                ),  # Convert to float to match DoubleType requirement
                "longitude": float(
                    fake.longitude()
                ),  # Convert to float to match DoubleType requirement
            },
        },
        "purchases": [
            {
                "item": {
                    "item_id": fake.uuid4(),
                    "item_name": fake.word(),
                    "category": [
                        fake.word() for _ in range(fake.random_int(min=1, max=3))
                    ],
                    "price": Decimal(
                        round(
                            fake.pyfloat(min_value=10, max_value=1000, right_digits=2),
                            2,
                        )
                    ),  # Convert to float to match DecimalType requirement
                    "purchase_date": fake.date_between_dates(
                        today - datetime.timedelta(days=365), today
                    ),
                },
                "shipping_details": {
                    "address": {
                        "street": fake.street_name(),
                        "city": fake.city(),
                        "postal_code": fake.postcode(),
                    },
                    "expected_delivery": fake.date_between(
                        start_date=today, end_date=today + datetime.timedelta(days=30)
                    ),
                },
            }
            for _ in range(fake.random_int(min=1, max=5))
        ],
    }


def generate_dataframe():
    """Generate batch user activity log data as dataframe"""
    user_activity_data = [generate_user_activity() for _ in range(args.batch_size)]
    df = spark.createDataFrame(
        [
            (
                d["user_id"],
                d["event_id"],
                d["age"],
                d["event_time"],
                d["is_active"],
                d["rating"],
                d["profile_picture"],
                d["signup_date"],
                d["tags"],
                d["preferences"],
                d["address"],
                d["purchases"],
            )
            for d in user_activity_data
        ],
        schema=schema,
    )
    return df


def generate_dataframes():
    """Generate multiple batches of user activity log data as dataframes"""
    return [generate_dataframe() for _ in range(args.batch_num)]


# Initialize Spark session
# spark-sql --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'
#           --conf 'spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension'
#           --conf 'spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog'
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


# Define schema for the data
schema = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("event_id", IntegerType(), False),
        StructField("age", IntegerType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("is_active", BooleanType(), True),
        StructField("rating", DecimalType(10, 2), True),
        StructField("profile_picture", BinaryType(), True),
        StructField("signup_date", DateType(), True),
        StructField("tags", ArrayType(StringType()), True),
        StructField("preferences", MapType(StringType(), StringType()), True),
        StructField(
            "address",
            StructType(
                [
                    StructField("city", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("postal_code", StringType(), True),
                    StructField(
                        "coordinates",
                        StructType(
                            [
                                StructField("latitude", DoubleType(), True),
                                StructField("longitude", DoubleType(), True),
                            ]
                        ),
                    ),
                ]
            ),
        ),
        StructField(
            "purchases",
            ArrayType(
                StructType(
                    [
                        StructField(
                            "item",
                            StructType(
                                [
                                    StructField("item_id", StringType(), True),
                                    StructField("item_name", StringType(), True),
                                    StructField(
                                        "category", ArrayType(StringType()), True
                                    ),
                                    StructField("price", DecimalType(8, 2), True),
                                    StructField("purchase_date", DateType(), True),
                                ]
                            ),
                        ),
                        StructField(
                            "shipping_details",
                            StructType(
                                [
                                    StructField(
                                        "address",
                                        StructType(
                                            [
                                                StructField(
                                                    "street", StringType(), True
                                                ),
                                                StructField("city", StringType(), True),
                                                StructField(
                                                    "postal_code", StringType(), True
                                                ),
                                            ]
                                        ),
                                    ),
                                    StructField("expected_delivery", DateType(), True),
                                ]
                            ),
                        ),
                    ]
                )
            ),
        ),
    ]
)


def create_hudi_table(table_name, table_path, table_type, partition_field=None):
    """Function to create Hudi tables based on type and partition"""

    # SQL template for creating Hudi tables
    sql_template = """
        CREATE TABLE {table_name} (
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
        )
        USING hudi
        {partition_clause}
        LOCATION '{table_path}'
        OPTIONS (
            type = '{table_type}',
            primaryKey = 'user_id',
            preCombineField = 'event_time'
        )
    """
    partition_clause = f"PARTITIONED BY ({partition_field})" if partition_field else ""
    create_table_sql = sql_template.format(
        table_name=table_name,
        table_path=table_path,
        table_type=table_type,
        partition_clause=partition_clause,
    )
    spark.sql("drop table if exists " + table_name)
    print(create_table_sql)
    spark.sql(create_table_sql)


def write_hudi_table(
    table_name,
    table_path,
    table_type,
    database,
    dataframes,
    partition_field=None,
):
    """Write DataFrame to Hudi table with specified configurations"""
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.database.name": database,
        "hoodie.datasource.write.recordkey.field": "user_id",
        "hoodie.datasource.write.precombine.field": "event_time",
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.insert.shuffle.parallelism": 2,
        "hoodie.upsert.shuffle.parallelism": 2,
    }
    if partition_field:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partition_field

    for i, df in enumerate(dataframes):
        if i == 0:
            df.write.format("hudi").options(**hudi_options).mode("overwrite").save(
                table_path
            )
        else:
            df.write.format("hudi").options(**hudi_options).mode("append").save(
                table_path
            )
        print(f"Batch {i+1}/{len(dataframes)} written to {table_name}!")


def init_hudi_table(table_name, table_type, database, dataframes, partition_field=None):
    """Initialize a Hudi table with the specified configurations"""
    path_base = "hdfs://hdfs-cluster/user/hive/warehouse/"
    table_path = path_base + database + ".db/" + table_name
    create_hudi_table(table_name, table_path, table_type, partition_field)
    write_hudi_table(
        table_name, table_path, table_type, database, dataframes, partition_field
    )
    print(f"Table {table_name} successfully initialized!")


# Generate dataframes
data = generate_dataframes()

# Define table path and base configurations
TABLE_NAME = "user_activity_log"
DATABASE = "regression_hudi"

spark.sql("create database if not exists " + DATABASE)
spark.sql("use " + DATABASE)
# Create various Hudi tables
init_hudi_table(TABLE_NAME + "_cow_non_partition", "COPY_ON_WRITE", DATABASE, data)
init_hudi_table(TABLE_NAME + "_mor_non_partition", "MERGE_ON_READ", DATABASE, data)
init_hudi_table(
    TABLE_NAME + "_cow_partition", "COPY_ON_WRITE", DATABASE, data, "signup_date"
)
init_hudi_table(
    TABLE_NAME + "_mor_partition", "MERGE_ON_READ", DATABASE, data, "signup_date"
)

print("All Hudi tables successfully written!")
