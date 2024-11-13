"""
    This script generates random user activity log data 
and writes it to Hudi tables in various configurations.
    It uses Faker to generate synthetic data and PySpark 
to create DataFrames and write to Hudi tables.
"""

import argparse
import datetime
import random
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

fake = Faker()

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
    .getOrCreate()
)

# Parse command-line arguments
parser = argparse.ArgumentParser(
    description="Generate random user activity log data and write to Hudi tables."
)
parser.add_argument(
    "--num_records",
    type=int,
    default=100,
    help="Number of user activity records to generate",
)
args = parser.parse_args()


def generate_user_activity():
    """Generate random user activity log data"""
    return {
        "user_id": fake.uuid4(),
        "event_id": random.randint(1, 1000000),
        "age": random.randint(18, 70),
        "event_time": datetime.datetime.now(),
        "is_active": random.choice([True, False]),
        "rating": Decimal(
            round(random.uniform(1, 5), 2)
        ),  # Convert to float to match DecimalType requirement
        "profile_picture": None,  # Removed image generation to avoid PIL dependency
        "signup_date": fake.date_this_decade(),
        "tags": [fake.word() for _ in range(random.randint(1, 5))],
        "preferences": {fake.word(): fake.word() for _ in range(random.randint(1, 3))},
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
                    "category": [fake.word() for _ in range(random.randint(1, 3))],
                    "price": Decimal(
                        round(random.uniform(10, 1000), 2)
                    ),  # Convert to float to match DecimalType requirement
                    "purchase_date": fake.date_this_year(),
                },
                "shipping_details": {
                    "address": {
                        "street": fake.street_name(),
                        "city": fake.city(),
                        "postal_code": fake.postcode(),
                    },
                    "expected_delivery": fake.date_between(
                        start_date="today", end_date="+30d"
                    ),
                },
            }
            for _ in range(random.randint(1, 5))
        ],
    }


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
    spark.sql(create_table_sql)


def write_hudi_table(
    table_name,
    table_path,
    table_type,
    dataframe,
    partition_field=None,
):
    """Write DataFrame to Hudi table with specified configurations"""
    hudi_options = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.recordkey.field": "user_id",
        "hoodie.datasource.write.precombine.field": "event_time",
        "hoodie.datasource.write.table.type": table_type,
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.insert.shuffle.parallelism": 2,
        "hoodie.upsert.shuffle.parallelism": 2,
    }
    if partition_field:
        hudi_options["hoodie.datasource.write.partitionpath.field"] = partition_field

    dataframe.write.format("hudi").options(**hudi_options).mode("overwrite").save(
        table_path
    )


def init_hudi_table(table_name, table_type, database, dataframe, partition_field=None):
    """Initialize a Hudi table with the specified configurations"""
    path_base = "hdfs://hdfs-cluster/user/hive/warehouse/"
    table_path = path_base + database + ".db/" + table_name
    write_hudi_table(table_name, table_path, table_type, dataframe, partition_field)
    create_hudi_table(table_name, table_path, table_type, partition_field)
    print(f"Table {table_name} successfully initialized!")


# Generate a list of random user activity data
user_activity_data = [generate_user_activity() for _ in range(args.num_records)]

# Convert list of dictionaries to DataFrame
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

# Define table path and base configurations
TABLE_NAME = "user_activity_log"
DATABASE = "hudi_p2"

# Create various Hudi tables
init_hudi_table(TABLE_NAME + "_cow_non_partition", "COPY_ON_WRITE", DATABASE, df)
init_hudi_table(TABLE_NAME + "_mor_non_partition", "MERGE_ON_READ", DATABASE, df)
init_hudi_table(
    TABLE_NAME + "_cow_partition", "COPY_ON_WRITE", DATABASE, df, "signup_date"
)
init_hudi_table(
    TABLE_NAME + "_mor_partition", "MERGE_ON_READ", DATABASE, df, "signup_date"
)

print("All Hudi tables successfully written!")
