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


def write_hudi_table(
    dataframe, table_name, table_path, table_type, partition_field=None
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


# Define table path and base configurations
TABLE_PATH = "hdfs://hdfs-cluster/user/hive/warehouse/syt_hudi_test.db/"
TABLE_NAME = "user_activity_log"

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

# Create various Hudi tables
write_hudi_table(
    df,
    TABLE_NAME + "_cow_non_partition",
    TABLE_PATH + "_cow_non_partition",
    "COPY_ON_WRITE",
)
write_hudi_table(
    df,
    TABLE_NAME + "_mor_non_partition",
    TABLE_PATH + "_mor_non_partition",
    "MERGE_ON_READ",
)
write_hudi_table(
    df,
    TABLE_NAME + "_cow_partition",
    TABLE_PATH + "_cow_partition",
    "COPY_ON_WRITE",
    partition_field="signup_date",
)
write_hudi_table(
    df,
    TABLE_NAME + "_mor_partition",
    TABLE_PATH + "_mor_partition",
    "MERGE_ON_READ",
    partition_field="signup_date",
)

print("All Hudi tables successfully written!")
