from faker import Faker
import random
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DecimalType,
    BinaryType,
    ArrayType,
    MapType,
    DoubleType,
    TimestampType,
    DateType,
)

fake = Faker()

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Hudi Data Generator")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .getOrCreate()
)

# Define table path and base configurations
table_path = "/tmp/hudi/user_activity_log"
table_name = "user_activity_log"

# Delete existing Hudi table if it exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")


# Generate random user activity log data
def generate_user_activity():
    return {
        "user_id": fake.uuid4(),
        "event_id": random.randint(1, 1000000),
        "age": random.randint(18, 70),
        "event_time": datetime.datetime.now(),
        "is_active": random.choice([True, False]),
        "rating": round(random.uniform(1, 5), 2),
        "profile_picture": bytearray(fake.image(size=(100, 100))),
        "signup_date": fake.date_this_decade(),
        "tags": [fake.word() for _ in range(random.randint(1, 5))],
        "preferences": {fake.word(): fake.word() for _ in range(random.randint(1, 3))},
        "address": {
            "city": fake.city(),
            "state": fake.state(),
            "postal_code": fake.postcode(),
            "coordinates": {"latitude": fake.latitude(), "longitude": fake.longitude()},
        },
        "purchases": [
            {
                "item": {
                    "item_id": fake.uuid4(),
                    "item_name": fake.word(),
                    "category": [fake.word() for _ in range(random.randint(1, 3))],
                    "price": round(random.uniform(10, 1000), 2),
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


# Generate a list of random user activity data
user_activity_data = [generate_user_activity() for _ in range(100)]

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


# Function to write DataFrame to Hudi table
def write_hudi_table(df, table_name, table_path, table_type, partition_field=None):
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

    df.write.format("hudi").options(**hudi_options).mode("overwrite").save(table_path)


# Create various Hudi tables
write_hudi_table(
    df,
    table_name + "_cow_non_partition",
    table_path + "_cow_non_partition",
    "COPY_ON_WRITE",
)
write_hudi_table(
    df,
    table_name + "_mor_non_partition",
    table_path + "_mor_non_partition",
    "MERGE_ON_READ",
)
write_hudi_table(
    df,
    table_name + "_cow_partition",
    table_path + "_cow_partition",
    "COPY_ON_WRITE",
    partition_field="signup_date",
)
write_hudi_table(
    df,
    table_name + "_mor_partition",
    table_path + "_mor_partition",
    "MERGE_ON_READ",
    partition_field="signup_date",
)

print("All Hudi tables successfully written!")
