"""
This module generates random user activity data and writes it to a CSV file.
The data includes user information, event details, and purchase history.
"""

import csv
import random
import string
from datetime import datetime, timedelta


# 定义数据生成函数
def random_string(length=8):
    return "".join(random.choices(string.ascii_letters, k=length))


def random_tags():
    return [random_string(5) for _ in range(random.randint(1, 5))]


def random_preferences():
    return {random_string(5): random_string(5) for _ in range(random.randint(1, 3))}


def random_coordinates():
    return (round(random.uniform(-90, 90), 6), round(random.uniform(-180, 180), 6))


def random_purchase():
    return {
        "item_id": random_string(6),
        "item_name": random_string(10),
        "category": [random_string(5) for _ in range(random.randint(1, 3))],
        "price": round(random.uniform(10, 1000), 2),
        "purchase_date": datetime.now().date(),
    }


def generate_data(num_records=100):
    data = []
    for _ in range(num_records):
        user_id = random_string(10)
        event_id = random.randint(1000, 9999)
        age = random.randint(18, 65)
        event_time = datetime.now() - timedelta(days=random.randint(0, 30))
        is_active = random.choice([True, False])
        rating = round(random.uniform(1, 5), 2)
        profile_picture = "".join(
            random.choices(string.ascii_letters + string.digits, k=20)
        ).encode("utf-8")
        signup_date = (datetime.now() - timedelta(days=random.randint(30, 365))).date()
        tags = random_tags()
        preferences = random_preferences()
        coordinates = random_coordinates()
        purchases = [random_purchase() for _ in range(random.randint(1, 3))]

        data.append(
            [
                user_id,
                event_id,
                age,
                event_time,
                is_active,
                rating,
                profile_picture,
                signup_date,
                tags,
                preferences,
                coordinates,
                purchases,
            ]
        )
    return data


# 写入 CSV 文件
output_file = "user_activity_log.csv"

with open(output_file, mode="w", newline="") as file:
    writer = csv.writer(file)
    # 写入表头
    writer.writerow(
        [
            "user_id",
            "event_id",
            "age",
            "event_time",
            "is_active",
            "rating",
            "profile_picture",
            "signup_date",
            "tags",
            "preferences",
            "coordinates",
            "purchases",
        ]
    )
    # 写入随机生成的数据
    for record in generate_data(100):
        writer.writerow(record)

print(f"数据已写入到 {output_file}")
