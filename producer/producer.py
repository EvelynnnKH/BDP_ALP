from kafka import KafkaProducer
import pandas as pd
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(
    '../data/e-shop clothing 2008.csv',
    sep=';'
)

for _, row in df.iterrows():

    event_time = f"{row['year']}-{row['month']:02d}-{row['day']:02d}"

    event = {
        "event_time": event_time,
        "session_id": int(row["session ID"]),
        "country": int(row["country"]),
        "main_category": int(row["page 1 (main category)"]),
        "clothing_model": str(row["page 2 (clothing model)"]),
        "colour": int(row["colour"]),
        "location": int(row["location"]),
        "price": int(row["price"]),
        "price_level": int(row["price 2"]),
        "page": int(row["page"]),
        "order_in_session": int(row["order"])
    }

    producer.send(
        'clickstream-fashion-events',
        value=event
    )

    print("Sent:", event)

    time.sleep(1)

producer.flush()