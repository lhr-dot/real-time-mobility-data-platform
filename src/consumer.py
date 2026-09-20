import json

import psycopg2
from kafka import KafkaConsumer


BATCH_SIZE = 50

consumer = KafkaConsumer(
    "vehicle_positions",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)

conn = psycopg2.connect(
    host="localhost",
    port=5433,
    database="mobility",
    user="mobility",
    password="mobility",
)

cursor = conn.cursor()

print("Consumer connecté à Redpanda.")
print("PostgreSQL connecté.")
print(f"En attente de messages... Batch de {BATCH_SIZE}")

insert_query = """
    INSERT INTO vehicle_positions (
        vehicle_id,
        vehicle_label,
        trip_id,
        route_id,
        direction_id,
        latitude,
        longitude,
        bearing,
        speed_kmh,
        odometer,
        stop_sequence,
        status,
        stop_id,
        event_timestamp
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s
    )
"""

batch = []

try:
    for message in consumer:
        event = message.value

        batch.append(
            (
                event["vehicle_id"],
                event["vehicle_label"],
                event["trip_id"],
                event["route_id"],
                event["direction_id"],
                event["latitude"],
                event["longitude"],
                event["bearing"],
                event["speed_kmh"],
                event["odometer"],
                event["stop_sequence"],
                event["status"],
                event["stop_id"],
                event["timestamp"],
            )
        )

        if len(batch) >= BATCH_SIZE:
            cursor.executemany(insert_query, batch)
            conn.commit()

            print(f"Batch inséré : {len(batch)} événements")

            batch.clear()

except KeyboardInterrupt:
    print("\nArrêt du consumer...")

finally:
    if batch:
        cursor.executemany(insert_query, batch)
        conn.commit()
        print(f"Dernier batch inséré : {len(batch)} événements")

    cursor.close()
    conn.close()
    consumer.close()

    print("Consumer arrêté proprement.")
