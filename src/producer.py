import json
import os
import time
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer


load_dotenv()

URL = os.getenv("TBM_API_URL")

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


try:
    while True:
        response = requests.get(URL, timeout=10)
        response.raise_for_status()

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)

        print(
            f"\n[{datetime.now().strftime('%H:%M:%S')}] "
            f"{len(feed.entity)} événements récupérés"
        )

        sent = 0

        for entity in feed.entity:
            vehicle = entity.vehicle

            status_name = (
                vehicle.DESCRIPTOR
                .fields_by_name["current_status"]
                .enum_type
                .values_by_number[vehicle.current_status]
                .name
            )

            event = {
                "vehicle_id": vehicle.vehicle.id,
                "vehicle_label": vehicle.vehicle.label,
                "trip_id": vehicle.trip.trip_id,
                "route_id": vehicle.trip.route_id,
                "direction_id": vehicle.trip.direction_id,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "bearing": vehicle.position.bearing,
                "speed_kmh": round(vehicle.position.speed * 3.6, 2),
                "odometer": vehicle.position.odometer,
                "stop_sequence": vehicle.current_stop_sequence,
                "status": status_name,
                "stop_id": vehicle.stop_id,
                "timestamp": datetime.fromtimestamp(
                    vehicle.timestamp,
                    tz=timezone.utc,
                ).isoformat(),
            }

            producer.send("vehicle_positions", value=event)
            sent += 1

        producer.flush()

        print(f"{sent} événements envoyés à Redpanda.")
        print("Prochaine récupération dans 10 secondes...")

        time.sleep(10)

except KeyboardInterrupt:
    print("\nArrêt du producer...")

except Exception as error:
    print(f"\nErreur : {error}")

finally:
    producer.close()
    print("Producer arrêté proprement.")
