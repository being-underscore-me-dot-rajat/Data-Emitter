import json
import random
import uuid
import logging
from datetime import datetime, timezone

import azure.functions as func
from azure.eventhub import EventHubProducerClient, EventData
import os


# ---- Event Hub configuration (use App Settings in Azure) ----
EVENT_HUB_CONN_STR = os.environ["EVENT_HUB_CONNECTION_STRING"]
EVENT_HUB_NAME = os.environ["EVENT_HUB_NAME"]


# ---- Controlled dictionaries & ranges ----
PRODUCTS = ["P1001", "P1002", "P1003", "P2001", "P3001"]
REGIONS = ["NA", "EU", "APAC"]
ORDER_STATUS = ["CREATED", "CONFIRMED", "SHIPPED"]

PRICE_RANGE = {
    "P1001": (10.0, 20.0),
    "P1002": (15.0, 25.0),
    "P1003": (5.0, 15.0),
    "P2001": (100.0, 150.0),
    "P3001": (250.0, 400.0)
}


def generate_order_event():
    product_id = random.choice(PRODUCTS)
    price_min, price_max = PRICE_RANGE[product_id]

    return {
        "order_id": str(uuid.uuid4()),
        "order_time": datetime.now(timezone.utc).isoformat(),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": product_id,
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(price_min, price_max), 2),
        "order_status": random.choice(ORDER_STATUS),
        "region": random.choice(REGIONS)
    }


def main(mytimer: func.TimerRequest) -> None:
    producer = EventHubProducerClient.from_connection_string(
        conn_str=EVENT_HUB_CONN_STR
    )

    event = generate_order_event()
    event_json = json.dumps(event)

    with producer:
        batch = producer.create_batch()
        batch.add(EventData(event_json))
        producer.send_batch(batch)

    logging.info(f"Sent event: {event_json}")
