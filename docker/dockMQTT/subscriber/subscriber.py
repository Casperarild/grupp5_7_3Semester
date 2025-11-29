import os
import json
import time
import paho.mqtt.client as mqtt
import psycopg2
from psycopg2.extras import execute_values

# --- Environment Variables ---
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

QDB_HOST = os.getenv("QDB_HOST", "questdb")
QDB_PORT = int(os.getenv("QDB_PORT", 8812))
QDB_USER = os.getenv("QDB_USER", "admin")
QDB_PASSWORD = os.getenv("QDB_PASSWORD", "quest")
QDB_DB = os.getenv("QDB_DB", "qdb")

QDB_CONN = f"postgresql://{QDB_USER}:{QDB_PASSWORD}@{QDB_HOST}:{QDB_PORT}/{QDB_DB}"

def create_table(retries=10, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            with psycopg2.connect(QDB_CONN) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS Olimex_Vent (
                            ts TIMESTAMP,
                            outdoorTemp DOUBLE,
                            extractAirTemp DOUBLE,
                            humidityOutdoor DOUBLE,
                            humidityRoom DOUBLE,
                            supplyAirPress DOUBLE,
                            exhaustAirPress DOUBLE,
                            supplyAirFlow DOUBLE,
                            exhaustAirFlow DOUBLE
                        ) timestamp(ts);
                    """)
                conn.commit()
            print("QuestDB table ready!")
            return
        except psycopg2.OperationalError as e:
            attempt += 1
            print(f"QuestDB not ready (attempt {attempt}/{retries}), retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Failed to connect to QuestDB after multiple attempts.")

def insert_metrics_single_row(metrics):
    metric_dict = {m.get("name"): float(m.get("value")) for m in metrics}

    row = (
        metric_dict.get("Temperature outside"),
        metric_dict.get("Temperature inside"),
        metric_dict.get("Humidity outside"),
        metric_dict.get("Humidity inside"),
        metric_dict.get("Airpressure supplied to unit"),
        metric_dict.get("Exhaust airpressure"),
        metric_dict.get("Air flow on supply"),
        metric_dict.get("Air flow from exhaust")
    )

    with psycopg2.connect(QDB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Olimex_Vent (
                    ts, outdoorTemp, extractAirTemp, humidityOutdoor, humidityRoom,
                    supplyAirPress, exhaustAirPress, supplyAirFlow, exhaustAirFlow
                ) VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
        conn.commit()

# --- MQTT callbacks ---    
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe("spBv1.0/#")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        metrics = payload.get("metrics", [])
        insert_metrics_single_row(metrics)
        print(f"Inserted all metrics from topic {msg.topic} in one row")
    except Exception as e:
        print(f"Error processing message {msg.topic}: {e}")

# --- Main ---
if __name__ == "__main__":
    print("Starting MQTT subscriber...")
    create_table()

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT)
            client.loop_forever()
        except Exception as e:
            print(f"MQTT connection failed: {e}, retrying in 5s...")
            time.sleep(5)
