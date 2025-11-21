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

# --- Connect to QuestDB with retry ---
def create_table(retries=10, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            with psycopg2.connect(QDB_CONN) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS mqtt_metrics (
                            namespace SYMBOL,
                            group_name SYMBOL,
                            type SYMBOL,
                            node SYMBOL,
                            device SYMBOL,
                            metric_name SYMBOL,
                            metric_alias INT,
                            value DOUBLE,
                            ts TIMESTAMP
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


# --- Insert metrics into QuestDB ---
def insert_metrics(topic, metrics):
    parts = topic.split("/")
    namespace = parts[0] if len(parts) > 0 else None
    group = parts[1] if len(parts) > 1 else None
    mtype = parts[2] if len(parts) > 2 else None
    node = parts[3] if len(parts) > 3 else None
    device = parts[4] if len(parts) > 4 else None

    rows = []
    for m in metrics:
        rows.append((
            namespace,
            group,
            mtype,
            node,
            device,
            m.get("name"),
            m.get("alias"),
            float(m.get("value")) if m.get("value") is not None else None,
            m.get("timestamp") * 1000 
        ))

    if not rows:
        return

    with psycopg2.connect(QDB_CONN) as conn:
        with conn.cursor() as cur:
            execute_values(cur, """
                INSERT INTO mqtt_metrics (
                    namespace, group_name, type, node, device,
                    metric_name, metric_alias, value, ts
                ) VALUES %s
            """, rows)
        conn.commit()

# --- MQTT Callbacks ---
def on_connect(client, userdata, flags, rc):
    print(f"Connected to MQTT broker with result code {rc}")
    client.subscribe("spBv1.0/#")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        metrics = payload.get("metrics", [])
        insert_metrics(msg.topic, metrics)
        print(f"Inserted {len(metrics)} metrics from topic {msg.topic}")
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
