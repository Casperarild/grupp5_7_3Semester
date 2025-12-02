import os
import json
import time
import paho.mqtt.client as mqtt
import psycopg2

# --- Environment Variables ---
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))

QDB_HOST = os.getenv("QDB_HOST", "questdb")
QDB_PORT = int(os.getenv("QDB_PORT", 8812))
QDB_USER = os.getenv("QDB_USER", "admin")
QDB_PASSWORD = os.getenv("QDB_PASSWORD", "quest")
QDB_DB = os.getenv("QDB_DB", "qdb")

QDB_CONN = f"postgresql://{QDB_USER}:{QDB_PASSWORD}@{QDB_HOST}:{QDB_PORT}/{QDB_DB}"


# --- Table creation ---
def create_table(retries=10, delay=5):
    for attempt in range(retries):
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
        except psycopg2.OperationalError:
            print(f"QuestDB not ready, retrying in {delay}s...")
            time.sleep(delay)
    raise Exception("Failed to connect to QuestDB after multiple attempts.")


# --- Insert based on NEW JSON field names ---
def insert_metrics_from_payload(payload):
    row = (
        payload.get("outdoorTemp"),
        payload.get("airTemp"),
        payload.get("humidtyOutdoor"),
        payload.get("humidtyRoom"),
        payload.get("supplyAirPressure"),
        payload.get("exhaustAirPressure"),
        payload.get("supplyAirFlow"),
        payload.get("exhaustAirFlow")
    )

    with psycopg2.connect(QDB_CONN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO Olimex_Vent (
                    ts,
                    outdoorTemp,
                    extractAirTemp,
                    humidityOutdoor,
                    humidityRoom,
                    supplyAirPress,
                    exhaustAirPress,
                    supplyAirFlow,
                    exhaustAirFlow
                ) VALUES (now(), %s, %s, %s, %s, %s, %s, %s, %s)
            """, row)
        conn.commit()


# --- MQTT callbacks ---
def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("spBv1.0/#")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        insert_metrics_from_payload(payload)
        print(f"Inserted metrics from topic {msg.topic}")
    except Exception as e:
        print(f"Error processing {msg.topic}: {e}")


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

