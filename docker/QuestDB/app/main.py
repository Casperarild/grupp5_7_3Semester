import asyncpg
from fastapi import FastAPI, HTTPException
import os

DATABASE_CONFIG = {
    "host": os.getenv("QDB_HOST", "questdb"),
    "port": int(os.getenv("QDB_PORT", 8812)),
    "user": os.getenv("QDB_USER", "admin"),
    "password": os.getenv("QDB_PASSWORD", "quest"),
    "database": os.getenv("QDB_DB", "qdb")
}

app = FastAPI(title="QuestDB FastAPI Service")


@app.on_event("startup")
async def startup():
    try:
        app.state.db_pool = await asyncpg.create_pool(**DATABASE_CONFIG)
        print("Successfully created asyncpg connection pool to QuestDB.")
    except Exception as e:
        print(f"FATAL: Could not connect to QuestDB on startup: {e}")
        raise

@app.on_event("shutdown")
async def shutdown():
    if hasattr(app.state, 'db_pool') and app.state.db_pool:
        await app.state.db_pool.close()
        print("Closed asyncpg connection pool.")

@app.get("/")
def read_root():
    """Health check."""
    return {"status": "ok", "message": "FastAPI is running"}


@app.get("/qdbversion")
async def get_questdb_version():
    try:
        async with app.state.db_pool.acquire() as conn:
            now_time = await conn.fetchval("SELECT now()")

            return {
                "QuestDB_version": "CONNECTED",
                "detail": f"Successfully fetched current time from QuestDB: {now_time}"
            }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error in /qdbversion: {str(e)}")


@app.get("/extractAirTemp")
async def get_roomhumidity_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "extractAirTemp" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")
            grafana_data = []
            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["extractAirTemp"])
                grafana_data.append([ms_timestamp, value])
            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /extractAirTemp: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/tempout")
async def get_roomhumidity_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "outdoorTemp" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")
            grafana_data = []
            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["outdoorTemp"])
                grafana_data.append([ms_timestamp, value])
            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /tempout: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/outhumidity")
async def get_outhumidity_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "humidityOutdoor" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")
            grafana_data = []
            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["humidityOutdoor"])
                grafana_data.append([ms_timestamp, value])
            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /outhumidity: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/roomhumidity")
async def get_roomhumidity_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "humidityRoom" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")
            grafana_data = []
            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["humidityRoom"])
                grafana_data.append([ms_timestamp, value])
            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /roomhumidity: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/supplyAirPress")
async def get_sAirPress_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "supplyAirPress" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")
            grafana_data = []
            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["supplyAirPress"])
                grafana_data.append([ms_timestamp, value])
            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /supplyAirPress: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/exhaustAirPress")
async def get_eAirPress_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "exhaustAirPress" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grafana_data = []

            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["exhaustAirPress"])

                grafana_data.append([ms_timestamp, value])

            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /exhaustAirPress: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/supplyAirFlow")
async def get_sAirFlow_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "supplyAirFlow" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grafana_data = []

            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["supplyAirFlow"])

                grafana_data.append([ms_timestamp, value])

            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /supplyAirFlow: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/exhaustAirFlow")
async def get_eAirFlow_for_grafana():
    try:
        async with app.state.db_pool.acquire() as conn:

            rows = await conn.fetch("""SELECT ts, "exhaustAirFlow" FROM Olimex_Vent ORDER BY ts DESC LIMIT 100""")
            if not rows:
                raise HTTPException(status_code=404, detail="No data found")

            grafana_data = []

            for row in reversed(rows):
                timestamp_dt = row["ts"]
                ms_timestamp = int(timestamp_dt.timestamp() * 1000)
                value = float(row["exhaustAirFlow"])

                grafana_data.append([ms_timestamp, value])

            return grafana_data

    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /exhaustAirFlow: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
