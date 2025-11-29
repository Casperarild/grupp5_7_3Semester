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
        # Re-raise the exception to prevent the application from starting without a DB connection
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
            # FIX: Use a safe, supported QuestDB query to confirm connectivity.
            # 'select now()' works and confirms the database is alive.
            now_time = await conn.fetchval("SELECT now()")
            
            return {
                "QuestDB_version": "CONNECTED", 
                "detail": f"Successfully fetched current time from QuestDB: {now_time}"
            }
    except Exception as e:
        # Catch and re-raise any database-related errors
        raise HTTPException(status_code=500, detail=f"Database error in /qdbversion: {str(e)}")


@app.get("/exhtemperatur")
async def get_latest_exhtemperature():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_temp = row["extractAirTemp"]
            
            return {"extractAirTemp": latest_temp}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/outtemperatur")
async def get_latest_outtemperature():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_outtemp = row["outdoorTemp"]
            
            return {"outdoorTemp": latest_outtemp}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")


@app.get("/outhumidity")
async def get_latest_outhumidity():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_outhdumidity = row["humidityOutdoor"]
            
            return {"humidityOutdoor": latest_outhdumidity}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/roomhumidity")
async def get_latest_roomhumidity():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_roomhdumidity = row["humidityRoom"]
            
            return {"humidityRoom": latest_roomhdumidity}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/supplyAirPress")
async def get_latest_sAirPress():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_suAirPRess = row["supplyAirPress"]
            
            return {"supplyAirPress": latest_suAirPRess}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/exhaustAirPress")
async def get_latest_exAirPress():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_exAirPRess = row["exhaustAirPress"]
            
            return {"exhaustAirPress": latest_exAirPRess}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    
@app.get("/supplyAirFlow")
async def get_latest_sAirFlow():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_suAirFlow = row["supplyAirFlow"]
            
            return {"supplyAirFlow": latest_suAirFlow}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")

@app.get("/exhaustAirFlow")
async def get_latest_exAirFlow():
    try:
        async with app.state.db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM Olimex_Vent ORDER BY ts DESC LIMIT 1")
            
            if row is None:
                raise HTTPException(status_code=404, detail="No data found")
            
            latest_exAirFlow = row["exhaustAirFlow"]
            
            return {"exhaustAirFlow": latest_exAirFlow}
            
    except asyncpg.exceptions.UndefinedTableError:
        raise HTTPException(status_code=500, detail="Table does not exist")
    except Exception as e:
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")