import asyncpg
from fastapi import FastAPI, HTTPException
import os

# Get configuration from environment variables (set in docker-compose.yaml)
DATABASE_CONFIG = {
    "host": os.getenv("QDB_HOST", "questdb"),
    "port": int(os.getenv("QDB_PORT", 8812)),
    "user": os.getenv("QDB_USER", "admin"),
    "password": os.getenv("QDB_PASSWORD", "quest"),
    "database": os.getenv("QDB_DB", "qdb")
}

app = FastAPI(title="QuestDB FastAPI Service")

# --- Database Pool Management ---

@app.on_event("startup")
async def startup():
    """Create a connection pool to QuestDB upon application startup."""
    try:
        app.state.db_pool = await asyncpg.create_pool(**DATABASE_CONFIG)
        print("Successfully created asyncpg connection pool to QuestDB.")
    except Exception as e:
        print(f"FATAL: Could not connect to QuestDB on startup: {e}")
        # Re-raise the exception to prevent the application from starting without a DB connection
        raise

@app.on_event("shutdown")
async def shutdown():
    """Close the connection pool upon application shutdown."""
    if hasattr(app.state, 'db_pool') and app.state.db_pool:
        await app.state.db_pool.close()
        print("Closed asyncpg connection pool.")

# --- API Endpoints ---

@app.get("/")
def read_root():
    """Simple health check."""
    return {"status": "ok", "message": "FastAPI is running"}


@app.get("/qdbversion")
async def get_questdb_version():
    """Query QuestDB for a simple connectivity check using a safe, QuestDB-supported function."""
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


@app.get("/temperatur")
async def get_latest_temperature():
    """
    Retrieves the latest temperature reading from the 'Temp_data' table.
    """
    try:
        async with app.state.db_pool.acquire() as conn:
            # NOTE: We use "Temperatur" in the SQL.
            row = await conn.fetchrow("SELECT Temperatur FROM Temp_data ORDER BY Time DESC LIMIT 1")
            
            if row is None:
                # 404 is correct if the table exists but is empty
                raise HTTPException(status_code=404, detail="No data found in Temp_data table.")
            
            # Access the row using the capitalized key 'Temperatur' 
            # to match the SELECT statement case.
            latest_temp = row["Temperatur"]
            
            return {"Temperatur": latest_temp}
            
    except asyncpg.exceptions.UndefinedTableError:
         # Explicitly catch if the table doesn't exist
        raise HTTPException(status_code=500, detail="The table 'Temp_data' does not exist in QuestDB. Please create it.")
    except Exception as e:
        # Catch other database or unexpected errors
        print(f"Error in /temperatur: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
