import requests
from datetime import datetime
import logging
import oracledb

log = logging.getLogger(__name__)

city = "PARIS"
API_KEY = "YOUR_KEY"
DB_USER = "USER"
DB_PASS = "PASSWORD"
DB_DSN = "localhost:1521/FREEPDB1"

def fetch_weather(city):
    url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": city,
        "appid": API_KEY,
        "units": "metric"
    }

    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()

def transform_weather(raw):
    return {
        "city": raw["name"],
        "event_ts": datetime.utcfromtimestamp(raw["dt"]),
        "temperature_c": raw["main"]["temp"],
        "humidity_pct": raw["main"]["humidity"],
        "weather_category": raw["weather"][0]["main"],
        "weather_desc": raw["weather"][0]["description"],
        "wind_speed": raw["wind"]["speed"],
        "source": "openweather",
        "load_ts": datetime.utcnow()
    }

def get_last_processed_ts(conn, city):
    sql = """
        SELECT NVL(MAX(event_ts), TIMESTAMP '1970-01-01 00:00:00')
        FROM weather_observation
        WHERE city = :city
    """
    with conn.cursor() as cur:
        cur.execute(sql, city=city)
        return cur.fetchone()[0]

def is_new_event(event_ts, last_processed_ts):
    return event_ts > last_processed_ts

MERGE_SQL = """
MERGE INTO weather_observation tgt
USING (
    SELECT
        :city AS city,
        :event_ts AS event_ts,
        :temperature_c AS temperature_c,
        :humidity_pct AS humidity_pct,
        :weather_category AS weather_category,
        :weather_desc AS weather_desc,
        :wind_speed AS wind_speed,
        :source AS source,
        :load_ts AS load_ts
    FROM dual
) src
ON (
    tgt.city = src.city
    AND tgt.event_ts = src.event_ts
)
WHEN MATCHED THEN
    UPDATE SET
        tgt.temperature_c = src.temperature_c,
        tgt.humidity_pct = src.humidity_pct,
        tgt.weather_category = src.weather_category,
        tgt.weather_desc = src.weather_desc,
        tgt.wind_speed = src.wind_speed,
        tgt.load_ts = src.load_ts
WHEN NOT MATCHED THEN
    INSERT (
        city, event_ts, temperature_c, humidity_pct,
        weather_category, weather_desc, wind_speed,
        source, load_ts
    )
    VALUES (
        src.city, src.event_ts, src.temperature_c, src.humidity_pct,
        src.weather_category, src.weather_desc, src.wind_speed,
        src.source, src.load_ts
    )
"""

CREATE_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS weather_observation (
    city              VARCHAR2(50),
    event_ts          TIMESTAMP,
    temperature_c     NUMBER,
    humidity_pct      NUMBER,
    weather_category  VARCHAR2(50),
    weather_desc      VARCHAR2(100),
    wind_speed        NUMBER,
    source            VARCHAR2(20),
    load_ts           TIMESTAMP,
    CONSTRAINT pk_weather PRIMARY KEY (city, event_ts))"""

def create_table(conn):
    create_query = CREATE_TABLE_QUERY
    with conn.cursor() as cur:
        cur.execute(create_query)

def upsert_weather(conn, record):
    with conn.cursor() as cur:
        cur.execute(MERGE_SQL, record)

def run_weather_etl(city):
    conn = oracledb.connect(
        user=DB_USER,
        password=DB_PASS,
        host= "localhost",
        port=1521,
        service_name= "FREEPDB1"
    )

    try:
        raw = fetch_weather(city)
        record = transform_weather(raw)

        last_ts = get_last_processed_ts(conn, city)

        if is_new_event(record["event_ts"], last_ts):
            create_table(conn)
            conn.commit()
            upsert_weather(conn, record)
            conn.commit()
            log.info("Weather data upserted for %s at %s", city, record["event_ts"])
        else:
            log.info("No new data for %s", city)

    finally:
        conn.close()


if __name__ == "__main__":
    run_weather_etl(city)