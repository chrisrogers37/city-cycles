-- DDL for raw_nyc_legacy
CREATE TABLE IF NOT EXISTS raw_nyc_legacy (
    tripduration INTEGER,
    starttime TEXT,
    stoptime TEXT,
    start_station_id TEXT,
    start_station_name TEXT,
    start_station_latitude TEXT,
    start_station_longitude TEXT,
    end_station_id TEXT,
    end_station_name TEXT,
    end_station_latitude TEXT,
    end_station_longitude TEXT,
    bikeid TEXT,
    usertype TEXT,
    birth_year INTEGER,
    gender INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DDL for raw_nyc_modern
CREATE TABLE IF NOT EXISTS raw_nyc_modern (
    ride_id TEXT,
    rideable_type TEXT,
    started_at TEXT,
    ended_at TEXT,
    start_station_name TEXT,
    start_station_id TEXT,
    end_station_name TEXT,
    end_station_id TEXT,
    start_lat TEXT,
    start_lng TEXT,
    end_lat TEXT,
    end_lng TEXT,
    member_casual TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DDL for raw_london_legacy
CREATE TABLE IF NOT EXISTS raw_london_legacy (
    rental_id TEXT,
    duration INTEGER,
    bike_id TEXT,
    end_date TIMESTAMP,
    end_station_id TEXT,
    end_station_name TEXT,
    start_date TIMESTAMP,
    start_station_id TEXT,
    start_station_name TEXT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DDL for raw_london_modern
CREATE TABLE IF NOT EXISTS raw_london_modern (
    number TEXT,
    start_date TIMESTAMP,
    start_station_number TEXT,
    start_station TEXT,
    end_date TIMESTAMP,
    end_station_number TEXT,
    end_station TEXT,
    bike_number TEXT,
    bike_model TEXT,
    total_duration TEXT,
    total_duration_ms INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
); 