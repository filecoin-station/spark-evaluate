CREATE TABLE daily_stations (
    day DATE NOT NULL,
    station_id TEXT NOT NULL,
    PRIMARY KEY (day, station_id)
)
