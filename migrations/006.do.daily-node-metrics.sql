CREATE TABLE daily_node_metrics (
    day DATE NOT NULL,
    station_id TEXT NOT NULL,
    PRIMARY KEY (day, station_id)
)
