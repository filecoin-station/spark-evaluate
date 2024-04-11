CREATE TABLE daily_node_metrics (
    metric_date DATE NOT NULL,
    station_id TEXT NOT NULL,
    PRIMARY KEY (metric_date, station_id)
)
