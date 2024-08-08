CREATE TABLE recent_station_details (
    day DATE NOT NULL,
    station_id TEXT NOT NULL,
    participant_id INTEGER NOT NULL REFERENCES participants(id),
    accepted_measurement_count INTEGER NOT NULL,
    total_measurement_count INTEGER NOT NULL,
    PRIMARY KEY (day, station_id, participant_id)
);
CREATE INDEX recent_station_details_day ON recent_station_details(day);

CREATE TABLE recent_participant_subnets (
    day date NOT NULL,
    participant_id INTEGER NOT NULL REFERENCES participants(id),
    subnet TEXT NOT NULL,
    PRIMARY KEY (day, participant_id, subnet)
);
CREATE INDEX recent_participant_subnets_day ON recent_participant_subnets(day);

CREATE TABLE recent_active_stations (
    day DATE NOT NULL,
    station_id TEXT NOT NULL,
    PRIMARY KEY (day, station_id)
);
CREATE INDEX recent_active_stations_day ON recent_active_stations(day);

CREATE TABLE monthly_active_station_count (
    month DATE NOT NULL PRIMARY KEY,
    station_count INTEGER NOT NULL
);

CREATE TABLE daily_measurements_summary (
    day DATE NOT NULL PRIMARY KEY,
    accepted_measurement_count INTEGER NOT NULL,
    total_measurement_count INTEGER NOT NULL,
    distinct_active_station_count INTEGER NOT NULL,
    distinct_participant_address_count INTEGER NOT NULL,
    distinct_inet_group_count INTEGER NOT NULL
);