ALTER TABLE daily_stations
ADD COLUMN inet_group TEXT NOT NULL DEFAULT 'pre_column_addition',
ADD COLUMN participant_address TEXT NOT NULL DEFAULT 'pre_column_addition';

-- Drop existing primary key
ALTER TABLE daily_stations
DROP CONSTRAINT daily_stations_pkey;

-- Add new primary key
ALTER TABLE daily_stations
ADD PRIMARY KEY (day, station_id, inet_group, participant_address);
