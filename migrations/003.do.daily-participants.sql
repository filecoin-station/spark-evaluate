CREATE TABLE participants (
  id SERIAL NOT NULL PRIMARY KEY,
  participant_address TEXT NOT NULL UNIQUE
);

CREATE TABLE daily_participants (
  day DATE NOT NULL,
  participant_id INTEGER NOT NULL REFERENCES participants(id),
  PRIMARY KEY (day, participant_id)
);
CREATE INDEX daily_participants_day ON daily_participants (day);
