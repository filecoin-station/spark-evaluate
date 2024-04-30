CREATE TABLE daily_fil (
    day DATE NOT NULL,
    to_address TEXT NOT NULL,
    amount BIGINT NOT NULL,
    PRIMARY KEY (day, to_address)
)