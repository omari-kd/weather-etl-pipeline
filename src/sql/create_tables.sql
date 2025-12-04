CREATE TABLE weather_daily (
    date DATE NOT NULL,
    temperature FLOAT,
    city VARCHAR(50) NOT NULL,
    PRIMARY KEY (date, city)
);