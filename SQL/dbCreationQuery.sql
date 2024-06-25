CREATE TABLE date_time_dim (
    date_time_id INT PRIMARY KEY,
    pickup_datetime DATETIME,
    pickup_hour INT,
    pickup_day INT,
    pickup_month INT,
    pickup_year INT,
    pickup_weekday INT,
    dropoff_datetime DATETIME,
    dropoff_hour INT,
    dropoff_day INT,
    dropoff_month INT,
    dropoff_year INT,
    dropoff_weekday INT
);

CREATE TABLE passenger_count_dim (
    passenger_count_id INT PRIMARY KEY,
    passenger_count INT
);

CREATE TABLE trip_distance_dim (
    trip_distance_id INT PRIMARY KEY,
    trip_distance DECIMAL(10, 2)
);

CREATE TABLE pickup_location_dim (
    pickup_location_id INT PRIMARY KEY,
    pickup_longitude DECIMAL(9, 6),
    pickup_latitude DECIMAL(9, 6)
);

CREATE TABLE dropoff_location_dim (
    dropoff_location_id INT PRIMARY KEY,
    dropoff_longitude DECIMAL(9, 6),
    dropoff_latitude DECIMAL(9, 6)
);

CREATE TABLE ratecode_dim (
    ratecode_id INT PRIMARY KEY,
    ratecode VARCHAR(50),
    ratecode_name VARCHAR(100)
);

CREATE TABLE payment_type_dim (
    payment_type_id INT PRIMARY KEY,
    payment_type VARCHAR(50),
    payment_type_name VARCHAR(100)
);

CREATE TABLE Fact (
    trip_id INT PRIMARY KEY,
    date_time_id INT,
    passenger_count_id INT,
    trip_distance_id INT,
    pickup_location_id INT,
    dropoff_location_id INT,
    ratecode_id INT,
    payment_type_id INT,
    vendor_id VARCHAR(50),
    fare_amount DECIMAL(10, 2),
    extra DECIMAL(10, 2),
    mta_tax DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    improvement_surcharge DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    FOREIGN KEY (date_time_id) REFERENCES date_time_dim(date_time_id),
    FOREIGN KEY (passenger_count_id) REFERENCES passenger_count_dim(passenger_count_id),
    FOREIGN KEY (trip_distance_id) REFERENCES trip_distance_dim(trip_distance_id),
    FOREIGN KEY (pickup_location_id) REFERENCES pickup_location_dim(pickup_location_id),
    FOREIGN KEY (dropoff_location_id) REFERENCES dropoff_location_dim(dropoff_location_id),
    FOREIGN KEY (ratecode_id) REFERENCES ratecode_dim(ratecode_id),
    FOREIGN KEY (payment_type_id) REFERENCES payment_type_dim(payment_type_id)
);

