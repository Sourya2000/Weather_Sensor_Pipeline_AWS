-- Raw sensor data table (stores every single reading)
CREATE TABLE raw_sensor_data (
    id BIGSERIAL PRIMARY KEY,
    formatted_date TIMESTAMP WITH TIME ZONE,
    summary VARCHAR(100),
    precip_type VARCHAR(50),
    temperature_c DECIMAL(10,4),
    apparent_temperature_c DECIMAL(10,4),
    humidity DECIMAL(8,4),
    wind_speed_kmh DECIMAL(10,4),
    wind_bearing INTEGER,
    visibility_km DECIMAL(10,4),
    loud_cover INTEGER,
    pressure_millibars DECIMAL(10,4),
    daily_summary TEXT,
    source_file VARCHAR(255),
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    file_name VARCHAR(255)
);

-- Create index for faster searching
CREATE INDEX idx_formatted_date ON raw_sensor_data(formatted_date);

-- Aggregated metrics table (stores min/max/average per file)
CREATE TABLE sensor_aggregates (
    id BIGSERIAL PRIMARY KEY,
    source_file VARCHAR(255),
    sensor_type VARCHAR(100),
    min_temperature DECIMAL(10,4),
    max_temperature DECIMAL(10,4),
    avg_temperature DECIMAL(10,4),
    stddev_temperature DECIMAL(10,4),
    min_humidity DECIMAL(8,4),
    max_humidity DECIMAL(8,4),
    avg_humidity DECIMAL(8,4),
    record_count INTEGER,
    calculation_date DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Quarantine log table (stores information about bad data)
CREATE TABLE quarantine_log (
    id BIGSERIAL PRIMARY KEY,
    file_name VARCHAR(255),
    error_reason TEXT,
    raw_data TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
