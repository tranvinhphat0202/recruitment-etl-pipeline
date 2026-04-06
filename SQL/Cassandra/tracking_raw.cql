-- Tạo Keyspace (Database) cho Data Lake
CREATE KEYSPACE IF NOT EXISTS recruitment_dl 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Chọn Keyspace vừa tạo
USE recruitment_dl;

-- Tạo bảng dữ liệu thô tracking_raw_csv
CREATE TABLE IF NOT EXISTS tracking_raw_csv (
    create_time timeuuid,
    ts timestamp,
    job_id text,
    custom_track text,
    bid double,
    campaign_id text,
    group_id text,
    publisher_id text,
    PRIMARY KEY ((job_id), ts, create_time)
) WITH CLUSTERING ORDER BY (ts DESC, create_time DESC);
