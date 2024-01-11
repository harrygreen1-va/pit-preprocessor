DROP TABLE claim_batch_log IF EXISTS;
CREATE TABLE IF NOT EXISTS claim_batch_log  (
    etl_batch_id varchar(50) NOT NULL,
    batch_status varchar(20) NULL,
    to_score_indicator char(1) NULL,
    source_system varchar(20) NULL,
    last_updated_date timestamp NULL,
    last_updated_user varchar(128),
    eci_id int NULL,
    feed_date timestamp NULL,
    start_date_time timestamp NULL,
    end_date_time timestamp NULL,
    file_name varchar(50) NULL,
    file_size int NULL,
    file_drop_dt timestamp NULL,
    number_of_rows int NULL,
    error_text varchar(max) NULL
);
