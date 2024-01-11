DROP TABLE ETL_CST_INTERFACE IF EXISTS;
CREATE TABLE IF NOT EXISTS ETL_CST_INTERFACE  (
    eci_id bigint auto_increment,
    eci_status varchar(30),
    last_updated_date timestamp NULL,
    last_updated_user varchar(128),
);
