alter table dim_patient
    add mvi_check_date_time datetime2;
alter table dim_patient
    add is_updated_from_mvi bit;
alter table dim_patient
    add mvi_connection_error varchar(max)
alter table dim_patient
    add mvi_connection_error_date_time datetime2


alter table dim_patient
    alter column address1 varchar(1000)
alter table dim_patient
    alter column address2 varchar(1000)
alter table dim_patient
    alter column city varchar(500)

alter table dim_patient
    alter column icn_status varchar(20)

