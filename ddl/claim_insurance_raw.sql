if object_id('claim_insurance_raw', 'U') is not null
    drop table claim_insurance_raw;
go

create table claim_insurance_raw
(

    claim_insurance_raw_key bigint identity primary key,
    claim_key int,
    file_row_num int,
    claim_id varchar(60) not null,
    fms_vendor_id pit_long_name,
    clia_lab_number pit_long_name,

    insured_name_a pit_long_name,
    insured_name_b pit_long_name,
    insured_name_c pit_long_name,

    insured_id_a pit_natural_key,
    insured_id_b pit_natural_key,
    insured_id_c pit_natural_key,

    insurance_group_no_a varchar(50),
    insurance_group_no_b varchar(50),
    insurance_group_no_c varchar(50),

    group_name_a pit_long_name,
    group_name_b pit_long_name,
    group_name_c pit_long_name,

    health_plan_id_a varchar(50),
    health_plan_id_b varchar(50),
    health_plan_id_c varchar(50),

    relationship_to_insured_a pit_long_name,
    relationship_to_insured_b pit_long_name,
    relationship_to_insured_c pit_long_name,

    prior_payments_a pit_money,
    prior_payments_b pit_money,
    prior_payments_c pit_money,

    insurance_company_name_a varchar(120),
    insurance_company_name_b varchar(120),
    insurance_company_name_c varchar(120),

    last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate(),

    etl_batch_id pit_natural_key not null,

    constraint ins_fk_claim_key foreign key (claim_key)
        references dim_va_claim
        on delete cascade
)


-- indexes:
if exists (select name from sysindexes where name = 'ins_claim_key')
    drop index  ins_claim_key on claim_insurance_raw
go
create nonclustered index ins_claim_key on claim_insurance_raw (claim_key)
    include (claim_insurance_raw_key);

if exists (select name from sysindexes where name = 'ins_claim_id')
    drop index  ins_claim_id on claim_insurance_raw
go
create nonclustered index ins_claim_id on claim_insurance_raw (claim_id)
    include (claim_insurance_raw_key);


if exists (select name from sysindexes where name = 'ins_last_updated_date')
    drop index  ins_last_updated_date on claim_insurance_raw
go
create nonclustered index ins_last_updated_date on claim_insurance_raw (last_updated_date desc);    


if exists (select name from sysindexes where name = 'ins_etl_batch_id')
    drop index  ins_etl_batch_id on claim_insurance_raw
go
create nonclustered index ins_etl_batch_id on claim_insurance_raw (etl_batch_id);