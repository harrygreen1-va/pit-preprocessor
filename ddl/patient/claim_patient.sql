if object_id('claim_patient', 'U') is not null
    drop table claim_patient;
go

create table claim_patient
(
    claim_patient_key bigint identity constraint XPKCLAIM_PATIENT primary key,
    claim_key int,
    claim_id varchar(60) not null,
    patient_name pit_long_name,
    sex char(1),
    date_of_birth pit_date,

    insured_id pit_natural_key,
    patient_id varchar(50),

    insured_id_a pit_natural_key,
    insured_id_b pit_natural_key,
    insured_id_c pit_natural_key,

    insured_name_a pit_long_name,
    insured_name_b pit_long_name,
    insured_name_c pit_long_name,


    relationship_to_insured_a pit_natural_key,
    relationship_to_insured_b pit_natural_key,
    relationship_to_insured_c pit_natural_key,

    account_number varchar(50),
    vista_key pit_natural_key,
    icn varchar(20),
    ssn_match_source varchar(20),
    etl_batch_id pit_natural_key not null,
    last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate(),


    constraint pat_fk_claim_key foreign key (claim_key)
        references dim_va_claim
        on delete cascade
)

-- indexes:
if exists (select name from sysindexes where name = 'IDX_PAT_CLAIM_KEY')
    drop index  IDX_PAT_CLAIM_KEY on claim_patient
go
create nonclustered index IDX_PAT_CLAIM_KEY on claim_patient (claim_key)
    include (claim_patient_key);

if exists (select name from sysindexes where name = 'IDX_PAT_CLAIM_ID')
    drop index  IDX_PAT_CLAIM_ID on claim_patient
go
create nonclustered index IDX_PAT_CLAIM_ID on claim_patient (claim_id)
    include (claim_patient_key);


if exists (select name from sysindexes where name = 'IDX_PAT_LAST_UPDATED_DATE')
    drop index  IDX_PAT_LAST_UPDATED_DATE on claim_patient
go
create nonclustered index IDX_PAT_LAST_UPDATED_DATE on claim_patient (last_updated_date desc);


if exists (select name from sysindexes where name = 'IDX_PAT_ETL_BATCH_ID')
    drop index  IDX_PAT_ETL_BATCH_ID on claim_patient
go
create nonclustered index IDX_PAT_ETL_BATCH_ID on claim_patient (etl_batch_id);