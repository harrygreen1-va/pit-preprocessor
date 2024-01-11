if object_id('json_auth', 'U') is not null
    drop table json_auth;
go

create table json_auth
(
    json_auth_key bigint identity constraint XPKJSON_AUTH primary key,
    --etl_batch_id pit_natural_key not null,
    guid varchar(40) null,
    program_id  varchar(20) null,

    ICN varchar(20) null,
    last_name varchar(60) null,
    first_name varchar(50) null,
    middle_name varchar(35) null,
    gender char(1) null,
    date_of_birth  pit_date null,
    ssn  pit_short_name  null,
    diagnosis_code varchar(30) null,
    medical_necessity nvarchar(MAX) null,
    payor_status varchar(20) null,
    primary_provider_NPI pit_natural_key null,
    treating_provider_NPI pit_natural_key null,
    referral_category varchar(220) null,
    from_date pit_date null,
    to_date pit_end_date null,
    auth_number varchar(220) null,
    consult_id_vista varchar(220) null,
    referring_provider_NPI pit_natural_key null,
    auth_status varchar(20) null,
    network_code varchar(50) null,
    clin_code varchar(220) null,
    payment_auth_code varchar(220) null,
    category_of_care  varchar(50) null,
    station_id varchar(6) null,
    cost_estimate money null,
    --comments nvarchar(MAX) null,

    CDW_pov varchar(220) null,

    SEOC_id bigint null,
    SEOC_group_name varchar(220) null,
    SEOC_description nvarchar(MAX) null,
    SEOC_effective_date pit_date null,
    SEOC_end_date pit_end_date null,
    SEOC_disclaimer nvarchar(MAX) null,
    SEOC_status varchar(220) null,
    SEOC_category_of_care varchar(50) null,

    SEOC_services_id int null,
    SEOC_services_desc nvarchar(MAX) null,
    SEOC_services_frequency smallint null,
    SEOC_allowable_visits smallint null,
    SEOC_billing_code  varchar(20) null,
    SEOC_billing_code_type  varchar(20) null,

	last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate()	--,
    --constraint json_auth_fk_etl_batch_id foreign key (etl_batch_id)
    --        references claim_batch_log
    --        on delete cascade
)

if exists (select name from sysindexes where name = 'IDX_JSON_LAST_UPDATED_DATE')
    drop index  IDX_JSON_LAST_UPDATED_DATE on json_auth
go
create nonclustered index IDX_JSON_LAST_UPDATED_DATE on json_auth (last_updated_date desc);

