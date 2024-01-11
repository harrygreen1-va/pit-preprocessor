if object_id('dbo.claim_provider', 'u') is not null
    drop table dbo.claim_provider;
go

create table claim_provider
(

    claim_provider_key bigint identity
        constraint xpk_claim_provider primary key,
    claim_key int,

    claim_id varchar(60) not null,

    billing_provider_source_id pit_natural_key,

    billing_provider_npi pit_natural_key,
    billing_provider_name varchar(80),
    billing_provider_tax_id varchar(20),

    referring_provider_npi pit_natural_key,
    referring_provider_name varchar(80),

    --dim_prof
    rendering_facility_provider_npi pit_natural_key,
    rendering_facility_provider_name varchar(80),

    rendering_facility_address1 varchar(1000),
    rendering_facility_address2 varchar(1000),
    rendering_facility_city varchar(500),
    rendering_facility_state varchar(40),
    rendering_facility_state_zip varchar(20),

    --dim_inst
    operating_provider_npi pit_natural_key,
    operating_provider_name varchar(80),

    attending_provider_npi pit_natural_key,

    other_provider_npi pit_natural_key,
    other_provider_name varchar(80),

    otherB_provider_npi pit_natural_key,
    otherB_provider_name varchar(80),


    service_facility_npi pit_natural_key,

    vista_id pit_long_name,

    billing_provider_taxonomy pit_long_name,
    pay_to_provider_taxonomy pit_long_name,
    referring_provider_taxonomy pit_long_name,
    attending_physician_taxonomy pit_long_name,

    last_updated_user varchar(128) default (suser_name()),
    last_updated_date datetime default (getdate()),

    etl_batch_id pit_natural_key not null,

    constraint claim_prov_fk_claim_key foreign key (claim_key)
        references dim_va_claim
        on delete cascade
)

-- indexes:

drop index if exists prov_claim_key on claim_provider
create nonclustered index prov_claim_key on claim_provider (claim_key)
    include (claim_provider_key);


drop index if exists prov_claim_id on claim_provider
create nonclustered index prov_claim_id on claim_provider (claim_id)
    include (claim_provider_key);



drop index if exists provlast_updated_date on claim_provider
create nonclustered index prov_last_updated_date on claim_provider (last_updated_date desc);


drop index if exists prov_etl_batch_id on claim_provider
create nonclustered index prov_etl_batch_id on claim_provider (etl_batch_id);