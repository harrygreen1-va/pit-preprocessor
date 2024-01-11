if object_id('dbo.line_provider', 'u') is not null
    drop table dbo.line_provider;
go
create table dbo.line_provider
(
    line_provider_key bigint identity
        constraint xpkline_provider primary key,
    claim_key int,
    claim_detail_key_pr int,

    claim_id varchar(60) not null,
    source_claim_line_id varchar(60),

    rendering_provider_npi pit_natural_key,
    rendering_provider_name varchar(80),
    rendering_provider_taxonomy pit_long_name,

    etl_batch_id pit_natural_key not null,

    last_updated_date datetime not null default (getdate()),
    last_updated_user varchar(128) not null default (suser_name()),

    constraint lp_fk_claim_key foreign key (claim_key)
        references dim_va_claim
        on delete cascade,

    constraint lp_fk_claim_key_claim_detail_key_pr foreign key (claim_detail_key_pr)
        references f_professional_medical_claim_details (claim_detail_key)
        on delete set null,
)
go

-- indexes:
drop index if exists lp_claim_key on line_provider

create nonclustered index lp_claim_key on line_provider (claim_key)
    include (line_provider_key);

if exists(select name
          from sysindexes
          where name = 'lp_line_key_pr')
    drop index lp_line_key_pr on line_provider
go
create nonclustered index lp_line_key_pr on line_provider (claim_detail_key_pr)
    include (line_provider_key);

if exists(select name
          from sysindexes
          where name = 'lp_claim_id')
    drop index lp_claim_id on line_provider
go
create nonclustered index lp_claim_id on line_provider (claim_id)
    include (line_provider_key);


if exists(select name
          from sysindexes
          where name = 'lp_last_updated_date')
    drop index lp_last_updated_date on line_provider
go
create nonclustered index lp_last_updated_date on line_provider (last_updated_date desc);


if exists(select name
          from sysindexes
          where name = 'lp_etl_batch_id')
    drop index lp_etl_batch_id on line_provider
go
create nonclustered index lp_etl_batch_id on line_provider (etl_batch_id);