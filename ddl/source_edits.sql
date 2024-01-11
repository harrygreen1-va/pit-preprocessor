if object_id('source_edits', 'U') is not null
    drop table source_edits;
go

create table source_edits
(
    source_edits_key bigint identity primary key,
    claim_key int,
    claim_detail_key_in int,
    claim_detail_key_pr int,

    claim_id varchar(60) not null,
    source_claim_line_id varchar(60),
    source_edit_id varchar(60),

    -- EditCode
    carc_list pit_description,

    -- EditSubCode
    rarc_list pit_description,
    carc1 pit_short_name,
    carc1_desc varchar(max),

    carc2 pit_short_name,
    carc3 pit_short_name,
    carc4 pit_short_name,
    carc5 pit_short_name,

    rarc1 pit_short_name,
    rarc2 pit_short_name,
    rarc3 pit_short_name,
    rarc4 pit_short_name,
    rarc5 pit_short_name,


    last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate(),

    etl_batch_id pit_natural_key not null,

    constraint se_fk_claim_key foreign key (claim_key)
        references dim_va_claim
        on delete cascade,

    constraint se_fk_claim_key_claim_detail_key_pr foreign key (claim_detail_key_pr)
        references f_professional_medical_claim_details(claim_detail_key)
        on delete set null,

    constraint se_fk_claim_key_claim_detail_key_in foreign key (claim_detail_key_in)
        references f_institutional_medical_claim_details(claim_detail_key)
        on delete set null,
);
go
-- indexes:
if exists(select name
          from sysindexes
          where name = 'se_claim_key')
    drop index se_claim_key on source_edits
go
create nonclustered index se_claim_key on source_edits (claim_key)
    include (source_edits_key);

if exists(select name
          from sysindexes
          where name = 'se_line_key_pr')
    drop index se_line_key_pr on source_edits
go
create nonclustered index se_line_key_pr on source_edits (claim_detail_key_pr)
    include (source_edits_key);

if exists(select name
          from sysindexes
          where name = 'se_line_key_in')
    drop index se_line_key_in on source_edits
go
create nonclustered index se_line_key_in on source_edits (claim_detail_key_in)
    include (source_edits_key);

if exists(select name
          from sysindexes
          where name = 'se_batch_id')
    drop index se_batch_id on source_edits
go
create nonclustered index se_batch_id on source_edits (etl_batch_id);

if exists(select name
          from sysindexes
          where name = 'se_claim_id')
    drop index se_claim_id on source_edits
go
create nonclustered index se_claim_id on source_edits (claim_id);

if exists(select name
          from sysindexes
          where name = 'se_line_id')
    drop index se_line_id on source_edits
go
create nonclustered index se_line_id on source_edits (source_claim_line_id);

if exists(select name
          from sysindexes
          where name = 'se_carc_list')
    drop index se_carc_list on source_edits
go
create nonclustered index se_carc_list on source_edits (carc_list);

if exists(select name
          from sysindexes
          where name = 'se_rarc_list')
    drop index se_rarc_list on source_edits
go
create nonclustered index se_rarc_list on source_edits (rarc_list);

if exists(select name
          from sysindexes
          where name = 'se_last_updated_date')
    drop index se_last_updated_date on source_edits
go
create nonclustered index se_last_updated_date on source_edits (last_updated_date desc);
