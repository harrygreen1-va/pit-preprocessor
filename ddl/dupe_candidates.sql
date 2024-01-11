--create schema stg

if object_id('stg.dupe_candidate', 'u') is not null
    drop table stg.dupe_candidate;
go

create table stg.dupe_candidate
(

    dupe_candidate_key bigint identity
        constraint xp_dupe primary key,
    claim_key int not null,
    claim_detail_key int not null,
    eci_id int not null,
    -- inst, prof
    claim_type varchar(20) not null,
    member_id varchar(20) not null,

    last_updated_user varchar(128) default (suser_name()),
    last_updated_date datetime default (getdate()),

)


-- indexes:
if exists(select name
          from sys.indexes
          where name = 'dupe_member_id')
    drop index dupe_member_id on stg.dupe_candidate
go
create nonclustered index dupe_member_id on stg.dupe_candidate (eci_id, member_id)
    include (claim_detail_key) on Third;

if exists(select name
          from sys.indexes
          where name = 'dupe_last_updated_date')
    drop index dupe_last_updated_date on stg.dupe_candidate
go
create nonclustered index dupe_last_updated_date on stg.dupe_candidate (last_updated_date)
    include (eci_id, member_id) on Third;


if exists(select name
          from sys.indexes
          where name = 'dupe_eci_id')
    drop index dupe_eci_id on stg.dupe_candidate
go
create nonclustered index dupe_eci_id on stg.dupe_candidate (eci_id) on Third;
