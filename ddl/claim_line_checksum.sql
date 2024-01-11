drop table if exists etl.claim_line_checksum;

create table etl.claim_line_checksum
(

    claim_line_checksum_key bigint identity constraint claim_line_checksum_pk primary key,

    claim_id varchar(60) not null,
    claim_line_id varchar(30) null,
    file_row_num int,

    checksum varchar(5000) not null,
    raw_fields varchar(max) not null,

    last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate(),

    etl_batch_id pit_natural_key not null,

)


drop index if exists cls_etl_batch_id on etl.claim_line_checksum;
create nonclustered index cls_etl_batch_id on etl.claim_line_checksum (etl_batch_id) include (claim_id, claim_line_id);
drop index if exists cls_claim_line_id on etl.claim_line_checksum;
create nonclustered index cls_claim_line_id on etl.claim_line_checksum (claim_line_id) include (claim_id, etl_batch_id);