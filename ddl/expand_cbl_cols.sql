alter table claim_batch_log alter column file_name varchar(120);
alter table claim_batch_log
    add file_size int;