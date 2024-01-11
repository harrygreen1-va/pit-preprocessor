select top 100 * from claim_batch_log order by end_date_time desc

delete from  batch_id_to_test
insert into batch_id_to_test
select top 4 etl_batch_id 
from claim_batch_log 
order by end_date_time desc

select * from batch_id_to_test
