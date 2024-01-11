-- to get the latest batch_id for files (for postprocessor)
/*
select etl_batch_id, file_name from claim_batch_log cbl where
file_name in ('') and etl_batch_id in 
(select top 1 etl_batch_id from claim_batch_log cbl_latest
where cbl_latest.file_name=cbl.file_name
and batch_status != 'in process'
order by last_updated_date desc)
*/
select top 3000 etl_batch_id, batch_status, to_score_indicator, source_system, eci_id, feed_date,start_date_time, end_date_time, file_name, number_of_rows, number_of_conformant_rows, error_text
from claim_batch_log
--where etl_batch_id like 'CC%'
--where eci_id=4929 
order by start_date_time desc

select feed_date, source_system as form, count(etl_batch_id) as number_of_batches
from claim_batch_log
where eci_id=4929
group by feed_date, source_system


select count(*) from f_institutional_medical_claim_details
where etl_batch_id='R3V9LX_U190520233958'


select * from f_professional_medical_claim_details
where etl_batch_id='CCNNC_H190517225904'