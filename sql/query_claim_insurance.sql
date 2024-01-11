select cbl.file_name, cbl.etl_batch_id, cbl.batch_status, ins.* from  claim_insurance_raw ins
join claim_batch_log cbl on cbl.etl_batch_id=ins.etl_batch_id
where ins.etl_batch_id='CCNNC_H190411112144'
order by ins.last_updated_date desc

select * from dim_va_claim 
where etl_batch_id='CCNNC_H190411112144'


select distinct claim_id from  claim_insurance_raw ins
join claim_batch_log cbl on cbl.etl_batch_id=ins.etl_batch_id
where ins.etl_batch_id='CCNNC_H190411112144'






delete ins from claim_insurance_raw ins 
join claim_batch_log cbl on cbl.etl_batch_id=ins.etl_batch_id
where cbl.batch_status='in process'

select * from dim_va_claim 
where etl_batch_id='CCNNC_H190315201434'
order by claim_id desc 

select distinct claim_id, is_current, source_system from dim_va_claim 
where etl_batch_id='CCNNC_H190411112144'
order by claim_id desc 

--select top * from claim_batch_log

select distinct claim_id, db_id from etl.FBCS_HCFA_CLAIMS
/*
 update dim_va_claim set is_current='N'
 where etl_batch_id='CCNNC_H190315201434'

 update dim_professional_claim set is_current='N'
 where etl_batch_id='CCNNC_H190315201434'

*/
