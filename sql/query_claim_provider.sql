select top 1000 cp.*
from claim_provider cp
join claim_batch_log cbl on cp.etl_batch_id=cbl.etl_batch_id
where 
cbl.eci_id=5016
and claim_key=null
order by cp.last_updated_date desc

select count(cp.claim_provider_key)
from claim_provider cp
join claim_batch_log cbl on cp.etl_batch_id=cbl.etl_batch_id
where 
cbl.eci_id=5016
--and claim_key=null


select top 1000 lp.*
from line_provider lp
join claim_batch_log cbl on lp.etl_batch_id=cbl.etl_batch_id
where cbl.eci_id=5016
and (claim_key=null or claim_detail_key_pr is null)
order by lp.last_updated_date desc

select count(lp.line_provider_key)
from line_provider lp
join claim_batch_log cbl on lp.etl_batch_id=cbl.etl_batch_id
where cbl.eci_id=5016


select top 1000 *
from line_provider
--where claim_key is null
order by last_updated_date desc


select top 100 *
from claim_batch_log
--where etl_batch_id = 'R4V5_H190627141305'
order by last_updated_date desc

select count(claim_detail_key) from f_professional_medical_claim_details
where etl_batch_id= 'R4V5_H190627141305'

select count ( claim_detail_key_pr) from line_provider where etl_batch_id = 'CCNNC_H190625005804'