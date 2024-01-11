select top 10 *
from claim_batch_log
order by last_updated_date desc

select count(claim_key)
from dim_va_claim
where etl_batch_id = 'CCNNC_H200625150151'
  and is_current = 'y'
-- 27421


select count(claim_detail_key)
from f_professional_medical_claim_details
where etl_batch_id = 'CCNNC_H200625150151'
  and is_current = 'y'

-- 48263