select top 1 *
from claim_batch_log
order by last_updated_date desc

select top 2 *
from claim_provider
order by claim_provider_key desc
select top 2 *
from source_edits
order by source_edits_key desc

select top 2 *
from claim_patient
order by claim_patient_key desc
select top 2 *
from claim_insurance_raw
order by claim_insurance_raw_key desc
-- all fiels are populated, file row num is populated
select top 5 *
from line_provider
order by line_provider_key desc
select top 5 *
from treasury_payment
order by treasury_payment_key desc