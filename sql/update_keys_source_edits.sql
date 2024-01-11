UPDATE edits
SET claim_key=claim.claim_key, claim_detail_key_pr=line.claim_detail_key, last_updated_date=GETDATE(), last_updated_user=SUSER_NAME()
FROM 
source_edits edits
JOIN dim_va_claim claim ON edits.claim_id=claim.claim_id AND edits.etl_batch_id=claim.etl_batch_id
join F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS line on line.claim_key=claim.claim_key and line.source_claim_line_id=edits.source_claim_line_id 
AND edits.etl_batch_id=line.etl_batch_id

UPDATE tblToUpdate
SET claim_key=claim.claim_key, last_updated_date=GETDATE()
FROM 
claim_insurance_raw tblToUpdate
JOIN dim_va_claim claim ON tblToUpdate.claim_id=claim.claim_id AND tblToUpdate.etl_batch_id=claim.etl_batch_id
AND tblToUpdate.etl_batch_id=claim.etl_batch_id


/*
select top 10 line.claim_key, line.claim_detail_key, line.source_system, line.source_claim_line_id, claim.claim_id, line.etl_batch_id, line.last_updated_date
from DIM_VA_CLAIM claim
join F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS line on line.claim_key=claim.claim_key
order by line.last_updated_date desc
*/