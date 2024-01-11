
exec stg.populate_dupe_candidates 5773

select * from stg.dupe_candidate
where eci_id=5771
and claim_type='prof'

select distinct  srri.claim_key, srri.claim_detail_key
from 
score_reason_related_items srri
join score_reason sr on srri.claim_score_key=sr.claim_score_key
where sr.eci_id=5771
and 
sr.is_current='y'
and (
--sr.reason_code like 'rl0042%'
sr.reason_code like 'rl0043%'
-- or sr.reason_code like 'rl00750%'
)

select distinct  sr.claim_detail_key, srri.claim_key as matching_claim_key, srri.claim_detail_key as matching_claim_detail_key
from 
score_reason_related_items srri
join score_reason sr on srri.claim_score_key=sr.claim_score_key
left join stg.dupe_candidate dupe on srri.claim_detail_key=dupe.claim_detail_key
where sr.eci_id=5771
and 
sr.is_current='y'
and (
--sr.reason_code like 'rl0042%'
sr.reason_code like 'rl0043%'
--or sr.reason_code like 'rl00750%'
)
and 
dupe.dupe_candidate_key is null
--and 
--srri.claim_detail_key=481766910
-- missing for ip
/*
missing for ip
336921067	495836261
missing for op 134
497246341	337584909	496970725
327033164	481766913
*/

select matching_line.is_current, matching_claim.status, pay_flag, service_date, matching_line.etl_batch_id, cbl.eci_id from 
f_institutional_medical_claim_details matching_line
join claim_batch_log cbl on cbl.etl_batch_id=matching_line.etl_batch_id
join dim_va_claim matching_claim on matching_line.claim_key = matching_claim.claim_key
where matching_line.claim_detail_key in (497246341,496970725)


select * from stg.dupe_candidate
where claim_key=327033164