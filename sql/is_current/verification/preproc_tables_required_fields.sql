
 -- tables populated by preprocessor
select count(*), 'source_edits-in' as test_name
--from dim_va_claim claim
from source_edits 
where etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
claim_detail_key_in is null
or claim_key is null
or file_row_num is null
or claim_id is null
or source_claim_line_id  is null
)

select count(*), 'source_edits-pr' as test_name
--from dim_va_claim claim
from source_edits 
where etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
claim_detail_key_in is null
or claim_key is null
or file_row_num is null
or claim_id is null
or source_claim_line_id  is null
)


select count(*), 'line_provider-pr' as test_name
from line_provider
where etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
claim_detail_key_pr is null
or claim_key is null
or file_row_num is null
or claim_id is null
or source_claim_line_id  is null
)

-- TODO: counts: matches line count

select count(*), 'claim_patient' as test_name
from claim_patient
where etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
claim_key is null
or file_row_num is null
or claim_id is null
)

-- TODO: counts: matches claim_count

select count(*), 'claim_provider' as test_name
from claim_provider
where etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
claim_key is null
or file_row_num is null
or claim_id is null
)

-- claim_insurance_raw

select count(*), 'claim_insurance_raw' as test_name
from claim_insurance_raw
where etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
claim_key is null
or file_row_num is null
or claim_id is null
)