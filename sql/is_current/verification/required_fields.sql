select count(child.claim_key), 'dim_va_claim' as test_name
from dim_va_claim child
where child.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
child.start_date is null
or claim_id is null
or db_id is null
or source_system is null
or source_entity is null
--or source_claim_pk is null
or file_row_num is null
or billing_provider_key=-1
or billing_provider_key is null
or patient_key =-1
)

select count(child.claim_key), 'dim_va_claim-CCRS' as test_name
from dim_va_claim child
where child.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)='CCR')
and (
source_claim_PK is null
)


select count(claim.claim_key), 'dim_professional_claim' as test_name
from dim_va_claim claim
join dim_professional_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or total_charges is null
)


select count(claim.claim_key), 'dim_professional_claim-required-pr' as test_name
from dim_va_claim claim
left join dim_professional_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
child.claim_key is null
)


select count(claim.claim_key), 'claim_diagnosis' as test_name
from dim_va_claim claim
join claim_diagnosis child on child.claim_key_pr=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
)

select count(claim.claim_key), 'claim_diagnosis-required-pr' as test_name
from dim_va_claim claim
left join claim_diagnosis child on child.claim_key_pr=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
child.claim_diagnosis_key is null
)

select count(claim.claim_key), 'claim_diagnosis-required-in' as test_name
from dim_va_claim claim
left join claim_diagnosis child on child.claim_key_in=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
child.claim_diagnosis_key is null
)

select count(claim.claim_key), 'claim_procedure' as test_name
from dim_va_claim claim
join claim_procedure child on child.claim_key_pr=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
)


select count(claim.claim_key) as claim_count, 'claim_ambulance' as test_name
from dim_va_claim claim
join claim_ambulance child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
)


select count(claim.claim_key) as claim_count, 'claim_insurance' as test_name
from dim_va_claim claim
join claim_insurance child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or insured_id is null
)

select count(claim.claim_key) as claim_count, 'f_professional_medical_claim_details' as test_name
from dim_va_claim claim
join f_professional_medical_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%')
and (
child.etl_batch_id is null
or child.etl_batch_id != claim.etl_batch_id
or child.source_system!=claim.source_system
or child.is_current!=claim.is_current
or child.start_date is null
or procedure_key is null
or child.file_row_num is null
)

select count(claim.claim_key) as claim_count, 'f_professional_medical_claim_details-required-pr' as test_name
from dim_va_claim claim
left join f_professional_medical_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%' )
and (
child.claim_detail_key is null
)

select count(line.claim_detail_key) as line_count, 'claim_line_diagnosis' as test_name
from f_professional_medical_claim_details line
join claim_line_diagnosis child on child.claim_detail_key=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
or pointer_order is null
)

-- TODO: duplicate pointer order

select count(line.claim_detail_key) as line_count, 'claim_line_modifier' as test_name
from f_professional_medical_claim_details line
join claim_line_modifier child on child.claim_detail_key_pr=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
or child.modifier_code is null
)

select count(line.claim_detail_key) as line_count, 'claim_line_adjudication' as test_name
from f_professional_medical_claim_details line
join claim_line_adjudication child on child.claim_detail_key_pr=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
)

-- required for prof
select count(claim.claim_key) as claim_count, 'f_professional_medical_claim_details' as test_name
from dim_va_claim claim
left join f_professional_medical_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_H%' )
and (
child.claim_detail_key is null
)

-- inst

select count(claim.claim_key), 'dim_institutional_claim'
from dim_va_claim claim
join dim_institutional_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or total_charges is null
or bill_type_key is null
)


select count(claim.claim_key), 'dim_institutional_claim-required'
from dim_va_claim claim
left join dim_institutional_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
child.claim_key is null
)

select count(claim.claim_key) as claim_count, 'f_institutional_medical_claim_details' as test_name
from dim_va_claim claim
join f_institutional_medical_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%' )
and (
child.etl_batch_id is null
or child.etl_batch_id != claim.etl_batch_id
or child.source_system!=claim.source_system
or child.is_current!=claim.is_current
or child.start_date is null
or procedure_key is null
or child.file_row_num is null
)

select count(claim.claim_key) as claim_count, 'f_institutional_medical_claim_details-required' as test_name
from dim_va_claim claim
left join f_institutional_medical_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%' )
and (
child.claim_detail_key is null
)


select count(claim.claim_key), 'claim_condition_code' as test_name
from dim_va_claim claim
join claim_condition_code child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or code is null
)

select count(claim.claim_key), 'claim_occurrence' as test_name
from dim_va_claim claim
join claim_occurrence child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or occurrence_code is null
)


select count(claim.claim_key), 'claim_occurrence_span' as test_name
from dim_va_claim claim
join claim_occurrence_span child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
child.etl_batch_id is null
or child.etl_batch_id != claim.etl_batch_id
or child.source_system!=claim.source_system
or child.is_current!='Y'
or child.source_entity!=claim.source_entity
or child.start_date is null
or child.end_date is not null

or occurrence_span_code is null
or occurrence_span_from is null
)

select count(claim.claim_key), 'claim_value_code' as test_name
from dim_va_claim claim
join claim_value_code child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_U%')
and (
child.etl_batch_id is null
or child.etl_batch_id != claim.etl_batch_id
or child.source_system!=claim.source_system
or child.is_current!='Y'
or child.source_entity!=claim.source_entity
or child.start_date is null
or child.end_date is not null
or value_code is null
)


select count(*), 'claim_line_checksum' as test_name
from etl.claim_line_checksum
where 
etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
 claim_id is null or file_row_num is null or claim_line_id is null
 )
