select count(claim.claim_key), 'dim_dental_claim' as test_name
from dim_va_claim claim
join dim_dental_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_D')
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
)


select count(claim.claim_key), 'dim_dental_claim-required' as test_name
from dim_va_claim claim
left join  dim_dental_claim child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_D%')
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

select count(claim.claim_key) as claim_count, 'claim_tooth_status' as test_name
from dim_va_claim claim
join claim_tooth_status child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != claim.etl_batch_id
or coalesce(child.source_system,'') != claim.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != claim.source_entity
or child.start_date is null
or child.end_date is not null
or tooth_number is null
or tooth_status is null
)

select count(claim.claim_key) as claim_count, 'f_dental_claim_details' as test_name
from dim_va_claim claim
join f_dental_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
child.etl_batch_id is null
or child.etl_batch_id != claim.etl_batch_id
or child.source_system!=claim.source_system
or child.is_current!=claim.is_current
or child.start_date is null
or procedure_key is null
or procedure_key=-1
)

select count(claim.claim_key) as claim_count, 'f_dental_claim_details-required-pr' as test_name
from dim_va_claim claim
left join f_dental_claim_details child on child.claim_key=claim.claim_key
where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where etl_batch_id like '%_D%' )
and (
child.claim_detail_key is null
)

select count(line.claim_detail_key) as line_count, 'claim_line_diagnosis' as test_name
from f_dental_claim_details line
join claim_line_diagnosis child on child.claim_detail_key_dt=line.claim_detail_key
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
from f_dental_claim_details line
join claim_line_modifier child on child.claim_detail_key_dt=line.claim_detail_key
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
from f_dental_claim_details line
join claim_line_adjudication child on child.claim_detail_key_dt=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
)

select count(line.claim_detail_key) as line_count, 'claim_line_oral_cavity' as test_name
from f_dental_claim_details line
join claim_line_oral_cavity child on child.claim_detail_key=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
)


select count(line.claim_detail_key) as line_count, 'claim_line_tooth_surface' as test_name
from f_dental_claim_details line
join claim_line_tooth_surface child on child.claim_detail_key=line.claim_detail_key
where line.etl_batch_id in (select etl_batch_id from batch_id_to_test)
and (
coalesce(child.etl_batch_id,'')  != line.etl_batch_id
or coalesce(child.source_system,'') != line.source_system
or coalesce(child.is_current,'') != 'Y'
or coalesce(child.source_entity,'') != line.source_entity
or child.start_date is null
or child.end_date is not null
)

-- TODO: claim line oral cavity, claim_line_tooth surface

-- current
select count(*) as claim_id_count, 'one-is-current'
from
(
select claim.claim_id
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
)
and claim.is_current='Y'
group by claim.claim_id
having count(claim.claim_key)>1 
) as claims

select count(claim.claim_key), 'non-current-end-date-null' as test_name
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
)
and claim.is_current='N' and end_date is null


select count(claim.claim_key), 'current-end-date-not-null' as test_name
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test)
)
and claim.is_current='Y' and end_date is not null
