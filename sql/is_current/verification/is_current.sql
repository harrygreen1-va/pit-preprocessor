-- current CCRS
select count(*) as claim_id_count, 'one-is-current'
from
(
select claim.source_claim_PK
from dim_va_claim claim
where claim.source_claim_PK in (
    select source_claim_PK from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)='CCR')
)
and claim.is_current='Y'
group by claim.source_claim_PK
having count(claim.claim_key)>1 
) as claims

select count(claim.claim_key), 'non-current-end-date-null' as test_name
from dim_va_claim claim
where claim.source_claim_PK in (
    select source_claim_PK from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)='CCR')
)
and claim.is_current='N' and end_date is null


select count(claim.claim_key), 'current-end-date-not-null' as test_name
from dim_va_claim claim
where claim.source_claim_PK in (
    select source_claim_PK from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)='CCR')
)
and claim.is_current='Y' and end_date is not null


-- non CCRS
select count(*) as claim_id_count, 'one-is-current'
from
(
select claim.claim_id
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)!='CCR')
)
and claim.is_current='Y'
group by claim.claim_id
having count(claim.claim_key)>1 
) as claims

select count(claim.claim_key), 'non-current-end-date-null' as test_name
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)!='CCR')
)
and claim.is_current='N' and end_date is null


select count(claim.claim_key), 'current-end-date-not-null' as test_name
from dim_va_claim claim
where claim.claim_id in (
    select claim_id from dim_va_claim claim_batch where claim.etl_batch_id in (select etl_batch_id from batch_id_to_test where left(etl_batch_id, 3)!='CCR')
)
and claim.is_current='Y' and end_date is not null

