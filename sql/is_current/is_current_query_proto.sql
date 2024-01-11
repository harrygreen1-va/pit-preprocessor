select top 100 * from claim_batch_log where end_date_time is not null order by last_updated_date desc--number_of_conformant_rows desc
/*
CCRS_H220822152348 -- 300000
CCRS_U201120092712  -- 1948891 292692 319659
*/

--CCNNC_U200224111036
/*
select count(claim.claim_key)
from dim_va_claim claim
where etl_batch_id='CCNNC_U200224140142'
*/

-- status
-- feed date
-- PK
select claim.claim_key, claim.claim_id, claim.source_claim_pk, claim.created_date, cbl.feed_date, claim.is_current, claim.etl_batch_id, claim.status,
(case when claim.status='paid' then 10 when claim.status='approved' then 0 else 3 end)
from dim_va_claim claim
join dim_va_claim claim_batch on claim_batch.claim_id=claim.claim_id and claim_batch.db_id=claim.db_id and claim_batch.source_entity =claim.source_entity
join claim_batch_log cbl on cbl.etl_batch_id=claim.etl_batch_id
where claim_batch.etl_batch_id='CCRS_U201120092712' 
order by claim.claim_id, claim.created_date desc, claim.source_claim_pk desc, cbl.feed_date desc, 
(case when claim.status='paid' then 10 when claim.status='approved' then 0 else 3 end) desc,
claim_key desc

-- CCRS query
select claim.claim_key,
    claim.source_claim_pk as claim_id,
    claim.created_date,
    cbl.feed_date,
    claim.is_current,
    claim.etl_batch_id,
    claim.status,
(case when claim.status='paid' then 10 when claim.status='approved' then 0 else 3 end)
from dim_va_claim claim
join dim_va_claim claim_batch on claim_batch.source_claim_PK=claim.source_claim_PK and claim_batch.source_system=claim.source_system and claim_batch.source_entity =claim.source_entity
join claim_batch_log cbl on cbl.etl_batch_id=claim.etl_batch_id
where claim_batch.etl_batch_id='CCRS_U201120092712'
order by claim.source_claim_pk, claim.created_date desc,
(case when claim.status='paid' then 10 when claim.status='approved' then 0 else 3 end) desc,
    cbl.feed_date desc,
claim_key desc
-- 12/07/2021 is the latest null PK

-- order by created_date desc, reopen_claim_id desc, source_claim_PK desc, claim_key desc
select top 20 * from etl.is_current_log where etl_batch_id='CCRS_U201120092712' --and claim_id='1025531' order by claim_id

select claim_key, claim_id, source_claim_pk, is_current, last_updated_date, created_date, etl_batch_id  from dim_va_claim where source_claim_PK='1025531' order by claim_key desc