
DROP INDEX IF EXISTS [idx_dim_va_claim_prior_claim_key] ON [dbo].[DIM_VA_CLAIM]

CREATE NONCLUSTERED INDEX idx_dim_va_claim_prior_claim_key ON DIM_VA_CLAIM (source_system_prior_claim_key) 
include (source_system, is_current, etl_batch_id, source_claim_PK)

--DROP INDEX IF EXISTS dvc_source_claim_pk_source_entity_source_system ON DIM_VA_CLAIM


CREATE NONCLUSTERED INDEX dvc_source_claim_pk_resubm ON DIM_VA_CLAIM(source_claim_PK)
INCLUDE(claim_key, source_system_prior_claim_key, source_system, is_current,etl_batch_id,claim_id)

CREATE NONCLUSTERED INDEX dvc_source_system_resubm
ON [dbo].[DIM_VA_CLAIM] ([source_system])
INCLUDE ([etl_batch_id],[source_system_prior_claim_key])

/*
select top 10 claim.claim_key, claim.claim_id, claim.etl_batch_id as updated_etl_batch_id, resub_claim.source_system_prior_claim_key, resub_claim.etl_batch_id as updating_etl_batch_id
from dim_va_claim claim
join dim_va_claim resub_claim on claim.source_claim_pk=resub_claim.source_system_prior_claim_key
join claim_batch_log cbl on resub_claim.etl_batch_id=cbl.etl_batch_id
where claim.is_current='Y'
and claim.source_system='CCRS' and resub_claim.source_system='CCRS'
and cbl.batch_status='TERMINAL'
-- date should be a parameter
and cbl.end_date_time>='2023-01-01'
-- update newer claims first
order by claim.claim_key desc
*/