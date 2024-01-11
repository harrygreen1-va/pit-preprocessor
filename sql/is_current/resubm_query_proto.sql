--alter table dim_va_claim add prior_claim_key varchar(60)
-- alter table etl.is_current_log add next_submission_claim_key bigint
-- ALTER TABLE [dbo].[DIM_VA_CLAIM] ADD source_system_prior_claim_key bigint ;

select top 10000 claim.claim_key, claim.claim_id, claim.source_claim_PK,claim.etl_batch_id as updated_etl_batch_id, resub_claim.claim_key as resub_claim_key, resub_claim.etl_batch_id as updating_etl_batch_id
from dim_va_claim claim
join dim_va_claim resub_claim on claim.source_claim_pk=CAST(resub_claim.source_system_prior_claim_key AS varchar)
join claim_batch_log cbl on resub_claim.etl_batch_id=cbl.etl_batch_id
where claim.is_current='Y'
and claim.source_system='CCRS' and resub_claim.source_system='CCRS'
and cbl.batch_status='TERMINAL'
-- date should be a parameter
and cbl.end_date_time>='2022-01-01'
-- update newer claims first
order by claim.claim_key desc




-- dedup by claim.claim_key
-- join with is_current log
-- naming of the new columns
-- indexes
-- second query -- make sure that the latest in the chain is current
-- this should work for a batch or for the chunk based on date
