
-- Prep work

if object_id('#$SQL_DB#.etl.all_resubm_claims_HCFA', 'u') is not null
    drop table #$SQL_DB#.etl.all_resubm_claims_HCFA;

--Select all claims with resubmission number suffix
;with CTA as (
select claim_id,reopen_claim_id,claim_key,  created_date,  end_date, is_current , source_claim_PK
from #$SQL_DB#.dbo.DIM_VA_CLAIM WITH (NOLOCK)
where len(claim_id)  > 19  and db_id = 'CCRS' and source_entity = 'HCFA'
)

select a.* into #$SQL_DB#.etl.all_resubm_claims_HCFA  from 
(
-- All claims that have a reopened claim id
select claim_id,reopen_claim_id,claim_key,  created_date,  end_date, is_current , source_claim_PK from CTA where reopen_claim_id != ''
UNION
-- all reopened claims
select claim_id,reopen_claim_id,claim_key,  created_date,  end_date, is_current , source_claim_PK from CTA
where claim_id in (select reopen_claim_id from CTA)
)a;



if object_id('#$SQL_DB#.etl.chain_history_HCFA', 'u') is not null 
    drop table #$SQL_DB#.etl.chain_history_HCFA;

-- add new roots
---insert into chain_history
-- All claims that have not been reopened
select  claim_id as root,  claim_id, reopen_claim_id, claim_key,  created_date, end_date, is_current,source_claim_PK  
into #$SQL_DB#.etl.chain_history_HCFA  from #$SQL_DB#.etl.all_resubm_claims_HCFA c  where 
NOT EXISTS (SELECT claim_key
                   FROM   #$SQL_DB#.etl.all_resubm_claims_HCFA s
                   WHERE  c.reopen_claim_id = s.claim_id);

				   

-- All claims reopened claim id points to 
while 1 = 1 begin
  insert into #$SQL_DB#.etl.chain_history_HCFA(root,claim_id, reopen_claim_id, claim_key,  created_date, end_date, is_current, source_claim_PK) 
  select distinct h.root,c.claim_id, c.reopen_claim_id, c.claim_key,  c.created_date, c.end_date, c.is_current, c.source_claim_PK  from #$SQL_DB#.etl.all_resubm_claims_HCFA c
  join #$SQL_DB#.etl.chain_history_HCFA h on c.reopen_claim_id = h.claim_id
  where 
  NOT EXISTS (SELECT claim_key
                   FROM   #$SQL_DB#.etl.chain_history_HCFA s
                   WHERE  c.claim_id = s.claim_id)
  if ROWCOUNT_BIG()  = 0 break
end;


-- Read chain and order according to the order in the chain. The top claim is current
select cast (a.rn as int) as rn, a.claim_key, a.claim_id , a.reopen_claim_id, 
a.created_date, 
 cast(a.is_current as char)  as is_current, a.root as chain, cast ((iif(a.rn=1, 'Y', 'N') ) as char)  as new_is_current
from 
(
select	row_number() over (partition by  root
	order by created_date desc,reopen_claim_id desc ,source_claim_PK desc, claim_key desc) rn, root,
claim_key,  claim_id, reopen_claim_id, created_date,  is_current
from  #$SQL_DB#.etl.chain_history_HCFA 
) a
where a.is_current != cast (iif(a.rn=1, 'Y', 'N') as CHAR) 
;
