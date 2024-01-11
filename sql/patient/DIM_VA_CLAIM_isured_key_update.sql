--drop table pit_history.dbo.claim_insured_key
--go

------CREATE TABLE pit_history.dbo.claim_insured_key ONLY ONCE--------------------------------------------------------------------------
SET ANSI_WARNINGS OFF GO
;With CTA as (select row_number() over (partition by member_id order by
  patient_key desc
  ) rn,patient_key, member_id,is_current
	from	dim_patient
	where	patient_key != -1
	and member_id in (
	select member_id from dim_patient
	group by member_id
	having count(member_id) > 1)
)
select a.rn, b.rn rn_new,
claim_key,c.insured_key,
 a.member_id member_id, b.patient_key old_patient_key, b.is_current N_current, a.patient_key new_patient_key, a.is_current Y_current
into pit_history.dbo.claim_insured_key from CTA  a
join CTA b on a.member_id = b.member_id
join dim_va_claim c on c.insured_key = b.patient_key
where b.rn >1 and a.rn=1
order by claim_key

--select count (*) from pit_history.dbo.claim_insured_key
--select top 100 * from pit_history.dbo.claim_insured_key
--select is_current, * from dim_patient where member_id in ('') and patient_key in (,)


-----------------------------------------------------
CREATE CLUSTERED INDEX IDX_CLM_INS
    ON pit_history.dbo.claim_insured_key (claim_key);
GO

CREATE NONCLUSTERED INDEX IDX_NP
    ON pit_history.dbo.claim_insured_key (new_patient_key) ;
GO

CREATE NONCLUSTERED INDEX IDX_OP
    ON pit_history.dbo.claim_insured_key (old_patient_key) ;
GO

----disable trigger every time when run update

disable trigger trg_last_updated_date_dim_va_claim ON DIM_VA_CLAIM;
GO
----start update------------------------------------------------------------------------------------------------------------------------------
begin

begin
declare @batchsize int, @loops int, @i int, @rn int
set @rn=1
set @batchsize = 1000000;
select @loops = max(claim_key)/@batchsize from dim_va_claim;
set @i = 0;
--set @i = (select min(c.claim_key) from dim_va_claim c join pit_history.dbo.claim_insured_key h on c.insured_key= old_patient_key )
while @i <=@loops
begin
--begin tran
	update	c set insured_key = k.new_patient_key
	from	pit_history.dbo.claim_insured_key k join DIM_VA_CLAIM c on c.claim_key = k.claim_key
	where  c.claim_key between @i* @batchsize and (@i+1)*@batchsize
--commit
	set @rn=@i* @batchsize
RAISERROR (' %d keys processed ', 0, 1, @rn) WITH NOWAIT
set @i = @i + 1
end
RAISERROR (' THE END ', 0,1 ) WITH NOWAIT;

end;

enable trigger trg_last_updated_date_dim_va_claim ON DIM_VA_CLAIM;

------validate result-------------------------------------------

select c.claim_key from dim_va_claim c
join pit_history.dbo.claim_insured_key h on c.insured_key= old_patient_key

select count(c.claim_key) from dim_va_claim c
join pit_history.dbo.claim_insured_key h on c.insured_key= old_patient_key

select min(c.claim_key) from dim_va_claim c
join pit_history.dbo.claim_insured_key h on c.insured_key= old_patient_key

select max (c.claim_key) from dim_va_claim c