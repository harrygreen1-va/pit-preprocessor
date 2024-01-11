-------dim_pharmacy_claim PATIENT_KEY UPDATE-----------------------------------------------------------------------------------------------------------------
--drop table pit_history.dbo.dim_pharmacy_patient_key


------CREATE TABLE pit_history.dbo.dim_pharmacy_patient_key ONLY ONCE--------------------------------------------------------------------------
SET ANSI_WARNINGS OFF
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
claim_key,c.patient_key,
 a.member_id member_id, b.patient_key old_patient_key, b.is_current N_current, a.patient_key new_patient_key, a.is_current Y_current
into pit_history.dbo.dim_pharmacy_patient_key from CTA  a
join CTA b on a.member_id = b.member_id
join dim_pharmacy_claim c on c.patient_key = b.patient_key
where b.rn >1 and a.rn=1
order by claim_key

GO
--select count (*) from pit_history.dbo.dim_pharmacy_patient_key
--select top 100 * from pit_history.dbo.dim_pharmacy_patient_key
--select is_current, * from dim_patient where member_id in ('001085188') and patient_key in (2354443,45112754)

--------------------------------------------------------------------------------------------------------------------------------------------
CREATE CLUSTERED INDEX IX_PHR_PATK
    ON pit_history.dbo.dim_pharmacy_patient_key (claim_key);
GO

CREATE NONCLUSTERED INDEX IDX_PHR_PATK_NP
    ON pit_history.dbo.dim_pharmacy_patient_key (new_patient_key) ;
GO

CREATE NONCLUSTERED INDEX IDX_PHR_PATK_OP
    ON pit_history.dbo.dim_pharmacy_patient_key (old_patient_key) ;
GO
---------start update---------------------------------------------------------------------------------------------------------

begin
	declare @batchsize int, @loops int, @i int, @rn int
	set @rn=1
	set @batchsize = 1000000
	select @loops = max(claim_key)/@batchsize from dim_pharmacy_claim
	set @i = 0
	---set @i = (select min(p.claim_key)/@batchsize from dim_pharmacy_claim p join pit_history.dbo.dim_pharmacy_patient_key h on p.patient_key = h.old_patient_key)
while @i <=@loops
begin

--begin tran
	update c set c.patient_key = h.new_patient_key
	from dim_pharmacy_claim c
	join pit_history.dbo.dim_pharmacy_patient_key h  on h.claim_key= c.claim_key
	where  c.claim_key between @i* @batchsize and (@i+1)*@batchsize
--commit

	set @rn=@i* @batchsize
	RAISERROR (' %d keys processed ', 0, 1, @rn) WITH NOWAIT
	set @i = @i + 1
end
	RAISERROR (' THE END ', 0,1 ) WITH NOWAIT;
end;


------validate result-------------------------------------------
--select p.claim_key from dim_pharmacy_claim p
--join pit_history.dbo.dim_pharmacy_patient_key h on p.patient_key = h.old_patient_key

--select min (p.claim_key)from dim_pharmacy_claim p
--join pit_history.dbo.dim_pharmacy_patient_key h on p.patient_key = h.old_patient_key

--select * from pit_history.dbo.dim_pharmacy_patient_key where claim_key = 28459775
--select count(*) from dim_pharmacy_claim
--select max(claim_key) from dim_pharmacy_claim
