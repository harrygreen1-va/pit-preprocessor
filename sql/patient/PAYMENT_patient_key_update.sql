-------DIM_VA_PAYMENT PATIENT_KEY UPDATE-----------------------------------------------------------------------------------------------------------------
--drop table pit_history.dbo.dim_va_payment_patient_key


--------CREATE TABLE pit_history.dbo.dim_va_payment_patient_key ONLY ONCE-----------------------------------------------------------------------------------

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
payment_key,c.patient_key,
 a.member_id member_id, b.patient_key old_patient_key, b.is_current N_current, a.patient_key new_patient_key, a.is_current Y_current
into pit_history.dbo.dim_va_payment_patient_key from CTA  a
join CTA b on a.member_id = b.member_id
join pmt.dim_va_payment c on c.patient_key = b.patient_key
where b.rn >1 and a.rn=1
order by payment_key

GO
--select * from dim_patient where member_id is null and is_current = 'N'
--select * from pit_history.dbo.dim_va_payment_patient_key where member_id  is null
--select count (*) from pit_history.dbo.dim_va_payment_patient_key


--select top 100 * from pit_history.dbo.dim_va_payment_patient_key
---- 189281068
--------------------------------------------------------------------------------------------------------------------------------------------
CREATE CLUSTERED INDEX IX_PMT_PATK
    ON pit_history.dbo.dim_va_payment_patient_key (payment_key);
GO


CREATE NONCLUSTERED INDEX IDX__DVP_PATK_NP
    ON pit_history.dbo.dim_va_payment_patient_key (new_patient_key) ;
GO
-- Disable patient_key constraint -----------------------------------------------------------------------------------------------------

ALTER TABLE pmt.dim_va_payment NOCHECK CONSTRAINT fk_dvp_patient_key
GO
-------------------------------------------------------------------------------------------------------------------------------------------
begin
	declare @batchsize int, @loops int, @i int, @rn int
	set @rn=1
	set @batchsize = 1000000
	select @loops = max(payment_key)/@batchsize from pmt.dim_va_payment
	set @i = 0

while @i <=@loops
begin

--begin tran
	update	pmt.dim_va_payment set patient_key = k.new_patient_key
	from	pit_history.dbo.dim_va_payment_patient_key k
	where   pmt.dim_va_payment.payment_key = k.payment_key
	and  pmt.dim_va_payment.payment_key between @i* @batchsize and (@i+1)*@batchsize
--commit

	RAISERROR (' %d mln key processed ', 0, 1, @i) WITH NOWAIT
	set @i = @i + 1
end
	RAISERROR (' THE END ', 0,1 ) WITH NOWAIT;
end;

-- Enable patient_key constraint

ALTER TABLE pmt.dim_va_payment WITH CHECK CHECK CONSTRAINT fk_dvp_patient_key

------validate
select p.payment_key from pmt.dim_va_payment p
join pit_history.dbo.dim_va_payment_patient_key h on p.patient_key = h.old_patient_key

select min (p.payment_key) from pmt.dim_va_payment p
join pit_history.dbo.dim_va_payment_patient_key h on p.patient_key = h.old_patient_key

select max (payment_key) from pmt.dim_va_payment