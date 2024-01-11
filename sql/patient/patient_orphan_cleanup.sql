-- drop table pit_history.dbo.patient_orphan

select patient_key
--into pit_history.dbo.patient_orphan
from dim_patient

EXCEPT
(
select dp.patient_key from DIM_PATIENT dp join dbo.DIM_VA_CLAIM c on c.patient_key=dp.patient_key
union
select dp.patient_key from DIM_PATIENT dp join dbo.DIM_VA_CLAIM c on c.insured_key=dp.patient_key
union
select dp.patient_key from DIM_PATIENT dp join dbo.DIM_PHARMACY_CLAIM f on f.patient_key=dp.patient_key
union
select dp.patient_key from DIM_PATIENT dp join pmt.DIM_VA_PAYMENT p on p.patient_key=dp.patient_key
union
select dp.patient_key from DIM_PATIENT dp join pmt.DIM_VA_PAYMENT p on p.sponsor_key=dp.patient_key
union
select dp.patient_key from DIM_PATIENT dp join pmt.DIM_VA_PAYMENT p on p.insured_key=dp.patient_key
union
------

 --select count (*) from DIM_PATIENT where is_current = 'N'
-- select count (*) from pit_history.dbo.patient_orphan


begin tran
delete top (1000000) from DIM_PATIENT where is_current = 'N'
commit

select * from DIM_PATIENT where patient_key in (select patient_key from pit_history.dbo.patient_orphan )
