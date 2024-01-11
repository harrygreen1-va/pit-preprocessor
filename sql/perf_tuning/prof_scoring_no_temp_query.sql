/*
select top 200 * from claim_batch_log
where etl_batch_id like 'ccn%'
and eci_id=5359
order by last_updated_date desc
*/
--CCNNC_H191030052836

DECLARE @IdTable IdListType

insert into @idTable
select distinct top 500 member_id
from dim_patient pat
where 
patient_key in (
select patient_key from dim_va_claim
where etl_batch_id='CCNNC_H191030052836' 
and patient_key is not null
order by claim_key)

-- 22679
/*
select count(distinct member_id)
from dim_patient pat
join dim_va_claim c on c.patient_key=pat.patient_key
where c.etl_batch_id='CCNNC_H191030052836'
*/

IF OBJECT_ID('tempdb..#ClaimLineKeys') IS NOT NULL 
BEGIN
    DROP TABLE #ClaimLineKeys
END

CREATE TABLE #ClaimLineKeys(
    id INT
)

declare @earliestVisitServiceDate DATE

-- find the earliest date

 
select  @earliestVisitServiceDate=min(fcd.service_date_from)
from  F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS fcd
where  fcd.etl_batch_id='CCNNC_H191030052836'

/*
insert into #ClaimLineKeys
select fcd.claim_detail_key
from  F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS fcd
join DIM_VA_CLAIM vac on vac.claim_key = fcd.claim_key
join DIM_PATIENT pat on pat.patient_key = vac.patient_key
join @idTable pIds on pIds.id = pat.member_id
where fcd.is_current = 'Y'
*/

print 'Start of the query'
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
-- the long running query
select * from vw_professional_claim
join @idTable pids on pids.id = patient_id
where visit_date >= @earliestVisitServiceDate 
/*
claim_line_number in (
    select fcd.claim_detail_key
    from  f_professional_medical_claim_details fcd
        join dim_va_claim vac on vac.claim_key = fcd.claim_key
        join dim_patient pat on pat.patient_key = vac.patient_key
        join @idTable pids on pids.id = pat.member_id
    where fcd.is_current = 'y')
*/
select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]

/*
1000
2:18, 95152 rows

500
0:21, 48236 rows

without temp table and without ex. plan
500
1000:
0:18 96644

*/
