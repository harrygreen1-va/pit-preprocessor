select top 100 * from DIM_PATIENT
where patient_key=-1
order by patient_key desc

select min(patient_key)
from DIM_PATIENT
where is_current='y'


select top 10 claim_key, source_system, source_entity, db_id, etl_batch_id, last_updated_date, is_current from DIM_VA_CLAIM
order by claim_key desc

--INSERT INTO PITEDR.etl.FBCS_HCFA_CLAIMS(claim_id,db_id,claim_line_id,claim_key,claim_key_n) VALUES(@P1,@P2,@P3,@P4,@P5)
--INSERT INTO PITEDR.dbo.CLAIM_LINE_DIAGNOSIS(diagnosis_key,start_date,claim_detail_key,pointer_order,source_entity,row_id,is_current,source_system) VALUES(@P1,@P2,@P3,@P4,@P5,@P6,@P7,@P8)
select diagnosis_key, cld.claim_detail_key, pointer_order, cld.row_id--, cld.last_updated_date, cld.is_current
from CLAIM_LINE_DIAGNOSIS cld
join F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS line on cld.claim_detail_key=line.claim_detail_key
where line.etl_batch_id='CCNNC_H200430181519' 
and line.is_current='n'
order by cld.last_updated_date desc
--CCNNC_H200625150151 -- from preprod 
--CCNNC_H200430181519
select top 10 diagnosis_key, cld.claim_detail_key, pointer_order, cld.row_id, cld.last_updated_date, cld.last_updated_user
from CLAIM_LINE_DIAGNOSIS cld
order by cld.last_updated_date desc

select top 100 *
from CLAIM_BATCH_LOG
where etl_batch_id like 'CCNNC_H191030052836'
order by last_updated_date desc
-- CCNNC_H200430181519 CCNN-ClaimsToScore-HCFA-CCNNC-20200430.txt
-- CCNNC_H191030052836

