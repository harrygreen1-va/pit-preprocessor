
--drop index prof_line_service_date_from on F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS 
CREATE NONCLUSTERED INDEX prof_line_service_date_from
ON [dbo].[F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS] ([is_current],[pay_flag],[service_date_from])
INCLUDE ([claim_detail_key],[claim_key],[procedure_key]) on Third
GO

CREATE NONCLUSTERED INDEX claim_status
ON dim_va_claim ([is_current],[status], etl_batch_id)
INCLUDE (claim_key, patient_key) on Third
GO

-- to run:
CREATE NONCLUSTERED INDEX inst_line_pay_flag
ON [dbo].[F_INSTITUTIONAL_MEDICAL_CLAIM_DETAILS] ([is_current],[pay_flag])
INCLUDE ([claim_detail_key],[claim_key],[service_date])  on Third
go


CREATE NONCLUSTERED INDEX claim_current_batch_id
ON [dbo].[DIM_VA_CLAIM] ([is_current],[etl_batch_id])
INCLUDE ([claim_key],[patient_key],[status]) on Third
go

CREATE NONCLUSTERED INDEX dupe_eci_id_claim_type
ON [stg].[dupe_candidate] ([eci_id],[claim_type])
INCLUDE ([dupe_candidate_key])



--truncate table [stg].[dupe_candidate]