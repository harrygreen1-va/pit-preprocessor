Prod:
SELECT  dpcd.CLAIM_KEY as CLAIM_KEY_N , dpcd.CLAIM_DETAIL_KEY AS CLAIM_DETAIL_KEY_N , dpcd.source_claim_line_id,fhc.db_id , fhc.claim_id, DPCD.is_current FROM  PITEDR.dbo.F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS  dpcd  INNER JOIN PITEDR.etl.FBCS_HCFA_CLAIMS FHC ON dpcd.claim_key=fhc.claim_key_n WHERE dpcd.IS_CURRENT = 'N'  AND dpcd.SOURCE_ENTITY = 'HCFA'



UPDATE "PITEDR"."dbo"."CLAIM_LINE_DIAGNOSIS" SET "is_current"=@P1,"end_date"=@P2 WHERE "source_entity"=@P3 AND "claim_detail_key"=@P4

---
insert into source_edits (rarc_list, rarc1, rarc2, rarc3, rarc4, rarc5, source_edit_id, carc1_desc, claim_id, carc4,
                          carc5, source_claim_line_id, carc_list, carc1, carc2, etl_batch_id, carc3, last_updated_user,
                          last_updated_date)
values (@p0, @p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, 'VAAUSELAPCI403$',
        getdate())


select diagnosis_key,
       upper(LTRIM(RTRIM(diagnosis_code)))     diagnosis_code,
       upper(ISNULL(LTRIM(RTRIM(version)), 0)) version
from pitedr.dbo.dim_diagnosis_code

select procedure_key, upper(ltrim(rtrim(procedure_code))) procedure_code
from pitedr.dbo.dim_procedure_code
where is_current = 'Y'

select npi, tax_id, provider_name, provider_key
from pitedr.dbo.dim_provider
where npi is not null
  and is_current = 'Y'
order by npi, tax_id


insert into pitedr.etl.fbcs_hcfa_claims(claim_id, db_id, claim_line_id, claim_key, claim_key_n)
values (@p1, @p2, @p3, @p4, @p5)


select patient_key, member_id
from pitedr.dbo.dim_patient
where is_current = 'Y'


insert into "dbo"."DIM_VA_CLAIM"("claim_id", "referring_provider_key", "patient_key", "image_id", "status",
                                 "check_number", "verified_by", "verified", "source_batch_id", "Contract_Claim",
                                 "Reopen_date", "AuthStation_IFN", "is_IPAC", "urgent_care_identifier", "form_type_key",
                                 "billing_provider_key", "station_key", "program_key", "insured_key",
                                 "submission_number", "transaction_type", "vista_id", "status_reason_key", "is_current",
                                 "start_date", "end_date", "source_entity", "row_id", "db_id", "date_of_service",
                                 "obligation_number", "invoice_number", "preauth_number", "verified_date",
                                 "created_date", "claim_type", "signature_date", "etl_batch_id",
                                 "pay_to_provider_taxonomy", "referring_provider_taxonomy", "billing_provider_taxonomy",
                                 "billing_provider_TIN", "Patient_Account_Number", "Memo", "FPPS_ID", "Reopen_reason",
                                 "Consult_IEN", "Care_Category", "va_contract_type", "claim_note", "reopen_claim_id",
                                 "source_system")
values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20,
        @p21, @p22, @p23, @p24, @p25, @p26, @p27, @p28, @p29, @p30, @p31, @p32, @p33, @p34, @p35, @p36, @p37, @p38,
        @p39, @p40, @p41, @p42, @p43, @p44, @p45, @p46, @p47, @p48, @p49, @p50, @p51, @p52) 96 seconds

update dbo.dim_professional_claim
set last_updated_date = GETDATE()
from dbo.dim_professional_claim d
         inner join inserted i on i.claim_key = d.claim_key

update "PITEDR"."dbo"."CLAIM_DIAGNOSIS"
set "is_current"=@p1,
    "end_date"=@p2
where "claim_key_pr" = @p3
  and "source_entity" = @p4 (@P1 varchar(1),@P2 datetime2,@P3 int,@P4 varchar(50))
update "PITEDR"."dbo"."CLAIM_DIAGNOSIS"
set "is_current"=@p1,
    "end_date"=@p2
where "claim_key_pr" = @p3
  and "source_entity" = @p4


insert into "PITEDR"."dbo"."F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS"("claim_key", "pos_key", "preauth_number",
                                                                  "unit_label", "claim_line_score", "service_date_from",
                                                                  "service_date_to", "other_allowable_type",
                                                                  "other_allowable_amount", "vista_allowable_amount",
                                                                  "CMS_allowable_amount", "program_type", "pay_flag",
                                                                  "obligation_number", "Minutes", "Mileage", "position",
                                                                  "rendering_provider_key", "procedure_key",
                                                                  "adjustment_reason_key", "denial_reason_key",
                                                                  "anst_proc_code", "drug_code", "repriced",
                                                                  "repriced_amount", "adjusted", "charge_amt", "unit1",
                                                                  "paid_amt", "date_paid", "start_date",
                                                                  "source_entity", "row_id", "etl_batch_id",
                                                                  "modifier_code", "source_claim_line_id",
                                                                  "vista_line_id", "is_current", "adjusted_amount",
                                                                  "allowable_type", "CMS_allowable_type",
                                                                  "invoice_number", "vista_allowable_type",
                                                                  "rendering_provider_taxonomy", "line_note",
                                                                  "source_system")
values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8, @p9, @p10, @p11, @p12, @p13, @p14, @p15, @p16, @p17, @p18, @p19, @p20,
        @p21, @p22, @p23, @p24, @p25, @p26, @p27, @p28, @p29, @p30, @p31, @p32, @p33, @p34, @p35, @p36, @p37, @p38,
        @p39, @p40, @p41, @p42, @p43, @p44, @p45, @p46)


insert into "PITEDR"."dbo"."CLAIM_DIAGNOSIS"("DIAGNOSIS_key", "source_system", "start_date", "claim_key_pr",
                                             "source_entity", "row_id", "is_current")
values (@p1, @p2, @p3, @p4, @p5, @p6, @p7)

select dpcd.claim_key as claim_key_y, dpcd.claim_detail_key as claim_detail_key_y, dpcd.source_claim_line_id
from pitedr.dbo.f_professional_medical_claim_details dpcd
         inner join pitedr.etl.fbcs_hcfa_claims fhc
                    on dpcd.claim_key = fhc.claim_key and dpcd.source_claim_line_id = fhc.claim_line_id
where dpcd.is_current = 'Y'
  and dpcd.source_entity = 'HCFA'
    22


insert into pitedr.dbo.claim_line_diagnosis(diagnosis_key, start_date, claim_detail_key, pointer_order, source_entity,
                                            row_id, is_current, source_system)
values (@p1, @p2, @p3, @p4, @p5, @p6, @p7, @p8)