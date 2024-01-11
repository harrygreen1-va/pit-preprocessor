drop procedure stg.populate_dupe_candidates
go

create procedure stg.populate_dupe_candidates @eci_id int
as

--declare @eci_id int=5771--5972--5928--5771
--create schema stg

if object_id('tempdb..#eci_claim_line') is not null
    begin
        drop table #eci_claim_line
    end
create table #eci_claim_line
(
    claim_key int,
    claim_detail_key int,
    member_id varchar(20),
    visit_date date,
    statement_from_date date,
    statement_to_date date,
    claim_type varchar(20),
    procedure_code varchar(20)
)

-- inst
insert into #eci_claim_line
select claim.claim_key,
       line.claim_detail_key,
       pat.member_id,
       isnull(line.service_date, claim.date_of_service) as visit_date,
       iclaim.statement_from_date,
       iclaim.statement_to_date,
       'INST',
       null
from dim_va_claim claim
         join dim_patient pat on pat.patient_key = claim.patient_key
         join dim_institutional_claim iclaim on claim.claim_key = iclaim.claim_key
         join claim_batch_log cbl on claim.etl_batch_id = cbl.etl_batch_id
         join f_institutional_medical_claim_details line on line.claim_key = claim.claim_key
where cbl.eci_id = @eci_id
  and claim.is_current = 'y'
  and line.is_current = 'y'
  and line.pay_flag = 'y'
  and (claim.status is null or claim.status not in ('rejected', 'denied'))

-- prof
insert into #eci_claim_line
select claim.claim_key,
       line.claim_detail_key,
       pat.member_id,
       line.service_date_from as visit_date,
       null,
       null,
       'PROF',
       procedure_code
from dim_va_claim claim
         join dim_patient pat on pat.patient_key = claim.patient_key
         join claim_batch_log cbl on claim.etl_batch_id = cbl.etl_batch_id
         join f_professional_medical_claim_details line on line.claim_key = claim.claim_key
         left join dim_procedure_code proc_code on proc_code.procedure_key=line.procedure_key
where cbl.eci_id = @eci_id
  and claim.is_current = 'y'
  and line.is_current = 'y'
  and line.pay_flag = 'y'
  and (claim.status is null or claim.status not in ('rejected', 'denied'))
  and line.service_date_from is not null

create nonclustered index eci_claim_line_member_id on #eci_claim_line (claim_key, claim_type, member_id)
    include (visit_date, statement_to_date, statement_from_date, procedure_code);

delete from stg.dupe_candidate
where eci_id=@eci_id and claim_type='INST'

insert into stg.dupe_candidate(claim_detail_key, claim_key, member_id, eci_id, claim_type)
select matching_line.claim_detail_key, matching_claim.claim_key, pat.member_id, @eci_id, 'INST'
from dim_va_claim matching_claim
         join dim_patient pat on pat.patient_key = matching_claim.patient_key and pat.is_current = 'y'
         join #eci_claim_line eci_claim_line on eci_claim_line.member_id = pat.member_id
         join f_institutional_medical_claim_details matching_line on matching_line.claim_key = matching_claim.claim_key
         join dim_institutional_claim matching_iclaim on matching_claim.claim_key = matching_iclaim.claim_key
where
  -- common conditions for all dupe rules
  matching_line.is_current = 'y'
  and matching_line.pay_flag = 'y'
  and (matching_claim.status is null or matching_claim.status not in ('rejected', 'denied'))
  and (
        -- op
        (
                eci_claim_line.claim_type = 'INST' and
                eci_claim_line.visit_date is not null and
                isnull(matching_line.service_date, matching_claim.date_of_service) = eci_claim_line.visit_date and
                eci_claim_line.claim_detail_key != matching_line.claim_detail_key

        )
        or
        -- regular ip based on the statement date
        (
                eci_claim_line.claim_type = 'INST' and
                eci_claim_line.claim_detail_key != matching_line.claim_detail_key and
                (
                                eci_claim_line.statement_from_date >= matching_iclaim.statement_from_date and
                                eci_claim_line.statement_from_date < matching_iclaim.statement_to_date
                        or
                                eci_claim_line.statement_from_date < matching_iclaim.statement_from_date and
                                eci_claim_line.statement_to_date > matching_iclaim.statement_from_date
                )
        )
        or
        -- matching prof with inst
        (
                eci_claim_line.claim_type = 'PROF' and
                eci_claim_line.visit_date>matching_iclaim.statement_from_date and
                eci_claim_line.visit_date<matching_iclaim.statement_to_date 
        )

    )



delete from stg.dupe_candidate
where eci_id=@eci_id and claim_type='PROF'

/*
and the service date from of this claim line data is the service date from of claimLineToScore 
and the procedure code of this claim line data is the procedure code of claimLineToScore
*/

insert into stg.dupe_candidate(claim_detail_key, claim_key, member_id, eci_id, claim_type)
select matching_line.claim_detail_key, matching_claim.claim_key, pat.member_id, @eci_id, 'PROF'
from dim_va_claim matching_claim
         join dim_patient pat on pat.patient_key = matching_claim.patient_key and pat.is_current = 'y'
         join #eci_claim_line eci_claim_line on eci_claim_line.member_id = pat.member_id
         join f_professional_medical_claim_details matching_line on matching_line.claim_key = matching_claim.claim_key
         join dim_procedure_code proc_code on proc_code.procedure_key=matching_line.procedure_key
where
  matching_line.is_current = 'y'
  and matching_line.pay_flag = 'y'
  and (matching_claim.status is null or matching_claim.status not in ('rejected', 'denied'))

  and eci_claim_line.claim_type = 'PROF'
  and eci_claim_line.visit_date = matching_line.service_date_from
  and eci_claim_line.procedure_code=proc_code.procedure_code
    

go
