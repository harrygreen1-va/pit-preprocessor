declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
--1
select top 100*
from dim_va_claim
select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]

------------------------------2-----------------------------------------------------
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
--2
select top 100*
from claim_batch_log
order by last_updated_date desc
select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]

---------------------------------3-------------------------------------------
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()

select provider.provider_id,
       convert(char(6), line.date_of_service, 112) yyyymm,
       'PHAR'                                      claim_type,
       'PHAR'                                      load_type,
       'service'                                   date_type,
       dbo.infer_source_system(claim.etl_batch_id) source_system,
       prog.program_name,
       sum(isnull(line.gross_amount_due, 0))       billed_amount,
       sum(isnull(null, 0))                        paid_amount,
       count(distinct claim.claim_key)             claim_cnt,
       count(line.claim_detail_key)                line_cnt
from.f_pharmacy_claim_details line
    join.dim_va_claim claim
on claim.claim_key = line.claim_key
    join dim_provider provider
    on claim.billing_provider_key = provider.provider_key
    left join dim_va_program prog
    on prog.program_key = claim.program_key
where line.is_current = 'Y'
  and claim.billing_provider_key is not null
  and claim.source_system <> 'CDW'
  and line.date_of_service is not null
  and line.date_of_service >= dateadd(month
    , -36
    , getdate())
group by
    provider_id,
    convert (char (6), line.date_of_service, 112),
    dbo.infer_source_system(claim.etl_batch_id),
    prog.program_name


select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]


---------------------------4-------------------------------------------

declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
--4
begin tran
    update top (100) dim_va_claim set is_current = is_current, last_updated_date = getDate()

    select @endtime = GETDATE()
    select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]
commit
------------------------------5----------------------------
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
--5
begin tran
    update top (1000) pmt.dim_va_payment set is_current = is_current, last_updated_date = getDate()

    select @endtime = GETDATE()
    select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]
commit

------------------------------6----------------------------
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()

update pmt.f_inpatient_payment_details
set is_current = 'N',
    end_date= GETDATE()
from (
         select pmt.dim_va_payment.payment_key
         from etl.stg_cpe_conformed
                  inner join pmt.dim_va_payment
                             on etl.stg_cpe_conformed.clm_id = pmt.dim_va_payment.claim_id
     ) pmt_key
where pmt.f_inpatient_payment_details.payment_key = pmt_key.payment_key
  and pmt.f_inpatient_payment_details.is_current = 'Y';

select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]


---------------------7------------------------------

declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()

truncate table etl.stg_cpe_conformed

select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]

-------------------------------------8,9,10,11
---update temp_prf_test set last_updated_user = 'Perf_Test' 
----------------------------------------------------------------
---select top 200000 * into temp_prf_test from dim_va_claim 

declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
begin
    declare @rc int = 0

    while @rc < 100 begin

        begin tran
            insert into dim_va_claim (claim_id,
                                      form_type_key,
                                      billing_provider_key,
                                      referring_provider_key,
                                      station_key,
                                      program_key,
                                      patient_key,
                                      insured_key,
                                      submission_number,
                                      billing_id_qualifier,
                                      transaction_type,
                                      image_id,
                                      patient_account_number,
                                      status_reason_key,
                                      status,
                                      claim_score,
                                      is_current,
                                      start_date,
                                      end_date,
                                      source_system,
                                      source_entity,
                                      row_id,
                                      db_id,
                                      etl_batch_id,
                                      invoice_number,
                                      check_number,
                                      date_of_service,
                                      signature_date,
                                      obligation_number,
                                      received_date,
                                      reviewed_date,
                                      preauth_number,
                                      other_insurance,
                                      verified_by,
                                      verified_date,
                                      verified,
                                      created_date,
                                      claim_type,
                                      source_batch_id,
                                      vista_id,
                                      last_updated_user,
                                      completed_date,
                                      cost_share,
                                      deductible,
                                      taxonomy,
                                      cpe_pdi_number,
                                      cpe_clm_allowed_amount,
                                      fpps_id,
                                      billing_provider_taxonomy)

            select top 100 claim_id,
                           form_type_key,
                           billing_provider_key,
                           referring_provider_key,
                           station_key,
                           program_key,
                           patient_key,
                           insured_key,
                           submission_number,
                           billing_id_qualifier,
                           transaction_type,
                           image_id,
                           patient_account_number,
                           status_reason_key,
                           status,
                           claim_score,
                           is_current,
                           start_date,
                           end_date,
                           source_system,
                           source_entity,
                           row_id,
                           db_id,
                           etl_batch_id,
                           invoice_number,
                           check_number,
                           date_of_service,
                           signature_date,
                           obligation_number,
                           received_date,
                           reviewed_date,
                           preauth_number,
                           other_insurance,
                           verified_by,
                           verified_date,
                           verified,
                           created_date,
                           claim_type,
                           source_batch_id,
                           vista_id,
                           last_updated_user,
                           completed_date,
                           cost_share,
                           deductible,
                           taxonomy,
                           cpe_pdi_number,
                           cpe_clm_allowed_amount,
                           fpps_id,
                           billing_provider_taxonomy


            from temp_prf_test

            select @rc = @rc + 1
        commit


--------
    end;
    raiserror ('Update', 0, 100000) with nowait
    select @endtime = GETDATE()
    select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]
end;
go

----12
declare @starttime datetime
declare @endtime datetime
select @starttime = GETDATE()
--12
delete
from dim_va_claim
where last_updated_user = 'Perf_Test'
select @endtime = GETDATE()
select DATEDIFF(ms, @starttime, @endtime) as [Duration in millisecs]