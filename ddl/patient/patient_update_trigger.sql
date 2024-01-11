 

 

create trigger dbo.trg_update_dim_patient on dbo.DIM_PATIENT

with execute as caller

after update

as

begin

  set nocount on

   if ( update (last_updated_date))

   insert  into pit_history.dbo.dim_patient_history

   (

  

       patient_key,

       fee_id_card_number,

       ssn,

       member_id,

       last_name,

       first_name,

       middle_initial,

       date_of_birth,

       sex,

       marital_status,

       employed,

       is_veteran,

       is_dependent,

       address1,

       address2,

       city,

       state_code,

       state ,

       employer_name,

       postal_code,

       country_code,

       country,

       latitude,

       longitude,

       main_phone,

       home_phone,

       work_phone,

       evening_phone,

       morning_phone,

       mobile_phone,

       email,

       is_current,

       start_date ,

       end_date,

       source_system,

       source_entity,

       row_id,

       insured_id ,

       vista_patient_id,

       etl_batch_id,

       last_updated_date,

       last_updated_user,

       cpe_elig_reason,

       cpe_inelig_reason,

       cpe_patient_status ,

       cpe_hicn,

       is_valid_address,

       medicaid_number,

       period_of_service,

       percent_service_connect,

       is_agent_orange_exposure,

       last_service_entry_date,

       last_service_sep_date,

       source_id,

       date_of_death,

       ADDRESS3,

       IS_SPONSOR,

       CPE_FILE_NUM,

       ICN,

       ICN_status,

       mvi_check_date_time,

       is_updated_from_mvi,

       mvi_connection_error,

       mvi_connection_error_date_time

       )

   select

    d.patient_key,

       d.fee_id_card_number,

       d.ssn,

       d.member_id,

       d.last_name,

       d.first_name,

       d.middle_initial,

       d.date_of_birth,

       d.sex,

       d.marital_status,

       d.employed,

       d.is_veteran,

       d.is_dependent,

       d.address1,

       d.address2,

       d.city,

       d.state_code,

       d.state ,

       d.employer_name,

       d.postal_code,

       d.country_code,

       d.country,

       d.latitude,

       d.longitude,

       d.main_phone,

       d.home_phone,

       d.work_phone,

       d.evening_phone,

       d.morning_phone,

       d.mobile_phone,

       d.email,

       d.is_current,

       d.start_date ,

       d.end_date,

       d.source_system,

       d.source_entity,

       d.row_id,

       d.insured_id ,

       d.vista_patient_id,

       d.etl_batch_id,

       d.last_updated_date,

       d.last_updated_user,

       d.cpe_elig_reason,

       d.cpe_inelig_reason,

       d.cpe_patient_status ,

       d.cpe_hicn,

       d.is_valid_address,

       d.medicaid_number,

       d.period_of_service,

       d.percent_service_connect,

       d.is_agent_orange_exposure,

       d.last_service_entry_date,

       d.last_service_sep_date,

       d.source_id,

       d.date_of_death,

       d.ADDRESS3,

       d.IS_SPONSOR,

       d.CPE_FILE_NUM,

       d.ICN,

       d.ICN_status,

       d.mvi_check_date_time,

       d.is_updated_from_mvi,

       d.mvi_connection_error,

       d.mvi_connection_error_date_time

      

   from deleted d

   inner join  dbo.dim_patient p on d.patient_key = p.patient_key


end

go

 

 

 