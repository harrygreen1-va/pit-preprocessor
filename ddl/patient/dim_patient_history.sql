
use pit_history
go

if object_id('dbo.dim_patient_history', 'u') is not null
     drop table dbodim_patient_history;
 go

create table dim_patient_history
(
        history_key  pit_key  identity(1,1) constraint XPK_PATIENT_HISTORY primary key not null,
        patient_key  pit_key  not null,
        fee_id_card_number  pit_natural_key  null,
        ssn  pit_short_name  null,
        member_id  pit_natural_key  null,
        last_name   varchar (50) null,
        first_name   varchar (50) null,
        middle_initial   char (1) null,
        date_of_birth  pit_date  null,
        sex   char (1) null,
        marital_status   varchar (25) null,
        employed   char (1) null,
        is_veteran   char (1) null,
        is_dependent   char (1) null,
        address1   varchar (1000) null,
        address2   varchar (1000) null,
        city   varchar (500) null,
        state_code   varchar (2) null,
        state  pit_name  null,
        employer_name  pit_long_name  null,
        postal_code  pit_zip  null,
        country_code  pit_3_char  null,
        country   varchar (100) null,
        latitude  pit_geo  null,
        longitude  pit_geo  null,
        main_phone   varchar (15) null,
        home_phone   varchar (50) null,
        work_phone   varchar (50) null,
        evening_phone   varchar (15) null,
        morning_phone   varchar (15) null,
        mobile_phone   varchar (50) null,
        email   varchar (50) null,
        is_current   char (1) not null,
        start_date  pit_date  null,
        end_date  pit_end_date  null,
        source_system   varchar (20) null,
        source_entity   varchar (50) null,
        row_id   varchar (20) null,
        insured_id  pit_natural_key  null,
        vista_patient_id  pit_natural_key  null,
        etl_batch_id  pit_natural_key  null,
        last_updated_date   datetime  null,
        last_updated_user   varchar (128) null,
        cpe_elig_reason   varchar (255) null,
        cpe_inelig_reason   varchar (255) null,
        cpe_patient_status   varchar (20) null,
        cpe_hicn   varchar (20) null,
        is_valid_address   char (1) null,
        medicaid_number   varchar (50) null,
        period_of_service   varchar (50) null,
        percent_service_connect   varchar (20) null,
        is_agent_orange_exposure   char (1) null,
        last_service_entry_date   date  null,
        last_service_sep_date   date  null,
        source_id   varchar (20) null,
        date_of_death   date  null,
        address3  pit_address  null,
        is_sponsor  pit_boolean  null,
        cpe_file_num   varchar (8) null,
        icn   varchar (20) null,
        icn_status   varchar (20) null,
        mvi_check_date_time   datetime2 (7) null,
        is_updated_from_mvi   bit  null,
        mvi_connection_error   varchar (max) null,
        mvi_connection_error_date_time   datetime2 (7) null,
        history_last_updated_date   datetime  null,
        history_last_updated_user   varchar (128) null
)

alter table dim_patient_history  add  constraint  dh_ph_last_updated_date   default (getdate()) for  history_last_updated_date

go

alter table dim_patient_history  add  constraint  dh_ph_last_updated_user   default (suser_name()) for  history_last_updated_user

go

if exists (select name from sysindexes where name = 'IDX_PH_PAT_KEY')
    drop index  IDX_PH_PAT_KEY on  dim_patient_history
go
create nonclustered index IDX_PH_PAT_KEY on dim_patient_history (patient_key)
    include (history_key);

if exists (select name from sysindexes where name = 'IDX_PAT_MEMBER_ID')
    drop index  IDX_PAT_MEMBER_ID on dim_patient_history
go
create nonclustered index IDX_PAT_MEMBER_ID on dim_patient_history (member_id)
    include (history_key);





 