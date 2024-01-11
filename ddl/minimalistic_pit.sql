use pitedr_testing

create type pit_boolean from char(1) not null
go

create type pit_date from date
go
create type pit_datestamp from datetime
go

create type pit_description from varchar(255)
go

create type pit_key from int
go

create type pit_long_name from varchar(50)
go

create type pit_money from money
go

create type pit_name from varchar(30)
go

create type pit_natural_key from varchar(20)
go

create type pit_short_name from varchar(15)
go

create type pit_zip from varchar(10)
go

create type pit_phone from varchar(15)
go

create type pit_address from varchar(50)
go

create type pit_1_char from char(1)
go


create type pit_3_char from varchar(3)
go

create type pit_2_char from varchar(2)
go

create type pit_long_number from bigint
go

create type pit_geo from decimal(9, 6)
go

create type pit_int from int
go

create type pit_end_date from date
go

create type pit_email from varchar(30)
go



create table dropzone_log
(
    dropzone_log_key bigint identity
        constraint pk_dropzone_log
            primary key,
    file_path varchar(200),
    file_name varchar(200) not null,
    file_timestamp datetime2,
    file_size int,
    when_arrived datetime2,
    feed_date date,
    archive_url varchar(200),
    is_missing bit default 0 not null,
    is_latest_missing bit default 0 not null,
    source_system varchar(20),
    form_type varchar(20),
    payment_status varchar(50),
    line_count int,
    claim_count int,
    paid_amount money,
    billed_amount money,
    missing_days_count int,
    last_updated_date datetime2 default getdate() not null,
    last_updated_user varchar(128) default suser_name() not null,
    is_source_system_missing bit default 0 not null
)
go

create index dropzone_log_last_updated_date
    on dropzone_log (last_updated_date) include (file_name)
go

create table etl_cst_interface
(
    eci_id pit_key identity
        constraint xpketl_cst_interface
            primary key,
    eci_status varchar(30),
    last_updated_date datetime default getdate(),
    last_updated_user varchar(128) default suser_name()
)
go

create sequence claim_batch_log_cxm_seq start with 1
go
create sequence claim_batch_log_ccrs_seq start with 1
go
create sequence claim_batch_log_ccnnc_seq start with 1
go

create table claim_batch_log
(
    etl_batch_id pit_natural_key not null
        constraint xpkclaim_batch_log
            primary key,
    batch_status pit_name,
    to_score_indicator char,
    source_system varchar(20),
    last_updated_date datetime
        constraint df_claim_batch_log_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_batch_log_last_updated_user default suser_name(),
    eci_id int
        constraint fk_cbl_eci_id
            references etl_cst_interface,
    feed_date pit_date,
    start_date_time pit_datestamp,
    end_date_time pit_datestamp,
    file_name pit_long_name,
    number_of_rows int,
    number_of_conformant_rows int,
    number_of_lines_written_to_outbound_file int,
    error_text varchar(max)
)
go

create index idx_cbl_eci_id
    on claim_batch_log (eci_id)
go

create table dim_patient
(
    patient_key pit_key identity
        constraint xpkdim_patient
            primary key,
    fee_id_card_number pit_natural_key,
    ssn pit_short_name,
    member_id pit_natural_key,
    last_name pit_name,
    first_name pit_name,
    middle_initial char,
    date_of_birth pit_date,
    sex char,
    marital_status pit_short_name,
    employed char,
    is_veteran char,
    is_dependent char,
    address1 varchar(1000),
    address2 varchar(1000),
    city varchar(500),
    state_code varchar(2),
    state pit_name,
    employer_name pit_long_name,
    postal_code varchar(50),
    country_code pit_3_char,
    country pit_name,
    latitude pit_geo,
    longitude pit_geo,
    main_phone varchar(15),
    home_phone varchar(15),
    work_phone varchar(15),
    evening_phone varchar(15),
    morning_phone varchar(15),
    mobile_phone varchar(15),
    email pit_email,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    insured_id pit_natural_key,
    vista_patient_id pit_natural_key,
    etl_batch_id pit_natural_key
        constraint fk_dp_etl_batch_id
            references claim_batch_log,
    last_updated_date datetime
        constraint df_dim_patient_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_patient_last_updated_user default suser_name(),
    date_deceased date,
    cpe_elig_reason varchar(255),
    cpe_inelig_reason varchar(255),
    cpe_patient_status varchar(10),
    cpe_hicn varchar(20),
    mvi_check_date_time datetime2,
    is_updated_from_mvi bit,
    mvi_connection_error varchar(max),
    mvi_connection_error_date_time datetime2,
    icn varchar(20),
    icn_status varchar(20),
    date_of_death date,
    address3 pit_address,
    is_sponsor char
)
go

create index idx_dpa_member_id
    on dim_patient (member_id)
go

create table dim_patient_status
(
    patient_status_key pit_key identity
        constraint xpkdim_patient_status
            primary key,
    code pit_short_name,
    description pit_description,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_patient_status_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_patient_status_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go

create table dim_taxonomy
(
    taxonomy_key pit_key identity (1,1) not for replication not null,
    taxonomy_code varchar(15) null,
    taxonomy_description varchar(255) null,
    type varchar(100) null,
    speciality_code varchar(20) null,
    speciality varchar(100) null,
    version varchar(20) null,
    vista_code pit_2_char null,
    is_current char(1) not null,
    start_date pit_date null,
    end_date pit_end_date null,
    source_system varchar(20) null,
    source_entity varchar(200) null,
    row_id varchar(20) null,
    last_updated_date datetime null,
    last_updated_user varchar(128) null,
    grouping varchar(300) null,
    classification varchar(100) null,
    specialization varchar(300) null,
    definition varchar(max) null,
    notes varchar(max) null,
    is_new bit null,
    is_inactive bit null,
    create_date date null,
    inactivate_date date null,
    changes varchar(1000) null,
    constraint xpkdim_taxonomy primary key clustered (taxonomy_key)
)
go

alter table dim_taxonomy
    add constraint df_dim_taxonomy_last_updated_date default (getdate()) for last_updated_date
go

alter table dim_taxonomy
    add constraint df_dim_taxonomy_last_updated_user default (suser_name()) for last_updated_user
go



create table dim_provider
(
    provider_key pit_long_number identity
        constraint xpkdim_provider
            primary key,
    in_oig_list char,
    tax_id varchar(20),
    npi pit_natural_key,
    source_id pit_natural_key,
    vista_id pit_natural_key,
    cpe_id pit_natural_key,
    small_business char,
    multiple_location char,
    inspected char,
    accredited char,
    certified_medicaid char,
    certified_medicare char,
    medicare_id pit_natural_key,
    is_current char not null,
    start_date pit_date default getdate(),
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    provider_name varchar(500),
    provider_type_name pit_long_name,
    provider_type_description pit_description,
    vista_type_code pit_short_name,
    provider_first_name varchar(120),
    provider_last_name varchar(120),
    provider_middle_name varchar(50),
    etl_batch_id varchar(20)
        constraint fk_dpr_etl_batch_id
            references claim_batch_log,
    last_updated_date datetime
        constraint df_dim_provider_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_provider_last_updated_user default suser_name(),
    row_id pit_natural_key,
    upin_id varchar(20),
    provider_entity_type char,
    nppes_last_updated_date date,
    npi_check_date_time datetime2,
    is_npi_valid bit,
    npi_validation_error varchar(max),
    invalid_npi_reason varchar(30),
    nppes_connection_error varchar(max),
    nppes_connection_error_date_time datetime2,
    tax_id_check_date_time datetime2,
    is_tax_id_valid bit,
    chva_create_date date,
    is_tpa_ccn bit,
    chva_row_id bigint,
    dea_number varchar(20),
    npi_deactivation_date date,
    npi_reactivation_date date,
    provider_enumeration_date date,
    provider_last_update_date date
)
go

create index idx_dp_npi
    on dim_provider (npi)
go

create index idx_is_current_npi
    on dim_provider (is_current, npi) include (provider_key, provider_last_name)
go

create index idx_is_current_tax_id
    on dim_provider (is_current, tax_id) include (provider_key, provider_last_name)
go

create index xie1_dim_provider
    on dim_provider (etl_batch_id)
go

create table provider_location
(
    provider_location_key pit_key identity,
    provider_key pit_long_number
        constraint fk_pl_provider_key
            references dim_provider,
    address1 pit_address,
    address2 pit_address,
    city pit_name,
    state_short_name pit_2_char,
    state_long_name pit_name,
    postal_code pit_zip,
    country_code pit_3_char,
    country varchar(50),
    latitude pit_geo,
    longitude pit_geo,
    provider_phone pit_phone,
    provider_fax pit_phone,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id pit_natural_key,
    last_updated_date datetime
        constraint df_provider_location_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_provider_location_last_updated_user default suser_name(),
    cpe_add_code varchar(2),
    cpe_specialty_code varchar(10),
    cpe_facility_type varchar(10),
    purpose varchar(40),
    to_delete bit,
    address_type char(1)
)
go

create table provider_taxonomy
(
    taxonomy_key pit_key null,
    provider_taxonomy_key pit_key identity (1,1) not for replication not null,
    provider_key pit_long_number null,
    is_current char(1) not null,
    start_date pit_date null,
    end_date pit_end_date null,
    source_system varchar(20) null,
    source_entity varchar(50) null,
    row_id pit_natural_key null,
    last_updated_date datetime null,
    last_updated_user varchar(128) null,
    primary_taxonomy_flag pit_boolean null,
    to_delete bit null,
    license varchar(255) null,
    state varchar(20) null,
    taxonomy_code varchar(20) null,
    taxonomy_desc varchar(max) null,
    constraint xpkprovider_taxonomy primary key clustered (provider_taxonomy_key)
)
go

alter table provider_taxonomy
    add constraint df_provider_taxonomy_last_updated_date default (getdate()) for last_updated_date
go

alter table provider_taxonomy
    add constraint df_provider_taxonomy_last_updated_user default (suser_name()) for last_updated_user
go

alter table provider_taxonomy
    with check add constraint fk_pt_provider_key foreign key (provider_key)
        references dim_provider (provider_key)
go

alter table provider_taxonomy
    check constraint fk_pt_provider_key
go

alter table provider_taxonomy
    with check add constraint fk_pt_taxonomy_key foreign key (taxonomy_key)
        references dim_taxonomy (taxonomy_key)
go

alter table provider_taxonomy
    check constraint fk_pt_taxonomy_key
go



create table dim_claim_form_type
(
    form_type_key pit_key identity
        constraint xpkdim_claim_form_type
            primary key,
    form_code pit_short_name,
    form_name pit_name,
    form_type_description pit_description,
    version varchar(15),
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_claim_form_type_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_claim_form_type_last_updated_user default suser_name()
)
go

create table dim_va_station
(
    station_key int identity
        constraint xpkdim_va_station
            primary key,
    station_id varchar(15),
    visn_id varchar(20),
    address1 varchar(50),
    address2 varchar(50),
    address3 varchar(50),
    city varchar(50),
    state_code varchar(2),
    state_name varchar(30),
    postal_code varchar(10),
    country_code varchar(3),
    country_name varchar(30),
    latitude decimal(9, 6),
    longitude decimal(9, 6),
    phone varchar(15),
    fax varchar(15),
    is_current char not null,
    start_date date,
    end_date date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    station_name varchar(80),
    visn_name varchar(80),
    server_id varchar(20),
    last_updated_date datetime
        constraint df_dim_va_station_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_va_station_last_updated_user default suser_name()
)
go

create table dim_va_program
(
    program_key pit_key identity
        constraint xpkdim_va_program
            primary key,
    program_name varchar(50),
    description pit_description,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_va_payer_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_va_payer_last_updated_user default suser_name(),
    short_name pit_short_name
)
go

create table dim_status_reason
(
    status_reason_key pit_key identity
        constraint xpkdim_status_reason
            primary key,
    status_reason_code varchar(50),
    status_reason pit_description,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    status_description varchar(255),
    last_updated_date datetime
        constraint df_dim_status_reason_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_status_reason_last_updated_user default suser_name()
)
go

create table dim_va_claim
(
    claim_key pit_key identity
        constraint xpkdim_va_claim
            primary key,
    claim_id varchar(60),
    form_type_key int
        constraint fk_dvc_form_type_key
            references dim_claim_form_type,
    billing_provider_key pit_long_number
        constraint fk_dvc_billing_provider_key
            references dim_provider,
    referring_provider_key pit_long_number
        constraint fk_dvc_referring_provider_key
            references dim_provider,
    station_key int
        constraint fk_dvc_station_key
            references dim_va_station,
    program_key int
        constraint fk_dvc_program_key
            references dim_va_program,
    patient_key int
        constraint fk_dvc_patient_key
            references dim_patient,
    insured_key int
        constraint fk_dvc_insured_key
            references dim_patient,
    submission_number varchar(50),
    billing_id_qualifier varchar(3),
    billing_provider_tin varchar(20),
    transaction_type pit_short_name,
    image_id pit_natural_key,
    patient_account_number varchar(50),
    status_reason_key int
        constraint fk_dvc_status_reason_key
            references dim_status_reason,
    status varchar(30),
    claim_score pit_int,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    db_id varchar(20),
    etl_batch_id pit_natural_key
        constraint fk_dvc_etl_batch_id
            references claim_batch_log,
    invoice_number pit_natural_key,
    check_number pit_natural_key,
    date_of_service pit_date,
    signature_date pit_date,
    obligation_number pit_natural_key,
    received_date pit_date,
    reviewed_date pit_date,
    preauth_number varchar(100),
    other_insurance char,
    verified_by pit_short_name,
    verified_date pit_date,
    verified char,
    created_date pit_date,
    claim_type char,
    source_batch_id varchar(30),
    vista_id varchar(20),
    last_updated_date datetime
        constraint df_dim_va_claim_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_va_claim_last_updated_user default suser_name(),
    completed_date date,
    cost_share money,
    deductible money,
    taxonomy varchar(20),
    cpe_pdi_number varchar(30),
    cpe_clm_allowed_amount money,
    fpps_id pit_natural_key,
    billing_provider_taxonomy pit_long_name,
    pay_to_provider_taxonomy pit_long_name,
    referring_provider_taxonomy pit_long_name,
    clearinghouse_trace_number varchar(50),
    original_reference_number varchar(50),
    frequency_code varchar(2),
    claim_note varchar(500),
    reopen_claim_id varchar(65),
    urgent_care_identifier varchar(100),
    source_claim_pk varchar(30),
    service_facility_key bigint,
    service_facility_zip varchar(20),
    va_contract_type varchar(50),
    contract_claim pit_1_char,
    reopen_date date,
    reopen_reason varchar(50),
    end_dating_id varchar(50),
    referral_number varchar(50)
)
go

create index idx_date_of_service
    on dim_va_claim (date_of_service)
go

create index idx_dvc_billing_provider
    on dim_va_claim (billing_provider_key)
go

create index idx_dvc_claim
    on dim_va_claim (claim_id) include (claim_key, form_type_key, billing_provider_key, patient_key, date_of_service)
go

create index idx_dvc_patient
    on dim_va_claim (patient_key)
go


create table dim_admission_source
(
    admission_source_key pit_key identity
        constraint xpkdim_admission_source
            primary key,
    code varchar(20),
    new_born char,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    description varchar(255),
    last_updated_date datetime
        constraint df_dim_admission_source_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_admission_source_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go

create table dim_admission_type
(
    admission_key pit_key identity
        constraint xpkdim_admission_type
            primary key,
    ub_admission_code pit_short_name,
    hl7_admission_code pit_short_name,
    code_extended_name varchar(50),
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_admission_type_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_admission_type_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go

create table dim_bill_type
(
    bill_type_key pit_key identity
        constraint xpkdim_bill_type
            primary key,
    bill_type_code pit_natural_key,
    type_of_facility pit_description,
    bill_classification pit_description,
    classification_type pit_description,
    frequency pit_description,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(200),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_bill_type_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_bill_type_last_updated_user default suser_name(),
    etl_batch_id varchar(50),
    change_date date,
    is_valid bit,
    bill_designation varchar(20),
    description varchar(max)
)
go

create table dim_procedure_code
(
    procedure_key pit_key identity
        constraint xpkdim_procedure_code
            primary key,
    procedure_code varchar(20),
    long_description varchar(max),
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_procedure_code_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_procedure_code_last_updated_user default suser_name(),
    short_description varchar(255),
    full_description varchar(max),
    is_valid bit,
    invalid_reason varchar(255),
    code_start_date date,
    code_end_date date,
    created_date datetime2,
    is_header bit,
    version varchar(20),
    procedure_code_type varchar(20),
    etl_batch_id varchar(50)
)
go

create table dim_place_of_service
(
    pos_key pit_key identity
        constraint xpkdim_place_of_service
            primary key,
    pos_description pit_description,
    pos_code pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    pos_name pit_long_name,
    cpe_pos pit_short_name,
    etl_batch_id varchar(50),
    last_updated_date datetime
        constraint df_dim_place_of_service_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_place_of_service_last_updated_user default suser_name()
)
go

create table dim_diagnosis_code
(
    diagnosis_key pit_key identity
        constraint xpkdim_diagnosis_code
            primary key,
    diagnosis_code varchar(30),
    long_description varchar(255),
    version pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_diagnosis_code_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_diagnosis_code_last_updated_user default suser_name(),
    short_description pit_description,
    is_valid bit,
    invalid_reason varchar(255),
    code_no_dot varchar(50),
    order_number int,
    is_header bit,
    codeset_release_date date,
    code_start_date date,
    code_end_date date,
    source_id varchar(50),
    etl_batch_id varchar(50)
)
go

create table dim_institutional_claim
(
    claim_key pit_key not null
        constraint xpkdim_institutional_claim
            primary key
        constraint fk_dic_claim_key
            references dim_va_claim,
    bill_type_key int
        constraint fk_dic_bill_type_key
            references dim_bill_type,
    admission_date pit_date,
    admission_time time,
    admission_source_key int
        constraint fk_dic_admission_source_key
            references dim_admission_source,
    admission_type_key int
        constraint fk_dic_admission_type_key
            references dim_admission_type,
    discharge_hour int,
    accident_state varchar(20),
    operating_provider_key pit_long_number
        constraint fk_dic_operating_provider_key
            references dim_provider,
    attending_provider_key pit_long_number
        constraint fk_dic_attending_provider_key
            references dim_provider,
    other_provider_key pit_long_number
        constraint fk_dic_other_provider_key
            references dim_provider,
    other_provider_qualifier pit_short_name,
    admitting_diagnosis_key pit_key
        constraint fk_dic_admitting_diagnosis_key
            references dim_diagnosis_code,
    patient_reason_code pit_natural_key,
    document_control_number varchar(256),
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_dic_etl_batch_id
            references claim_batch_log,
    patient_discharge_status int
        constraint fk_dic_patient_discharge_status
            references dim_patient_status,
    last_updated_date datetime
        constraint df_dim_institutional_claim_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_institutional_claim_last_updated_user default suser_name(),
    discharge_date date,
    pos_key int
        constraint fk_dic_pos_key
            references dim_place_of_service,
    attending_provider_taxonomy pit_long_name,
    inpatient pit_int,
    statement_from_date pit_date,
    statement_to_date pit_date,
    rendering_provider_key int,
    medical_record_number varchar(50),
    demonstration_project_identifier varchar(50),
    peer_review_authorization_number varchar(50),
    billing_note varchar(max),
    total_charges money,
    drg_number pit_natural_key,
    discharge_time time
)
go

create table claim_value_code
(
    value_code_key pit_key identity
        constraint xpkclaim_value_code
            primary key,
    claim_key pit_key
        constraint fk_cvc_claim_key
            references dim_institutional_claim,
    value_code pit_short_name,
    value_amt pit_money,
    start_date pit_date,
    end_date pit_end_date,
    is_current char not null,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_claim_value_code_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_value_code_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go
create table dim_professional_claim
(
    claim_key pit_key not null
        constraint xpkdim_professional_claim
            primary key
        constraint fk_dpc_claim_key
            references dim_va_claim,
    outside_lab char,
    total_charges pit_money,
    amount_paid pit_money,
    balance_due pit_money,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_dpc_etl_batch_id
            references claim_batch_log,
    last_updated_date datetime
        constraint df_dim_professional_claim_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_professional_claim_last_updated_user default suser_name(),
    condition_code_type pit_short_name,
    condition_date pit_date,
    pos_key pit_key
        constraint fk_dpc_pos_key
            references dim_place_of_service,
    rendering_facility_provider_key bigint
        constraint fk_dpc_rendering_facility_provider_key
            references dim_provider,
    amb_pop_zip varchar(15),
    claim_note varchar(max),
    onset_of_current_illness_or_injury_date date,
    initial_treatment_date date,
    last_seen_date date,
    acute_manifestation_date date,
    accident_date date,
    last_menstrual_period_date date,
    last_x_ray_date date,
    hearing_and_vision_prescription_date date,
    service_authorization_exception_code varchar(50),
    medicare_crossover_indicator varchar(50),
    mammography_certification_number varchar(50),
    clia_number varchar(50),
    repriced_claim_number varchar(50),
    adjusted_repriced_claim_number varchar(50),
    onset_or_injury_date date,
    medical_record_number varchar(50),
    demonstration_project_identifier varchar(50),
    peer_review_authorization_number varchar(50),
    pcp_provider_key int,
    rendering_provider_key int,
    supervising_provider_key int
)
go

create index idx_dprc_batch_id
    on dim_professional_claim (etl_batch_id)
go

create index idx_dim_professional_claim_rendering_facility_provider_key
    on dim_professional_claim (rendering_facility_provider_key)
    with (fillfactor = 100)
go

create index idx_dim_professional_claim_pos_key
    on dim_professional_claim (pos_key)
go

create index prof_claim_fams
    on dim_professional_claim (claim_key) include (rendering_facility_provider_key)
go

create index idx_dpc_is_current
    on dim_professional_claim (is_current)
go

create table claim_ambulance
(
    claim_key pit_key not null
        constraint fk_ca_claim_key
            references dim_professional_claim,
    claim_ambulance_key pit_key identity
        constraint xpkclaim_ambulance
            primary key,
    pickup_location pit_description,
    dropoff_location pit_description,
    last_updated_date datetime default getdate(),
    last_updated_user varchar(128) default suser_name(),
    weight_label varchar(2),
    patient_weight numeric(10),
    transport_reason_code char,
    distance_label char(2),
    transport_distance numeric(15),
    trip_purpose varchar(80),
    stretcher_purpose varchar(80),
    amb_is_certified char,
    pickup_address2 varchar(55),
    pickup_city varchar(30),
    pickup_state_short_name char(2),
    pickup_postal_code varchar(15),
    dropoff_address2 varchar(55),
    dropoff_city varchar(30),
    dropoff_state_short_name char(2),
    dropoff_postal_code varchar(15),
    amb_pat_condition_code_1 varchar(3),
    amb_pat_condition_code_2 varchar(3),
    amb_pat_condition_code_3 varchar(3),
    amb_pat_condition_code_4 varchar(3),
    amb_pat_condition_code_5 varchar(3),
    source_system varchar(20),
    source_entity varchar(50),
    is_current char,
    start_date pit_date,
    end_date pit_end_date,
    etl_batch_id varchar(50)
)
go



create table dim_reason_type
(
    reason_key pit_key identity
        constraint xpkdim_reason_type
            primary key,
    reason_code varchar(20),
    reason_code_description pit_description,
    reason_type pit_name,
    reason_type_description pit_description,
    vista_code pit_short_name,
    fbcs_code pit_short_name,
    cpe_code pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_dim_reason_type_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_reason_type_last_updated_user default suser_name()
)
go

create table cst_job_log
(
    cst_job_id pit_key identity
        constraint xpkcst_job_log
            primary key,
    cst_job_status varchar(20),
    job_start_time pit_datestamp,
    job_stop_time pit_datestamp,
    claim_lines_processed pit_long_number,
    last_updated_date datetime default getdate(),
    last_updated_user varchar(128) default suser_name(),
    eci_id pit_key
        constraint fk_cjl_eci_id
            references etl_cst_interface,
    benefit_type varchar(20),
    is_current pit_boolean default 'Y' not null,
    start_date pit_date,
    end_date pit_end_date,
    claim_lines_unscored pit_long_number
)
go

create index idx_cst_job_log_eci_id
    on cst_job_log (eci_id)
go



create table f_institutional_medical_claim_details
(
    claim_detail_key int identity
        constraint pk_f_institutional_medical_claim_details
            primary key,
    claim_key int not null
        constraint fk_ficd_claim_key
            references dim_institutional_claim,
    revenue_code varchar(20),
    procedure_key int
        constraint ficd_fk_procedure_key
            references dim_procedure_code,
    rate money,
    service_date date,
    service_unit int,
    unit_label varchar(20),
    charge_amt money,
    non_covered_charges money,
    adjusted char,
    adjustment_reason_key int
        constraint fk_ficd_adjustment_reason_key
            references dim_reason_type,
    adjustment_date date,
    denied char,
    denial_date date,
    denial_reason_key int
        constraint fk_ficd_denial_reason_key
            references dim_reason_type,
    nonpayment_key int,
    claim_line_score int,
    is_current char not null,
    start_date date,
    end_date date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_ficd_etl_batch_id
            references claim_batch_log,
    paid_amt money,
    repriced_amt money,
    source_claim_line_id varchar(30),
    repriced char,
    date_paid date,
    pos_key int
        constraint fk_ficd_pos_key
            references dim_place_of_service,
    vista_line_id varchar(20),
    last_updated_date datetime
        constraint df_f_institutional_medical_claim_details_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_f_institutional_medical_claim_details_last_updated_user default suser_name(),
    allowable_type varchar(20),
    other_allowable_type varchar(20),
    other_allowable_amount money,
    vista_allowable_type varchar(20),
    vista_allowable_amount pit_money,
    cms_allowable_type varchar(20),
    cms_allowable_amount pit_money,
    cst_job_id pit_key
        constraint fk_ficd_cst_job_id
            references cst_job_log,
    program_type pit_name,
    pay_flag char,
    drg varchar(10),
    service_line_number int,
    other_pay_amount money,
    drug_code varchar(30),
    drug_quantity int,
    drug_unit_label varchar(20),
    drug_prescription_number varchar(50),
    procedure_description varchar(256),
    service_tax_amount money,
    facility_tax_amount money,
    operating_provider_key pit_long_number
        constraint operating_fk
            references dim_provider,
    other_operating_provider_key pit_long_number
        constraint other_operating_fk
            references dim_provider,
    rendering_provider_key pit_long_number
        constraint rendering_fk
            references dim_provider,
    referring_provider_key pit_long_number
        constraint referring_fk
            references dim_provider,
    dq_issue_count int,
    dq_issues_text varchar(max),
    line_note varchar(max),
    line_status varchar(30),
    position varchar(10)
)
go

create index idc_claim_line_score_is_current
    on f_institutional_medical_claim_details (claim_line_score, is_current) include (claim_key, service_date, etl_batch_id)
go

create index idx_ficd_batch_id
    on f_institutional_medical_claim_details (etl_batch_id)
go

create index idx_ficd_claim_key
    on f_institutional_medical_claim_details (claim_key)
go

create index idx_fimcd_procedure_key
    on f_institutional_medical_claim_details (procedure_key)
go

create index idx_service_date
    on f_institutional_medical_claim_details (service_date)
go

create table claim_insurance
(
    insurance_key pit_key identity
        constraint xpkclaim_other_insurance
            primary key
                with (fillfactor = 100),
    claim_key pit_key
        constraint fk_ci_claim_key
            references dim_va_claim,
    insured_id varchar(120),
    insured_name pit_long_name,
    relation_to_the_insured varchar(20),
    other_insured_name varchar(500),
    insurance_policy_group pit_long_name,
    insurance_policy_number pit_natural_key,
    policy_holder_birth_date pit_date,
    policy_holder_gender char,
    insurance_begin_date pit_date,
    insurance_end_date pit_date,
    insurance_type pit_long_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    paid_amount pit_money,
    patient_responsibility pit_money,
    last_updated_date datetime
        constraint df_claim_insurance_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_insurance_last_updated_user default suser_name(),
    etl_batch_id varchar(50),
    non_covered_amount pit_money,
    payer_responsibility_sequence_number_code varchar,
    claim_filing_indicator_code varchar(2)
)
go

create index idx_ci_claim_key
    on claim_insurance (claim_key)
go

create table claim_payer
(
    claim_key pit_key
        constraint fk_cpyr_claim_key
            references dim_va_claim
            on update set null on delete set null,
    claim_payer_key pit_key identity
        constraint xpkclaim_payer
            primary key,
    payer_name pit_long_name,
    plan_id varchar(120),
    release_of_info pit_1_char,
    assignment_of_benefit pit_1_char,
    prior_payment pit_money,
    estimated_amount_due pit_money,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id pit_natural_key,
    last_updated_date pit_datestamp
        constraint df_claim_payer_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_payer_last_updated_user default suser_name(),
    claim_insurance_key int,
    etl_batch_id varchar(50),
    payer_claim_control_number varchar(30)
)
go

create index idx_cpa_claim_key
    on claim_payer (claim_key)
go

create table dim_dental_claim
(
    claim_key pit_key not null
        constraint xpkdim_dental_claim
            primary key
        constraint fk_ddc_claim_key
            references dim_va_claim,
    rendering_provider_key pit_long_number
        constraint fk_ddc_rendering_provider_key
            references dim_provider,
    admission_date pit_date,
    discharge_date pit_date,
    referral_date pit_date,
    accident_date pit_date,
    appliance_placement_date pit_date,
    total_claim_charge_amount pit_money,
    claim_frequency_type_code pit_natural_key,
    claim_note pit_description,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_ddc_etl_batch_id
            references claim_batch_log,
    last_updated_date datetime
        constraint df_dim_dental_claim_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_dim_dental_claim_last_updated_user default suser_name(),
    pos_key pit_key
        constraint fk_ddc_pos_key
            references dim_place_of_service,
    rendering_provider_taxonomy pit_long_name,
    rendering_facility_provider_key bigint
)
go

create index idx_ddc_rendering_provider
    on dim_dental_claim (rendering_provider_key)
go

create table claim_diagnosis
(
    claim_diagnosis_key pit_key identity
        constraint xpkclaim_diagnosis
            primary key,
    diagnosis_key pit_key
        constraint fk_cd_diagnosis_key
            references dim_diagnosis_code,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id pit_natural_key,
    diagnosis_poa pit_1_char,
    claim_key_in pit_key
        constraint fk_cd_claim_key_in
            references dim_institutional_claim
            on update set null on delete set null,
    last_updated_date pit_datestamp
        constraint df_claim_diagnosis_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_diagnosis_last_updated_user default suser_name(),
    claim_key_pr pit_key
        constraint fk_cd_claim_key_pr
            references dim_professional_claim
            on update set null on delete set null,
    claim_key_dt pit_key
        constraint fk_cd_claim_key_dt
            references dim_dental_claim
            on update set null on delete set null,
    etl_batch_id varchar(50),
    qualifier_description varchar(256),
    qualifier varchar(5)
)
go

create index idx_cd_claim_key_in
    on claim_diagnosis (claim_key_in)
go

create index idx_cd_diagnosis_key
    on claim_diagnosis (diagnosis_key)
go

create index idx_claim_key_in
    on claim_diagnosis (claim_key_in) include (claim_diagnosis_key, diagnosis_key)
go

create table claim_procedure
(
    claim_procedure_key pit_key identity
        constraint xpkclaim_procedure
            primary key,
    claim_key int
        constraint fk_cp_claim_key
            references dim_institutional_claim,
    claim_key_pr int,
    claim_key_dt int,
    procedure_key pit_key
        constraint fk_cp_procedure_key
            references dim_procedure_code,
    qualifier pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    procedure_date date,
    last_updated_date datetime
        constraint df_claim_procedure_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_procedure_last_updated_user default suser_name(),
    etl_batch_id varchar(50),
    qualifier_description varchar(256)
)
go

create index idx_cpr_claim_key
    on claim_procedure (claim_key) include (procedure_key)
go

create index idx_claim_procedure_procedure_key
    on claim_procedure (procedure_key)
go



create table claim_tooth_status
(
    claim_tooth_key pit_key identity
        constraint xpkclaim_tooth_status
            primary key
                with (fillfactor = 100),
    claim_key pit_key
        constraint fk_cts_claim_key
            references dim_dental_claim
            on update set null on delete set null,
    tooth_number varchar(10),
    tooth_status varchar(10),
    is_current pit_boolean not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system pit_short_name,
    source_entity pit_long_name,
    row_id pit_natural_key,
    last_updated_date pit_datestamp
        constraint d_dbo_claim_tooth_status_1 default getdate(),
    last_updated_user varchar(128)
        constraint d_dbo_claim_tooth_status_2 default suser_name(),
    etl_batch_id varchar(50)
)
go

create table f_professional_medical_claim_details
(
    claim_detail_key int identity
        constraint pk_f_professional_medical_claim_details
            primary key,
    claim_key int not null
        constraint fk_fpcd_claim_key
            references dim_professional_claim,
    procedure_key int
        constraint fk_fpcd_procedure_key
            references dim_procedure_code,
    pos_key int
        constraint fk_fpcd_pos_key
            references dim_place_of_service,
    adjustment_reason_key int
        constraint fk_fpcd_adjustment_reason_key
            references dim_reason_type,
    denial_reason_key int
        constraint fk_fpcd_denial_reason_key
            references dim_reason_type,
    preauth_number varchar(20),
    repriced char,
    repriced_amount money,
    adjusted char,
    adjustment_date date,
    charge_amt money,
    unit bigint,
    unit_label varchar(30),
    rendering_provider_key bigint
        constraint fk_fpcd_rendering_provider_key
            references dim_provider,
    rendering_provider_qualifier varchar(20),
    paid_amt money,
    date_paid date,
    adjusted_amount money,
    claim_line_score int,
    is_current char not null,
    start_date date,
    end_date date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_fpcd_etl_batch_id
            references claim_batch_log,
    service_date_from date,
    source_claim_line_id varchar(30),
    service_date_to date,
    vista_line_id varchar(20),
    last_updated_date datetime
        constraint df_f_professional_medical_claim_details_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_f_professional_medical_claim_details_last_updated_user default suser_name(),
    allowable_type varchar(20),
    other_allowable_type varchar(20),
    other_allowable_amount money,
    vista_allowable_type varchar(20),
    vista_allowable_amount pit_money,
    cms_allowable_type varchar(20),
    cms_allowable_amount pit_money,
    cst_job_id pit_key
        constraint fk_fpcd_cst_job_id
            references cst_job_log,
    lab_charges pit_money,
    program_type pit_name,
    modifier_code varchar(10),
    pay_flag char,
    service_line_number int,
    other_pay_amount money,
    drug_code varchar(30),
    rendering_provider_taxonomy pit_long_name,
    dq_issue_count int,
    dq_issues_text varchar(max),
    line_note varchar(max),
    referring_provider_key int,
    purchase_service_provider_key int,
    service_facility_provider_key int,
    supervising_provider_key int,
    ordering_provider_key int,
    line_status varchar(30),
    position varchar(10)
)
go

create index idx_claim_line_score_is_current
    on f_professional_medical_claim_details (claim_line_score, is_current) include (claim_key, etl_batch_id, service_date_from)
go

create index idx_fpcd_batch_id
    on f_professional_medical_claim_details (etl_batch_id)
go

create index idx_fpcd_claim_key
    on f_professional_medical_claim_details (claim_key)
go

create index idx_fpmcd_procedure_key
    on f_professional_medical_claim_details (procedure_key)
go

create index idx_fpmcd_rendering_provider
    on f_professional_medical_claim_details (rendering_provider_key)
go

create index idx_service_date
    on f_professional_medical_claim_details (service_date_from)
go

create table prof_line_extra
(
    claim_detail_key bigint
        constraint pk_prof_line_extra primary key,

    claim_key bigint not null,

    prescription_date date,
    last_seen_date date,

    is_current varchar(1),
    source_system varchar(20),
    source_entity varchar(20),
    etl_batch_id varchar(20),
    start_date pit_date,
    end_date pit_date,
    repriced_line_item_reference_number varchar(50),
    adjusted_repriced_line_item_reference_number varchar(50),

    ambulance_patient_count integer,
    third_party_organization_note varchar(max),

    last_updated_date datetime2 not null default (getdate()),
    last_updated_user varchar(128) not null default (suser_name()),
)
create table f_dental_claim_details
(
    claim_detail_key pit_key identity
        constraint xpkf_dental_claim_detail
            primary key,
    claim_key pit_key not null
        constraint fk_fdcd_claim_key
            references dim_dental_claim,
    pos_key pit_key
        constraint fk_fdcd_pos_key
            references dim_place_of_service,
    adjustment_reason_key pit_key
        constraint fk_fdcd_adjustment_reason_key
            references dim_reason_type,
    status char,
    service_line_number int,
    dental_service varchar(20),
    charged_amount pit_money,
    facility_code varchar(20),
    prosthesis_crown varchar(20),
    procedure_count numeric,
    line_item_control_number varchar(30),
    line_note varchar(80),
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    etl_batch_id varchar(20)
        constraint fk_fdcd_etl_batch_id
            references claim_batch_log,
    last_updated_date datetime
        constraint df_f_dental_claim_detail_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_f_dental_claim_detail_last_updated_user default suser_name(),
    cst_job_id pit_key
        constraint fk_fdcd_cst_job_id
            references cst_job_log,
    allowed_amount money,
    claim_line_score pit_int,
    date_of_service pit_date,
    paid_amount money,
    procedure_key pit_key
        constraint fk_fdcd_procedure_key
            references dim_procedure_code,
    date_paid date,
    other_pay_amount money,
    rendering_provider_key int,
    assistant_surgeon_provider_key int,
    supervising_provider_key int,
    service_facility_provider_key int,
    line_status varchar(30),
    position varchar(10)
)
go

create index idx_fdcd_batch_id
    on f_dental_claim_details (etl_batch_id)
go

create index idx_fdcd_claim_key
    on f_dental_claim_details (claim_key)
go



create table claim_line_modifier
(
    claim_line_modifier_key pit_key identity
        constraint xpkclaim_line_modifier
            primary key,
    claim_detail_key_in pit_key
        constraint fk_clm_claim_detail_key_in
            references f_institutional_medical_claim_details,
    modifier_code pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    claim_detail_key_pr pit_key
        constraint fk_clm_claim_detail_key_pr
            references f_professional_medical_claim_details,
    last_updated_date datetime
        constraint df_claim_line_modifier_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_line_modifier_last_updated_user default suser_name(),
    claim_detail_key_dt pit_key
        constraint fk_clm_claim_detail_key_dt
            references f_dental_claim_details,
    etl_batch_id varchar(50),
    pointer_order tinyint
)
go

create index idx_claim_detail_key_in
    on claim_line_modifier (claim_detail_key_in) include (claim_line_modifier_key, modifier_code)
go

create index idx_claim_line_modifier
    on claim_line_modifier (claim_detail_key_pr) include (claim_line_modifier_key, modifier_code)
go

create index idx_clm_claim_detail_key_dt
    on claim_line_modifier (claim_detail_key_dt)
go

create index idx_clm_claim_detail_key_in
    on claim_line_modifier (claim_detail_key_in)
go

create index idx_clm_claim_detail_key_pr
    on claim_line_modifier (claim_detail_key_pr)
go


create table claim_line_diagnosis
(
    claim_line_diagnosis_key pit_key identity
        constraint xpkclaim_line_diagnosis
            primary key,
    claim_detail_key pit_key
        constraint fk_cld_claim_detail_key
            references f_professional_medical_claim_details,
    diagnosis_key pit_key
        constraint fk_cld_diagnosis_key
            references dim_diagnosis_code,
    pointer_order pit_int,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_claim_line_diagnosis_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_line_diagnosis_last_updated_user default suser_name(),
    claim_detail_key_dt pit_key
        constraint fk_cld_claim_detail_key_dt
            references f_dental_claim_details
            on update set null on delete set null,
    etl_batch_id varchar(50)
)
go

create index idx_cld_claim_detail_key
    on claim_line_diagnosis (claim_detail_key)
go

create index idx_cld_diagnosis_key
    on claim_line_diagnosis (diagnosis_key)
go

create index xpointed_order
    on claim_line_diagnosis (pointer_order) include (claim_detail_key, diagnosis_key)
go

create table claim_line_oral_cavity
(
    claim_line_oral_cavity_key pit_key identity
        constraint xpkclaim_line_oral_cavity
            primary key
                with (fillfactor = 100),
    claim_detail_key pit_key
        constraint fk_cloc_claim_detail_key
            references f_dental_claim_details
            on update set null on delete set null,
    oral_cavity_designation_code pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id pit_short_name,
    last_updated_date pit_datestamp
        constraint df_claim_line_oral_cavity_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_line_oral_cavity_last_updated_user default suser_name(),
    oral_cavity_sequence pit_int,
    etl_batch_id varchar(50)
)
go

create table claim_line_tooth_surface
(
    claim_line_tooth_surface_key pit_key identity
        constraint xpkclaim_line_tooth_surface
            primary key
                with (fillfactor = 100),
    claim_detail_key pit_key
        constraint fk_clts_claim_detail_key
            references f_dental_claim_details
            on update set null on delete set null,
    tooth_surface_code pit_short_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id pit_short_name,
    last_updated_date pit_datestamp
        constraint df_claim_line_tooth_surface_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_line_tooth_surface_last_updated_user default suser_name(),
    tooth_surface_sequence pit_int,
    tooth_code varchar(20),
    etl_batch_id varchar(50)
)
go

create table claim_occurrence
(
    occurrence_key pit_key identity
        constraint xpkclaim_occurrence
            primary key,
    claim_key pit_key
        constraint fk_co_claim_key
            references dim_institutional_claim,
    occurrence_code pit_short_name,
    occurrence_date pit_date,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_claim_occurrence_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_occurrence_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go

create table claim_occurrence_span
(
    occurrence_span_key int identity
        constraint pk_claim_occurrence_span
            primary key,
    claim_key int not null
        constraint fk_cos_claim_key
            references dim_institutional_claim,
    occurrence_span_code varchar(20),
    occurrence_span_from date,
    occurrence_span_to date,
    is_current char not null,
    start_date date,
    end_date date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime
        constraint df_claim_occurrence_span_last_updated_date default getdate(),
    last_updated_user varchar(128)
        constraint df_claim_occurrence_span_last_updated_user default suser_name(),
    etl_batch_id varchar(50)
)
go

create index idx_cos_claim_key
    on claim_occurrence_span (claim_key)
go

create table claim_condition_code
(
    condition_key pit_key identity
        constraint xpkclaim_condition_code
            primary key
                with (fillfactor = 100),
    claim_key pit_key
        constraint fk_ccc_claim_key
            references dim_va_claim,
    code varchar(30),
    description pit_long_name,
    is_current char not null,
    start_date pit_date,
    end_date pit_end_date,
    source_system varchar(20),
    source_entity varchar(50),
    row_id varchar(20),
    last_updated_date datetime default getdate(),
    last_updated_user varchar(128) default suser_name(),
    etl_batch_id varchar(50)
)
go

create table lkup_revenue_code
(
    revenue_code_key bigint identity
        constraint pk_revenue_code
            primary key,
    code varchar(50) not null,
    sub_type varchar(100),
    description varchar(max),
    short_description varchar(200),
    start_date date,
    end_date date,
    change_date date,
    source_system varchar(20),
    source_entity varchar(200),
    last_updated_date datetime2 default getdate() not null,
    last_updated_user varchar(128) default suser_name() not null
)
go

create index rev_code_code
    on lkup_revenue_code (code) include (revenue_code_key)
go

create index rev_code_last_updated_date
    on lkup_revenue_code (last_updated_date) include (revenue_code_key, code)
go

create table claim_line_adjudication
(
    claim_line_adjudication_key bigint identity
        constraint pk_claim_line_adjudication primary key,

    claim_detail_key_in int,
    claim_detail_key_pr int,
    claim_detail_key_dt int,

    claim_insurance_key int,

    payer_id varchar(80),         -- SVD01
    paid_amount money,            -- SVD02
    patient_responsibility money, -- AMT*EAF AMT02

    /*
    composite_med_procedure_id varchar(2),  -- SVD03-1
    procedure_code varchar(48),     -- SVD03-2
    procedure_modifier_1 varchar(2), -- SVD03-3
    procedure_modifier_2 varchar(2), -- SVD03-4
    procedure_modifier_3 varchar(2), -- SVD03-5
    procedure_modifier_4 varchar(2), -- SVD03-6
    procedure_code_description varchar(80), -- SVD03-7
     */
    paid_service_units decimal,   -- SVD05
    bundled_line_number int,      -- SVD06
    paid_date date,               -- DTP*573 DTP03

    etl_batch_id varchar(50),

    is_current varchar(1),
    source_system varchar(20),
    source_entity varchar(20),
    last_updated_date datetime2 not null default (getdate()),
    last_updated_user varchar(128) not null default (suser_name()),
    start_date pit_date,
    end_date pit_date
)
go

create table json_auth
(
    json_auth_key bigint identity (1,1) not null,
    guid varchar(40) null,
    program_id varchar(20) null,
    file_name varchar(255) null,
    diagnosis_code varchar(100) null,
    medical_necessity nvarchar(max) null,
    payor_status varchar(20) null,
    primary_provider_npi pit_natural_key null,
    treating_provider_npi pit_natural_key null,
    [referral_category] varchar(220) null,
    from_date pit_date null,
    to_date pit_end_date null,
    auth_number varchar(220) null,
    consult_id_vista varchar(220) null,
    referring_provider_npi pit_natural_key null,
    auth_status varchar(50) null,
    network_code varchar(50) null,
    clin_code varchar(220) null,
    payment_auth_code varchar(220) null,
    category_of_care varchar(100) null,
    cost_estimate money null,
    cdw_pov varchar(220) null,
    seoc_id bigint null,
    seoc_seoc_key bigint null,
    seoc_seoc_id varchar(255) null,
    last_updated_user varchar(128) null,
    last_updated_date datetime null,
    etl_batch_id varchar(20) null,
    source_system varchar(50) null,
    created_date pit_date null,
    is_current char(1) null,
    start_date_time pit_datestamp null,
    end_date_time pit_datestamp null,
    patient_key pit_key null,
    station_key int null,
    patient_ssn varchar(50) null,
    icn varchar(20) null,
    is_seoc_valid char(1) null
        constraint xpkjson_auth primary key clustered (json_auth_key)
)
go

alter table json_auth
    add constraint df__json_auth__last___5182e3d5 default (suser_name()) for last_updated_user
go

alter table json_auth
    add constraint df__json_auth__last___5277080e default (getdate()) for last_updated_date
go

create table claim_provider_837
(
    claim_provider_key bigint identity
        constraint xpkclaim_provider
            primary key,
    claim_key int
        constraint prov_fk_claim_key
            references dim_va_claim
            on delete cascade,

    claim_detail_key_in bigint,
    claim_detail_key_pr bigint,
    claim_detail_key_dt bigint,

    provider_type varchar(50),
    entity_type varchar(20),
    identification_type varchar(20),


    tax_id varchar(20),
    npi pit_natural_key,
    secondary_id varchar(50),

    provider_name varchar(500),

    provider_first_name varchar(120),
    provider_last_name varchar(120),
    provider_middle_name varchar(50),

    address1 varchar(1000),
    address2 varchar(1000),
    city varchar(500),
    state varchar(40),
    postal_code varchar(20),

    is_current char(1),
    start_date date,
    end_date date,

    last_updated_user varchar(128) default suser_name(),
    last_updated_date datetime default getdate(),
    etl_batch_id pit_natural_key,
    source_system varchar(20)
)
go

create index prov_claim_key
    on claim_provider_837 (claim_key) include (claim_provider_key)
go


create index prov_last_updated_date
    on claim_provider_837 (last_updated_date desc)
    with (fillfactor = 100)
go

create index prov_etl_batch_id
    on claim_provider_837 (etl_batch_id)
go

create schema seoc
go

create table seoc.seoc
(
    seoc_pkey bigint identity (1,1) not null,
    seoc_id varchar(500) not null,
    version_number varchar(20) null,
    all_versions_seoc_key int null,
    is_prct bit null,
    qasp varchar(500) null,
    is_rev bit null,
    category_of_care varchar(100) null,
    name varchar(100) null,
    description varchar(max) null,
    disclaimer varchar(max) null,
    service_line varchar(500) null,
    effective_date date null,
    end_date date null,
    duration int null,
    max_allowable_visits int null,
    last_updated_date datetime2(7) not null,
    last_updated_user varchar(128) not null,
    constraint pk_seoc primary key clustered (seoc_pkey)
)
go

alter table seoc.seoc
    add default (getdate()) for last_updated_date
go

alter table seoc.seoc
    add default (suser_name()) for last_updated_user
go

create function splitstring(@string varchar(max), @delimiter char(1) = ',')
    returns @temptable table
                       (
                           item varchar(100)
                       )
as
begin
    declare @idx int
    declare @slice varchar(8000)

    select @idx = 1
    if len(@string) < 1 or @string is null return

    while @idx != 0
        begin
            set @idx = charindex(@delimiter, @string)
            if @idx != 0
                set @slice = left(@string, @idx - 1)
            else
                set @slice = @string

            if (len(@slice) > 0)
                insert into @temptable(item) values (@slice)

            set @string = right(@string, len(@string) - @idx)
            if len(@string) = 0 break
        end
    return
end
go