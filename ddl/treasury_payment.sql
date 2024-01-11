create table [dbo].[treasury_payment]
(

    [treasury_payment_key] [bigint] identity (1,1) not null,

    [claim_key] [int] null,

    [claim_type] [varchar](10) null,

    [check_number] [varchar](20) null,

    [check_date] [dbo].[pit_date] null,

    [treasury_amnt] [decimal](19, 4) null,

    [intrst_amnt] [decimal](19, 4) null,

    [invoice_number] [varchar](30) null,

    [obligation_number] [varchar](30) null,

    [submission_number] [varchar](50) null,

    [claim_id] [varchar](60) not null,

    [reopen_claim_id] [varchar](65) null,

    [etl_batch_id] [dbo].[pit_natural_key] not null,

    [source_claim_PK] [varchar](30) null,

    [frequency_code] [varchar](2) null,

    [FMS_date] [dbo].[pit_date] null,

    [last_updated_date] [datetime] not null,

    [last_updated_user] [varchar](128) not null,

    [cost_share] [decimal](19, 4) null,

    [provider_paid_amount] [decimal](19, 4) null,

    [deductible_amount] [decimal](19, 4) null,

    [invoice_received_date] [date] null,

    [disbursed_amount] [decimal](19, 4) null


)