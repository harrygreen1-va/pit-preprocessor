create table [etl].[rejected_lines]
(

    [rejected_key] [dbo].[pit_key] identity (1,1) primary key clustered,

    [row_id] [varchar](20) null,

    [claim_id] [varchar](60) null,

    [claim_line_id] [varchar](30) null,

    [field_name] [varchar](50) null,

    [severity] [varchar](50) null,

    [reason] [varchar](20) null,

    [loaded_to_db] [dbo].[pit_boolean] null,

    [source_system] [varchar](20) null,

    [source_entity] [varchar](50) null,

    [db_id] [varchar](20) null,

    [etl_batch_id] [varchar](20) null,

    [last_updated_date] [datetime] null,

    [last_updated_user] [varchar](128) null,
)