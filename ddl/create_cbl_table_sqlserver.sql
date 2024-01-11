USE [PITEDR]
GO

ALTER TABLE [dbo].[CLAIM_BATCH_LOG] DROP CONSTRAINT [fk_cbl_eci_id]
GO

/****** Object:  Table [dbo].[CLAIM_BATCH_LOG]    Script Date: 1/20/2017 11:48:10 AM ******/
DROP TABLE [dbo].[CLAIM_BATCH_LOG]
GO

/****** Object:  Table [dbo].[CLAIM_BATCH_LOG]    Script Date: 1/20/2017 11:48:10 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE [dbo].[CLAIM_BATCH_LOG](
	[etl_batch_id] [dbo].[pit_natural_key] NOT NULL,
	[batch_status] [dbo].[pit_name] NULL,
	[to_score_indicator] [char](1) NULL,
	[source_system] [varchar](20) NULL,
	[last_updated_date] [datetime] NULL CONSTRAINT [DF_CLAIM_BATCH_LOG_last_updated_date]  DEFAULT (getdate()),
	[last_updated_user] [varchar](128) NULL CONSTRAINT [DF_CLAIM_BATCH_LOG_last_updated_user]  DEFAULT (suser_name()),
	[ECI_ID] [dbo].[pit_key] NULL,
	[feed_date] [dbo].[pit_date] NULL,
	[start_date_time] [dbo].[pit_datestamp] NULL,
	[end_date_time] [dbo].[pit_datestamp] NULL,
	[file_name] [dbo].[pit_long_name] NULL,
	[file_size] [dbo].[pit_long_number] NULL,
	[file_drop_dt] [dbo].[pit_datestamp] NULL,
	[number_of_rows] [dbo].[pit_long_number] NULL,
	[min_cdw_extract_batch_id] [bigint] NULL,
	[max_cdw_extract_batch_id] [bigint] NULL,
 CONSTRAINT [XPKCLAIM_BATCH_LOG] PRIMARY KEY CLUSTERED 
(
	[etl_batch_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 100) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [dbo].[CLAIM_BATCH_LOG]  WITH CHECK ADD  CONSTRAINT [fk_cbl_eci_id] FOREIGN KEY([ECI_ID])
REFERENCES [dbo].[ETL_CST_INTERFACE] ([ECI_ID])
GO

ALTER TABLE [dbo].[CLAIM_BATCH_LOG] CHECK CONSTRAINT [fk_cbl_eci_id]
GO


