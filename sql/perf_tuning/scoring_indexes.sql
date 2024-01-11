/*
Missing Index Details from prof_scoring_quiery.sql - vaaussqlpci406.aac.dva.va.gov.PITEDR (famsdbadmin (63))
The Query Processor estimates that implementing the following index could improve the query cost by 99.1303%.
*/


USE [PITEDR]
GO
CREATE NONCLUSTERED INDEX fpmcd_etl_batch_id
ON [dbo].[F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS] ([etl_batch_id])
INCLUDE ([service_date_from])
GO

CREATE NONCLUSTERED INDEX claim_etl_batch_id
ON [dbo].[DIM_VA_CLAIM] ([etl_batch_id])
INCLUDE ([patient_key])
GO