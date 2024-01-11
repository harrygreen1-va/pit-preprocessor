/*
Missing Index Details from SQLQuery18.sql - vac20sqlpci352.va.gov.PITEDR (VHAMASTER\vhaiswanania0 (70))
The Query Processor estimates that implementing the following index could improve the query cost by 96.5109%.
*/


USE [PITEDR]
GO
CREATE NONCLUSTERED INDEX claim_insurance_batch_id
ON [dbo].[claim_insurance_raw] ([etl_batch_id])
INCLUDE ([claim_insurance_raw_key],[claim_key],[claim_id],[last_updated_date])
GO
