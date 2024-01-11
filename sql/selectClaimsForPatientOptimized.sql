DROP PROCEDURE dbo.selectClaimsForPatientsOpt
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[selectClaimsForPatientsOpt] @BenefitType VARCHAR(20), @IdList VARCHAR(max), @earliestVisitServiceDate DATE
AS
    SET NOCOUNT ON  -- this is needed so we don't return the count instead of result set
    
    DECLARE @IdTable IdListType
    
    -- split the comma-delimited list into the table
    INSERT INTO @IdTable SELECT * FROM SplitString(@IdList,',')

	IF @BenefitType='Institutional'
	BEGIN
		SELECT * FROM vw_institutional_claim 
        join @idTable pids on pids.id = patient_id
        where visit_date >= @earliestVisitServiceDate 
	END 
	ELSE IF @BenefitType='Professional'
	BEGIN
        select * from vw_professional_claim
        join @idTable pids on pids.id = patient_id
        where visit_date >= @earliestVisitServiceDate 
	END
	ELSE
    BEGIN
        RAISERROR ('Invalid benefit type', 10, 1);
	END

GO


