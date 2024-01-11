USE [PITEDR]
GO

/****** Object:  StoredProcedure [dbo].[selectClaimsForPatientsNew]    Script Date: 8/18/2020 12:13:22 PM ******/
DROP PROCEDURE [dbo].[selectClaimsForPatientsDupeCand]
GO

/****** Object:  StoredProcedure [dbo].[selectClaimsForPatientsNew]    Script Date: 8/18/2020 12:13:22 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


-- TODO: logic for the earliest date for pharmacy and dental
CREATE PROCEDURE [dbo].[selectClaimsForPatientsDupeCand] @BenefitType VARCHAR(20), @IdList VARCHAR(max), @eciId INT
AS
    SET NOCOUNT ON  -- this is needed so we don't return the count instead of result set
    
    DECLARE @IdTable IdListType
    DECLARE @SelectKeysStatement NVARCHAR(max)
    DECLARE @SelectKeysParmDefinition NVARCHAR (500)= N'@Ids IdListType READONLY'	--, @earliestDate DATE'
    DECLARE @FTableName VARCHAR(50)
    
    -- split the comma-delimited list into the table
    INSERT INTO @IdTable SELECT * FROM SplitString(@IdList,',')

    SET @FTableName=
      CASE @BenefitType
         WHEN 'Professional' THEN 'F_PROFESSIONAL_MEDICAL_CLAIM_DETAILS'
         WHEN 'Institutional' THEN 'F_INSTITUTIONAL_MEDICAL_CLAIM_DETAILS'
         WHEN 'Pharmacy' THEN 'F_PHARMACY_CLAIM_DETAILS'
         WHEN 'Dental' THEN 'F_DENTAL_CLAIM_DETAILS'
         ELSE 'ERROR!'
      END

	SET @SelectKeysStatement=N'select fcd.claim_detail_key '
		+ 'from ' + @FTableName + ' fcd '
		+ 'join DIM_VA_CLAIM vac on vac.claim_key = fcd.claim_key '
		+ 'join CLAIM_BATCH_LOG cbl on cbl.etl_batch_id = vac.etl_batch_id '
		+ 'join DIM_PATIENT pat on pat.patient_key = vac.patient_key '
		+ 'join @Ids pIds on pIds.id = pat.member_id '
		+ 'where fcd.is_current = ''Y'' '
		+ 'and cbl.eci_id = ' + CAST(@eciId as varchar(20)) + ' and cbl.to_score_indicator = ''Y'' '
		+ 'union ' 
		+ 'select claim_detail_key from stg.dupe_candidate dc join @Ids pIds on pIds.id = dc.member_id ' 
		+ 'where dc.eci_id = ' + CAST(@eciId as varchar(20)) 

	/*
    SET @SelectKeysStatement=N'' + @SelectKeysStatement 
		+ 'OPTION (MAXDOP 1)'      
    */
    IF OBJECT_ID('#ClaimLineKeys', 'U') IS NOT NULL 
    BEGIN
        DROP TABLE #ClaimLineKeys
    END
    
    CREATE TABLE #ClaimLineKeys(
        id INT
    )
    
    INSERT #ClaimLineKeys EXECUTE sp_executesql @SelectKeysStatement, @SelectKeysParmDefinition, @Ids = @IdTable

    -- Dynamic SQL has its own session, so it is not possible to use the same temp table
    -- Table variable is too slow.
    -- For now, we duplicated the same SQL, need to find a better way
	IF @BenefitType='Institutional'
	BEGIN
		SELECT * FROM vw_institutional_claim 
		WHERE claim_line_number in (select id from  #ClaimLineKeys) 
	END 
	ELSE IF @BenefitType='Professional'
	BEGIN
        RAISERROR ('Professional Not done yet!!! NULL earliest date for professional claims always have the earliest service date', 10, 1);
	END
	ELSE IF @BenefitType='Pharmacy'
	BEGIN
        RAISERROR ('Pharmacy Not done yet!!!', 10, 1)
		--SELECT * FROM vw_pharmacy_claim  
		--WHERE claim_line_number in (select id from  #ClaimLineKeys) 
	END
	ELSE IF @BenefitType='Dental'
	BEGIN
        RAISERROR ('Dental Not done yet!!!', 10, 1)
		SELECT * FROM vw_dental_claim  
		WHERE claim_line_number in (select id from  #ClaimLineKeys) 
	END

GO


