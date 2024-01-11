DROP PROCEDURE [dbo].[selectClaimsForPatients]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- TODO: logic for the earliest date for pharmacy and dental
CREATE PROCEDURE [dbo].[selectClaimsForPatients] @BenefitType VARCHAR(20), @IdList VARCHAR(max), @earliestVisitServiceDate DATE
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
		+ 'join DIM_PATIENT pat on pat.patient_key = vac.patient_key '
		+ 'join @Ids pIds on pIds.id = pat.member_id '
		+ 'where fcd.is_current = ''Y'''
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
	IF @earliestVisitServiceDate IS NULL
	BEGIN
		IF @BenefitType='Institutional'
		BEGIN
			SELECT * FROM vw_institutional_claim 
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
		END 
		ELSE IF @BenefitType='Professional'
		BEGIN
            RAISERROR ('NULL earliest date for professional claims always have the earliest service date', 10, 1);
		END
		ELSE IF @BenefitType='Pharmacy'
		BEGIN
			SELECT * FROM vw_pharmacy_claim  
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
		END
		ELSE IF @BenefitType='Dental'
		BEGIN
			SELECT * FROM vw_dental_claim  
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
		END
	END
	ELSE
	BEGIN
		IF @BenefitType='Institutional'
		BEGIN
			SELECT * FROM vw_institutional_claim 
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
		END 
		ELSE IF @BenefitType='Professional'
		BEGIN
			SELECT * FROM vw_professional_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys)
            and procedure_code is not null
            and PLACE_OF_SERVICE is not null
		END
		ELSE IF @BenefitType='Pharmacy'
		BEGIN
			SELECT * FROM vw_pharmacy_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
		END
		ELSE IF @BenefitType='Dental'
		BEGIN
			SELECT * FROM vw_dental_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
		END
	END

GO


