USE [PITEDR]
GO

/****** Object:  StoredProcedure [dbo].[selectClaimsForPatients]    Script Date: 7/24/2020 7:06:54 PM ******/
DROP PROCEDURE [dbo].[selectClaimsForPatients]
GO

/****** Object:  StoredProcedure [dbo].[selectClaimsForPatients]    Script Date: 7/24/2020 7:06:54 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[selectClaimsForPatients] @BenefitType VARCHAR(20), @IdList VARCHAR(max), @earliestVisitServiceDate DATE
/*, @EciId NVARCHAR(max)*/
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
	--IF @earliestVisitServiceDate IS NOT NULL
	--	  BEGIN
	--		SET @SelectKeysStatement=
	--		CASE @BenefitType
	--			WHEN 'Professional' THEN N'' + @SelectKeysStatement 
	--				+ 'where fcd.service_date_from >= @earliestDate '
	--			WHEN 'Institutional' THEN N'' + @SelectKeysStatement 
	--				+ 'where (fcd.service_date is not null and fcd.service_date >= @earliestDate) '
	--				+ 'or (fcd.service_date is null and vac.date_of_service is not null and vac.date_of_service >= @earliestDate) '
	--			WHEN 'Pharmacy' THEN N'' + @SelectKeysStatement 
	--				+ 'where vac.date_of_service >= @earliestDate '
	--			WHEN 'Dental' THEN N'' + @SelectKeysStatement 
	--				+ 'where fcd.date_of_service >= @earliestDate '
	--			ELSE 'ERROR!'
	--		END
	--	  END
	SET @SelectKeysStatement=N'' + @SelectKeysStatement 
		+ 'OPTION (MAXDOP 1)'      

    IF OBJECT_ID('#ClaimLineKeys', 'U') IS NOT NULL 
    BEGIN
        DROP TABLE #ClaimLineKeys
    END
    
    CREATE TABLE #ClaimLineKeys(
        id INT
    )
    
    --PRINT 'Executing the following statement: '+@SelectKeysStatement
	--PRINT 'Value of earliestDateTime: ' + CONVERT(varchar, @earliestVisitServiceDate)
    INSERT #ClaimLineKeys EXECUTE sp_executesql @SelectKeysStatement, @SelectKeysParmDefinition, @Ids = @IdTable
	--, @earliestDate = @earliestVisitServiceDate;

    -- Dynamic SQL has its own session, so it is not possible to use the same temp table
    -- Table variable is too slow.
    -- For now, we duplicated the same SQL, need to find a better way
	IF @earliestVisitServiceDate IS NULL
	BEGIN
		IF @BenefitType='Institutional'
		BEGIN
			SELECT * FROM vw_institutional_claim 
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END 
		ELSE IF @BenefitType='Professional'
		BEGIN
			SELECT * FROM vw_professional_claim  
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
		ELSE IF @BenefitType='Pharmacy'
		BEGIN
			SELECT * FROM vw_pharmacy_claim  
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
		ELSE IF @BenefitType='Dental'
		BEGIN
			SELECT * FROM vw_dental_claim  
			WHERE claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
	END
	ELSE
	BEGIN
		IF @BenefitType='Institutional'
		BEGIN
			SELECT * FROM vw_institutional_claim 
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END 
		ELSE IF @BenefitType='Professional'
		BEGIN
			SELECT * FROM vw_professional_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
		ELSE IF @BenefitType='Pharmacy'
		BEGIN
			SELECT * FROM vw_pharmacy_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
		ELSE IF @BenefitType='Dental'
		BEGIN
			SELECT * FROM vw_dental_claim  
			WHERE visit_date >= @earliestVisitServiceDate 
			and claim_line_number in (select id from  #ClaimLineKeys) 
--			AND to_score_indicator = 'Y'
--			OPTION (MAXDOP 1)
		END
	END

GO


