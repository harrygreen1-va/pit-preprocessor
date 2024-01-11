IF object_id('source_edits_tpa', 'U') IS NOT NULL
	DROP TABLE [dbo].source_edits_tpa;
	GO

CREATE TABLE [dbo].source_edits_tpa (
	source_edits_tpa_key BIGINT IDENTITY PRIMARY KEY
	,claim_key INT
	,claim_detail_key_in INT
	,claim_detail_key_pr INT
	,claim_id VARCHAR(60) NOT NULL
	,source_claim_line_id VARCHAR(60)
	,source_edit_id VARCHAR(60)
	,-- EditCode
	carc_list pit_description
	,-- EditSubCode
	rarc_list pit_description
	,carc1 pit_short_name
	,carc1_desc VARCHAR(MAX)
	,carc2 pit_short_name
	,carc3 pit_short_name
	,carc4 pit_short_name
	,carc5 pit_short_name
	,rarc1 pit_short_name
	,rarc2 pit_short_name
	,rarc3 pit_short_name
	,rarc4 pit_short_name
	,rarc5 pit_short_name
	,last_updated_user VARCHAR(128) DEFAULT suser_name()
	,last_updated_date DATETIME DEFAULT getdate()
	,etl_batch_id pit_natural_key NOT NULL
	,CONSTRAINT se_tpa_fk_claim_key FOREIGN KEY (claim_key) REFERENCES dim_va_claim ON DELETE CASCADE
	,CONSTRAINT se_tpa_fk_claim_key_claim_detail_key_pr FOREIGN KEY (claim_detail_key_pr) REFERENCES f_professional_medical_claim_details(claim_detail_key) ON DELETE SET NULL
	,CONSTRAINT se_tpa_fk_claim_key_claim_detail_key_in FOREIGN KEY (claim_detail_key_in) REFERENCES f_institutional_medical_claim_details(claim_detail_key) ON DELETE SET NULL
	,
	);
GO

-- indexes:
IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_claim_key'
		)
	DROP INDEX se_tpa_claim_key ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_claim_key ON source_edits_tpa (claim_key) include (source_edits_tpa_key);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_line_key_pr'
		)
	DROP INDEX se_tpa_line_key_pr ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_line_key_pr ON source_edits_tpa (claim_detail_key_pr) include (source_edits_tpa_key);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_line_key_in'
		)
	DROP INDEX se_tpa_line_key_in ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_line_key_in ON source_edits_tpa (claim_detail_key_in) include (source_edits_tpa_key);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_batch_id'
		)
	DROP INDEX se_tpa_batch_id ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_batch_id ON source_edits_tpa (etl_batch_id);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_claim_id'
		)
	DROP INDEX se_tpa_claim_id ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_claim_id ON source_edits_tpa (claim_id);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_line_id'
		)
	DROP INDEX se_tpa_line_id ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_line_id ON source_edits_tpa (source_claim_line_id);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_carc_list'
		)
	DROP INDEX se_tpa_carc_list ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_carc_list ON source_edits_tpa (carc_list);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_rarc_list'
		)
	DROP INDEX se_tpa_rarc_list ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_rarc_list ON source_edits_tpa (rarc_list);

IF EXISTS (
		SELECT name
		FROM sysindexes
		WHERE name = 'se_tpa_last_updated_date'
		)
	DROP INDEX se_tpa_last_updated_date ON source_edits_tpa
	GO

CREATE NONCLUSTERED INDEX se_tpa_last_updated_date ON source_edits_tpa (last_updated_date DESC);