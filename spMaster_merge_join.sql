CREATE PROCEDURE [dbo].[spMastering_Hari1_Merge] (
--Requirement 1
@MasteringType NVARCHAR(50),
-- Requirement 2
@ActionType NVARCHAR(20),
-- Requirement 3 - Project_name to be mastered
@projectName NVARCHAR(200))
AS
BEGIN
	--SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
	--SET NOCOUNT ON;
	DECLARE @masterName VARCHAR(50)
		   ,@sourceSystem AS VARCHAR(50)
		   ,@sourceProject AS VARCHAR(50)
		   ,@masterRecCount AS VARCHAR(10)
		   ,@masteredProjectsCount INT = 0

	--##############################################################################
	-- INPUT Validation to process the Stored Procedure
	--##############################################################################
	IF LEN(ISNULL(@MasteringType, '')) = 0
	BEGIN
		RAISERROR (N'ERROR: Mastering Type Cannot be blank!', 16, 1)
		RETURN 0;
	END

	IF (LEN(ISNULL(@ActionType, '')) = 0
		OR @ActionType <> 'ADD'
		AND @ActionType <> 'DELETE')
	BEGIN
		RAISERROR (N'ERROR: ActionType Cannot be blank! Enter either ADD/DELETE', 16, 1)
		RETURN 0;
	END

	IF (ISNULL(@projectName, '') = '')
	BEGIN
		RAISERROR (N'ERROR: Project Name cannot be blank!', 16, 1)
		RETURN 0;
	END
	IF NOT EXISTS (SELECT
				NULL
			FROM provider__C_HariTest
			WHERE Project_Name__c = @projectName)
	BEGIN
		RAISERROR (N'ERROR: Please Enter Valid Project Name', 16, 1)
		RETURN 0;
	END

	--##############################################################################
	-- Setting SourceProject and SourceSystem Values
	--##############################################################################

	SELECT
		@masterName = @MasteringType
	   ,  --parameter value to be passed
		@sourceSystem = Client_Name__c
	   ,@sourceProject = Project_Name__c
	FROM provider__C_HariTest
	WHERE Project_Name__c = @projectName

	SELECT
		@masterRecCount = COUNT(1)
	FROM [dw].masters_ctrlAllMasters_masteredProjectsList
	WHERE sourceProject = @projectName

	SELECT TOP 1
		@masteredProjectsCount = 1
	FROM ProviderLocationJunction__c
	WHERE Project_Name__c = @projectName

	--##############################################################################
	-- Add the Map and List tables if ActionType = 'ADD'
	--##############################################################################

	IF (@ActionType = 'ADD'
		AND @masterRecCount = 0)
	BEGIN
		IF @masteredProjectsCount = 1
			IF EXISTS (SELECT
						1
					FROM [dw].[masters_ctrlAllMasters_masteredProjectsList]
					WHERE sourceProject = @sourceProject
					AND sourceSystem = @sourceSystem
					AND masterName = @masterName)

				PRINT @masterName + '~' + @sourceSystem + '~' + @sourceProject + '~' + 'This combination Already Present'
			ELSE

				INSERT INTO [dw].[masters_ctrlAllMasters_masteredProjectsList] (MasteredProjetsListKey, masterName, sourceSystem, sourceProject)
					VALUES (NEXT VALUE FOR dw.masteredAutoSequence, @masterName, @sourceSystem, @sourceProject)


		IF OBJECT_ID('tempdb..#rel_qap_data') IS NOT NULL
			DROP TABLE #rel_qap_data
		IF OBJECT_ID('tempdb..#poc__location_master_hash_gen') IS NOT NULL
			DROP TABLE #poc__location_master_hash_gen;
		--(A) PROJECT SELECTION
		WITH project_selection
		AS
		(SELECT
				Client_Name__c
			   ,Project_Name__c
			   ,COUNT(1) count_recs
			FROM provider__C_HariTest
			WHERE 1 = 1
			AND Project_Name__c = @sourceProject
			AND Client_Name__c = @sourceSystem
			GROUP BY Client_Name__c
					,Project_Name__c)   --select * from project_selection --220325 



		--(B) From PL-Jn, prepare to remove LID duplicates at provider level & Collect Provider / LID combinations    
		,
		provider_loc_reference
		AS
		(SELECT
				ProviderLocationJunction__c.Provider__c
			   ,ProviderLocationJunction__c.Location__c
			   ,ROW_NUMBER() OVER (PARTITION BY ProviderLocationJunction__c.Location__c
				, ProviderLocationJunction__c.Provider__c
				ORDER BY ProviderLocationJunction__c.LastModifiedDate)
				AS rownum_remove_lid_dupes_for_a_provider
			   ,CASE
					WHEN ISNULL(ProviderLocationJunction__c.LastModifiedDate, ProviderLocationJunction__c.CreatedDate) > ProviderLocationJunction__c.CreatedDate THEN ProviderLocationJunction__c.LastModifiedDate
					ELSE ProviderLocationJunction__c.CreatedDate
				END AS plj_record_date
			FROM project_selection
			JOIN ProviderLocationJunction__c
				ON project_selection.Project_Name__c = ProviderLocationJunction__c.Project_Name__c) --select * from provider_loc_reference where rownum_remove_lid_dupes_for_a_provider=1 --(220638 row(s) affected)

		,
		rel_qap_data
		AS
		( --~~~~~~~~~~~~~~~~Logic to add data from more projects for day 0 mastering needs to be added here~~~~~~~~~~~~~~~~~~
			SELECT
				provider_loc_reference.Location__c
			   ,provider_loc_reference.Provider__c
			   ,provider_loc_reference.plj_record_date
			   ,Chart__c.Id chart_id

				---Provider Contact Data Set
			   ,provider__C_HariTest.NPI__c
			   ,provider__C_HariTest.TIN_ID__c
			   ,provider__C_HariTest.Provider_Full_Address__c
			   ,provider__C_HariTest.Provider_Full_Address_With_Suite__c
			   ,provider__C_HariTest.Address_Line_1__c Address_Line
			   ,provider__C_HariTest.Address_Line_2__c Address_Line_Suite
			   ,CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '') AS Address_With_Suite
			   ,provider__C_HariTest.City__c
			   ,provider__C_HariTest.State__c
			   ,provider__C_HariTest.ZIP_Code__c
			   ,CASE
					WHEN provider__C_HariTest.Address_Line_1__c IS NULL THEN ''
					ELSE provider__C_HariTest.Address_Line_1__c + ' '
				END
				+
				CASE
					WHEN provider__C_HariTest.Address_Line_2__c IS NULL THEN ''
					ELSE provider__C_HariTest.Address_Line_2__c + ' '
				END
				+
				CASE
					WHEN provider__C_HariTest.City__c IS NULL THEN ''
					ELSE provider__C_HariTest.City__c + ' '
				END
				+
				CASE
					WHEN provider__C_HariTest.State__c IS NULL THEN ''
					ELSE provider__C_HariTest.State__c + ' '
				END
				+
				CASE
					WHEN provider__C_HariTest.ZIP_Code__c IS NULL THEN ''
					ELSE provider__C_HariTest.ZIP_Code__c + ' '
				END
				AS full_address
				--
			   ,dbo.fn_cleanse_phone(ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)) Phone_Number_Select
			   ,dbo.fn_cleanse_phone(provider__C_HariTest.Phone_Number__c) Phone_Number_1
			   ,dbo.fn_cleanse_phone(provider__C_HariTest.Secondary_Phone_Number__c) Phone_Number_2
			   ,dbo.fn_cleanse_phone(provider__C_HariTest.Fax_Number__c) Fax_Number__c
				---Provider Location Data Control Field generation
			   ,ROW_NUMBER() OVER (PARTITION BY provider_loc_reference.Location__c
				ORDER BY plj_record_date)
				AS rownum_remove_location_repeatition_forMasterProcessing

			   ,ROW_NUMBER() OVER (PARTITION BY provider_loc_reference.Location__c
				, provider_loc_reference.Provider__c
				ORDER BY CASE
					WHEN ISNULL(Chart__c.LastModifiedDate, Chart__c.CreatedDate) > Chart__c.CreatedDate THEN Chart__c.LastModifiedDate
					ELSE Chart__c.CreatedDate
				END
				DESC) --sorting by chart dates, actually not required when we are intrested in removing chart info and go to location,provider level
				AS rownum_remove_chartLvl_reptions_for_cl_processing

				---Charts Retried at location by various compbinations of the key fields npi tin address and phone
			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider_loc_reference.Location__c)
				AS charts_at_loc

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, provider__C_HariTest.TIN_ID__c
				, CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiTinAddPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, provider__C_HariTest.TIN_ID__c
				, CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiTinAdd

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, provider__C_HariTest.TIN_ID__c
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiTinPhone


			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiAddPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiAdd

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, provider__C_HariTest.TIN_ID__c
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpiTin

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.TIN_ID__c
				, CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byTinAddPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.TIN_ID__c
				, ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byTinPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.TIN_ID__c
				, provider__C_HariTest.Provider_Full_Address_With_Suite__c + ' | ' + provider__C_HariTest.City__c + ' | ' + State__c + ' | ' + ZIP_Code__c
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byTinAdd

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY ISNULL(provider__C_HariTest.Phone_Number__c, Secondary_Phone_Number__c)
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byPhone

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY CASE
					WHEN Address_Line_1__c IS NULL THEN ''
					ELSE Address_Line_1__c + ' '
				END + ISNULL(Address_Line_2__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.City__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.State__c, '')
				+ ' | ' + ISNULL(provider__C_HariTest.ZIP_Code__c, '')
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byAddress

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.NPI__c
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byNpi

			   ,COUNT(Chart__c.Id)
				OVER (PARTITION BY provider__C_HariTest.TIN_ID__c
				, provider_loc_reference.Location__c)
				AS CountOfCharts_atLoc_byTin

				---Charts retreival numbers by type of retreival 
			   ,Chart__c.MRT_Name__c
			   ,CASE
					WHEN CHARINDEX('FTP', Chart__c.MRT_Name__c) > 0 THEN 'SFTP'
					WHEN CHARINDEX('Mail Room', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('MailRoom', Chart__c.MRT_Name__c) > 0 THEN 'Mail Room'
					WHEN CHARINDEX('Ringcentral', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Ring central', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Vitelity', Chart__c.MRT_Name__c) > 0 THEN 'Fax'
					WHEN CHARINDEX('EMail', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('email', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('E Mail', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('E-Mail', Chart__c.MRT_Name__c) > 0 THEN 'E-Mail'
					WHEN (CHARINDEX('Ciox', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Diversified', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Diversified Medical Records Services', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('DMRS', Chart__c.MRT_Name__c) > 0 OR
						Chart__c.MRT_Name__c = 'BOX' OR
						CHARINDEX('Chartswap', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Chart swap', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Sharecare', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Share care', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('MRO', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('DataFile', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Data File', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Scanstat', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Scan stat', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Health', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Integrity Document Solutions', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('IDS', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('BACTES', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('IOD', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('STAT Imaging', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Imaging', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Chart', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Photostat', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Photo stat', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Midwest ROI', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Verisma', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Dynamic Document Imaging', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('DDI', Chart__c.MRT_Name__c) > 0 OR
						CHARINDEX('Medical Records', Chart__c.MRT_Name__c) > 0) THEN 'CopyService'
					WHEN Chart__c.MRT_Name__c IS NULL THEN 'NullsOthers'
					ELSE 'Field'
				END Retreival_type
			FROM provider_loc_reference
			JOIN Chart__c
				ON (provider_loc_reference.Location__c = Chart__c.Location__c
				AND provider_loc_reference.Provider__c = Chart__c.Provider__c)
			JOIN provider__C_HariTest
				ON provider_loc_reference.Provider__c = provider__C_HariTest.Id
			WHERE FileBound_Disposition_Code__c = 'QAP'
			AND FileBound_Status__c = 'REL'
			AND MRT_Name__c <> 'Replication'
			AND provider_loc_reference.rownum_remove_lid_dupes_for_a_provider = 1)

		SELECT
			rel_qap_data.*
		   ,CASE
				WHEN Retreival_type = 'SFTP' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_SFTP
		   ,CASE
				WHEN Retreival_type = 'Mail Room' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_MailRoom
		   ,CASE
				WHEN Retreival_type = 'Fax' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_Fax
		   ,CASE
				WHEN Retreival_type = 'E-Mail' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_eMail
		   ,CASE
				WHEN Retreival_type = 'CopyService' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_CopyService
		   ,CASE
				WHEN Retreival_type = 'Field' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_Field
		   ,CASE
				WHEN Retreival_type = 'NullsOthers' THEN 1
				ELSE 0
			END AS location_charts_count_retreived_via_NullOthers INTO #rel_qap_data
		FROM rel_qap_data

		-------------------------------------------Collecting sum of retreivals types-------------------------------------------
		IF OBJECT_ID('tempdb..#locationWise_ChartsCollected_byTypeOfRetreival') IS NOT NULL
			DROP TABLE #locationWise_ChartsCollected_byTypeOfRetreival;
		WITH retreival_types
		AS
		(SELECT
				#rel_qap_data.Location__c
			   ,charts_at_loc
			   ,SUM(location_charts_count_retreived_via_SFTP) AS charts_retreived_via_SFTP
			   ,SUM(location_charts_count_retreived_via_MailRoom) AS charts_retreived_via_MailRoom
			   ,SUM(location_charts_count_retreived_via_Fax) AS charts_retreived_via_Fax
			   ,SUM(location_charts_count_retreived_via_eMail) AS charts_retreived_via_eMail
			   ,SUM(location_charts_count_retreived_via_CopyService) AS charts_retreived_via_CopyService
			   ,SUM(location_charts_count_retreived_via_Field) AS charts_retreived_via_Field
			   ,SUM(location_charts_count_retreived_via_NullOthers) AS charts_retreived_via_NullOthers
			FROM #rel_qap_data
			GROUP BY #rel_qap_data.Location__c
					,charts_at_loc) --SELECT * FROM RETREIVAL_TYPES

		SELECT
			Location__c
		   ,charts_at_loc
		   ,highestRetreivalType AS Best_Retreival_Type
		   ,charts_retreived_via_SFTP
		   ,charts_retreived_via_MailRoom
		   ,charts_retreived_via_Fax
		   ,charts_retreived_via_eMail
		   ,charts_retreived_via_CopyService
		   ,charts_retreived_via_Field
		   ,charts_retreived_via_NullOthers INTO #locationWise_ChartsCollected_byTypeOfRetreival
		FROM retreival_types
		CROSS APPLY (SELECT TOP 1
				location__c highestRetreivalType
			FROM (VALUES('SFTP', charts_retreived_via_SFTP)
			, ('MailRoom', charts_retreived_via_MailRoom)
			, ('Fax', charts_retreived_via_Fax)
			, ('eMail', charts_retreived_via_eMail)
			, ('CopyService', charts_retreived_via_CopyService)
			, ('Field', charts_retreived_via_Field)
			, ('NullOthers', charts_retreived_via_NullOthers)
			) x (location__c, value)
			ORDER BY value DESC) x




		--(C) Create data from Locations to be mastered and generate new_lid

		SELECT
			Location2__c.Id Location__c
		   ,Location2__c.Name SFDC_LID
			--LID Generation
		   ,CONVERT(VARCHAR(32), HASHBYTES('MD5',
			'#$%^&' +
			CASE
				WHEN Address_Line_1__c IS NULL THEN ''
				ELSE Address_Line_1__c + ' '
			END + ISNULL(Address_Line_2__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.City__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.State__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.ZIP_Code__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(dbo.fn_cleanse_phone(ISNULL(Location2__c.Phone_1__c, Location2__c.Phone_2__c)), '') + '&^%$#')
			, 2) NEW_LID_ALL

		   ,CONVERT(VARCHAR(32), HASHBYTES('MD5',
			'#$%^&' +
			CASE
				WHEN Address_Line_1__c IS NULL THEN ''
				ELSE Address_Line_1__c + ' '
			END + ISNULL(Address_Line_2__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.City__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.State__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.ZIP_Code__c, '') + '&^%$#')
			, 2) NEW_LID_ADD

		   ,CONVERT(VARCHAR(32), HASHBYTES('MD5', '#$%^&' + ISNULL(dbo.fn_cleanse_phone(ISNULL(Location2__c.Phone_1__c, Location2__c.Phone_2__c)), '') + '&^%$#')
			, 2) NEW_LID_PHONE

		   ,SUBSTRING(ISNULL(dbo.fn_cleanse_phone(ISNULL(Location2__c.Phone_1__c, Location2__c.Phone_2__c)), ''), 1, 9) base_phone   ---BASE PHONE

		   ,CONVERT(VARCHAR(32), HASHBYTES('MD5',
			'#$%^&' +
			CASE
				WHEN Address_Line_1__c IS NULL THEN ''
				ELSE Address_Line_1__c + ' '
			END + ISNULL(Address_Line_2__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.City__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.State__c, '') + '&^%$#'
			+ '#$%^&' + ISNULL(Location2__c.ZIP_Code__c, '') + '&^%$#'
			+ '#$%^&' + SUBSTRING(ISNULL(dbo.fn_cleanse_phone(ISNULL(Location2__c.Phone_1__c, Location2__c.Phone_2__c)), ''), 1, 9) + '&^%$#')
			, 2) NEW_LID_ADD_BASE_PHONE

			--Contact Info
		   ,Location2__c.Client_Name__c
		   ,Location2__c.Contact_Person__c
		   ,Location2__c.Contact_Person_2__c
		   ,Location2__c.Practice_Name__c
		   ,Location2__c.Provider_Full_Address__c
		   ,Location2__c.Provider_Full_Address_With_Suite__c
		   ,Address_Line_1__c Address_Line
		   ,Address_Line_2__c Address_Suite_No
		   ,CASE
				WHEN Address_Line_1__c IS NULL THEN ''
				ELSE Address_Line_1__c + ' '
			END + ISNULL(Address_Line_2__c, '') AS Address_With_Suite
		   ,Location2__c.City__c
		   ,Location2__c.State__c
		   ,CASE
				WHEN LEN(Location2__c.ZIP_Code__c) < 5 THEN '0' + Location2__c.ZIP_Code__c
				ELSE Location2__c.ZIP_Code__c
			END AS ZIP_Code__c
		   ,dbo.fn_cleanse_phone(Location2__c.Phone_1__c) Phone_1__c
		   ,Location2__c.Phone_1_Ext__c
		   ,dbo.fn_cleanse_phone(Location2__c.Phone_2__c) Phone_2__c
		   ,Location2__c.Phone_2_Ext__c
		   ,dbo.fn_cleanse_phone(Location2__c.Fax_Number__c) AS Fax_Number__c
		   ,dbo.fn_cleanse_phone(Location2__c.Fax_Number_Console__c) AS Fax_Number_Console__c
		   ,dbo.fn_cleanse_phone(Location2__c.Fax_Number_For_Display__c) AS Fax_Number_For_Display__c

			--Medical Records Storage
		   ,Location2__c.Record_Storage_Type__c
		   ,Location2__c.EMR_System__c
		   ,Location2__c.EMR_Version__c
		   ,Location2__c.Other_EMR_System__c


			--,case when Location2__c.Provider_Full_Address__c is null 
			--           and isnull(Location2__c.Phone_1__c,Location2__c.Phone_2__c) is null
			--      then '1'
			--      else '0'
			--  end FaxBlastRetreival_Flag


			--Charts Information
		   ,Location2__c.CreatedDate
		   ,Location2__c.Num_of_Charts__c
		   ,#rel_qap_data.charts_at_loc charts_recovered_at_loc

		   ,Best_Retreival_Type
		   ,charts_retreived_via_SFTP
		   ,charts_retreived_via_MailRoom
		   ,charts_retreived_via_Fax
		   ,charts_retreived_via_eMail
		   ,charts_retreived_via_CopyService
		   ,charts_retreived_via_Field
		   ,charts_retreived_via_NullOthers
			--,Location2__c.Num_of_Charts_Recovered__c
			--,Location2__c.Charts_Remaining__c
			--,Location2__c.Num_of_Charts_Scheduled__c
			--,Location2__c.of_charts_REL__c
			--,Location2__c.Percent_of_Charts_Recovered__c

			--Other Info
		   ,Location2__c.Appointment_Type__c
		   ,Location2__c.Workflow_Status__c
		   ,Location2__c.Invoices__c
		   ,Location2__c.PNP_Reason__c
		   ,Location2__c.Location__Latitude__s
		   ,Location2__c.Location__Longitude__s INTO #poc__location_master_hash_gen
		FROM #rel_qap_data
		JOIN Location2__c
			ON #rel_qap_data.Location__c = Location2__c.Id
				AND rownum_remove_location_repeatition_forMasterProcessing = 1
		JOIN #locationWise_ChartsCollected_byTypeOfRetreival
			ON #rel_qap_data.Location__c = #locationWise_ChartsCollected_byTypeOfRetreival.Location__c
		ORDER BY Provider_Full_Address__c
		, Phone_2__c

		--(D) SFLID-NEWLID_MAP (THE MERGE REPORT!!!!!!)
		IF OBJECT_ID('tempdb..#poc__lm__sfdcLid_newLid_map') IS NOT NULL
			DROP TABLE #poc__lm__sfdcLid_newLid_map
		SELECT
			New_lid_all
		   ,NEW_LID_ADD
		   ,NEW_LID_PHONE
		   ,NEW_LID_ADD_BASE_PHONE
		   ,Location__c
		   ,SFDC_LID
		   ,CreatedDate sfdcLidCreatedDate
		   ,COUNT(SFDC_LID) OVER (PARTITION BY New_lid_all) AS count_sfLid_at_NewLIdAll
		   ,COUNT(SFDC_LID) OVER (PARTITION BY NEW_LID_ADD) AS count_sfLid_at_NewLIdAdd
		   ,COUNT(SFDC_LID) OVER (PARTITION BY NEW_LID_PHONE) AS count_sfLid_at_NewLIdPhone
		   ,COUNT(SFDC_LID) OVER (PARTITION BY NEW_LID_ADD_BASE_PHONE) AS count_sfLid_at_NewLIdBasePhone INTO #poc__lm__sfdcLid_newLid_map
		FROM #poc__location_master_hash_gen
		ORDER BY COUNT(SFDC_LID) OVER (PARTITION BY New_lid_all) DESC

		--(F) LOCATION MASTERING
		IF OBJECT_ID('tempdb..#poc__lm__locations_master') IS NOT NULL
			DROP TABLE #poc__lm__locations_master;
		WITH buildMaster
		AS
		(SELECT
				COUNT(SFDC_LID)
				OVER (PARTITION BY New_lid_all)
				AS count_sfLid_at_NewLId
			   ,ROW_NUMBER()
				OVER (PARTITION BY New_lid_all
				ORDER BY charts_recovered_at_loc DESC)
				AS masteringFilter
			   ,#poc__location_master_hash_gen.*
			FROM #poc__location_master_hash_gen) --select * from buildMaster ORDER BY NEW_LID_ALL  
		SELECT
			New_lid_all

			--Contact Info
		   ,'Contact Info=>' DATASET_CONTACT_INFO
			--,Client_Name__c
		   ,Contact_Person__c
		   ,Contact_Person_2__c
		   ,Practice_Name__c
		   ,Address_Line
		   ,Address_Suite_No
		   ,Address_With_Suite
		   ,City__c
		   ,State__c
		   ,ZIP_Code__c
		   ,Phone_1__c
		   ,Phone_1_Ext__c
		   ,Phone_2__c
		   ,Phone_2_Ext__c
		   ,Fax_Number__c
		   ,Fax_Number_Console__c
		   ,Fax_Number_For_Display__c

			--Charts Information
		   ,'Charts Info=>' DATASET_CHARTS_INFO
		   ,CreatedDate                                --INCLUDED TOWARDS CHART SELECTION BY MOST RECENT LOCATION
		   ,Num_of_Charts__c charts_numbers_at_loc
		   ,charts_recovered_at_loc
		   ,CAST(ROUND(100 * (charts_recovered_at_loc / Num_of_Charts__c), 2, 1) AS DECIMAL(18, 2)) Percent_of_Charts_Recovered__c
		   ,Best_Retreival_Type
		   ,charts_retreived_via_SFTP
		   ,charts_retreived_via_MailRoom
		   ,charts_retreived_via_Fax
		   ,charts_retreived_via_eMail
		   ,charts_retreived_via_CopyService
		   ,charts_retreived_via_Field
		   ,charts_retreived_via_NullOthers

			--Medical Records Storage
		   ,'Med. Rec. Storage Info=>' DATASET_STORAGE_INFO
		   ,Record_Storage_Type__c
		   ,EMR_System__c
		   ,EMR_Version__c
		   ,Other_EMR_System__c

			--Other Info
		   ,'Other Info=>' DATASET_OTHER_INFO
		   ,Appointment_Type__c
		   ,Workflow_Status__c
		   ,Invoices__c
		   ,PNP_Reason__c
		   ,Location__Latitude__s
		   ,Location__Longitude__s INTO #poc__lm__locations_master
		FROM buildMaster --48902
		WHERE masteringFilter = 1
		ORDER BY New_lid_all
		, Provider_Full_Address__c
		, ISNULL(Phone_1__c, Phone_2__c)
		

		--(G) CL_NEW-LID_MAP
		IF OBJECT_ID('tempdb..#poc__lm__chaseList_newLid_map') IS NOT NULL
			DROP TABLE #poc__lm__chaseList_newLid_map

		SELECT
			@sourceSystem AS Client_name
		   ,@sourceProject AS Project_Name
		   ,#poc__lm__sfdcLid_newLid_map.New_lid_all
		   ,MAX(#poc__lm__sfdcLid_newLid_map.sfdcLidCreatedDate) AS CreatedDate
		   ,SUM(#rel_qap_data.charts_at_loc) charts_at_loc
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiTinAddPhone) CountOfCharts_atLoc_byNpiTinAddPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiAddPhone) CountOfCharts_atLoc_byNpiAddPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiPhone) CountOfCharts_atLoc_byNpiPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiAdd) CountOfCharts_atLoc_byNpiAdd
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiTinAdd) CountOfCharts_atLoc_byNpiTinAdd
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiTin) CountOfCharts_atLoc_byNpiTin
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpiTinPhone) CountOfCharts_atLoc_byNpiTinPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byTinAddPhone) CountOfCharts_atLoc_byTinAddPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byTinPhone) CountOfCharts_atLoc_byTinPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byTinAdd) CountOfCharts_atLoc_byTinAdd
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byPhone) CountOfCharts_atLoc_byPhone
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byAddress) CountOfCharts_atLoc_byAddress
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byNpi) CountOfCharts_atLoc_byNpi
		   ,SUM(#rel_qap_data.CountOfCharts_atLoc_byTin) CountOfCharts_atLoc_byTin 
		   INTO #poc__lm__chaseList_newLid_map
		   FROM #poc__lm__sfdcLid_newLid_map
		JOIN #rel_qap_data
			ON #poc__lm__sfdcLid_newLid_map.Location__c = #rel_qap_data.Location__c
				AND rownum_remove_chartLvl_reptions_for_cl_processing = 1
		GROUP BY #poc__lm__sfdcLid_newLid_map.New_lid_all
		ORDER BY New_lid_all DESC


		SELECT
			*
		FROM dw.masters_ctrlAllMasters_masteredProjectsList
		WHERE sourceProject = @sourceProject
		AND sourceSystem = @sourceSystem
		AND masterName = @masterName


		-- Control Table

		MERGE poc__locationMastering__chaseList_newLid_map AS TARGET USING #poc__lm__chaseList_newLid_map AS SOURCE
		ON (TARGET.New_lid_all = SOURCE.New_lid_all)
		WHEN MATCHED
			THEN UPDATE
				SET TARGET.charts_at_loc = TARGET.charts_at_loc + SOURCE.charts_at_loc
				   ,TARGET.CountOfCharts_atLoc_byNpiTinAddPhone = TARGET.CountOfCharts_atLoc_byNpiTinAddPhone + SOURCE.CountOfCharts_atLoc_byNpiTinAddPhone
				   ,TARGET.CountOfCharts_atLoc_byNpiAddPhone = TARGET.CountOfCharts_atLoc_byNpiAddPhone + SOURCE.CountOfCharts_atLoc_byNpiAddPhone
				   ,TARGET.CountOfCharts_atLoc_byNpiPhone = TARGET.CountOfCharts_atLoc_byNpiPhone + SOURCE.CountOfCharts_atLoc_byNpiPhone
				   ,TARGET.CountOfCharts_atLoc_byNpiAdd = TARGET.CountOfCharts_atLoc_byNpiAdd + SOURCE.CountOfCharts_atLoc_byNpiAdd
				   ,TARGET.CountOfCharts_atLoc_byNpiTinAdd = TARGET.CountOfCharts_atLoc_byNpiTinAdd + SOURCE.CountOfCharts_atLoc_byNpiTinAdd
				   ,TARGET.CountOfCharts_atLoc_byNpiTin = TARGET.CountOfCharts_atLoc_byNpiTin + SOURCE.CountOfCharts_atLoc_byNpiTin
				   ,TARGET.CountOfCharts_atLoc_byNpiTinPhone = TARGET.CountOfCharts_atLoc_byNpiTinPhone + SOURCE.CountOfCharts_atLoc_byNpiTinPhone
				   ,TARGET.CountOfCharts_atLoc_byTinAddPhone = TARGET.CountOfCharts_atLoc_byTinAddPhone + SOURCE.CountOfCharts_atLoc_byTinAddPhone
				   ,TARGET.CountOfCharts_atLoc_byTinPhone = TARGET.CountOfCharts_atLoc_byTinPhone + SOURCE.CountOfCharts_atLoc_byTinPhone
				   ,TARGET.CountOfCharts_atLoc_byTinAdd = TARGET.CountOfCharts_atLoc_byTinAdd + SOURCE.CountOfCharts_atLoc_byTinAdd
				   ,TARGET.CountOfCharts_atLoc_byPhone = TARGET.CountOfCharts_atLoc_byPhone + SOURCE.CountOfCharts_atLoc_byPhone
				   ,TARGET.CountOfCharts_atLoc_byAddress = TARGET.CountOfCharts_atLoc_byAddress + SOURCE.CountOfCharts_atLoc_byAddress
				   ,TARGET.CountOfCharts_atLoc_byNpi = TARGET.CountOfCharts_atLoc_byNpi + SOURCE.CountOfCharts_atLoc_byNpi
				   ,TARGET.CountOfCharts_atLoc_byTin = TARGET.CountOfCharts_atLoc_byTin + SOURCE.CountOfCharts_atLoc_byTin
		WHEN NOT MATCHED BY TARGET
			THEN INSERT (Client_name, Project_Name, New_lid_all, CreatedDate, charts_at_loc, CountOfCharts_atLoc_byNpiTinAddPhone, CountOfCharts_atLoc_byNpiAddPhone, CountOfCharts_atLoc_byNpiPhone, CountOfCharts_atLoc_byNpiAdd, CountOfCharts_atLoc_byNpiTinAdd, CountOfCharts_atLoc_byNpiTin, CountOfCharts_atLoc_byNpiTinPhone, CountOfCharts_atLoc_byTinAddPhone, CountOfCharts_atLoc_byTinPhone, CountOfCharts_atLoc_byTinAdd, CountOfCharts_atLoc_byPhone, CountOfCharts_atLoc_byAddress, CountOfCharts_atLoc_byNpi, CountOfCharts_atLoc_byTin)
					VALUES (SOURCE.Client_name, SOURCE.Project_Name, SOURCE.New_lid_all, SOURCE.CreatedDate, SOURCE.charts_at_loc, SOURCE.CountOfCharts_atLoc_byNpiTinAddPhone, SOURCE.CountOfCharts_atLoc_byNpiAddPhone, SOURCE.CountOfCharts_atLoc_byNpiPhone, SOURCE.CountOfCharts_atLoc_byNpiAdd, SOURCE.CountOfCharts_atLoc_byNpiTinAdd, SOURCE.CountOfCharts_atLoc_byNpiTin, SOURCE.CountOfCharts_atLoc_byNpiTinPhone, SOURCE.CountOfCharts_atLoc_byTinAddPhone, SOURCE.CountOfCharts_atLoc_byTinPhone, SOURCE.CountOfCharts_atLoc_byTinAdd, SOURCE.CountOfCharts_atLoc_byPhone, SOURCE.CountOfCharts_atLoc_byAddress, SOURCE.CountOfCharts_atLoc_byNpi, SOURCE.CountOfCharts_atLoc_byTin)
		OUTPUT $ACTION
			  ,DELETED.*
			  ,INSERTED.*;

		-- Mastering table
		INSERT INTO poc_locationMastering_Mastering_HariTest
		SELECT *FROM #poc__lm__locations_master
		WHERE NEW_LID_ALL NOT IN (SELECT new_lid_all FROM poc_locationMastering_Mastering_HariTest)


	END

	ELSE IF (@ActionType = 'DELETE')
	BEGIN  
         	IF NOT EXISTS (SELECT
						1
					FROM [dw].[masters_ctrlAllMasters_masteredProjectsList]
					WHERE sourceProject = @sourceProject
					AND sourceSystem = @sourceSystem
					AND masterName = @masterName)

                    PRINT @masterName + '~' + @sourceSystem + '~' + @sourceProject + '~' + 'This combination Yet to Mastered!'
             ELSE 
			   
			   INSERT INTO poc__locationMastering__chaseList_newLid_map_backup
			   SELECT * FROM poc__locationMastering__chaseList_newLid_map WHERE
			   Project_Name = @sourceProject
			   
			   DELETE FROM dw.masters_ctrlAllMasters_masteredProjectsList
               OUTPUT DELETED.*
			   WHERE sourceProject = @sourceProject
			   AND sourceSystem = @sourceSystem

			   DELETE FROM poc__locationMastering__chaseList_newLid_map
			   OUTPUT DELETED.*
			   WHERE Project_Name = @sourceProject

			   PRINT @masterName + '~' + @sourceSystem + '~' + @sourceProject + '~' + 'This combination has been deleted'
	 
	END
    
	ELSE     
	BEGIN
		PRINT 'Project Already Mastered.!'
	END
END