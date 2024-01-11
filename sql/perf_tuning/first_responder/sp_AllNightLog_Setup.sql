set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

if OBJECT_ID('dbo.sp_AllNightLog_Setup') is null
    exec ('CREATE PROCEDURE dbo.sp_AllNightLog_Setup AS RETURN 0;');
go


alter procedure dbo.sp_allnightlog_setup @rposeconds bigint = 30,
                                         @rtoseconds bigint = 30,
                                         @backuppath nvarchar(max) = null,
                                         @restorepath nvarchar(max) = null,
                                         @jobs tinyint = 10,
                                         @runsetup bit = 0,
                                         @updatesetup bit = 0,
                                         @enablebackupjobs int = null,
                                         @enablerestorejobs int = null,
                                         @debug bit = 0,
                                         @firstfullbackup bit = 0,
                                         @firstdiffbackup bit = 0,
                                         @help bit = 0,
                                         @version varchar(30) = null output,
                                         @versiondate datetime = null output,
                                         @versioncheckmode bit = 0
    with recompile
as
    set nocount on;

begin
    ;

    select @version = '3.96', @versiondate = '20200712';

    if (@versioncheckmode = 1)
        begin
            return;
        end;

    if @help = 1
        begin

            print '
		/*


		sp_AllNightLog_Setup from http://FirstResponderKit.org
		
		This script sets up a database, tables, rows, and jobs for sp_AllNightLog, including:

		* Creates a database
			* Right now it''s hard-coded to use msdbCentral, that might change later
	
		* Creates tables in that database!
			* dbo.backup_configuration
				* Hold variables used by stored proc to make runtime decisions
					* RPO: Seconds, how often we look for databases that need log backups
					* Backup Path: The path we feed to Ola H''s backup proc
			* dbo.backup_worker
				* Holds list of databases and some information that helps our Agent jobs figure out if they need to take another log backup
		
		* Creates tables in msdb
			* dbo.restore_configuration
				* Holds variables used by stored proc to make runtime decisions
					* RTO: Seconds, how often to look for log backups to restore
					* Restore Path: The path we feed to sp_DatabaseRestore 
			* dbo.restore_worker
				* Holds list of databases and some information that helps our Agent jobs figure out if they need to look for files to restore
	
		 * Creates agent jobs
			* 1 job that polls sys.databases for new entries
			* 10 jobs that run to take log backups
			 * Based on a queue table
			 * Requires Ola Hallengren''s Database Backup stored proc
	
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
	
		Known limitations of this version:
		 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000! And really, maybe not even anything less than 2016. Heh.
		 - The repository database name is hard-coded to msdbCentral.
	
		Unknown limitations of this version:
		 - None.  (If we knew them, they would be known. Duh.)
	
	     Changes - for the full list of improvements and fixes in this version, see:
	     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	
		Parameter explanations:
	
		  @RunSetup	BIT, defaults to 0. When this is set to 1, it will run the setup portion to create database, tables, and worker jobs.
		  @UpdateSetup BIT, defaults to 0. When set to 1, will update existing configs for RPO/RTO and database backup/restore paths.
		  @RPOSeconds BIGINT, defaults to 30. Value in seconds you want to use to determine if a new log backup needs to be taken.
		  @BackupPath NVARCHAR(MAX), defaults to = ''D:\Backup''. You 99.99999% will need to change this path to something else. This tells Ola''s job where to put backups.
		  @Debug BIT, defaults to 0. Whent this is set to 1, it prints out dynamic SQL commands
	
	    Sample call:
		EXEC dbo.sp_AllNightLog_Setup
			@RunSetup = 1,
			@RPOSeconds = 30,
			@BackupPath = N''M:\MSSQL\Backup'',
			@Debug = 1


		For more documentation: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	    MIT License
		
		Copyright (c) 2020 Brent Ozar Unlimited
	
		Permission is hereby granted, free of charge, to any person obtaining a copy
		of this software and associated documentation files (the "Software"), to deal
		in the Software without restriction, including without limitation the rights
		to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
		copies of the Software, and to permit persons to whom the Software is
		furnished to do so, subject to the following conditions:
	
		The above copyright notice and this permission notice shall be included in all
		copies or substantial portions of the Software.
	
		THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
		IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
		FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
		AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
		LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
		OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
		SOFTWARE.


		*/';

            return;
        end; /* IF @Help = 1 */

    declare @database nvarchar(128) = null; --Holds the database that's currently being processed
    declare @error_number int = null; --Used for TRY/CATCH
    declare @error_severity int; --Used for TRY/CATCH
    declare @error_state int; --Used for TRY/CATCH
    declare @msg nvarchar(4000) = N''; --Used for RAISERROR
    declare @rpo int; --Used to hold the RPO value in our configuration table
    declare @backup_path nvarchar(max); --Used to hold the backup path in our configuration table
    declare @db_sql nvarchar(max) = N''; --Used to hold the dynamic SQL to create msdbCentral
    declare @tbl_sql nvarchar(max) = N''; --Used to hold the dynamic SQL that creates tables in msdbCentral
    declare @database_name nvarchar(256) = N'msdbCentral';
    --Used to hold the name of the database we create to centralize data
    --Right now it's hardcoded to msdbCentral, but I made it dynamic in case that changes down the line


/*These variables control the loop to create/modify jobs*/
    declare @job_sql nvarchar(max) = N''; --Used to hold the dynamic SQL that creates Agent jobs
    declare @counter int = 0; --For looping to create 10 Agent jobs
    declare @job_category nvarchar(max) = N'''Database Maintenance'''; --Job category
    declare @job_owner nvarchar(128) = QUOTENAME(SUSER_SNAME(0x01), ''''); -- Admin user/owner
    declare @jobs_to_change table
                            (
                                name sysname
                            ); -- list of jobs we need to enable or disable
    declare @current_job_name sysname; -- While looping through Agent jobs to enable or disable
    declare @active_start_date int = (CONVERT(int, CONVERT(varchar(10), GETDATE(), 112)));
    declare @started_waiting_for_jobs datetime;
    --We need to wait for a while when disabling jobs

/*Specifically for Backups*/
    declare @job_name_backups nvarchar(max) = N'''sp_AllNightLog_Backup_Job_'''; --Name of log backup job
    declare @job_description_backups nvarchar(max) = N'''This is a worker for the purposes of taking log backups from msdbCentral.dbo.backup_worker queue table.'''; --Job description
    declare @job_command_backups nvarchar(max) = N'''EXEC sp_AllNightLog @Backup = 1''';
    --Command the Agent job will run

/*Specifically for Restores*/
    declare @job_name_restores nvarchar(max) = N'''sp_AllNightLog_Restore_Job_'''; --Name of log backup job
    declare @job_description_restores nvarchar(max) = N'''This is a worker for the purposes of restoring log backups from msdb.dbo.restore_worker queue table.'''; --Job description
    declare @job_command_restores nvarchar(max) = N'''EXEC sp_AllNightLog @Restore = 1''';
    --Command the Agent job will run


/*

Sanity check some variables

*/


    if ((@runsetup = 0 or @runsetup is null) and (@updatesetup = 0 or @updatesetup is null))
        begin

            raiserror ('You have to either run setup or update setup. You can''t not do neither nor, if you follow. Or not.', 0, 1) with nowait;

            return;

        end;


/*

Should be a positive number

*/

    if (@rposeconds < 0)
        begin
            raiserror ('Please choose a positive number for @RPOSeconds', 0, 1) with nowait;

            return;
        end;


/*

Probably shouldn't be more than 20

*/

    if (@jobs > 20) or (@jobs < 1)
        begin
            raiserror ('We advise sticking with 1-20 jobs.', 0, 1) with nowait;

            return;
        end;

/*

Probably shouldn't be more than 4 hours

*/

    if (@rposeconds >= 14400)
        begin

            raiserror ('If your RPO is really 4 hours, perhaps you''d be interested in a more modest recovery model, like SIMPLE?', 0, 1) with nowait;

            return;
        end;


/*

Can't enable both the backup and restore jobs at the same time

*/

    if @enablebackupjobs = 1 and @enablerestorejobs = 1
        begin

            raiserror ('You are not allowed to enable both the backup and restore jobs at the same time. Pick one, bucko.', 0, 1) with nowait;

            return;
        end;

/*
Make sure xp_cmdshell is enabled
*/
    if not EXISTS(select * from sys.configurations where name = 'xp_cmdshell' and value_in_use = 1)
        begin
            raiserror ('xp_cmdshell must be enabled so we can get directory contents to check for new databases to restore.', 0, 1) with nowait

            return;
        end

/*
Make sure Ola Hallengren's scripts are installed in master
*/
    if 2 <> (select COUNT(*) from master.sys.procedures where name in ('CommandExecute', 'DatabaseBackup'))
        begin
            raiserror ('Ola Hallengren''s CommandExecute and DatabaseBackup must be installed in the master database. More info: http://ola.hallengren.com', 0, 1) with nowait

            return;
        end

/*
Make sure sp_DatabaseRestore is installed in master
*/
    if not EXISTS(select * from master.sys.procedures where name = 'sp_DatabaseRestore')
        begin
            raiserror ('sp_DatabaseRestore must be installed in master. To get it: http://FirstResponderKit.org', 0, 1) with nowait

            return;
        end

/*

Basic path sanity checks

*/

    if (@backuppath not like '[c-zC-Z]:\%') --Local path, don't think anyone has A or B drives
        and (@backuppath not like '\\[a-zA-Z0-9]%\%') --UNC path

        begin
            raiserror ('Are you sure that''s a real path?', 0, 1) with nowait;

            return;
        end;

/*

If you want to update the table, one of these has to not be NULL

*/

    if @updatesetup = 1
        and (@rposeconds is null
            and @backuppath is null
            and @rposeconds is null
            and @restorepath is null
            and @enablebackupjobs is null
            and @enablerestorejobs is null
           )
        begin

            raiserror ('If you want to update configuration settings, they can''t be NULL. Please Make sure @RPOSeconds / @RTOSeconds or @BackupPath / @RestorePath has a value', 0, 1) with nowait;

            return;

        end;


    if @updatesetup = 1
        goto updateconfigs;

    if @runsetup = 1
        begin
            begin try

                begin


                    /*

                    First check to see if Agent is running -- we'll get errors if it's not

                    */


                    if (select 1
                        from sys.all_objects
                        where name = 'dm_server_services') is not null
                        begin

                            if EXISTS(
                                    select 1
                                    from sys.dm_server_services
                                    where servicename like 'SQL Server Agent%'
                                      and status_desc = 'Stopped'
                                )
                                begin

                                    raiserror ('SQL Server Agent is not currently running -- it needs to be enabled to add backup worker jobs and the new database polling job', 0, 1) with nowait;

                                    return;

                                end;

                        end


                    begin


                        /*

                        Check to see if the database exists

                        */

                        raiserror ('Checking for msdbCentral', 0, 1) with nowait;

                        set @db_sql += N'

							IF DATABASEPROPERTYEX(' + QUOTENAME(@database_name, '''') + ', ''Status'') IS NULL

								BEGIN

									RAISERROR(''Creating msdbCentral'', 0, 1) WITH NOWAIT;

									CREATE DATABASE ' + QUOTENAME(@database_name) + ';
									
									ALTER DATABASE ' + QUOTENAME(@database_name) + ' SET RECOVERY FULL;
								
								END

							';


                        if @debug = 1
                            begin
                                raiserror (@db_sql, 0, 1) with nowait;
                            end;


                        if @db_sql is null
                            begin
                                raiserror ('@db_sql is NULL for some reason', 0, 1) with nowait;
                            end;


                        exec sp_executesql @db_sql;


                        /*

                        Check for tables and stuff

                        */


                        raiserror ('Checking for tables in msdbCentral', 0, 1) with nowait;

                        set @tbl_sql += N'
							
									USE ' + QUOTENAME(@database_name) + '
									
									
									IF OBJECT_ID(''' + QUOTENAME(@database_name) + '.dbo.backup_configuration'') IS NULL
									
										BEGIN
										
										RAISERROR(''Creating table dbo.backup_configuration'', 0, 1) WITH NOWAIT;
											
											CREATE TABLE dbo.backup_configuration (
																			database_name NVARCHAR(256), 
																			configuration_name NVARCHAR(512), 
																			configuration_description NVARCHAR(512), 
																			configuration_setting NVARCHAR(MAX)
																			);
											
										END
										
									ELSE 
										
										BEGIN
											
											
											RAISERROR(''Backup configuration table exists, truncating'', 0, 1) WITH NOWAIT;
										
											
											TRUNCATE TABLE dbo.backup_configuration

										
										END


											RAISERROR(''Inserting configuration values'', 0, 1) WITH NOWAIT;

											
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''log backup frequency'', ''The length of time in second between Log Backups.'', ''' +
                                        CONVERT(nvarchar(10), @rposeconds) + ''');
											
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''log backup path'', ''The path to which Log Backups should go.'', ''' +
                                        @backuppath + ''');
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''change backup type'', ''For Ola Hallengren DatabaseBackup @ChangeBackupType param: Y = escalate to fulls, MSDB = escalate by checking msdb backup history.'', ''MSDB'');									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''encrypt'', ''For Ola Hallengren DatabaseBackup: Y = encrypt the backup. N (default) = do not encrypt.'', NULL);									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''encryptionalgorithm'', ''For Ola Hallengren DatabaseBackup: native 2014 choices include TRIPLE_DES_3KEY, AES_128, AES_192, AES_256.'', NULL);									
									
											INSERT dbo.backup_configuration (database_name, configuration_name, configuration_description, configuration_setting) 
															  VALUES (''all'', ''servercertificate'', ''For Ola Hallengren DatabaseBackup: server certificate that is used to encrypt the backup.'', NULL);									
									
																		
									IF OBJECT_ID(''' + QUOTENAME(@database_name) + '.dbo.backup_worker'') IS NULL
										
										BEGIN
										
										
											RAISERROR(''Creating table dbo.backup_worker'', 0, 1) WITH NOWAIT;
											
												CREATE TABLE dbo.backup_worker (
																				id INT IDENTITY(1, 1) PRIMARY KEY CLUSTERED, 
																				database_name NVARCHAR(256), 
																				last_log_backup_start_time DATETIME DEFAULT ''19000101'', 
																				last_log_backup_finish_time DATETIME DEFAULT ''99991231'', 
																				is_started BIT DEFAULT 0, 
																				is_completed BIT DEFAULT 0, 
																				error_number INT DEFAULT NULL, 
																				last_error_date DATETIME DEFAULT NULL,
																				ignore_database BIT DEFAULT 0,
																				full_backup_required BIT DEFAULT ' +
                                        case when @firstfullbackup = 0 then N'0,' else N'1,' end + CHAR(10) +
                                        N'diff_backup_required BIT DEFAULT ' +
                                        case when @firstdiffbackup = 0 then N'0' else N'1' end + CHAR(10) +
                                        N');

  END;

ELSE

  BEGIN


      RAISERROR(''Backup worker table exists, truncating'', 0, 1) WITH NOWAIT;


      TRUNCATE TABLE dbo.backup_worker


  END


      RAISERROR(''Inserting databases for backups'', 0, 1) WITH NOWAIT;

      INSERT ' + QUOTENAME(@database_name) + '.dbo.backup_worker (database_name)
											SELECT d.name
											FROM sys.databases d
											WHERE NOT EXISTS (
												SELECT * 
												FROM msdbCentral.dbo.backup_worker bw
												WHERE bw.database_name = d.name
															)
											AND d.database_id > 4;
									
									';


                        if @debug = 1
                            begin
                                set @msg = SUBSTRING(@tbl_sql, 0, 2044)
                                raiserror (@msg, 0, 1) with nowait;
                                set @msg = SUBSTRING(@tbl_sql, 2044, 4088)
                                raiserror (@msg, 0, 1) with nowait;
                                set @msg = SUBSTRING(@tbl_sql, 4088, 6132)
                                raiserror (@msg, 0, 1) with nowait;
                                set @msg = SUBSTRING(@tbl_sql, 6132, 8176)
                                raiserror (@msg, 0, 1) with nowait;
                            end;


                        if @tbl_sql is null
                            begin
                                raiserror ('@tbl_sql is NULL for some reason', 0, 1) with nowait;
                            end;


                        exec sp_executesql @tbl_sql;


                        /*

                        This section creates tables for restore workers to work off of

                        */


                        /*

                        In search of msdb

                        */

                        raiserror ('Checking for msdb. Yeah, I know...', 0, 1) with nowait;

                        if DATABASEPROPERTYEX('msdb', 'Status') is null
                            begin

                                raiserror ('YOU HAVE NO MSDB WHY?!', 0, 1) with nowait;

                                return;

                            end;


                        /* In search of restore_configuration */

                        raiserror ('Checking for Restore Worker tables in msdb', 0, 1) with nowait;

                        if OBJECT_ID('msdb.dbo.restore_configuration') is null
                            begin

                                raiserror ('Creating restore_configuration table in msdb', 0, 1) with nowait;

                                create table msdb.dbo.restore_configuration
                                (
                                    database_name nvarchar(256),
                                    configuration_name nvarchar(512),
                                    configuration_description nvarchar(512),
                                    configuration_setting nvarchar(max)
                                );

                            end;


                        else


                            begin

                                raiserror ('Restore configuration table exists, truncating', 0, 1) with nowait;

                                truncate table msdb.dbo.restore_configuration;

                            end;


                        raiserror ('Inserting configuration values to msdb.dbo.restore_configuration', 0, 1) with nowait;

                        insert msdb.dbo.restore_configuration (database_name, configuration_name,
                                                               configuration_description, configuration_setting)
                        values ('all', 'log restore frequency', 'The length of time in second between Log Restores.',
                                @rtoseconds);

                        insert msdb.dbo.restore_configuration (database_name, configuration_name,
                                                               configuration_description, configuration_setting)
                        values ('all', 'log restore path', 'The path to which Log Restores come from.', @restorepath);


                        if OBJECT_ID('msdb.dbo.restore_worker') is null
                            begin


                                raiserror ('Creating table msdb.dbo.restore_worker', 0, 1) with nowait;

                                create table msdb.dbo.restore_worker
                                (
                                    id int identity (1, 1) primary key clustered,
                                    database_name nvarchar(256),
                                    last_log_restore_start_time datetime default '19000101',
                                    last_log_restore_finish_time datetime default '99991231',
                                    is_started bit default 0,
                                    is_completed bit default 0,
                                    error_number int default null,
                                    last_error_date datetime default null,
                                    ignore_database bit default 0,
                                    full_backup_required bit default 0,
                                    diff_backup_required bit default 0
                                );


                                raiserror ('Inserting databases for restores', 0, 1) with nowait;

                                insert msdb.dbo.restore_worker (database_name)
                                select d.name
                                from sys.databases d
                                where not EXISTS(
                                        select *
                                        from msdb.dbo.restore_worker bw
                                        where bw.database_name = d.name
                                    )
                                  and d.database_id > 4;


                            end;


                        /*

                        Add Jobs

                        */


                        /*

                        Look for our ten second schedule -- all jobs use this to restart themselves if they fail

                        Fun fact: you can add the same schedule name multiple times, so we don't want to just stick it in there

                        */


                        raiserror ('Checking for ten second schedule', 0, 1) with nowait;

                        if not EXISTS(
                                select 1
                                from msdb.dbo.sysschedules
                                where name = 'ten_seconds'
                            )
                            begin


                                raiserror ('Creating ten second schedule', 0, 1) with nowait;


                                exec msdb.dbo.sp_add_schedule @schedule_name= ten_seconds,
                                     @enabled = 1,
                                     @freq_type = 4,
                                     @freq_interval = 1,
                                     @freq_subday_type = 2,
                                     @freq_subday_interval = 10,
                                     @freq_relative_interval = 0,
                                     @freq_recurrence_factor = 0,
                                     @active_start_date = @active_start_date,
                                     @active_end_date = 99991231,
                                     @active_start_time = 0,
                                     @active_end_time = 235959;

                            end;


                        /*

                        Look for Backup Pollster job -- this job sets up our watcher for new databases to back up

                        */


                        raiserror ('Checking for pollster job', 0, 1) with nowait;


                        if not EXISTS(
                                select 1
                                from msdb.dbo.sysjobs
                                where name = 'sp_AllNightLog_PollForNewDatabases'
                            )
                            begin


                                raiserror ('Creating pollster job', 0, 1) with nowait;

                                if @enablebackupjobs = 1
                                    begin
                                        exec msdb.dbo.sp_add_job @job_name = sp_allnightlog_pollfornewdatabases,
                                             @description = 'This is a worker for the purposes of polling sys.databases for new entries to insert to the worker queue table.',
                                             @category_name = 'Database Maintenance',
                                             @owner_login_name = 'sa',
                                             @enabled = 1;
                                    end
                                else
                                    begin
                                        exec msdb.dbo.sp_add_job @job_name = sp_allnightlog_pollfornewdatabases,
                                             @description = 'This is a worker for the purposes of polling sys.databases for new entries to insert to the worker queue table.',
                                             @category_name = 'Database Maintenance',
                                             @owner_login_name = 'sa',
                                             @enabled = 0;
                                    end


                                raiserror ('Adding job step', 0, 1) with nowait;


                                exec msdb.dbo.sp_add_jobstep @job_name = sp_allnightlog_pollfornewdatabases,
                                     @step_name = sp_allnightlog_pollfornewdatabases,
                                     @subsystem = 'TSQL',
                                     @command = 'EXEC sp_AllNightLog @PollForNewDatabases = 1';


                                raiserror ('Adding job server', 0, 1) with nowait;


                                exec msdb.dbo.sp_add_jobserver @job_name = sp_allnightlog_pollfornewdatabases;


                                raiserror ('Attaching schedule', 0, 1) with nowait;


                                exec msdb.dbo.sp_attach_schedule @job_name = sp_allnightlog_pollfornewdatabases,
                                     @schedule_name = ten_seconds;


                            end;


                        /*

                        Look for Restore Pollster job -- this job sets up our watcher for new databases to back up

                        */


                        raiserror ('Checking for restore pollster job', 0, 1) with nowait;


                        if not EXISTS(
                                select 1
                                from msdb.dbo.sysjobs
                                where name = 'sp_AllNightLog_PollDiskForNewDatabases'
                            )
                            begin


                                raiserror ('Creating restore pollster job', 0, 1) with nowait;


                                if @enablerestorejobs = 1
                                    begin
                                        exec msdb.dbo.sp_add_job @job_name = sp_allnightlog_polldiskfornewdatabases,
                                             @description = 'This is a worker for the purposes of polling your restore path for new entries to insert to the worker queue table.',
                                             @category_name = 'Database Maintenance',
                                             @owner_login_name = 'sa',
                                             @enabled = 1;
                                    end
                                else
                                    begin
                                        exec msdb.dbo.sp_add_job @job_name = sp_allnightlog_polldiskfornewdatabases,
                                             @description = 'This is a worker for the purposes of polling your restore path for new entries to insert to the worker queue table.',
                                             @category_name = 'Database Maintenance',
                                             @owner_login_name = 'sa',
                                             @enabled = 0;
                                    end


                                raiserror ('Adding restore job step', 0, 1) with nowait;


                                exec msdb.dbo.sp_add_jobstep @job_name = sp_allnightlog_polldiskfornewdatabases,
                                     @step_name = sp_allnightlog_polldiskfornewdatabases,
                                     @subsystem = 'TSQL',
                                     @command = 'EXEC sp_AllNightLog @PollDiskForNewDatabases = 1';


                                raiserror ('Adding restore job server', 0, 1) with nowait;


                                exec msdb.dbo.sp_add_jobserver @job_name = sp_allnightlog_polldiskfornewdatabases;


                                raiserror ('Attaching schedule', 0, 1) with nowait;


                                exec msdb.dbo.sp_attach_schedule @job_name = sp_allnightlog_polldiskfornewdatabases,
                                     @schedule_name = ten_seconds;


                            end;


                        /*

                        This section creates @Jobs (quantity) of worker jobs to take log backups with

                        They work in a queue

                        It's queuete

                        */


                        raiserror ('Checking for sp_AllNightLog backup jobs', 0, 1) with nowait;


                        select @counter = COUNT(*) + 1
                        from msdb.dbo.sysjobs
                        where name like 'sp[_]AllNightLog[_]Backup[_]%';

                        set @msg = 'Found ' + CONVERT(nvarchar(10), (@counter - 1)) + ' backup jobs -- ' + case
                                                                                                               when @counter < @jobs
                                                                                                                   then + 'starting loop!'
                                                                                                               when @counter >= @jobs
                                                                                                                   then 'skipping loop!'
                                                                                                               else 'Oh woah something weird happened!'
                            end;

                        raiserror (@msg, 0, 1) with nowait;


                        while @counter <= @jobs
                            begin


                                raiserror ('Setting job name', 0, 1) with nowait;

                                set @job_name_backups = N'sp_AllNightLog_Backup_' + case
                                                                                        when @counter < 10
                                                                                            then N'0' + CONVERT(nvarchar(10), @counter)
                                                                                        when @counter >= 10
                                                                                            then CONVERT(nvarchar(10), @counter)
                                    end;


                                raiserror ('Setting @job_sql', 0, 1) with nowait;


                                set @job_sql = N'
							
											EXEC msdb.dbo.sp_add_job @job_name = ' + @job_name_backups + ', 
																	 @description = ' + @job_description_backups + ', 
																	 @category_name = ' + @job_category + ', 
																	 @owner_login_name = ' + @job_owner + ',';
                                if @enablebackupjobs = 1
                                    begin
                                        set @job_sql = @job_sql + ' @enabled = 1; ';
                                    end
                                else
                                    begin
                                        set @job_sql = @job_sql + ' @enabled = 0; ';
                                    end


                                set @job_sql = @job_sql + '
											EXEC msdb.dbo.sp_add_jobstep @job_name = ' + @job_name_backups + ', 
																		 @step_name = ' + @job_name_backups + ', 
																		 @subsystem = ''TSQL'', 
																		 @command = ' + @job_command_backups + ';
								  
											
											EXEC msdb.dbo.sp_add_jobserver @job_name = ' + @job_name_backups + ';
											
											
											EXEC msdb.dbo.sp_attach_schedule  @job_name = ' + @job_name_backups + ', 
																			  @schedule_name = ten_seconds;
											
											';


                                set @counter += 1;


                                if @debug = 1
                                    begin
                                        raiserror (@job_sql, 0, 1) with nowait;
                                    end;


                                if @job_sql is null
                                    begin
                                        raiserror ('@job_sql is NULL for some reason', 0, 1) with nowait;
                                    end;


                                exec sp_executesql @job_sql;


                            end;


                        /*

                        This section creates @Jobs (quantity) of worker jobs to restore logs with

                        They too work in a queue

                        Like a queue-t 3.14

                        */


                        raiserror ('Checking for sp_AllNightLog Restore jobs', 0, 1) with nowait;


                        select @counter = COUNT(*) + 1
                        from msdb.dbo.sysjobs
                        where name like 'sp[_]AllNightLog[_]Restore[_]%';

                        set @msg = 'Found ' + CONVERT(nvarchar(10), (@counter - 1)) + ' restore jobs -- ' + case
                                                                                                                when @counter < @jobs
                                                                                                                    then + 'starting loop!'
                                                                                                                when @counter >= @jobs
                                                                                                                    then 'skipping loop!'
                                                                                                                else 'Oh woah something weird happened!'
                            end;

                        raiserror (@msg, 0, 1) with nowait;


                        while @counter <= @jobs
                            begin


                                raiserror ('Setting job name', 0, 1) with nowait;

                                set @job_name_restores = N'sp_AllNightLog_Restore_' + case
                                                                                          when @counter < 10
                                                                                              then N'0' + CONVERT(nvarchar(10), @counter)
                                                                                          when @counter >= 10
                                                                                              then CONVERT(nvarchar(10), @counter)
                                    end;


                                raiserror ('Setting @job_sql', 0, 1) with nowait;


                                set @job_sql = N'
							
											EXEC msdb.dbo.sp_add_job @job_name = ' + @job_name_restores + ', 
																	 @description = ' + @job_description_restores + ', 
																	 @category_name = ' + @job_category + ', 
																	 @owner_login_name = ' + @job_owner + ',';
                                if @enablerestorejobs = 1
                                    begin
                                        set @job_sql = @job_sql + ' @enabled = 1; ';
                                    end
                                else
                                    begin
                                        set @job_sql = @job_sql + ' @enabled = 0; ';
                                    end


                                set @job_sql = @job_sql + '
											
											EXEC msdb.dbo.sp_add_jobstep @job_name = ' + @job_name_restores + ', 
																		 @step_name = ' + @job_name_restores + ', 
																		 @subsystem = ''TSQL'', 
																		 @command = ' + @job_command_restores + ';
								  
											
											EXEC msdb.dbo.sp_add_jobserver @job_name = ' + @job_name_restores + ';
											
											
											EXEC msdb.dbo.sp_attach_schedule  @job_name = ' + @job_name_restores + ', 
																			  @schedule_name = ten_seconds;
											
											';


                                set @counter += 1;


                                if @debug = 1
                                    begin
                                        raiserror (@job_sql, 0, 1) with nowait;
                                    end;


                                if @job_sql is null
                                    begin
                                        raiserror ('@job_sql is NULL for some reason', 0, 1) with nowait;
                                    end;


                                exec sp_executesql @job_sql;


                            end;


                        raiserror ('Setup complete!', 0, 1) with nowait;

                    end; --End for the Agent job creation

                end;--End for Database and Table creation

            end try
            begin catch


                select @msg = N'Error occurred during setup: ' + CONVERT(nvarchar(10), ERROR_NUMBER()) +
                              ', error message is ' + ERROR_MESSAGE(),
                       @error_severity = ERROR_SEVERITY(),
                       @error_state = ERROR_STATE();

                raiserror (@msg, @error_severity, @error_state) with nowait;


                while @@TRANCOUNT > 0
                    rollback;

            end catch;

        end; /* IF @RunSetup = 1 */

    return;


    updateconfigs:

    if @updatesetup = 1
        begin

            /* If we're enabling backup jobs, we may need to run restore with recovery on msdbCentral to bring it online: */
            if @enablebackupjobs = 1 and EXISTS(select * from sys.databases where name = 'msdbCentral' and state = 1)
                begin
                    raiserror ('msdbCentral exists, but is in restoring state. Running restore with recovery...', 0, 1) with nowait;

                    begin try
                        restore database [msdbCentral] with recovery;
                    end try
                    begin catch

                        select @error_number = ERROR_NUMBER(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();

                        select @msg = N'Error running restore with recovery on msdbCentral, error number is ' +
                                      CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();

                        raiserror (@msg, @error_severity, @error_state) with nowait;

                    end catch;

                end

            /* Only check for this after trying to restore msdbCentral: */
            if @enablebackupjobs = 1 and
               not EXISTS(select * from sys.databases where name = 'msdbCentral' and state = 0)
                begin
                    raiserror ('msdbCentral is not online. Repair that first, then try to enable backup jobs.', 0, 1) with nowait;
                    return
                end


            if OBJECT_ID('msdbCentral.dbo.backup_configuration') is not null
                raiserror ('Found backup config, checking variables...', 0, 1) with nowait;

            begin

                begin try


                    if @rposeconds is not null
                        begin

                            raiserror ('Attempting to update RPO setting', 0, 1) with nowait;

                            update c
                            set c.configuration_setting = CONVERT(nvarchar(10), @rposeconds)
                            from msdbcentral.dbo.backup_configuration as c
                            where c.configuration_name = N'log backup frequency';

                        end;


                    if @backuppath is not null
                        begin

                            raiserror ('Attempting to update Backup Path setting', 0, 1) with nowait;

                            update c
                            set c.configuration_setting = @backuppath
                            from msdbcentral.dbo.backup_configuration as c
                            where c.configuration_name = N'log backup path';


                        end;

                end try
                begin catch


                    select @error_number = ERROR_NUMBER(),
                           @error_severity = ERROR_SEVERITY(),
                           @error_state = ERROR_STATE();

                    select @msg = N'Error updating backup configuration setting, error number is ' +
                                  CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                           @error_severity = ERROR_SEVERITY(),
                           @error_state = ERROR_STATE();

                    raiserror (@msg, @error_severity, @error_state) with nowait;


                end catch;

            end;


            if OBJECT_ID('msdb.dbo.restore_configuration') is not null
                raiserror ('Found restore config, checking variables...', 0, 1) with nowait;

            begin

                begin try

                    exec msdb.dbo.sp_update_schedule @name = ten_seconds, @active_start_date = @active_start_date,
                         @active_start_time = 000000;

                    if @enablerestorejobs is not null
                        begin
                            raiserror ('Changing restore job status based on @EnableBackupJobs parameter...', 0, 1) with nowait;
                            insert into @jobs_to_change(name)
                            select name
                            from msdb.dbo.sysjobs
                            where name like 'sp_AllNightLog_Restore%'
                               or name = 'sp_AllNightLog_PollDiskForNewDatabases';
                            declare jobs_cursor cursor for
                                select name
                                from @jobs_to_change

                            open jobs_cursor
                            fetch next from jobs_cursor into @current_job_name

                            while @@FETCH_STATUS = 0
                                begin
                                    raiserror (@current_job_name, 0, 1) with nowait;
                                    exec msdb.dbo.sp_update_job @job_name=@current_job_name,
                                         @enabled = @enablerestorejobs;
                                    fetch next from jobs_cursor into @current_job_name
                                end

                            close jobs_cursor
                            deallocate jobs_cursor
                            delete @jobs_to_change;
                        end;

                    /* If they wanted to turn off restore jobs, wait to make sure that finishes before we start enabling the backup jobs */
                    if @enablerestorejobs = 0
                        begin
                            set @started_waiting_for_jobs = GETDATE();
                            select @counter = COUNT(*)
                            from [msdb].[dbo].[sysjobactivity] [ja]
                                     inner join [msdb].[dbo].[sysjobs] [j] on [ja].[job_id] = [j].[job_id]
                            where [ja].[session_id] = (
                                select top 1 [session_id]
                                from [msdb].[dbo].[syssessions]
                                order by [agent_start_date] desc
                            )
                              and [start_execution_date] is not null
                              and [stop_execution_date] is null
                              and [j].[name] like 'sp_AllNightLog_Restore%';

                            while @counter > 0
                                begin
                                    if DATEADD(ss, 120, @started_waiting_for_jobs) < GETDATE()
                                        begin
                                            raiserror ('OH NOES! We waited 2 minutes and restore jobs are still running. We are stopping here - get a meatbag involved to figure out if restore jobs need to be killed, and the backup jobs will need to be enabled manually.', 16, 1) with nowait;
                                            return
                                        end
                                    set @msg = N'Waiting for ' + CAST(@counter as nvarchar(100)) +
                                               N' sp_AllNightLog_Restore job(s) to finish.'
                                    raiserror (@msg, 0, 1) with nowait;
                                    waitfor delay '0:00:01'; -- Wait until the restore jobs are fully stopped

                                    select @counter = COUNT(*)
                                    from [msdb].[dbo].[sysjobactivity] [ja]
                                             inner join [msdb].[dbo].[sysjobs] [j] on [ja].[job_id] = [j].[job_id]
                                    where [ja].[session_id] = (
                                        select top 1 [session_id]
                                        from [msdb].[dbo].[syssessions]
                                        order by [agent_start_date] desc
                                    )
                                      and [start_execution_date] is not null
                                      and [stop_execution_date] is null
                                      and [j].[name] like 'sp_AllNightLog_Restore%';
                                end
                        end /* IF @EnableRestoreJobs = 0 */


                    if @enablebackupjobs is not null
                        begin
                            raiserror ('Changing backup job status based on @EnableBackupJobs parameter...', 0, 1) with nowait;
                            insert into @jobs_to_change(name)
                            select name
                            from msdb.dbo.sysjobs
                            where name like 'sp_AllNightLog_Backup%'
                               or name = 'sp_AllNightLog_PollForNewDatabases';
                            declare jobs_cursor cursor for
                                select name
                                from @jobs_to_change

                            open jobs_cursor
                            fetch next from jobs_cursor into @current_job_name

                            while @@FETCH_STATUS = 0
                                begin
                                    raiserror (@current_job_name, 0, 1) with nowait;
                                    exec msdb.dbo.sp_update_job @job_name=@current_job_name,
                                         @enabled = @enablebackupjobs;
                                    fetch next from jobs_cursor into @current_job_name
                                end

                            close jobs_cursor
                            deallocate jobs_cursor
                            delete @jobs_to_change;
                        end;


                    if @rtoseconds is not null
                        begin

                            raiserror ('Attempting to update RTO setting', 0, 1) with nowait;

                            update c
                            set c.configuration_setting = CONVERT(nvarchar(10), @rtoseconds)
                            from msdb.dbo.restore_configuration as c
                            where c.configuration_name = N'log restore frequency';

                        end;


                    if @restorepath is not null
                        begin

                            raiserror ('Attempting to update Restore Path setting', 0, 1) with nowait;

                            update c
                            set c.configuration_setting = @restorepath
                            from msdb.dbo.restore_configuration as c
                            where c.configuration_name = N'log restore path';


                        end;

                end try
                begin catch


                    select @error_number = ERROR_NUMBER(),
                           @error_severity = ERROR_SEVERITY(),
                           @error_state = ERROR_STATE();

                    select @msg = N'Error updating restore configuration setting, error number is ' +
                                  CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                           @error_severity = ERROR_SEVERITY(),
                           @error_state = ERROR_STATE();

                    raiserror (@msg, @error_severity, @error_state) with nowait;


                end catch;

            end;

            raiserror ('Update complete!', 0, 1) with nowait;

            return;

        end; --End updates to configuration table


end; -- Final END for stored proc
go

