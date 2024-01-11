set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

if OBJECT_ID('dbo.sp_AllNightLog') is null
    exec ('CREATE PROCEDURE dbo.sp_AllNightLog AS RETURN 0;')
go


alter procedure dbo.sp_allnightlog @pollfornewdatabases bit = 0, /* Formerly Pollster */
                                   @backup bit = 0, /* Formerly LogShaming */
                                   @polldiskfornewdatabases bit = 0,
                                   @restore bit = 0,
                                   @debug bit = 0,
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


		sp_AllNightLog from http://FirstResponderKit.org
		
		* @PollForNewDatabases = 1 polls sys.databases for new entries
			* Unfortunately no other way currently to automate new database additions when restored from backups
				* No triggers or extended events that easily do this
	
		* @Backup = 1 polls msdbCentral.dbo.backup_worker for databases not backed up in [RPO], takes LOG backups
			* Will switch to a full backup if none exists
	
	
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
	
		Known limitations of this version:
		 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000! And really, maybe not even anything less than 2016. Heh.
         - When restoring encrypted backups, the encryption certificate must already be installed.
	
		Unknown limitations of this version:
		 - None.  (If we knew them, they would be known. Duh.)
	
	     Changes - for the full list of improvements and fixes in this version, see:
	     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
	
	
		Parameter explanations:
	
		  @PollForNewDatabases BIT, defaults to 0. When this is set to 1, runs in a perma-loop to find new entries in sys.databases 
		  @Backup BIT, defaults to 0. When this is set to 1, runs in a perma-loop checking the backup_worker table for databases that need to be backed up
		  @Debug BIT, defaults to 0. Whent this is set to 1, it prints out dynamic SQL commands
		  @RPOSeconds BIGINT, defaults to 30. Value in seconds you want to use to determine if a new log backup needs to be taken.
		  @BackupPath NVARCHAR(MAX), defaults to = ''D:\Backup''. You 99.99999% will need to change this path to something else. This tells Ola''s job where to put backups.
	
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

            return
        end

    declare @database nvarchar(128) = null; --Holds the database that's currently being processed
    declare @error_number int = null; --Used for TRY/CATCH
    declare @error_severity int; --Used for TRY/CATCH
    declare @error_state int; --Used for TRY/CATCH
    declare @msg nvarchar(4000) = N''; --Used for RAISERROR
    declare @rpo int; --Used to hold the RPO value in our configuration table
    declare @rto int; --Used to hold the RPO value in our configuration table
    declare @backup_path nvarchar(max); --Used to hold the backup path in our configuration table
    declare @changebackuptype nvarchar(max); --Config table: Y = escalate to full backup, MSDB = escalate if MSDB history doesn't show a recent full.
    declare @encrypt nvarchar(max); --Config table: Y = encrypt the backup. N (default) = do not encrypt.
    declare @encryptionalgorithm nvarchar(max); --Config table: native 2014 choices include TRIPLE_DES_3KEY, AES_128, AES_192, AES_256
    declare @servercertificate nvarchar(max); --Config table: server certificate that is used to encrypt the backup
    declare @restore_path_base nvarchar(max); --Used to hold the base backup path in our configuration table
    declare @restore_path_full nvarchar(max); --Used to hold the full backup path in our configuration table
    declare @restore_path_log nvarchar(max); --Used to hold the log backup path in our configuration table
    declare @db_sql nvarchar(max) = N''; --Used to hold the dynamic SQL to create msdbCentral
    declare @tbl_sql nvarchar(max) = N''; --Used to hold the dynamic SQL that creates tables in msdbCentral
    declare @database_name nvarchar(256) = N'msdbCentral';
    --Used to hold the name of the database we create to centralize data
    --Right now it's hardcoded to msdbCentral, but I made it dynamic in case that changes down the line
    declare @cmd nvarchar(4000) = N'' --Holds dir cmd
    declare @filelist table
                      (
                          backupfile nvarchar(255)
                      ); --Where we dump @cmd
    declare @restore_full bit = 0 --We use this one
    declare @only_logs_after nvarchar(30) = N''


/*

Make sure we're doing something

*/

    if (
            @pollfornewdatabases = 0
            and @polldiskfornewdatabases = 0
            and @backup = 0
            and @restore = 0
            and @help = 0
        )
        begin
            raiserror ('You don''t seem to have picked an action for this stored procedure to take.', 0, 1) with nowait

            return;
        end

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


    if (@polldiskfornewdatabases = 1 or @restore = 1) and OBJECT_ID('msdb.dbo.restore_configuration') is not null
        begin

            if @debug = 1 raiserror ('Checking restore path', 0, 1) with nowait;

            select @restore_path_base = CONVERT(nvarchar(512), configuration_setting)
            from msdb.dbo.restore_configuration c
            where configuration_name = N'log restore path';


            if @restore_path_base is null
                begin
                    raiserror ('@restore_path cannot be NULL. Please check the msdb.dbo.restore_configuration table', 0, 1) with nowait;
                    return;
                end;

            if CHARINDEX('**', @restore_path_base) <> 0
                begin

                    /* If they passed in a dynamic **DATABASENAME**, stop at that folder looking for databases. More info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/993 */
                    if CHARINDEX('**DATABASENAME**', @restore_path_base) <> 0
                        begin
                            set @restore_path_base = SUBSTRING(@restore_path_base, 1,
                                                               CHARINDEX('**DATABASENAME**', @restore_path_base) - 2);
                        end;

                    set @restore_path_base = REPLACE(@restore_path_base, '**AVAILABILITYGROUP**', '');
                    set @restore_path_base = REPLACE(@restore_path_base, '**BACKUPTYPE**', 'FULL');
                    set @restore_path_base = REPLACE(@restore_path_base, '**SERVERNAME**', REPLACE(
                            CAST(SERVERPROPERTY('servername') as nvarchar(max)), '\', '$'));

                    if CHARINDEX('\', CAST(SERVERPROPERTY('servername') as nvarchar(max))) > 0
                        begin
                            set @restore_path_base =
                                    REPLACE(@restore_path_base, '**SERVERNAMEWITHOUTINSTANCE**', SUBSTRING(
                                            CAST(SERVERPROPERTY('servername') as nvarchar(max)), 1,
                                            (CHARINDEX('\', CAST(SERVERPROPERTY('servername') as nvarchar(max))) - 1)));
                            set @restore_path_base = REPLACE(@restore_path_base, '**INSTANCENAME**', SUBSTRING(
                                    CAST(SERVERPROPERTY('servername') as nvarchar(max)), CHARINDEX('\',
                                                                                                   CAST(SERVERPROPERTY('servername') as nvarchar(max))),
                                    (LEN(CAST(SERVERPROPERTY('servername') as nvarchar(max))) - CHARINDEX('\',
                                                                                                          CAST(SERVERPROPERTY('servername') as nvarchar(max)))) +
                                    1));
                        end
                    else /* No instance installed */
                        begin
                            set @restore_path_base = REPLACE(@restore_path_base, '**SERVERNAMEWITHOUTINSTANCE**',
                                                             CAST(SERVERPROPERTY('servername') as nvarchar(max)));
                            set @restore_path_base = REPLACE(@restore_path_base, '**INSTANCENAME**', 'DEFAULT');
                        end

                    if CHARINDEX('**CLUSTER**', @restore_path_base) <> 0
                        begin
                            declare @clustername nvarchar(128);
                            if EXISTS(select * from sys.all_objects where name = 'dm_hadr_cluster')
                                begin
                                    select @clustername = cluster_name from sys.dm_hadr_cluster;
                                end
                            set @restore_path_base = REPLACE(@restore_path_base, '**CLUSTER**',
                                                             COALESCE(@clustername, ''));
                        end;

                end /* IF CHARINDEX('**', @restore_path_base) <> 0 */

        end
    /* IF @PollDiskForNewDatabases = 1 OR @Restore = 1 */


/*

Certain variables necessarily skip to parts of this script that are irrelevant
in both directions to each other. They are used for other stuff.

*/


/*

Pollster use happens strictly to check for new databases in sys.databases to place them in a worker queue

*/

    if @pollfornewdatabases = 1
        goto pollster;

/*

LogShamer happens when we need to find and assign work to a worker job for backups

*/

    if @backup = 1
        goto logshamer;

/*

Pollster use happens strictly to check for new databases in sys.databases to place them in a worker queue

*/

    if @polldiskfornewdatabases = 1
        goto diskpollster;


/*

Restoregasm Addict happens when we need to find and assign work to a worker job for restores

*/

    if @restore = 1
        goto restoregasm_addict;


    /*

    Begin Polling section

    */


/*

This section runs in a loop checking for new databases added to the server, or broken backups

*/


    pollster:

    if @debug = 1 raiserror ('Beginning Pollster', 0, 1) with nowait;

    if OBJECT_ID('msdbCentral.dbo.backup_worker') is not null
        begin

            while @pollfornewdatabases = 1
                begin

                    begin try

                        if @debug = 1 raiserror ('Checking for new databases...', 0, 1) with nowait;

                        /*

                        Look for new non-system databases -- there should probably be additional filters here for accessibility, etc.

                        */

                        insert msdbcentral.dbo.backup_worker (database_name)
                        select d.name
                        from sys.databases d
                        where not EXISTS(
                                select 1
                                from msdbcentral.dbo.backup_worker bw
                                where bw.database_name = d.name
                            )
                          and d.database_id > 4;

                        if @debug = 1 raiserror ('Checking for wayward databases', 0, 1) with nowait;

                        /*

                        This section aims to find databases that have
                            * Had a log backup ever (the default for finish time is 9999-12-31, so anything with a more recent finish time has had a log backup)
                            * Not had a log backup start in the last 5 minutes (this could be trouble! or a really big log backup)
                            * Also checks msdb.dbo.backupset to make sure the database has a full backup associated with it (otherwise it's the first full, and we don't need to start taking log backups yet)

                        */

                        if EXISTS(
                                select 1
                                from msdbcentral.dbo.backup_worker bw with (readpast)
                                where bw.last_log_backup_finish_time < '99991231'
                                  and bw.last_log_backup_start_time < DATEADD(minute, -5, GETDATE())
                                  and EXISTS(
                                        select 1
                                        from msdb.dbo.backupset b
                                        where b.database_name = bw.database_name
                                          and b.type = 'D'
                                    )
                            )
                            begin

                                if @debug = 1
                                    raiserror ('Resetting databases with a log backup and no log backup in the last 5 minutes', 0, 1) with nowait;


                                update bw
                                set bw.is_started                 = 0,
                                    bw.is_completed               = 1,
                                    bw.last_log_backup_start_time = '19000101'
                                from msdbcentral.dbo.backup_worker bw
                                where bw.last_log_backup_finish_time < '99991231'
                                  and bw.last_log_backup_start_time < DATEADD(minute, -5, GETDATE())
                                  and EXISTS(
                                        select 1
                                        from msdb.dbo.backupset b
                                        where b.database_name = bw.database_name
                                          and b.type = 'D'
                                    );


                            end;
                        --End check for wayward databases

                        /*

                        Wait 1 minute between runs, we don't need to be checking this constantly

                        */


                        if @debug = 1 raiserror ('Waiting for 1 minute', 0, 1) with nowait;

                        waitfor delay '00:01:00.000';

                    end try
                    begin catch


                        select @msg = N'Error inserting databases to msdbCentral.dbo.backup_worker, error number is ' +
                                      CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();

                        raiserror (@msg, @error_severity, @error_state) with nowait;


                        while @@TRANCOUNT > 0
                            rollback;


                    end catch;


                end;

            /* Check to make sure job is still enabled */
            if not EXISTS(
                    select *
                    from msdb.dbo.sysjobs
                    where name = 'sp_AllNightLog_PollForNewDatabases'
                      and enabled = 1
                )
                begin
                    raiserror ('sp_AllNightLog_PollForNewDatabases job is disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) with nowait;
                    return;
                end

        end;-- End Pollster loop

    else

        begin

            raiserror ('msdbCentral.dbo.backup_worker does not exist, please create it.', 0, 1) with nowait;
            return;

        end;
    return;


    /*

    End of Pollster

    */


/*

Begin DiskPollster

*/


/*

This section runs in a loop checking restore path for new databases added to the server, or broken restores

*/

    diskpollster:

    if @debug = 1 raiserror ('Beginning DiskPollster', 0, 1) with nowait;

    if OBJECT_ID('msdb.dbo.restore_configuration') is not null
        begin

            while @polldiskfornewdatabases = 1
                begin

                    begin try

                        if @debug = 1 raiserror ('Checking for new databases in: ', 0, 1) with nowait;
                        if @debug = 1 raiserror (@restore_path_base, 0, 1) with nowait;

                        /*

                        Look for new non-system databases -- there should probably be additional filters here for accessibility, etc.

                        */

                        /*

                        This setups up the @cmd variable to check the restore path for new folders

                        In our case, a new folder means a new database, because we assume a pristine path

                        */

                        set @cmd = N'DIR /b "' + @restore_path_base + N'"';

                        if @debug = 1
                            begin
                                print @cmd;
                            end


                        delete @filelist;
                        insert into @filelist (backupfile)
                            exec master.sys.xp_cmdshell @cmd;

                        if (
                               select COUNT(*)
                               from @filelist as fl
                               where fl.backupfile = 'The system cannot find the path specified.'
                                  or fl.backupfile = 'File Not Found'
                           ) = 1
                            begin

                                raiserror ('No rows were returned for that database\path', 0, 1) with nowait;

                            end;

                        if (
                               select COUNT(*)
                               from @filelist as fl
                               where fl.backupfile = 'Access is denied.'
                           ) = 1
                            begin

                                raiserror ('Access is denied to %s', 16, 1, @restore_path_base) with nowait;

                            end;

                        if (
                               select COUNT(*)
                               from @filelist as fl
                           ) = 1
                            and (
                                    select COUNT(*)
                                    from @filelist as fl
                                    where fl.backupfile is null
                                ) = 1
                            begin

                                raiserror ('That directory appears to be empty', 0, 1) with nowait;

                                return;

                            end

                        if (
                               select COUNT(*)
                               from @filelist as fl
                               where fl.backupfile = 'The user name or password is incorrect.'
                           ) = 1
                            begin

                                raiserror ('Incorrect user name or password for %s', 16, 1, @restore_path_base) with nowait;

                            end;

                        insert msdb.dbo.restore_worker (database_name)
                        select fl.backupfile
                        from @filelist as fl
                        where fl.backupfile is not null
                          and fl.backupfile not in (select name from sys.databases where database_id < 5)
                          and not EXISTS
                            (
                                select 1
                                from msdb.dbo.restore_worker rw
                                where rw.database_name = fl.backupfile
                            )

                        if @debug = 1 raiserror ('Checking for wayward databases', 0, 1) with nowait;

                        /*

                        This section aims to find databases that have
                            * Had a log restore ever (the default for finish time is 9999-12-31, so anything with a more recent finish time has had a log restore)
                            * Not had a log restore start in the last 5 minutes (this could be trouble! or a really big log restore)
                            * Also checks msdb.dbo.backupset to make sure the database has a full backup associated with it (otherwise it's the first full, and we don't need to start adding log restores yet)

                        */

                        if EXISTS(
                                select 1
                                from msdb.dbo.restore_worker rw with (readpast)
                                where rw.last_log_restore_finish_time < '99991231'
                                  and rw.last_log_restore_start_time < DATEADD(minute, -5, GETDATE())
                                  and EXISTS(
                                        select 1
                                        from msdb.dbo.restorehistory r
                                        where r.destination_database_name = rw.database_name
                                          and r.restore_type = 'D'
                                    )
                            )
                            begin

                                if @debug = 1
                                    raiserror ('Resetting databases with a log restore and no log restore in the last 5 minutes', 0, 1) with nowait;


                                update rw
                                set rw.is_started                  = 0,
                                    rw.is_completed                = 1,
                                    rw.last_log_restore_start_time = '19000101'
                                from msdb.dbo.restore_worker rw
                                where rw.last_log_restore_finish_time < '99991231'
                                  and rw.last_log_restore_start_time < DATEADD(minute, -5, GETDATE())
                                  and EXISTS(
                                        select 1
                                        from msdb.dbo.restorehistory r
                                        where r.destination_database_name = rw.database_name
                                          and r.restore_type = 'D'
                                    );


                            end;
                        --End check for wayward databases

                        /*

                        Wait 1 minute between runs, we don't need to be checking this constantly

                        */

                        /* Check to make sure job is still enabled */
                        if not EXISTS(
                                select *
                                from msdb.dbo.sysjobs
                                where name = 'sp_AllNightLog_PollDiskForNewDatabases'
                                  and enabled = 1
                            )
                            begin
                                raiserror ('sp_AllNightLog_PollDiskForNewDatabases job is disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) with nowait;
                                return;
                            end

                        if @debug = 1 raiserror ('Waiting for 1 minute', 0, 1) with nowait;

                        waitfor delay '00:01:00.000';

                    end try
                    begin catch


                        select @msg = N'Error inserting databases to msdb.dbo.restore_worker, error number is ' +
                                      CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();

                        raiserror (@msg, @error_severity, @error_state) with nowait;


                        while @@TRANCOUNT > 0
                            rollback;


                    end catch;


                end;

        end;-- End Pollster loop

    else

        begin

            raiserror ('msdb.dbo.restore_worker does not exist, please create it.', 0, 1) with nowait;
            return;

        end;
    return;


/*

Begin LogShamer

*/

    logshamer:

    if @debug = 1 raiserror ('Beginning Backups', 0, 1) with nowait;

    if OBJECT_ID('msdbCentral.dbo.backup_worker') is not null
        begin

            /*

            Make sure configuration table exists...

            */

            if OBJECT_ID('msdbCentral.dbo.backup_configuration') is not null
                begin

                    if @debug = 1 raiserror ('Checking variables', 0, 1) with nowait;

                    /*

                    These settings are configurable

                    I haven't found a good way to find the default backup path that doesn't involve xp_regread

                    */

                    select @rpo = CONVERT(int, configuration_setting)
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'log backup frequency'
                      and database_name = N'all';


                    if @rpo is null
                        begin
                            raiserror ('@rpo cannot be NULL. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) with nowait;
                            return;
                        end;


                    select @backup_path = CONVERT(nvarchar(512), configuration_setting)
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'log backup path'
                      and database_name = N'all';


                    if @backup_path is null
                        begin
                            raiserror ('@backup_path cannot be NULL. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) with nowait;
                            return;
                        end;

                    select @changebackuptype = configuration_setting
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'change backup type'
                      and database_name = N'all';

                    select @encrypt = configuration_setting
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'encrypt'
                      and database_name = N'all';

                    select @encryptionalgorithm = configuration_setting
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'encryptionalgorithm'
                      and database_name = N'all';

                    select @servercertificate = configuration_setting
                    from msdbcentral.dbo.backup_configuration c
                    where configuration_name = N'servercertificate'
                      and database_name = N'all';

                    if @encrypt = N'Y' and (@encryptionalgorithm is null or @servercertificate is null)
                        begin
                            raiserror ('If encryption is Y, then both the encryptionalgorithm and servercertificate must be set. Please check the msdbCentral.dbo.backup_configuration table', 0, 1) with nowait;
                            return;
                        end;

                end;

            else

                begin

                    raiserror ('msdbCentral.dbo.backup_configuration does not exist, please run setup script', 0, 1) with nowait;
                    return;

                end;


            while @backup = 1

                /*

                Start loop to take log backups

                */


                begin

                    begin try

                        begin tran;

                        if @debug = 1 raiserror ('Begin tran to grab a database to back up', 0, 1) with nowait;


                        /*

                        This grabs a database for a worker to work on

                        The locking hints hope to provide some isolation when 10+ workers are in action

                        */


                        select top (1) @database = bw.database_name
                        from msdbcentral.dbo.backup_worker bw
                        with (updlock, holdlock, rowlock)
                        where ( /*This section works on databases already part of the backup cycle*/
                                bw.is_started = 0
                                and bw.is_completed = 1
                                and bw.last_log_backup_start_time < DATEADD(second, (@rpo * -1), GETDATE())
                                and
                                (bw.error_number is null or bw.error_number > 0) /* negative numbers indicate human attention required */
                                and bw.ignore_database = 0
                            )
                           or ( /*This section picks up newly added databases by Pollster*/
                                bw.is_started = 0
                                and bw.is_completed = 0
                                and bw.last_log_backup_start_time = '1900-01-01 00:00:00.000'
                                and bw.last_log_backup_finish_time = '9999-12-31 00:00:00.000'
                                and
                                (bw.error_number is null or bw.error_number > 0) /* negative numbers indicate human attention required */
                                and bw.ignore_database = 0
                            )
                        order by bw.last_log_backup_start_time asc, bw.last_log_backup_finish_time asc,
                                 bw.database_name asc;


                        if @database is not null
                            begin
                                set @msg = N'Updating backup_worker for database ' +
                                           ISNULL(@database, 'UH OH NULL @database');
                                if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                /*

                                Update the worker table so other workers know a database is being backed up

                                */


                                update bw
                                set bw.is_started                 = 1,
                                    bw.is_completed               = 0,
                                    bw.last_log_backup_start_time = GETDATE()
                                from msdbcentral.dbo.backup_worker bw
                                where bw.database_name = @database;
                            end
                        commit;

                    end try
                    begin catch

                        /*

                        Do I need to build retry logic in here? Try to catch deadlocks? I don't know yet!

                        */

                        select @msg = N'Error securing a database to backup, error number is ' +
                                      CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();
                        raiserror (@msg, @error_severity, @error_state) with nowait;

                        set @database = null;

                        while @@TRANCOUNT > 0
                            rollback;

                    end catch;


                    /* If we don't find a database to work on, wait for a few seconds */
                    if @database is null
                        begin
                            if @debug = 1
                                raiserror ('No databases to back up right now, starting 3 second throttle', 0, 1) with nowait;
                            waitfor delay '00:00:03.000';

                            /* Check to make sure job is still enabled */
                            if not EXISTS(
                                    select *
                                    from msdb.dbo.sysjobs
                                    where name like 'sp_AllNightLog_Backup%'
                                      and enabled = 1
                                )
                                begin
                                    raiserror ('sp_AllNightLog_Backup jobs are disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) with nowait;
                                    return;
                                end


                        end


                    begin try

                        begin

                            if @database is not null

                                /*

                                Make sure we have a database to work on -- I should make this more robust so we do something if it is NULL, maybe

                                */


                                begin

                                    set @msg = N'Taking backup of ' + ISNULL(@database, 'UH OH NULL @database');
                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    /*

                                    Call Ola's proc to backup the database

                                    */

                                    if @encrypt = 'Y'
                                        exec master.dbo.databasebackup
                                             @databases = @database, --Database we're working on
                                             @backuptype = 'LOG', --Going for the LOGs
                                             @directory = @backup_path, --The path we need to back up to
                                             @verify = 'N', --We don't want to verify these, it eats into job time
                                             @changebackuptype = @changebackuptype, --If we need to switch to a FULL because one hasn't been taken
                                             @checksum = 'Y', --These are a good idea
                                             @compress = 'Y', --This is usually a good idea
                                             @logtotable = 'Y', --We should do this for posterity
                                             @encrypt = @encrypt,
                                             @encryptionalgorithm = @encryptionalgorithm,
                                             @servercertificate = @servercertificate;

                                    else
                                        exec master.dbo.databasebackup
                                             @databases = @database, --Database we're working on
                                             @backuptype = 'LOG', --Going for the LOGs
                                             @directory = @backup_path, --The path we need to back up to
                                             @verify = 'N', --We don't want to verify these, it eats into job time
                                             @changebackuptype = @changebackuptype, --If we need to switch to a FULL because one hasn't been taken
                                             @checksum = 'Y', --These are a good idea
                                             @compress = 'Y', --This is usually a good idea
                                             @logtotable = 'Y';
                                    --We should do this for posterity


                                    /*

                                    Catch any erroneous zones

                                    */

                                    select @error_number = ERROR_NUMBER(),
                                           @error_severity = ERROR_SEVERITY(),
                                           @error_state = ERROR_STATE();

                                end; --End call to dbo.DatabaseBackup

                        end; --End successful check of @database (not NULL)

                    end try
                    begin catch

                        if @error_number is not null

                            /*

                            If the ERROR() function returns a number, update the table with it and the last error date.

                            Also update the last start time to 1900-01-01 so it gets picked back up immediately -- the query to find a log backup to take sorts by start time

                            */

                            begin

                                set @msg = N'Error number is ' + CONVERT(nvarchar(10), ERROR_NUMBER());
                                raiserror (@msg, @error_severity, @error_state) with nowait;

                                set @msg = N'Updating backup_worker for database ' +
                                           ISNULL(@database, 'UH OH NULL @database') + ' for unsuccessful backup';
                                raiserror (@msg, 0, 1) with nowait;


                                update bw
                                set bw.is_started                 = 0,
                                    bw.is_completed               = 1,
                                    bw.last_log_backup_start_time = '19000101',
                                    bw.error_number               = @error_number,
                                    bw.last_error_date            = GETDATE()
                                from msdbcentral.dbo.backup_worker bw
                                where bw.database_name = @database;


                                /*

                                Set @database back to NULL to avoid variable assignment weirdness

                                */

                                set @database = null;


                                /*

                                Wait around for a second so we're not just spinning wheels -- this only runs if the BEGIN CATCH is triggered by an error

                                */

                                if @debug = 1 raiserror ('Starting 1 second throttle', 0, 1) with nowait;

                                waitfor delay '00:00:01.000';

                            end; -- End update of unsuccessful backup

                    end catch;

                    if @database is not null and @error_number is null

                        /*

                        If no error, update everything normally

                        */


                        begin

                            if @debug = 1 raiserror ('Error number IS NULL', 0, 1) with nowait;

                            set @msg = N'Updating backup_worker for database ' +
                                       ISNULL(@database, 'UH OH NULL @database') + ' for successful backup';
                            if @debug = 1 raiserror (@msg, 0, 1) with nowait;


                            update bw
                            set bw.is_started                  = 0,
                                bw.is_completed                = 1,
                                bw.last_log_backup_finish_time = GETDATE()
                            from msdbcentral.dbo.backup_worker bw
                            where bw.database_name = @database;


                            /*

                            Set @database back to NULL to avoid variable assignment weirdness

                            */

                            set @database = null;


                        end; -- End update for successful backup


                end; -- End @Backup WHILE loop


        end; -- End successful check for backup_worker and subsequent code


    else

        begin

            raiserror ('msdbCentral.dbo.backup_worker does not exist, please run setup script', 0, 1) with nowait;

            return;

        end;
    return;


/*

Begin Restoregasm_Addict section

*/

    restoregasm_addict:

    if @restore = 1
        if @debug = 1 raiserror ('Beginning Restores', 0, 1) with nowait;

    /* Check to make sure backup jobs aren't enabled */
    if EXISTS(
            select *
            from msdb.dbo.sysjobs
            where name like 'sp_AllNightLog_Backup%'
              and enabled = 1
        )
        begin
            raiserror ('sp_AllNightLog_Backup jobs are enabled, so gracefully exiting. You do not want to accidentally do restores over top of the databases you are backing up.', 0, 1) with nowait;
            return;
        end

    if OBJECT_ID('msdb.dbo.restore_worker') is not null
        begin

            /*

            Make sure configuration table exists...

            */

            if OBJECT_ID('msdb.dbo.restore_configuration') is not null
                begin

                    if @debug = 1 raiserror ('Checking variables', 0, 1) with nowait;

                    /*

                    These settings are configurable

                    */

                    select @rto = CONVERT(int, configuration_setting)
                    from msdb.dbo.restore_configuration c
                    where configuration_name = N'log restore frequency';


                    if @rto is null
                        begin
                            raiserror ('@rto cannot be NULL. Please check the msdb.dbo.restore_configuration table', 0, 1) with nowait;
                            return;
                        end;


                end;

            else

                begin

                    raiserror ('msdb.dbo.restore_configuration does not exist, please run setup script', 0, 1) with nowait;

                    return;

                end;


            while @restore = 1

                /*

                Start loop to restore log backups

                */


                begin

                    begin try

                        begin tran;

                        if @debug = 1 raiserror ('Begin tran to grab a database to restore', 0, 1) with nowait;


                        /*

                        This grabs a database for a worker to work on

                        The locking hints hope to provide some isolation when 10+ workers are in action

                        */


                        select top (1) @database = rw.database_name,
                                       @only_logs_after = REPLACE(REPLACE(REPLACE(
                                                                                  CONVERT(nvarchar(30), rw.last_log_restore_start_time, 120),
                                                                                  ' ', ''), '-', ''), ':', ''),
                                       @restore_full = case
                                                           when rw.is_started = 0
                                                               and rw.is_completed = 0
                                                               and
                                                                rw.last_log_restore_start_time = '1900-01-01 00:00:00.000'
                                                               and
                                                                rw.last_log_restore_finish_time = '9999-12-31 00:00:00.000'
                                                               then 1
                                                           else 0
                                           end
                        from msdb.dbo.restore_worker rw
                        with (updlock, holdlock, rowlock)
                        where ( /*This section works on databases already part of the backup cycle*/
                                rw.is_started = 0
                                and rw.is_completed = 1
                                and rw.last_log_restore_start_time < DATEADD(second, (@rto * -1), GETDATE())
                                and
                                (rw.error_number is null or rw.error_number > 0) /* negative numbers indicate human attention required */
                            )
                           or ( /*This section picks up newly added databases by DiskPollster*/
                                      rw.is_started = 0
                                      and rw.is_completed = 0
                                      and rw.last_log_restore_start_time = '1900-01-01 00:00:00.000'
                                      and rw.last_log_restore_finish_time = '9999-12-31 00:00:00.000'
                                      and
                                      (rw.error_number is null or rw.error_number > 0) /* negative numbers indicate human attention required */
                                  )
                            and rw.ignore_database = 0
                        order by rw.last_log_restore_start_time asc, rw.last_log_restore_finish_time asc,
                                 rw.database_name asc;


                        if @database is not null
                            begin
                                set @msg = N'Updating restore_worker for database ' +
                                           ISNULL(@database, 'UH OH NULL @database');
                                if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                /*

                                Update the worker table so other workers know a database is being restored

                                */


                                update rw
                                set rw.is_started                  = 1,
                                    rw.is_completed                = 0,
                                    rw.last_log_restore_start_time = GETDATE()
                                from msdb.dbo.restore_worker rw
                                where rw.database_name = @database;
                            end
                        commit;

                    end try
                    begin catch

                        /*

                        Do I need to build retry logic in here? Try to catch deadlocks? I don't know yet!

                        */

                        select @msg = N'Error securing a database to restore, error number is ' +
                                      CONVERT(nvarchar(10), ERROR_NUMBER()) + ', error message is ' + ERROR_MESSAGE(),
                               @error_severity = ERROR_SEVERITY(),
                               @error_state = ERROR_STATE();
                        raiserror (@msg, @error_severity, @error_state) with nowait;

                        set @database = null;

                        while @@TRANCOUNT > 0
                            rollback;

                    end catch;


                    /* If we don't find a database to work on, wait for a few seconds */
                    if @database is null
                        begin
                            if @debug = 1
                                raiserror ('No databases to restore up right now, starting 3 second throttle', 0, 1) with nowait;
                            waitfor delay '00:00:03.000';

                            /* Check to make sure backup jobs aren't enabled */
                            if EXISTS(
                                    select *
                                    from msdb.dbo.sysjobs
                                    where name like 'sp_AllNightLog_Backup%'
                                      and enabled = 1
                                )
                                begin
                                    raiserror ('sp_AllNightLog_Backup jobs are enabled, so gracefully exiting. You do not want to accidentally do restores over top of the databases you are backing up.', 0, 1) with nowait;
                                    return;
                                end

                            /* Check to make sure job is still enabled */
                            if not EXISTS(
                                    select *
                                    from msdb.dbo.sysjobs
                                    where name like 'sp_AllNightLog_Restore%'
                                      and enabled = 1
                                )
                                begin
                                    raiserror ('sp_AllNightLog_Restore jobs are disabled, so gracefully exiting. It feels graceful to me, anyway.', 0, 1) with nowait;
                                    return;
                                end

                        end


                    begin try

                        begin

                            if @database is not null

                                /*

                                Make sure we have a database to work on -- I should make this more robust so we do something if it is NULL, maybe

                                */


                                begin

                                    set @msg = case
                                                   when @restore_full = 0
                                                       then N'Restoring logs for '
                                                   else N'Restoring full backup for '
                                                   end
                                        + ISNULL(@database, 'UH OH NULL @database');

                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    /*

                                    Call sp_DatabaseRestore to backup the database

                                    */

                                    set @restore_path_full = @restore_path_base + N'\' + @database + N'\' + N'FULL\'

                                    set @msg = N'Path for FULL backups for ' + @database + N' is ' + @restore_path_full
                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    set @restore_path_log = @restore_path_base + N'\' + @database + N'\' + N'LOG\'

                                    set @msg = N'Path for LOG backups for ' + @database + N' is ' + @restore_path_log
                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    if @restore_full = 0
                                        begin

                                            if @debug = 1 raiserror ('Starting Log only restores', 0, 1) with nowait;

                                            exec master.dbo.sp_databaserestore @database = @database,
                                                 @backuppathfull = @restore_path_full,
                                                 @backuppathlog = @restore_path_log,
                                                 @continuelogs = 1,
                                                 @runrecovery = 0,
                                                 @onlylogsafter = @only_logs_after,
                                                 @debug = @debug

                                        end

                                    if @restore_full = 1
                                        begin

                                            if @debug = 1
                                                raiserror ('Starting first Full restore from: ', 0, 1) with nowait;
                                            if @debug = 1 raiserror (@restore_path_full, 0, 1) with nowait;

                                            exec master.dbo.sp_databaserestore @database = @database,
                                                 @backuppathfull = @restore_path_full,
                                                 @backuppathlog = @restore_path_log,
                                                 @continuelogs = 0,
                                                 @runrecovery = 0,
                                                 @debug = @debug

                                        end


                                    /*

                                    Catch any erroneous zones

                                    */

                                    select @error_number = ERROR_NUMBER(),
                                           @error_severity = ERROR_SEVERITY(),
                                           @error_state = ERROR_STATE();

                                end; --End call to dbo.sp_DatabaseRestore

                        end; --End successful check of @database (not NULL)

                    end try
                    begin catch

                        if @error_number is not null

                            /*

                            If the ERROR() function returns a number, update the table with it and the last error date.

                            Also update the last start time to 1900-01-01 so it gets picked back up immediately -- the query to find a log restore to take sorts by start time

                            */

                            begin

                                set @msg = N'Error number is ' + CONVERT(nvarchar(10), ERROR_NUMBER());
                                raiserror (@msg, @error_severity, @error_state) with nowait;

                                set @msg = N'Updating restore_worker for database ' +
                                           ISNULL(@database, 'UH OH NULL @database') + ' for unsuccessful backup';
                                raiserror (@msg, 0, 1) with nowait;


                                update rw
                                set rw.is_started                  = 0,
                                    rw.is_completed                = 1,
                                    rw.last_log_restore_start_time = '19000101',
                                    rw.error_number                = @error_number,
                                    rw.last_error_date             = GETDATE()
                                from msdb.dbo.restore_worker rw
                                where rw.database_name = @database;


                                /*

                                Set @database back to NULL to avoid variable assignment weirdness

                                */

                                set @database = null;


                                /*

                                Wait around for a second so we're not just spinning wheels -- this only runs if the BEGIN CATCH is triggered by an error

                                */

                                if @debug = 1 raiserror ('Starting 1 second throttle', 0, 1) with nowait;

                                waitfor delay '00:00:01.000';

                            end; -- End update of unsuccessful restore

                    end catch;


                    if @database is not null and @error_number is null

                        /*

                        If no error, update everything normally

                        */


                        begin

                            if @debug = 1 raiserror ('Error number IS NULL', 0, 1) with nowait;

                            /* Make sure database actually exists and is in the restoring state */
                            if EXISTS(select * from sys.databases where name = @database and state = 1) /* Restoring */
                                begin
                                    set @msg = N'Updating backup_worker for database ' +
                                               ISNULL(@database, 'UH OH NULL @database') + ' for successful backup';
                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    update rw
                                    set rw.is_started                   = 0,
                                        rw.is_completed                 = 1,
                                        rw.last_log_restore_finish_time = GETDATE()
                                    from msdb.dbo.restore_worker rw
                                    where rw.database_name = @database;

                                end
                            else /* The database doesn't exist, or it's not in the restoring state */
                                begin
                                    set @msg = N'Updating backup_worker for database ' +
                                               ISNULL(@database, 'UH OH NULL @database') + ' for UNsuccessful backup';
                                    if @debug = 1 raiserror (@msg, 0, 1) with nowait;

                                    update rw
                                    set rw.is_started      = 0,
                                        rw.is_completed    = 1,
                                        rw.error_number    = -1, /* unknown, human attention required */
                                        rw.last_error_date = GETDATE()
                                        /* rw.last_log_restore_finish_time = GETDATE()    don't change this - the last log may still be successful */
                                    from msdb.dbo.restore_worker rw
                                    where rw.database_name = @database;
                                end


                            /*

                            Set @database back to NULL to avoid variable assignment weirdness

                            */

                            set @database = null;


                        end; -- End update for successful backup

                end; -- End @Restore WHILE loop


        end; -- End successful check for restore_worker and subsequent code


    else

        begin

            raiserror ('msdb.dbo.restore_worker does not exist, please run setup script', 0, 1) with nowait;

            return;

        end;
    return;


end; -- Final END for stored proc

go
