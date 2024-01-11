if OBJECT_ID('dbo.sp_DatabaseRestore') is null
    exec ('CREATE PROCEDURE dbo.sp_DatabaseRestore AS RETURN 0;');
go
alter procedure [dbo].[sp_DatabaseRestore] @database nvarchar(128) = null,
                                           @restoredatabasename nvarchar(128) = null,
                                           @backuppathfull nvarchar(260) = null,
                                           @backuppathdiff nvarchar(260) = null,
                                           @backuppathlog nvarchar(260) = null,
                                           @movefiles bit = 1,
                                           @movedatadrive nvarchar(260) = null,
                                           @movelogdrive nvarchar(260) = null,
                                           @movefilestreamdrive nvarchar(260) = null,
                                           @buffercount int = null,
                                           @maxtransfersize int = null,
                                           @blocksize int = null,
                                           @testrestore bit = 0,
                                           @runcheckdb bit = 0,
                                           @restorediff bit = 0,
                                           @continuelogs bit = 0,
                                           @standbymode bit = 0,
                                           @standbyundopath nvarchar(max) = null,
                                           @runrecovery bit = 0,
                                           @forcesimplerecovery bit = 0,
                                           @existingdbaction tinyint = 0,
                                           @stopat nvarchar(14) = null,
                                           @onlylogsafter nvarchar(14) = null,
                                           @simplefolderenumeration bit = 0,
                                           @databaseowner sysname = null,
                                           @execute char(1) = y,
                                           @debug int = 0,
                                           @help bit = 0,
                                           @version varchar(30) = null output,
                                           @versiondate datetime = null output,
                                           @versioncheckmode bit = 0
as
    set nocount on;

/*Versioning details*/

select @version = '7.97', @versiondate = '20200712';

    if (@versioncheckmode = 1)
        begin
            return;
        end;


    if @help = 1
        begin
            print '
	/*
		sp_DatabaseRestore from http://FirstResponderKit.org
			
		This script will restore a database from a given file path.
		
		To learn more, visit http://FirstResponderKit.org where you can download new
		versions for free, watch training videos on how it works, get more info on
		the findings, contribute your own code, and more.
		
		Known limitations of this version:
			- Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
			- Tastes awful with marmite.
		
		Unknown limitations of this version:
			- None.  (If we knew them, they would be known. Duh.)
		
		    Changes - for the full list of improvements and fixes in this version, see:
		    https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/
		
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
		
	*/
	';

            print '
	/*
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 0, 
		@RunRecovery = 0;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 1, 
		@RunRecovery = 0;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 1, 
		@RunRecovery = 1;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@ContinueLogs = 0, 
		@RunRecovery = 1;
		
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''D:\Backup\SQL2016PROD1A\LogShipMe\FULL\'', 
		@BackupPathDiff = ''D:\Backup\SQL2016PROD1A\LogShipMe\DIFF\'',
		@BackupPathLog = ''D:\Backup\SQL2016PROD1A\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1;
		 
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@TestRestore = 1,
		@RunCheckDB = 1,
		@Debug = 0;

	EXEC dbo.sp_DatabaseRestore 
		@Database = ''LogShipMe'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'',
		@StandbyMode = 1,
		@StandbyUndoPath = ''D:\Data\'',
		@ContinueLogs = 1, 
		@RunRecovery = 0,
		@Debug = 0;

	-- Restore from stripped backup set when multiple paths are used. This example will restore stripped full backup set along with stripped transactional logs set from multiple backup paths
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''D:\Backup1\DBA\FULL,D:\Backup2\DBA\FULL'', 
		@BackupPathLog = ''D:\Backup1\DBA\LOG,D:\Backup2\DBA\LOG'', 
		@StandbyMode = 0,
		@ContinueLogs = 1, 
		@RunRecovery = 0,
		@Debug = 0;
		
	--This example will restore the latest differential backup, and stop transaction logs at the specified date time.  It will execute and print debug information.
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@StopAt = ''20170508201501'',
		@Debug = 1;

	--This example NOT execute the restore.  Commands will be printed in a copy/paste ready format only
	EXEC dbo.sp_DatabaseRestore 
		@Database = ''DBA'', 
		@BackupPathFull = ''\\StorageServer\LogShipMe\FULL\'', 
		@BackupPathDiff = ''\\StorageServer\LogShipMe\DIFF\'',
		@BackupPathLog = ''\\StorageServer\LogShipMe\LOG\'', 
		@RestoreDiff = 1,
		@ContinueLogs = 0, 
		@RunRecovery = 1,
		@TestRestore = 1,
		@RunCheckDB = 1,
		@Debug = 0,
		@Execute = ''N'';
	';

            return;
        end;

    -- Get the SQL Server version number because the columns returned by RESTORE commands vary by version
-- Based on: https://www.brentozar.com/archive/2015/05/sql-server-version-detection/
-- Need to capture BuildVersion because RESTORE HEADERONLY changed with 2014 CU1, not RTM
declare @productversion as nvarchar(20) = CAST(SERVERPROPERTY('productversion') as nvarchar(20));
declare @majorversion as smallint = CAST(PARSENAME(@productversion, 4) as smallint);
declare @minorversion as smallint = CAST(PARSENAME(@productversion, 3) as smallint);
declare @buildversion as smallint = CAST(PARSENAME(@productversion, 2) as smallint);

    if @majorversion < 10
        begin
            raiserror ('Sorry, DatabaseRestore doesn''t work on versions of SQL prior to 2008.', 15, 1);
            return;
        end;


declare
    @cmd                    nvarchar(4000) = N'', --Holds xp_cmdshell command
    @sql                    nvarchar(max)  = N'', --Holds executable SQL commands
    @lastfullbackup         nvarchar(500)  = N'', --Last full backup name
    @lastdiffbackup         nvarchar(500)  = N'', --Last diff backup name
    @lastdiffbackupdatetime nvarchar(500)  = N'', --Last diff backup date
    @backupfile             nvarchar(500)  = N'', --Name of backup file
    @backupdatetime as      char(15)       = N'', --Used for comparisons to generate ordered backup files/create a stopat point
    @fulllastlsn            numeric(25, 0), --LSN for full
    @difflastlsn            numeric(25, 0), --LSN for diff
    @headerssql as          nvarchar(4000) = N'', --Dynamic insert into #Headers table (deals with varying results from RESTORE FILELISTONLY across different versions)
    @moveoption as          nvarchar(max)  = N'', --If you need to move restored files to a different directory
    @logrecoveryoption as   nvarchar(max)  = N'', --Holds the option to cause logs to be restored in standby mode or with no recovery
    @databaselastlsn        numeric(25, 0), --redo_start_lsn of the current database
    @i                      tinyint        = 1, --Maintains loop to continue logs
    @logrestoreranking      smallint       = 1, --Holds Log iteration # when multiple paths & backup files are being stripped
    @logfirstlsn            numeric(25, 0), --Holds first LSN in log backup headers
    @loglastlsn             numeric(25, 0), --Holds last LSN in log backup headers
    @filelistparamsql       nvarchar(4000) = N'', --Holds INSERT list for #FileListParameters
    @backupparameters       nvarchar(500)  = N'', --Used to save BlockSize, MaxTransferSize and BufferCount
    @restoredatabaseid      smallint; --Holds DB_ID of @RestoreDatabaseName

declare
    @filelistsimple table
                    (
                        backupfile nvarchar(255) not null,
                        depth int not null,
                        [file] int not null
                    );

declare
    @filelist table
              (
                  backuppath nvarchar(255) null,
                  backupfile nvarchar(255) null
              );

declare
    @pathitem table
              (
                  pathitem nvarchar(512)
              );


    if OBJECT_ID(N'tempdb..#FileListParameters') is not null drop table #filelistparameters;
    create table #filelistparameters
    (
        logicalname nvarchar(128) not null,
        physicalname nvarchar(260) not null,
        [Type] char(1) not null,
        filegroupname nvarchar(120) null,
        size numeric(20, 0) not null,
        maxsize numeric(20, 0) not null,
        fileid bigint null,
        createlsn numeric(25, 0) null,
        droplsn numeric(25, 0) null,
        uniqueid uniqueidentifier null,
        readonlylsn numeric(25, 0) null,
        readwritelsn numeric(25, 0) null,
        backupsizeinbytes bigint null,
        sourceblocksize int null,
        filegroupid int null,
        loggroupguid uniqueidentifier null,
        differentialbaselsn numeric(25, 0) null,
        differentialbaseguid uniqueidentifier null,
        isreadonly bit null,
        ispresent bit null,
        tdethumbprint varbinary(32) null,
        snapshoturl nvarchar(360) null
    );

    if OBJECT_ID(N'tempdb..#Headers') is not null drop table #headers;
    create table #headers
    (
        backupname nvarchar(256),
        backupdescription nvarchar(256),
        backuptype nvarchar(256),
        expirationdate nvarchar(256),
        compressed nvarchar(256),
        position nvarchar(256),
        devicetype nvarchar(256),
        username nvarchar(256),
        servername nvarchar(256),
        databasename nvarchar(256),
        databaseversion nvarchar(256),
        databasecreationdate nvarchar(256),
        backupsize nvarchar(256),
        firstlsn nvarchar(256),
        lastlsn nvarchar(256),
        checkpointlsn nvarchar(256),
        databasebackuplsn nvarchar(256),
        backupstartdate nvarchar(256),
        backupfinishdate nvarchar(256),
        sortorder nvarchar(256),
        [CodePage] nvarchar(256),
        unicodelocaleid nvarchar(256),
        unicodecomparisonstyle nvarchar(256),
        compatibilitylevel nvarchar(256),
        softwarevendorid nvarchar(256),
        softwareversionmajor nvarchar(256),
        softwareversionminor nvarchar(256),
        softwareversionbuild nvarchar(256),
        machinename nvarchar(256),
        flags nvarchar(256),
        bindingid nvarchar(256),
        recoveryforkid nvarchar(256),
        collation nvarchar(256),
        familyguid nvarchar(256),
        hasbulkloggeddata nvarchar(256),
        issnapshot nvarchar(256),
        isreadonly nvarchar(256),
        issingleuser nvarchar(256),
        hasbackupchecksums nvarchar(256),
        isdamaged nvarchar(256),
        beginslogchain nvarchar(256),
        hasincompletemetadata nvarchar(256),
        isforceoffline nvarchar(256),
        iscopyonly nvarchar(256),
        firstrecoveryforkid nvarchar(256),
        forkpointlsn nvarchar(256),
        recoverymodel nvarchar(256),
        differentialbaselsn nvarchar(256),
        differentialbaseguid nvarchar(256),
        backuptypedescription nvarchar(256),
        backupsetguid nvarchar(256),
        compressedbackupsize nvarchar(256),
        containment nvarchar(256),
        keyalgorithm nvarchar(32),
        encryptorthumbprint varbinary(20),
        encryptortype nvarchar(32),
        --
        -- Seq added to retain order by
        --
        seq int not null identity (1, 1)
    );

    /*
    Correct paths in case people forget a final "\"
    */
/*Full*/
    if (select RIGHT(@backuppathfull, 1)) <> '\' and CHARINDEX('\', @backuppathfull) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathFull to add a "\"', 0, 1) with nowait;
            set @backuppathfull += N'\';
        end;
    else
        if (select RIGHT(@backuppathfull, 1)) <> '/' and CHARINDEX('/', @backuppathfull) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathFull to add a "/"', 0, 1) with nowait;
                set @backuppathfull += N'/';
            end;
/*Diff*/
    if (select RIGHT(@backuppathdiff, 1)) <> '\' and CHARINDEX('\', @backuppathdiff) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathDiff to add a "\"', 0, 1) with nowait;
            set @backuppathdiff += N'\';
        end;
    else
        if (select RIGHT(@backuppathdiff, 1)) <> '/' and CHARINDEX('/', @backuppathdiff) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathDiff to add a "/"', 0, 1) with nowait;
                set @backuppathdiff += N'/';
            end;
/*Log*/
    if (select RIGHT(@backuppathlog, 1)) <> '\' and CHARINDEX('\', @backuppathlog) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathLog to add a "\"', 0, 1) with nowait;
            set @backuppathlog += N'\';
        end;
    else
        if (select RIGHT(@backuppathlog, 1)) <> '/' and CHARINDEX('/', @backuppathlog) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing @BackupPathLog to add a "/"', 0, 1) with nowait;
                set @backuppathlog += N'/';
            end;
/*Move Data File*/
    if NULLIF(@movedatadrive, '') is null
        begin
            if @execute = 'Y' or @debug = 1
                raiserror ('Getting default data drive for @MoveDataDrive', 0, 1) with nowait;
            set @movedatadrive = CAST(SERVERPROPERTY('InstanceDefaultDataPath') as nvarchar(260));
        end;
    if (select RIGHT(@movedatadrive, 1)) <> '\' and CHARINDEX('\', @movedatadrive) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @MoveDataDrive to add a "\"', 0, 1) with nowait;
            set @movedatadrive += N'\';
        end;
    else
        if (select RIGHT(@movedatadrive, 1)) <> '/' and CHARINDEX('/', @movedatadrive) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing @MoveDataDrive to add a "/"', 0, 1) with nowait;
                set @movedatadrive += N'/';
            end;
/*Move Log File*/
    if NULLIF(@movelogdrive, '') is null
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Getting default log drive for @MoveLogDrive', 0, 1) with nowait;
            set @movelogdrive = CAST(SERVERPROPERTY('InstanceDefaultLogPath') as nvarchar(260));
        end;
    if (select RIGHT(@movelogdrive, 1)) <> '\' and CHARINDEX('\', @movelogdrive) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @MoveLogDrive to add a "\"', 0, 1) with nowait;
            set @movelogdrive += N'\';
        end;
    else
        if (select RIGHT(@movelogdrive, 1)) <> '/' and CHARINDEX('/', @movelogdrive) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing@MoveLogDrive to add a "/"', 0, 1) with nowait;
                set @movelogdrive += N'/';
            end;
/*Move Filestream File*/
    if NULLIF(@movefilestreamdrive, '') is null
        begin
            if @execute = 'Y' or @debug = 1
                raiserror ('Setting default data drive for @MoveFilestreamDrive', 0, 1) with nowait;
            set @movefilestreamdrive = CAST(SERVERPROPERTY('InstanceDefaultDataPath') as nvarchar(260));
        end;
    if (select RIGHT(@movefilestreamdrive, 1)) <> '\' and CHARINDEX('\', @movefilestreamdrive) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @MoveFilestreamDrive to add a "\"', 0, 1) with nowait;
            set @movefilestreamdrive += N'\';
        end;
    else
        if (select RIGHT(@movefilestreamdrive, 1)) <> '/' and
           CHARINDEX('/', @movefilestreamdrive) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1
                    raiserror ('Fixing @MoveFilestreamDrive to add a "/"', 0, 1) with nowait;
                set @movefilestreamdrive += N'/';
            end;
/*Standby Undo File*/
    if (select RIGHT(@standbyundopath, 1)) <> '\' and CHARINDEX('\', @standbyundopath) > 0 --Has to end in a '\'
        begin
            if @execute = 'Y' or @debug = 1 raiserror ('Fixing @StandbyUndoPath to add a "\"', 0, 1) with nowait;
            set @standbyundopath += N'\';
        end;
    else
        if (select RIGHT(@standbyundopath, 1)) <> '/' and CHARINDEX('/', @standbyundopath) > 0 --Has to end in a '/'
            begin
                if @execute = 'Y' or @debug = 1 raiserror ('Fixing @StandbyUndoPath to add a "/"', 0, 1) with nowait;
                set @standbyundopath += N'/';
            end;
    if @restoredatabasename is null
        begin
            set @restoredatabasename = @database;
        end;

/*check input parameters*/
    if not @maxtransfersize is null
        begin
            if @maxtransfersize > 4194304
                begin
                    raiserror ('@MaxTransferSize can not be greater then 4194304', 0, 1) with nowait;
                end

            if @maxtransfersize % 64 <> 0
                begin
                    raiserror ('@MaxTransferSize has to be a multiple of 65536', 0, 1) with nowait;
                end
        end;

    if not @blocksize is null
        begin
            if @blocksize not in (512, 1024, 2048, 4096, 8192, 16384, 32768, 65536)
                begin
                    raiserror ('Supported values for @BlockSize are 512, 1024, 2048, 4096, 8192, 16384, 32768, and 65536', 0, 1) with nowait;
                end
        end

    set @restoredatabaseid = DB_ID(@restoredatabasename);
    set @restoredatabasename = QUOTENAME(@restoredatabasename);
--If xp_cmdshell is disabled, force use of xp_dirtree
    if not EXISTS(select *
                  from sys.configurations
                  where name = 'xp_cmdshell'
                    and value_in_use = 1)
        set @simplefolderenumeration = 1;

    set @headerssql =
            N'INSERT INTO #Headers WITH (TABLOCK)
              (BackupName, BackupDescription, BackupType, ExpirationDate, Compressed, Position, DeviceType, UserName, ServerName
              ,DatabaseName, DatabaseVersion, DatabaseCreationDate, BackupSize, FirstLSN, LastLSN, CheckpointLSN, DatabaseBackupLSN
              ,BackupStartDate, BackupFinishDate, SortOrder, CodePage, UnicodeLocaleId, UnicodeComparisonStyle, CompatibilityLevel
              ,SoftwareVendorId, SoftwareVersionMajor, SoftwareVersionMinor, SoftwareVersionBuild, MachineName, Flags, BindingID
              ,RecoveryForkID, Collation, FamilyGUID, HasBulkLoggedData, IsSnapshot, IsReadOnly, IsSingleUser, HasBackupChecksums
              ,IsDamaged, BeginsLogChain, HasIncompleteMetaData, IsForceOffline, IsCopyOnly, FirstRecoveryForkID, ForkPointLSN
              ,RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescription, BackupSetGUID, CompressedBackupSize';

    if @majorversion >= 11
        set @headerssql += NCHAR(13) + NCHAR(10) + N', Containment';

    if @majorversion >= 13 or (@majorversion = 12 and @buildversion >= 2342)
        set @headerssql += N', KeyAlgorithm, EncryptorThumbprint, EncryptorType';

    set @headerssql += N')' + NCHAR(13) + NCHAR(10);
    set @headerssql += N'EXEC (''RESTORE HEADERONLY FROM DISK=''''{Path}'''''')';

    if @backuppathfull is not null
        begin
            declare @currentbackuppathfull nvarchar(255);

            -- Split CSV string logic has taken from Ola Hallengren's :)
            with backuppaths (
                              startposition, endposition, pathitem
                )
                     as (
                    select 1                                                                               as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathfull, 1), 0),
                                  LEN(@backuppathfull) + 1)                                                as endposition,
                           SUBSTRING(@backuppathfull, 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathfull, 1), 0), LEN(@backuppathfull) + 1) -
                                     1)                                                                    as pathitem
                    where @backuppathfull is not null
                    union all
                    select CAST(endposition as int) + 1                                  as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathfull, endposition + 1), 0),
                                  LEN(@backuppathfull) + 1)                              as endposition,
                           SUBSTRING(@backuppathfull, endposition + 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathfull, endposition + 1), 0),
                                            LEN(@backuppathfull) + 1) - endposition - 1) as pathitem
                    from backuppaths
                    where endposition < LEN(@backuppathfull) + 1
                )
            insert
            into @pathitem
            select case RIGHT(pathitem, 1) when '\' then pathitem else pathitem + '\' end
            from backuppaths;

            while 1 = 1
                begin

                    select top 1 @currentbackuppathfull = pathitem
                    from @pathitem
                    where pathitem > COALESCE(@currentbackuppathfull, '')
                    order by pathitem;
                    if @@rowcount = 0 break;

                    if @simplefolderenumeration = 1
                        begin
                            -- Get list of files
                            insert into @filelistsimple (backupfile, depth, [file]) exec master.sys.xp_dirtree @currentbackuppathfull, 1, 1;
                            insert @filelist (backuppath, backupfile)
                            select @currentbackuppathfull, backupfile
                            from @filelistsimple;
                            delete from @filelistsimple;
                        end
                    else
                        begin
                            set @cmd = N'DIR /b "' + @currentbackuppathfull + N'"';
                            if @debug = 1
                                begin
                                    if @cmd is null print '@cmd is NULL for @CurrentBackupPathFull';
                                    print @cmd;
                                end;
                            insert into @filelist (backupfile) exec master.sys.xp_cmdshell @cmd;
                            update @filelist
                            set backuppath = @currentbackuppathfull
                            where backuppath is null;
                        end;

                    if @debug = 1
                        begin
                            select backuppath, backupfile from @filelist;
                        end;
                    if @simplefolderenumeration = 1
                        begin
                            /*Check what we can*/
                            if not EXISTS(select * from @filelist)
                                begin
                                    raiserror ('(FULL) No rows were returned for that database in path %s', 16, 1, @currentbackuppathfull) with nowait;
                                    return;
                                end;
                        end
                    else
                        begin
                            /*Full Sanity check folders*/
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The system cannot find the path specified.'
                                      or fl.backupfile = 'File Not Found'
                               ) = 1
                                begin
                                    raiserror ('(FULL) No rows or bad value for path %s', 16, 1, @currentbackuppathfull) with nowait;
                                    return;
                                end;
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'Access is denied.'
                               ) = 1
                                begin
                                    raiserror ('(FULL) Access is denied to %s', 16, 1, @currentbackuppathfull) with nowait;
                                    return;
                                end;
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                               ) = 1
                                and
                               (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile is null
                               ) = 1
                                begin
                                    raiserror ('(FULL) Empty directory %s', 16, 1, @currentbackuppathfull) with nowait;
                                    return;
                                end
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The user name or password is incorrect.'
                               ) = 1
                                begin
                                    raiserror ('(FULL) Incorrect user name or password for %s', 16, 1, @currentbackuppathfull) with nowait;
                                    return;
                                end;
                        end;
                end
            /*End folder sanity check*/

            -- Find latest full backup
            select @lastfullbackup = MAX(backupfile)
            from @filelist
            where backupfile like N'%.bak'
              and backupfile like N'%' + @database + N'%'
              and (@stopat is null or REPLACE(RIGHT(REPLACE(@lastfullbackup, RIGHT(@lastfullbackup,
                                                                                   PATINDEX('%_[0-9][0-9]%', REVERSE(@lastfullbackup))),
                                                            ''), 16), '_', '') <= @stopat);

            /*	To get all backups that belong to the same set we can do two things:
                    1.	RESTORE HEADERONLY of ALL backup files in the folder and look for BackupSetGUID.
                        Backups that belong to the same split will have the same BackupSetGUID.
                    2.	Olla Hallengren's solution appends file index at the end of the name:
                        SQLSERVER1_TEST_DB_FULL_20180703_213211_1.bak
                        SQLSERVER1_TEST_DB_FULL_20180703_213211_2.bak
                        SQLSERVER1_TEST_DB_FULL_20180703_213211_N.bak
                        We can and find all related files with the same timestamp but different index.
                        This option is simpler and requires less changes to this procedure */

            if @lastfullbackup is null
                begin
                    raiserror ('No backups for "%s" found in "%s"', 16, 1, @database, @backuppathfull) with nowait;
                    return;
                end;

            select backuppath, backupfile
            into #splitfullbackups
            from @filelist
            where LEFT(backupfile, LEN(backupfile) - PATINDEX('%[_]%', REVERSE(backupfile))) =
                  LEFT(@lastfullbackup, LEN(@lastfullbackup) - PATINDEX('%[_]%', REVERSE(@lastfullbackup)))
              and PATINDEX('%[_]%', REVERSE(@lastfullbackup)) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Ola only supports up to 64 file split.
            order by REPLACE(RIGHT(REPLACE(backupfile,
                                           RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))), ''), 16),
                             '_', '') desc;

            -- File list can be obtained by running RESTORE FILELISTONLY of any file from the given BackupSet therefore we do not have to cater for split backups when building @FileListParamSQL

            set @filelistparamsql =
                    N'INSERT INTO #FileListParameters WITH (TABLOCK)
                     (LogicalName, PhysicalName, Type, FileGroupName, Size, MaxSize, FileID, CreateLSN, DropLSN
                     ,UniqueID, ReadOnlyLSN, ReadWriteLSN, BackupSizeInBytes, SourceBlockSize, FileGroupID, LogGroupGUID
                     ,DifferentialBaseLSN, DifferentialBaseGUID, IsReadOnly, IsPresent, TDEThumbprint';

            if @majorversion >= 13
                begin
                    set @filelistparamsql += N', SnapshotUrl';
                end;

            set @filelistparamsql += N')' + NCHAR(13) + NCHAR(10);
            set @filelistparamsql += N'EXEC (''RESTORE FILELISTONLY FROM DISK=''''{Path}'''''')';

            -- get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement as well as Non-Split Backups Restore Command
            select top 1 @currentbackuppathfull = backuppath, @lastfullbackup = backupfile
            from @filelist
            order by REPLACE(RIGHT(REPLACE(backupfile,
                                           RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))), ''), 16),
                             '_', '') desc;

            set @sql = REPLACE(@filelistparamsql, N'{Path}', @currentbackuppathfull + @lastfullbackup);

            if @debug = 1
                begin
                    if @sql is null
                        print '@sql is NULL for INSERT to #FileListParameters: @BackupPathFull + @LastFullBackup';
                    print @sql;
                end;

            exec (@sql);
            if @debug = 1
                begin
                    select '#FileListParameters' as table_name, * from #filelistparameters;
                    select '@FileList' as table_name, backuppath, backupfile
                    from @filelist
                    where backupfile is not null;
                end

            --get the backup completed data so we can apply tlogs from that point forwards
            set @sql = REPLACE(@headerssql, N'{Path}', @currentbackuppathfull + @lastfullbackup);

            if @debug = 1
                begin
                    if @sql is null
                        print '@sql is NULL for get backup completed data: @BackupPathFull, @LastFullBackup';
                    print @sql;
                end;
            execute (@sql);
            if @debug = 1
                begin
                    select '#Headers' as table_name, @lastfullbackup as fullbackupfile, * from #headers
                end;

            --Ensure we are looking at the expected backup, but only if we expect to restore a FULL backups
            if not EXISTS(select * from #headers h where h.databasename = @database)
                begin
                    raiserror ('Backupfile "%s" does not match @Database parameter "%s"', 16, 1, @lastfullbackup, @database) with nowait;
                    return;
                end;

            if not @buffercount is null
                begin
                    set @backupparameters += N', BufferCount=' + cast(@buffercount as nvarchar(10))
                end

            if not @maxtransfersize is null
                begin
                    set @backupparameters += N', MaxTransferSize=' + cast(@maxtransfersize as nvarchar(7))
                end

            if not @blocksize is null
                begin
                    set @backupparameters += N', BlockSize=' + cast(@blocksize as nvarchar(5))
                end

            if @movefiles = 1
                begin
                    if @execute = 'Y' raiserror ('@MoveFiles = 1, adjusting paths', 0, 1) with nowait;

                    with files
                             as (
                            select case
                                       when type = 'D' then @movedatadrive
                                       when type = 'L' then @movelogdrive
                                       when type = 'S' then @movefilestreamdrive
                                       end + case
                                                 when @database = @restoredatabasename then REVERSE(
                                                         LEFT(REVERSE(physicalname),
                                                              CHARINDEX('\', REVERSE(physicalname), 1) - 1))
                                                 else REPLACE(REVERSE(LEFT(REVERSE(physicalname),
                                                                           CHARINDEX('\', REVERSE(physicalname), 1) - 1)),
                                                              @database,
                                                              SUBSTRING(@restoredatabasename, 2, LEN(@restoredatabasename) - 2))
                                       end as targetphysicalname,
                                   physicalname,
                                   logicalname
                            from #filelistparameters)
                    select @moveoption =
                           @moveoption + N', MOVE ''' + files.logicalname + N''' TO ''' + files.targetphysicalname +
                           ''''
                    from files
                    where files.targetphysicalname <> files.physicalname;

                    if @debug = 1 print @moveoption
                end;

            /*Process @ExistingDBAction flag */
            if @existingdbaction between 1 and 4
                begin
                    if @restoredatabaseid is not null
                        begin
                            if @existingdbaction = 1
                                begin
                                    raiserror ('Setting single user', 0, 1) with nowait;
                                    set @sql = N'ALTER DATABASE ' + @restoredatabasename +
                                               ' SET SINGLE_USER WITH ROLLBACK IMMEDIATE; ' + NCHAR(13);
                                    if @debug = 1 or @execute = 'N'
                                        begin
                                            if @sql is null print '@sql is NULL for SINGLE_USER';
                                            print @sql;
                                        end;
                                    if @debug in (0, 1) and @execute = 'Y'
                                        execute master.sys.sp_executesql @stmt = @sql;
                                    if @debug in (0, 1) and @execute = 'Y' and
                                       DATABASEPROPERTYEX(@restoredatabasename, 'STATUS') != 'RESTORING'
                                        execute @sql = [dbo].[CommandExecute] @command = @sql,
                                                       @commandtype = 'ALTER DATABASE SINGLE_USER', @mode = 1,
                                                       @databasename = @database, @logtotable = 'Y', @execute = 'Y';
                                end
                            if @existingdbaction in (2, 3)
                                begin
                                    raiserror ('Killing connections', 0, 1) with nowait;
                                    set @sql = N'/* Kill connections */' + NCHAR(13);
                                    select @sql = @sql + N'KILL ' + CAST(spid as nvarchar(5)) + N';' + NCHAR(13)
                                    from
                                        --database_ID was only added to sys.dm_exec_sessions in SQL Server 2012 but we need to support older
                                        sys.sysprocesses
                                    where dbid = @restoredatabaseid;
                                    if @debug = 1 or @execute = 'N'
                                        begin
                                            if @sql is null print '@sql is NULL for Kill connections';
                                            print @sql;
                                        end;
                                    if @debug in (0, 1) and @execute = 'Y'
                                        execute master.sys.sp_executesql @stmt = @sql;
                                end
                            if @existingdbaction = 3
                                begin
                                    raiserror ('Dropping database', 0, 1) with nowait;

                                    set @sql = N'DROP DATABASE ' + @restoredatabasename + NCHAR(13);
                                    if @debug = 1 or @execute = 'N'
                                        begin
                                            if @sql is null print '@sql is NULL for DROP DATABASE';
                                            print @sql;
                                        end;
                                    if @debug in (0, 1) and @execute = 'Y'
                                        execute master.sys.sp_executesql @stmt = @sql;
                                end
                            if @existingdbaction = 4
                                begin
                                    raiserror ('Offlining database', 0, 1) with nowait;

                                    set @sql = N'ALTER DATABASE ' + @restoredatabasename + SPACE(1) +
                                               'SET OFFLINE WITH ROLLBACK IMMEDIATE';
                                    if @debug = 1 or @execute = 'N'
                                        begin
                                            if @sql is null print '@sql is NULL for Offline database';
                                            print @sql;
                                        end;
                                    if @debug in (0, 1) and @execute = 'Y' and
                                       DATABASEPROPERTYEX(@restoredatabasename, 'STATUS') != 'RESTORING'
                                        execute @sql = [dbo].[CommandExecute] @command = @sql,
                                                       @commandtype = 'OFFLINE DATABASE', @mode = 1,
                                                       @databasename = @database, @logtotable = 'Y', @execute = 'Y';
                                end;
                        end
                    else
                        raiserror ('@ExistingDBAction > 0, but no existing @RestoreDatabaseName', 0, 1) with nowait;
                end
            else
                if @execute = 'Y' or @debug = 1
                    raiserror ('@ExistingDBAction %u so do nothing', 0, 1, @existingdbaction) with nowait;

            if @continuelogs = 0
                begin
                    if @execute = 'Y' raiserror ('@ContinueLogs set to 0', 0, 1) with nowait;

                    /* now take split backups into account */
                    if (select COUNT(*) from #splitfullbackups) > 0
                        begin
                            raiserror ('Split backups found', 0, 1) with nowait;

                            set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM '
                                + STUFF(
                                               (select CHAR(10) + ',DISK=''' + backuppath + backupfile + ''''
                                                from #splitfullbackups
                                                order by backupfile
                                                for xml path ('')),
                                               1,
                                               2,
                                               '') + N' WITH NORECOVERY, REPLACE' + @backupparameters + @moveoption +
                                       NCHAR(13) + NCHAR(10);
                        end;
                    else
                        begin
                            set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM DISK = ''' +
                                       @currentbackuppathfull + @lastfullbackup + N''' WITH NORECOVERY, REPLACE' +
                                       @backupparameters + @moveoption + NCHAR(13) + NCHAR(10);
                        end
                    if (@standbymode = 1)
                        begin
                            if (@standbyundopath is null)
                                begin
                                    if @execute = 'Y' or @debug = 1
                                        raiserror ('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) with nowait;
                                end
                            else
                                if (select COUNT(*) from #splitfullbackups) > 0
                                    begin
                                        set @sql = @sql + ', STANDBY = ''' + @standbyundopath + @database +
                                                   'Undo.ldf''' + NCHAR(13) + NCHAR(10);
                                    end
                                else
                                    begin
                                        set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM DISK = ''' +
                                                   @currentbackuppathfull + @lastfullbackup + N''' WITH  REPLACE' +
                                                   @backupparameters + @moveoption + N' , STANDBY = ''' +
                                                   @standbyundopath + @database + 'Undo.ldf''' + NCHAR(13) + NCHAR(10);
                                    end
                        end;
                    if @debug = 1 or @execute = 'N'
                        begin
                            if @sql is null
                                print '@sql is NULL for RESTORE DATABASE: @BackupPathFull, @LastFullBackup, @MoveOption';
                            print @sql;
                        end;

                    if @debug in (0, 1) and @execute = 'Y'
                        execute master.sys.sp_executesql @stmt = @sql;

                    -- We already loaded #Headers above

                    --setting the @BackupDateTime to a numeric string so that it can be used in comparisons
                    set @backupdatetime = REPLACE(RIGHT(REPLACE(@lastfullbackup, RIGHT(@lastfullbackup,
                                                                                       PATINDEX('%_[0-9][0-9]%', REVERSE(@lastfullbackup))),
                                                                ''), 16), '_', '');

                    select @fulllastlsn = CAST(lastlsn as numeric(25, 0)) from #headers where backuptype = 1;
                    if @debug = 1
                        begin
                            if @backupdatetime is null print '@BackupDateTime is NULL for REPLACE: @LastFullBackup';
                            print @backupdatetime;
                        end;

                end;
            else
                begin

                    select @databaselastlsn = CAST(f.redo_start_lsn as numeric(25, 0))
                    from master.sys.databases d
                             join master.sys.master_files f on d.database_id = f.database_id
                    where d.name = SUBSTRING(@restoredatabasename, 2, LEN(@restoredatabasename) - 2)
                      and f.file_id = 1;

                end;
        end;

    if @backuppathfull is null and @continuelogs = 1
        begin

            select @databaselastlsn = CAST(f.redo_start_lsn as numeric(25, 0))
            from master.sys.databases d
                     join master.sys.master_files f on d.database_id = f.database_id
            where d.name = SUBSTRING(@restoredatabasename, 2, LEN(@restoredatabasename) - 2)
              and f.file_id = 1;

        end;

    if @backuppathdiff is not null
        begin
            delete from @filelist;
            delete from @filelistsimple;
            delete from @pathitem;

            declare @currentbackuppathdiff nvarchar(512);
            -- Split CSV string logic has taken from Ola Hallengren's :)
            with backuppaths (
                              startposition, endposition, pathitem
                )
                     as (
                    select 1                                                                               as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathdiff, 1), 0),
                                  LEN(@backuppathdiff) + 1)                                                as endposition,
                           SUBSTRING(@backuppathdiff, 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathdiff, 1), 0), LEN(@backuppathdiff) + 1) -
                                     1)                                                                    as pathitem
                    where @backuppathdiff is not null
                    union all
                    select CAST(endposition as int) + 1                                  as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathdiff, endposition + 1), 0),
                                  LEN(@backuppathdiff) + 1)                              as endposition,
                           SUBSTRING(@backuppathdiff, endposition + 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathdiff, endposition + 1), 0),
                                            LEN(@backuppathdiff) + 1) - endposition - 1) as pathitem
                    from backuppaths
                    where endposition < LEN(@backuppathdiff) + 1
                )
            insert
            into @pathitem
            select case RIGHT(pathitem, 1) when '\' then pathitem else pathitem + '\' end
            from backuppaths;

            while 1 = 1
                begin

                    select top 1 @currentbackuppathdiff = pathitem
                    from @pathitem
                    where pathitem > COALESCE(@currentbackuppathdiff, '')
                    order by pathitem;
                    if @@rowcount = 0 break;

                    if @simplefolderenumeration = 1
                        begin
                            -- Get list of files
                            insert into @filelistsimple (backupfile, depth, [file]) exec master.sys.xp_dirtree @currentbackuppathdiff, 1, 1;
                            insert @filelist (backuppath, backupfile)
                            select @currentbackuppathdiff, backupfile
                            from @filelistsimple;
                            delete from @filelistsimple;
                        end
                    else
                        begin
                            set @cmd = N'DIR /b "' + @currentbackuppathdiff + N'"';
                            if @debug = 1
                                begin
                                    if @cmd is null print '@cmd is NULL for @CurrentBackupPathDiff';
                                    print @cmd;
                                end;
                            insert into @filelist (backupfile) exec master.sys.xp_cmdshell @cmd;
                            update @filelist set backuppath = @currentbackuppathdiff where backuppath is null;
                        end;

                    if @debug = 1
                        begin
                            select backuppath, backupfile from @filelist where backupfile is not null;
                        end;
                    if @simplefolderenumeration = 0
                        begin
                            /*Full Sanity check folders*/
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The system cannot find the path specified.'
                               ) = 1
                                begin
                                    raiserror ('(DIFF) Bad value for path %s', 16, 1, @currentbackuppathdiff) with nowait;
                                    return;
                                end;
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'Access is denied.'
                               ) = 1
                                begin
                                    raiserror ('(DIFF) Access is denied to %s', 16, 1, @currentbackuppathdiff) with nowait;
                                    return;
                                end;
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The user name or password is incorrect.'
                               ) = 1
                                begin
                                    raiserror ('(DIFF) Incorrect user name or password for %s', 16, 1, @currentbackuppathdiff) with nowait;
                                    return;
                                end;
                        end;
                end
            /*End folder sanity check*/
            -- Find latest diff backup
            select @lastdiffbackup = MAX(backupfile)
            from @filelist
            where backupfile like N'%.bak'
              and backupfile like N'%' + @database + '%'
              and (@stopat is null or REPLACE(RIGHT(REPLACE(@lastdiffbackup, RIGHT(@lastdiffbackup,
                                                                                   PATINDEX('%_[0-9][0-9]%', REVERSE(@lastdiffbackup))),
                                                            ''), 16), '_', '') <= @stopat);

            -- Load FileList data into Temp Table sorted by DateTime Stamp desc
            select backuppath, backupfile
            into #splitdiffbackups
            from @filelist
            where LEFT(backupfile, LEN(backupfile) - PATINDEX('%[_]%', REVERSE(backupfile))) =
                  LEFT(@lastdiffbackup, LEN(@lastdiffbackup) - PATINDEX('%[_]%', REVERSE(@lastdiffbackup)))
              and PATINDEX('%[_]%', REVERSE(@lastdiffbackup)) <= 7 -- there is a 1 or 2 digit index at the end of the string which indicates split backups. Olla only supports up to 64 file split.
            order by REPLACE(RIGHT(REPLACE(backupfile,
                                           RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))), ''), 16),
                             '_', '') desc;

            --No file = no backup to restore
            set @lastdiffbackupdatetime = REPLACE(RIGHT(REPLACE(@lastdiffbackup, RIGHT(@lastdiffbackup,
                                                                                       PATINDEX('%_[0-9][0-9]%', REVERSE(@lastdiffbackup))),
                                                                ''), 16), '_', '');

            -- Get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement as well as non-split backups
            select top 1 @currentbackuppathdiff = backuppath, @lastdiffbackup = backupfile
            from @filelist
            order by REPLACE(RIGHT(REPLACE(backupfile,
                                           RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))), ''), 16),
                             '_', '') desc;

            if @restorediff = 1 and @backupdatetime < @lastdiffbackupdatetime
                begin

                    if (select COUNT(*) from #splitdiffbackups) > 0
                        begin
                            raiserror ('Split backups found', 0, 1) with nowait;
                            set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM '
                                + STUFF(
                                               (select CHAR(10) + ',DISK=''' + backuppath + backupfile + ''''
                                                from #splitdiffbackups
                                                order by backupfile
                                                for xml path ('')),
                                               1,
                                               2,
                                               '') + N' WITH NORECOVERY, REPLACE' + @backupparameters + @moveoption +
                                       NCHAR(13) + NCHAR(10);
                        end;
                    else
                        set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM DISK = ''' +
                                   @currentbackuppathdiff + @lastdiffbackup + N''' WITH NORECOVERY' +
                                   @backupparameters + @moveoption + NCHAR(13) + NCHAR(10);

                    if (@standbymode = 1)
                        begin
                            if (@standbyundopath is null)
                                begin
                                    if @execute = 'Y' or @debug = 1
                                        raiserror ('The file path of the undo file for standby mode was not specified. The database will not be restored in standby mode.', 0, 1) with nowait;
                                end
                            else
                                if (select COUNT(*) from #splitdiffbackups) > 0
                                    set @sql = @sql + ', STANDBY = ''' + @standbyundopath + @database + 'Undo.ldf''' +
                                               NCHAR(13) + NCHAR(10);
                                else
                                    set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' FROM DISK = ''' +
                                               @backuppathdiff + @lastdiffbackup + N''' WITH STANDBY = ''' +
                                               @standbyundopath + @database + 'Undo.ldf''' + @backupparameters +
                                               @moveoption + NCHAR(13) + NCHAR(10);
                        end;
                    if @debug = 1 or @execute = 'N'
                        begin
                            if @sql is null print '@sql is NULL for RESTORE DATABASE: @BackupPathDiff, @LastDiffBackup';
                            print @sql;
                        end;
                    if @debug in (0, 1) and @execute = 'Y'
                        execute master.sys.sp_executesql @stmt = @sql;

                    --get the backup completed data so we can apply tlogs from that point forwards
                    set @sql = REPLACE(@headerssql, N'{Path}', @currentbackuppathdiff + @lastdiffbackup);

                    if @debug = 1
                        begin
                            if @sql is null print '@sql is NULL for REPLACE: @CurrentBackupPathDiff, @LastDiffBackup';
                            print @sql;
                        end;

                    execute (@sql);
                    if @debug = 1
                        begin
                            select '#Headers' as table_name, @lastdiffbackup as diffbackupfile, *
                            from #headers as h
                            where h.backuptype = 5;
                        end

                    --set the @BackupDateTime to the date time on the most recent differential
                    set @backupdatetime = ISNULL(@lastdiffbackupdatetime, @backupdatetime);
                    if @debug = 1
                        begin
                            if @backupdatetime is null
                                print '@BackupDateTime is NULL for REPLACE: @LastDiffBackupDateTime';
                            print @backupdatetime;
                        end;
                    select @difflastlsn = CAST(lastlsn as numeric(25, 0))
                    from #headers
                    where backuptype = 5;
                end;

            if @difflastlsn is null
                begin
                    set @difflastlsn = @fulllastlsn
                end
        end
    if @backuppathlog is not null
        begin
            delete from @filelist;
            delete from @filelistsimple;
            delete from @pathitem;

            declare @currentbackuppathlog nvarchar(512);
            -- Split CSV string logic has taken from Ola Hallengren's :)
            with backuppaths (
                              startposition, endposition, pathitem
                )
                     as (
                    select 1                                                                             as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathlog, 1), 0), LEN(@backuppathlog) + 1) as endposition,
                           SUBSTRING(@backuppathlog, 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathlog, 1), 0), LEN(@backuppathlog) + 1) -
                                     1)                                                                  as pathitem
                    where @backuppathlog is not null
                    union all
                    select CAST(endposition as int) + 1                                 as startposition,
                           ISNULL(NULLIF(CHARINDEX(',', @backuppathlog, endposition + 1), 0),
                                  LEN(@backuppathlog) + 1)                              as endposition,
                           SUBSTRING(@backuppathlog, endposition + 1,
                                     ISNULL(NULLIF(CHARINDEX(',', @backuppathlog, endposition + 1), 0),
                                            LEN(@backuppathlog) + 1) - endposition - 1) as pathitem
                    from backuppaths
                    where endposition < LEN(@backuppathlog) + 1
                )
            insert
            into @pathitem
            select case RIGHT(pathitem, 1) when '\' then pathitem else pathitem + '\' end
            from backuppaths;

            while 1 = 1
                begin
                    select top 1 @currentbackuppathlog = pathitem
                    from @pathitem
                    where pathitem > COALESCE(@currentbackuppathlog, '')
                    order by pathitem;
                    if @@rowcount = 0 break;

                    if @simplefolderenumeration = 1
                        begin
                            -- Get list of files
                            insert into @filelistsimple (backupfile, depth, [file]) exec master.sys.xp_dirtree @backuppathlog, 1, 1;
                            insert @filelist (backuppath, backupfile)
                            select @currentbackuppathlog, backupfile
                            from @filelistsimple;
                            delete from @filelistsimple;
                        end
                    else
                        begin
                            set @cmd = N'DIR /b "' + @currentbackuppathlog + N'"';
                            if @debug = 1
                                begin
                                    if @cmd is null print '@cmd is NULL for @CurrentBackupPathLog';
                                    print @cmd;
                                end;
                            insert into @filelist (backupfile) exec master.sys.xp_cmdshell @cmd;
                            update @filelist
                            set backuppath = @currentbackuppathlog
                            where backuppath is null;
                        end;

                    if @simplefolderenumeration = 1
                        begin
                            /*Check what we can*/
                            if not EXISTS(select * from @filelist)
                                begin
                                    raiserror ('(LOG) No rows were returned for that database %s in path %s', 16, 1, @database, @currentbackuppathlog) with nowait;
                                    return;
                                end;
                        end
                    else
                        begin
                            /*Full Sanity check folders*/
                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The system cannot find the path specified.'
                                      or fl.backupfile = 'File Not Found'
                               ) = 1
                                begin
                                    raiserror ('(LOG) No rows or bad value for path %s', 16, 1, @currentbackuppathlog) with nowait;
                                    return;
                                end;

                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'Access is denied.'
                               ) = 1
                                begin
                                    raiserror ('(LOG) Access is denied to %s', 16, 1, @currentbackuppathlog) with nowait;
                                    return;
                                end;

                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                               ) = 1
                                and
                               (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile is null
                               ) = 1
                                begin
                                    raiserror ('(LOG) Empty directory %s', 16, 1, @currentbackuppathlog) with nowait;
                                    return;
                                end

                            if (
                                   select COUNT(*)
                                   from @filelist as fl
                                   where fl.backupfile = 'The user name or password is incorrect.'
                               ) = 1
                                begin
                                    raiserror ('(LOG) Incorrect user name or password for %s', 16, 1, @currentbackuppathlog) with nowait;
                                    return;
                                end;
                        end;
                end
            /*End folder sanity check*/

            if @debug = 1
                begin
                    select * from @filelist where backupfile is not null;
                end

            if (@onlylogsafter is not null)
                begin

                    if @execute = 'Y' or @debug = 1
                        raiserror ('@OnlyLogsAfter is NOT NULL, deleting from @FileList', 0, 1) with nowait;

                    delete fl
                    from @filelist as fl
                    where backupfile like N'%.trn'
                      and backupfile like N'%' + @database + N'%'
                      and REPLACE(RIGHT(REPLACE(fl.backupfile,
                                                RIGHT(fl.backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(fl.backupfile))),
                                                ''), 16), '_', '') < @onlylogsafter;

                end


-- Check for log backups
            if (@stopat is null and @onlylogsafter is null)
                begin
                    delete
                    from @filelist
                    where backupfile like N'%.trn'
                      and backupfile like N'%' + @database + N'%'
                      and not (@continuelogs = 1 or (@continuelogs = 0 and REPLACE(RIGHT(REPLACE(backupfile,
                                                                                                 RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                                                 ''), 16), '_', '') >=
                                                                           @backupdatetime));
                end;


            if (@stopat is null and @onlylogsafter is not null)
                begin
                    delete
                    from @filelist
                    where backupfile like N'%.trn'
                      and backupfile like N'%' + @database + N'%'
                      and not (@continuelogs = 1 or (@continuelogs = 0 and REPLACE(RIGHT(REPLACE(backupfile,
                                                                                                 RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                                                 ''), 16), '_', '') >=
                                                                           @onlylogsafter));
                end;


            if (@stopat is not null and @onlylogsafter is null)
                begin
                    delete
                    from @filelist
                    where backupfile like N'%.trn'
                      and backupfile like N'%' + @database + N'%'
                      and not (@continuelogs = 1 or (@continuelogs = 0 and REPLACE(RIGHT(REPLACE(backupfile,
                                                                                                 RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                                                 ''), 16), '_', '') >=
                                                                           @backupdatetime) and REPLACE(RIGHT(REPLACE(
                                                                                                                      backupfile,
                                                                                                                      RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                                                                      ''),
                                                                                                              16), '_',
                                                                                                        '') <= @stopat)
                      and not ((@continuelogs = 1 and REPLACE(RIGHT(REPLACE(backupfile,
                                                                            RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                            ''), 16), '_', '') <= @stopat) or
                               (@continuelogs = 0 and REPLACE(RIGHT(REPLACE(backupfile,
                                                                            RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                            ''), 16), '_', '') >= @backupdatetime) and
                               REPLACE(RIGHT(REPLACE(backupfile,
                                                     RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                     ''), 16), '_', '') <= @stopat)

                end;

            if (@stopat is not null and @onlylogsafter is not null)
                begin
                    declare backupfiles cursor for
                        select backupfile
                        from @filelist
                        where backupfile like N'%.trn'
                          and backupfile like N'%' + @database + N'%'
                          and (@continuelogs = 1 or (@continuelogs = 0 and
                                                     REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') >=
                                                     @backupdatetime) and
                                                    REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') <= @stopat)
                          and ((@continuelogs = 1 and REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') <= @stopat) or
                               (@continuelogs = 0 and
                                REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') >= @backupdatetime) and
                               REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') <= @stopat)
                          and (@continuelogs = 1 or (@continuelogs = 0 and
                                                     REPLACE(LEFT(RIGHT(backupfile, 19), 15), '_', '') >= @onlylogsafter))
                        order by backupfile;

                    open backupfiles;
                end;


            if (@standbymode = 1)
                begin
                    if (@standbyundopath is null)
                        begin
                            if @execute = 'Y' or @debug = 1
                                raiserror ('The file path of the undo file for standby mode was not specified. Logs will not be restored in standby mode.', 0, 1) with nowait;
                        end;
                    else
                        set @logrecoveryoption = N'STANDBY = ''' + @standbyundopath + @database + 'Undo.ldf''';
                end;

            if (@logrecoveryoption = N'')
                begin
                    set @logrecoveryoption = N'NORECOVERY';
                end;

            -- Group Ordering based on Backup File Name excluding Index {#} to construct coma separated string in "Restore Log" Command
            select backuppath,
                   backupfile,
                   DENSE_RANK() over (order by REPLACE(RIGHT(REPLACE(backupfile,
                                                                     RIGHT(backupfile, PATINDEX('%_[0-9][0-9]%', REVERSE(backupfile))),
                                                                     ''), 16), '_', '')) as denserank
            into #splitlogbackups
            from @filelist
            where backupfile is not null;

-- Loop through all the files for the database  
            while 1 = 1
                begin

                    -- Get the TOP record to use in "Restore HeaderOnly/FileListOnly" statement
                    select top 1 @currentbackuppathlog = backuppath, @backupfile = backupfile
                    from #splitlogbackups
                    where denserank = @logrestoreranking;
                    if @@rowcount = 0 break;

                    if @i = 1
                        begin
                            set @sql = REPLACE(@headerssql, N'{Path}', @currentbackuppathlog + @backupfile);

                            if @debug = 1
                                begin
                                    if @sql is null
                                        print '@sql is NULL for REPLACE: @HeadersSQL, @CurrentBackupPathLog, @BackupFile';
                                    print @sql;
                                end;

                            execute (@sql);

                            select top 1 @logfirstlsn = CAST(firstlsn as numeric(25, 0)),
                                         @loglastlsn = CAST(lastlsn as numeric(25, 0))
                            from #headers
                            where backuptype = 2;

                            if (@continuelogs = 0 and @logfirstlsn <= @fulllastlsn and @fulllastlsn <= @loglastlsn and
                                @restorediff = 0) or (@continuelogs = 1 and @logfirstlsn <= @databaselastlsn and
                                                      @databaselastlsn < @loglastlsn and @restorediff = 0)
                                set @i = 2;

                            if (@continuelogs = 0 and @logfirstlsn <= @difflastlsn and @difflastlsn <= @loglastlsn and
                                @restorediff = 1) or (@continuelogs = 1 and @logfirstlsn <= @databaselastlsn and
                                                      @databaselastlsn < @loglastlsn and @restorediff = 1)
                                set @i = 2;

                            delete from #headers where backuptype = 2;


                        end;

                    if @i = 1
                        begin
                            if @debug = 1 raiserror ('No Log to Restore', 0, 1) with nowait;
                        end

                    if @i = 2
                        begin
                            if @execute = 'Y' raiserror ('@i set to 2, restoring logs', 0, 1) with nowait;

                            if (select COUNT(*) from #splitlogbackups where denserank = @logrestoreranking) > 1
                                begin
                                    raiserror ('Split backups found', 0, 1) with nowait;
                                    set @sql = N'RESTORE LOG ' + @restoredatabasename + N' FROM '
                                        + STUFF(
                                                       (select CHAR(10) + ',DISK=''' + backuppath + backupfile + ''''
                                                        from #splitlogbackups
                                                        where denserank = @logrestoreranking
                                                        order by backupfile
                                                        for xml path ('')),
                                                       1,
                                                       2,
                                                       '') + N' WITH ' + @logrecoveryoption + NCHAR(13) + NCHAR(10);
                                end;
                            else
                                set @sql = N'RESTORE LOG ' + @restoredatabasename + N' FROM DISK = ''' +
                                           @currentbackuppathlog + @backupfile + N''' WITH ' + @logrecoveryoption +
                                           NCHAR(13) + NCHAR(10);

                            if @debug = 1 or @execute = 'N'
                                begin
                                    if @sql is null
                                        print '@sql is NULL for RESTORE LOG: @RestoreDatabaseName, @CurrentBackupPathLog, @BackupFile';
                                    print @sql;
                                end;

                            if @debug in (0, 1) and @execute = 'Y'
                                execute master.sys.sp_executesql @stmt = @sql;
                        end;

                    set @logrestoreranking += 1;
                end;


            if @debug = 1
                begin
                    select '#SplitLogBackups' as table_name, backuppath, backupfile from #splitlogbackups;
                end
        end

-- Put database in a useable state 
    if @runrecovery = 1
        begin
            set @sql = N'RESTORE DATABASE ' + @restoredatabasename + N' WITH RECOVERY' + NCHAR(13);

            if @debug = 1 or @execute = 'N'
                begin
                    if @sql is null print '@sql is NULL for RESTORE DATABASE: @RestoreDatabaseName';
                    print @sql;
                end;

            if @debug in (0, 1) and @execute = 'Y'
                execute master.sys.sp_executesql @stmt = @sql;
        end;

-- Ensure simple recovery model
    if @forcesimplerecovery = 1
        begin
            set @sql = N'ALTER DATABASE ' + @restoredatabasename + N' SET RECOVERY SIMPLE' + NCHAR(13);

            if @debug = 1 or @execute = 'N'
                begin
                    if @sql is null print '@sql is NULL for SET RECOVERY SIMPLE: @RestoreDatabaseName';
                    print @sql;
                end;

            if @debug in (0, 1) and @execute = 'Y'
                execute master.sys.sp_executesql @stmt = @sql;
        end;

    -- Run checkdb against this database
    if @runcheckdb = 1
        begin
            set @sql = N'DBCC CHECKDB (' + @restoredatabasename + N') WITH NO_INFOMSGS, ALL_ERRORMSGS, DATA_PURITY;';

            if @debug = 1 or @execute = 'N'
                begin
                    if @sql is null print '@sql is NULL for Run Integrity Check: @RestoreDatabaseName';
                    print @sql;
                end;

            if @debug in (0, 1) and @execute = 'Y'
                execute master.sys.sp_executesql @stmt = @sql;
        end;


    if @databaseowner is not null
        begin
            if EXISTS(select * from master.dbo.syslogins where syslogins.loginname = @databaseowner)
                begin
                    set @sql = N'ALTER AUTHORIZATION ON DATABASE::' + @restoredatabasename + ' TO [' + @databaseowner +
                               ']';

                    if @debug = 1 or @execute = 'N'
                        begin
                            if @sql is null print '@sql is NULL for Set Database Owner';
                            print @sql;
                        end;

                    if @debug in (0, 1) and @execute = 'Y'
                        execute (@sql);
                end
            else
                begin
                    print @databaseowner + ' is not a valid Login. Database Owner not set.'
                end
        end;

    -- If test restore then blow the database away (be careful)
    if @testrestore = 1
        begin
            set @sql = N'DROP DATABASE ' + @restoredatabasename + NCHAR(13);

            if @debug = 1 or @execute = 'N'
                begin
                    if @sql is null print '@sql is NULL for DROP DATABASE: @RestoreDatabaseName';
                    print @sql;
                end;

            if @debug in (0, 1) and @execute = 'Y'
                execute master.sys.sp_executesql @stmt = @sql;

        end;

-- Clean-Up Tempdb Objects
    if OBJECT_ID('tempdb..#SplitFullBackups') is not null drop table #splitfullbackups;
    if OBJECT_ID('tempdb..#SplitDiffBackups') is not null drop table #splitdiffbackups;
    if OBJECT_ID('tempdb..#SplitLogBackups') is not null drop table #splitlogbackups;
go
