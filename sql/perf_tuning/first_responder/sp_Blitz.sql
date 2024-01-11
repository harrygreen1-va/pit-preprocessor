if OBJECT_ID('dbo.sp_Blitz') is null
    exec ('CREATE PROCEDURE dbo.sp_Blitz AS RETURN 0;');
go

alter procedure [dbo].[sp_Blitz] @help tinyint = 0,
                                 @checkuserdatabaseobjects tinyint = 1,
                                 @checkprocedurecache tinyint = 0,
                                 @outputtype varchar(20) = 'TABLE',
                                 @outputprocedurecache tinyint = 0,
                                 @checkprocedurecachefilter varchar(10) = null,
                                 @checkserverinfo tinyint = 0,
                                 @skipchecksserver nvarchar(256) = null,
                                 @skipchecksdatabase nvarchar(256) = null,
                                 @skipchecksschema nvarchar(256) = null,
                                 @skipcheckstable nvarchar(256) = null,
                                 @ignoreprioritiesbelow int = null,
                                 @ignoreprioritiesabove int = null,
                                 @outputservername nvarchar(256) = null,
                                 @outputdatabasename nvarchar(256) = null,
                                 @outputschemaname nvarchar(256) = null,
                                 @outputtablename nvarchar(256) = null,
                                 @outputxmlasnvarchar tinyint = 0,
                                 @emailrecipients varchar(max) = null,
                                 @emailprofile sysname = null,
                                 @summarymode tinyint = 0,
                                 @bringthepain tinyint = 0,
                                 @usualdbowner sysname = null,
                                 @skipblockingchecks tinyint = 1,
                                 @debug tinyint = 0,
                                 @version varchar(30) = null output,
                                 @versiondate datetime = null output,
                                 @versioncheckmode bit = 0
    with recompile
as
    set nocount on;
    set transaction isolation level read uncommitted;


select @version = '7.97', @versiondate = '20200712';
    set @outputtype = UPPER(@outputtype);

    if (@versioncheckmode = 1)
        begin
            return;
        end;

    if @help = 1
        print '
	/*
	sp_Blitz from http://FirstResponderKit.org

	This script checks the health of your SQL Server and gives you a prioritized
	to-do list of the most urgent things you should consider fixing.

	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
	 - If a database name has a question mark in it, some tests will fail. Gotta
	   love that unsupported sp_MSforeachdb.
	 - If you have offline databases, sp_Blitz fails the first time you run it,
	   but does work the second time. (Hoo, boy, this will be fun to debug.)
      - @OutputServerName will output QueryPlans as NVARCHAR(MAX) since Microsoft
	    has refused to support XML columns in Linked Server queries. The bug is now
		16 years old! *~ \o/ ~*

	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)

     Changes - for the full list of improvements and fixes in this version, see:
     https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/

	Parameter explanations:

	@CheckUserDatabaseObjects	1=review user databases for triggers, heaps, etc. Takes more time for more databases and objects.
	@CheckServerInfo			1=show server info like CPUs, memory, virtualization
	@CheckProcedureCache		1=top 20-50 resource-intensive cache plans and analyze them for common performance issues.
	@OutputProcedureCache		1=output the top 20-50 resource-intensive plans even if they did not trigger an alarm
	@CheckProcedureCacheFilter	''CPU'' | ''Reads'' | ''Duration'' | ''ExecCount''
	@OutputType					''TABLE''=table | ''COUNT''=row with number found | ''MARKDOWN''=bulleted list | ''SCHEMA''=version and field list | ''XML'' =table output as XML | ''NONE'' = none
	@IgnorePrioritiesBelow		50=ignore priorities below 50
	@IgnorePrioritiesAbove		50=ignore priorities above 50
	For the rest of the parameters, see https://www.BrentOzar.com/blitz/documentation for details.

    MIT License

	Copyright for portions of sp_Blitz are held by Microsoft as part of project
	tigertoolbox and are provided under the MIT license:
	https://github.com/Microsoft/tigertoolbox

	All other copyrights for sp_Blitz are held by Brent Ozar Unlimited, 2020.

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
    else
        if @outputtype = 'SCHEMA'
            begin
                select fieldlist = '[Priority] TINYINT, [FindingsGroup] VARCHAR(50), [Finding] VARCHAR(200), [DatabaseName] NVARCHAR(128), [URL] VARCHAR(200), [Details] NVARCHAR(4000), [QueryPlan] NVARCHAR(MAX), [QueryPlanFiltered] NVARCHAR(MAX), [CheckID] INT';

            end;/* IF @OutputType = 'SCHEMA' */
        else
            begin

                declare @stringtoexecute nvarchar(4000)
                    ,@curr_tracefilename nvarchar(500)
                    ,@base_tracefilename nvarchar(500)
                    ,@indx int
                    ,@query_result_separator char(1)
                    ,@emailsubject nvarchar(255)
                    ,@emailbody nvarchar(max)
                    ,@emailattachmentfilename nvarchar(255)
                    ,@productversion nvarchar(128)
                    ,@productversionmajor decimal(10, 2)
                    ,@productversionminor decimal(10, 2)
                    ,@currentname nvarchar(128)
                    ,@currentdefaultvalue nvarchar(200)
                    ,@currentcheckid int
                    ,@currentpriority int
                    ,@currentfinding varchar(200)
                    ,@currenturl varchar(200)
                    ,@currentdetails nvarchar(4000)
                    ,@mssincewaitscleared decimal(38, 0)
                    ,@cpumssincewaitscleared decimal(38, 0)
                    ,@resulttext nvarchar(max)
                    ,@crlf nvarchar(2)
                    ,@processors int
                    ,@numanodes int
                    ,@minservermemory bigint
                    ,@maxservermemory bigint
                    ,@columnstoreindexesinuse bit
                    ,@tracefileissue bit
                    -- Flag for Windows OS to help with Linux support
                    ,@iswindowsoperatingsystem bit
                    ,@daysuptime numeric(23, 2)
                    /* For First Responder Kit consistency check:*/
                    ,@spblitzfullname varchar(1024)
                    ,@blitzisoutdatedcomparedtoothers bit
                    ,@tsql nvarchar(max)
                    ,@versioncheckmodeexiststsql nvarchar(max)
                    ,@blitzprocdbname varchar(256)
                    ,@execret int
                    ,@innerexecret int
                    ,@tmpcnt int
                    ,@previouscomponentname varchar(256)
                    ,@previouscomponentfullpath varchar(1024)
                    ,@currentstatementid int
                    ,@currentcomponentschema varchar(256)
                    ,@currentcomponentname varchar(256)
                    ,@currentcomponenttype varchar(256)
                    ,@currentcomponentversiondate datetime2
                    ,@currentcomponentfullname varchar(1024)
                    ,@currentcomponentmandatory bit
                    ,@maximumversiondate datetime
                    ,@statementcheckname varchar(256)
                    ,@statementoutputscounter bit
                    ,@outputcounterexpectedvalue int
                    ,@statementoutputsexecret bit
                    ,@statementoutputsdatetime bit
                    ,@currentcomponentmandatorycheckok bit
                    ,@currentcomponentversioncheckmodeok bit
                    ,@canexitloop bit
                    ,@frkisconsistent bit
                    ,@needtoturnnumericroundabortbackon bit;

                /* End of declarations for First Responder Kit consistency check:*/
                ;

                set @crlf = NCHAR(13) + NCHAR(10);
                set @resulttext = 'sp_Blitz Results: ' + @crlf;

                /* Last startup */
                select @daysuptime = CAST(DATEDIFF(hour, create_date, GETDATE()) / 24. as numeric(23, 2))
                from sys.databases
                where database_id = 2;

                if @daysuptime = 0
                    set @daysuptime = .01;

                /*
		Set the session state of Numeric_RoundAbort to off if any databases have Numeric Round-Abort enabled.
		Stops arithmetic overflow errors during data conversion. See Github issue #2302 for more info.
		*/
                if ((8192 & @@OPTIONS) = 8192) /* Numeric RoundAbort is currently on, so we may need to turn it off temporarily */
                    begin
                        if EXISTS(select 1
                                  from sys.databases
                                  where is_numeric_roundabort_on = 1) /* A database has it turned on */
                            begin
                                set @needtoturnnumericroundabortbackon = 1;
                                set numeric_roundabort off;
                            end;
                    end;


                /*
		--TOURSTOP01--
		See https://www.BrentOzar.com/go/blitztour for a guided tour.

		We start by creating #BlitzResults. It's a temp table that will store all of
		the results from our checks. Throughout the rest of this stored procedure,
		we're running a series of checks looking for dangerous things inside the SQL
		Server. When we find a problem, we insert rows into #BlitzResults. At the
		end, we return these results to the end user.

		#BlitzResults has a CheckID field, but there's no Check table. As we do
		checks, we insert data into this table, and we manually put in the CheckID.
		For a list of checks, visit http://FirstResponderKit.org.
		*/
                if OBJECT_ID('tempdb..#BlitzResults') is not null
                    drop table #blitzresults;
                create table #blitzresults
                (
                    id int identity (1, 1),
                    checkid int,
                    databasename nvarchar(128),
                    priority tinyint,
                    findingsgroup varchar(50),
                    finding varchar(200),
                    url varchar(200),
                    details nvarchar(4000),
                    queryplan [XML] null,
                    queryplanfiltered [NVARCHAR](max) null
                );

                if OBJECT_ID('tempdb..#TemporaryDatabaseResults') is not null
                    drop table #temporarydatabaseresults;
                create table #temporarydatabaseresults
                (
                    databasename nvarchar(128),
                    finding nvarchar(128)
                );

                /* First Responder Kit consistency (temporary tables) */

                if (OBJECT_ID('tempdb..#FRKObjects') is not null)
                    begin
                        exec sp_executesql N'DROP TABLE #FRKObjects;';
                    end;

                -- this one represents FRK objects
                create table #frkobjects
                (
                    databasename varchar(256) not null,
                    objectschemaname varchar(256) null,
                    objectname varchar(256) not null,
                    objecttype varchar(256) not null,
                    mandatorycomponent bit not null
                );


                if (OBJECT_ID('tempdb..#StatementsToRun4FRKVersionCheck') is not null)
                    begin
                        exec sp_executesql N'DROP TABLE #StatementsToRun4FRKVersionCheck;';
                    end;


                -- This one will contain the statements to be executed
                -- order: 1- Mandatory, 2- VersionCheckMode, 3- VersionCheck

                create table #statementstorun4frkversioncheck
                (
                    statementid int identity (1,1),
                    checkname varchar(256),
                    subjectname varchar(256),
                    subjectfullpath varchar(1024),
                    statementtext nvarchar(max),
                    statementoutputscounter bit,
                    outputcounterexpectedvalue int,
                    statementoutputsexecret bit,
                    statementoutputsdatetime bit
                );

                /* End of First Responder Kit consistency (temporary tables) */


                /*
		You can build your own table with a list of checks to skip. For example, you
		might have some databases that you don't care about, or some checks you don't
		want to run. Then, when you run sp_Blitz, you can specify these parameters:
		@SkipChecksDatabase = 'DBAtools',
		@SkipChecksSchema = 'dbo',
		@SkipChecksTable = 'BlitzChecksToSkip'
		Pass in the database, schema, and table that contains the list of checks you
		want to skip. This part of the code checks those parameters, gets the list,
		and then saves those in a temp table. As we run each check, we'll see if we
		need to skip it.

		Really anal-retentive users will note that the @SkipChecksServer parameter is
		not used. YET. We added that parameter in so that we could avoid changing the
		stored proc's surface area (interface) later.
		*/
                /* --TOURSTOP07-- */
                if OBJECT_ID('tempdb..#SkipChecks') is not null
                    drop table #skipchecks;
                create table #skipchecks
                (
                    databasename nvarchar(128),
                    checkid int,
                    servername nvarchar(128)
                );
                create clustered index ix_checkid_databasename on #skipchecks (checkid, databasename);

                if (OBJECT_ID('tempdb..#InvalidLogins') is not null)
                    begin
                        exec sp_executesql N'DROP TABLE #InvalidLogins;';
                    end;

                create table #invalidlogins
                (
                    loginsid varbinary(85),
                    loginname varchar(256)
                );

                if @skipcheckstable is not null
                    and @skipchecksschema is not null
                    and @skipchecksdatabase is not null
                    begin

                        if @debug in (1, 2) raiserror ('Inserting SkipChecks', 0, 1) with nowait;

                        set @stringtoexecute = 'INSERT INTO #SkipChecks(DatabaseName, CheckID, ServerName )
				SELECT DISTINCT DatabaseName, CheckID, ServerName
				FROM ' + QUOTENAME(@skipchecksdatabase) + '.' + QUOTENAME(@skipchecksschema) + '.' +
                                               QUOTENAME(@skipcheckstable)
                            +
                                               ' WHERE ServerName IS NULL OR ServerName = SERVERPROPERTY(''ServerName'') OPTION (RECOMPILE);';
                        exec (@stringtoexecute);
                    end;

                -- Flag for Windows OS to help with Linux support
                if EXISTS(select 1
                          from sys.all_objects
                          where name = 'dm_os_host_info')
                    begin
                        select @iswindowsoperatingsystem = case when host_platform = 'Windows' then 1 else 0 end
                        from sys.dm_os_host_info;
                    end;
                else
                    begin
                        select @iswindowsoperatingsystem = 1 ;
                    end;

                if not EXISTS(select 1
                              from #skipchecks
                              where databasename is null
                                and checkid = 106)
                    and
                   (select convert(int, value_in_use) from sys.configurations where name = 'default trace enabled') = 1
                    begin

                        select @curr_tracefilename = [path] from sys.traces where is_default = 1;
                        set @curr_tracefilename = reverse(@curr_tracefilename);

                        -- Set the trace file path separator based on underlying OS
                        if (@iswindowsoperatingsystem = 1) and @curr_tracefilename is not null
                            begin
                                select @indx = patindex('%\%', @curr_tracefilename);
                                set @curr_tracefilename = reverse(@curr_tracefilename);
                                set @base_tracefilename = left(@curr_tracefilename, len(@curr_tracefilename) - @indx) +
                                                          '\log.trc';
                            end;
                        else
                            begin
                                select @indx = patindex('%/%', @curr_tracefilename);
                                set @curr_tracefilename = reverse(@curr_tracefilename);
                                set @base_tracefilename = left(@curr_tracefilename, len(@curr_tracefilename) - @indx) +
                                                          '/log.trc';
                            end;

                    end;

                /* If the server has any databases on Antiques Roadshow, skip the checks that would break due to CTEs. */
                if @checkuserdatabaseobjects = 1 and EXISTS(select * from sys.databases where compatibility_level < 90)
                    begin
                        set @checkuserdatabaseobjects = 0;
                        print 'Databases with compatibility level < 90 found, so setting @CheckUserDatabaseObjects = 0.';
                        print 'The database-level checks rely on CTEs, which are not supported in SQL 2000 compat level databases.';
                        print 'Get with the cool kids and switch to a current compatibility level, Grandpa. To find the problems, run:';
                        print 'SELECT * FROM sys.databases WHERE compatibility_level < 90;';
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        select 204                                                                                                                                                                             as checkid,
                               0                                                                                                                                                                               as priority,
                               'Informational'                                                                                                                                                                 as findingsgroup,
                               '@CheckUserDatabaseObjects Disabled'                                                                                                                                            as finding,
                               'https://www.BrentOzar.com/blitz/'                                                                                                                                              as url,
                               'Since you have databases with compatibility_level < 90, we can''t run @CheckUserDatabaseObjects = 1. To find them: SELECT * FROM sys.databases WHERE compatibility_level < 90' as details;
                    end;

                /* --TOURSTOP08-- */
                /* If the server is Amazon RDS, skip checks that it doesn't allow */
                if LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') as varchar(8000)), 8) = 'EC2AMAZ-'
                    and LEFT(CAST(SERVERPROPERTY('MachineName') as varchar(8000)), 8) = 'EC2AMAZ-'
                    and LEFT(CAST(SERVERPROPERTY('ServerName') as varchar(8000)), 8) = 'EC2AMAZ-'
                    and db_id('rdsadmin') is not null
                    and EXISTS(select *
                               from master.sys.all_objects
                               where name in ('rds_startup_tasks', 'rds_help_revlogin', 'rds_hexadecimal',
                                              'rds_failover_tracking', 'rds_database_tracking', 'rds_track_change'))
                    begin
                        insert into #skipchecks (checkid) values (6);
                        insert into #skipchecks (checkid) values (29);
                        insert into #skipchecks (checkid) values (30);
                        insert into #skipchecks (checkid) values (31);
                        insert into #skipchecks (checkid) values (40); /* TempDB only has one data file */
                        insert into #skipchecks (checkid) values (57);
                        insert into #skipchecks (checkid) values (59);
                        insert into #skipchecks (checkid) values (61);
                        insert into #skipchecks (checkid) values (62);
                        insert into #skipchecks (checkid) values (68);
                        insert into #skipchecks (checkid) values (69);
                        insert into #skipchecks (checkid) values (73);
                        insert into #skipchecks (checkid) values (79);
                        insert into #skipchecks (checkid) values (92);
                        insert into #skipchecks (checkid) values (94);
                        insert into #skipchecks (checkid) values (96);
                        insert into #skipchecks (checkid) values (98);
                        insert into #skipchecks (checkid) values (100); /* Remote DAC disabled */
                        insert into #skipchecks (checkid) values (123);
                        insert into #skipchecks (checkid) values (177);
                        insert into #skipchecks (checkid) values (180); /* 180/181 are maintenance plans */
                        insert into #skipchecks (checkid) values (181);
                        insert into #skipchecks (checkid) values (184); /* xp_readerrorlog checking for IFI */
                        insert into #skipchecks (checkid) values (211); /* xp_regread checking for power saving */
                        insert into #skipchecks (checkid) values (212); /* xp_regread */
                        insert into #skipchecks (checkid) values (219);
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        select 223                                                                                                             as checkid,
                               0                                                                                                               as priority,
                               'Informational'                                                                                                 as findingsgroup,
                               'Some Checks Skipped'                                                                                           as finding,
                               'https://aws.amazon.com/rds/sqlserver/'                                                                         as url,
                               'Amazon RDS detected, so we skipped some checks that are not currently possible, relevant, or practical there.' as details;
                    end;
                /* Amazon RDS skipped checks */

                /* If the server is ExpressEdition, skip checks that it doesn't allow */
                if CAST(SERVERPROPERTY('Edition') as nvarchar(1000)) like N'%Express%'
                    begin
                        insert into #skipchecks (checkid) values (30); /* Alerts not configured */
                        insert into #skipchecks (checkid) values (31); /* Operators not configured */
                        insert into #skipchecks (checkid) values (61); /* Agent alerts 19-25 */
                        insert into #skipchecks (checkid) values (73); /* Failsafe operator */
                        insert into #skipchecks (checkid) values (96); /* Agent alerts for corruption */
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        select 223                                                                                                                  as checkid,
                               0                                                                                                                    as priority,
                               'Informational'                                                                                                      as findingsgroup,
                               'Some Checks Skipped'                                                                                                as finding,
                               'https://stackoverflow.com/questions/1169634/limitations-of-sql-server-express'                                      as url,
                               'Express Edition detected, so we skipped some checks that are not currently possible, relevant, or practical there.' as details;
                    end;
                /* Express Edition skipped checks */

                /* If the server is an Azure Managed Instance, skip checks that it doesn't allow */
                if SERVERPROPERTY('EngineEdition') = 8
                    begin
                        insert into #skipchecks (checkid) values (1); /* Full backups - because of the MI GUID name bug mentioned here: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1481 */
                        insert into #skipchecks (checkid) values (2); /* Log backups - because of the MI GUID name bug mentioned here: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1481 */
                        insert into #skipchecks (checkid) values (6); /* Security - Jobs Owned By Users per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */
                        insert into #skipchecks (checkid) values (21); /* Informational - Database Encrypted per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */
                        insert into #skipchecks (checkid) values (24); /* File Configuration - System Database on C Drive per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */
                        insert into #skipchecks (checkid) values (50); /* Max Server Memory Set Too High - because they max it out */
                        insert into #skipchecks (checkid) values (55); /* Security - Database Owner <> sa per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */
                        insert into #skipchecks (checkid) values (74); /* TraceFlag On - because Azure Managed Instances go wild and crazy with the trace flags */
                        insert into #skipchecks (checkid) values (97); /* Unusual SQL Server Edition */
                        insert into #skipchecks (checkid) values (100); /* Remote DAC disabled - but it's working anyway, details here: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1481 */
                        insert into #skipchecks (checkid) values (186); /* MSDB Backup History Purged Too Frequently */
                        insert into #skipchecks (checkid) values (199); /* Default trace, details here: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1481 */
                        insert into #skipchecks (checkid) values (211); /*Power Plan */
                        insert into #skipchecks (checkid, databasename) values (80, 'master'); /* Max file size set */
                        insert into #skipchecks (checkid, databasename) values (80, 'model'); /* Max file size set */
                        insert into #skipchecks (checkid, databasename) values (80, 'msdb'); /* Max file size set */
                        insert into #skipchecks (checkid, databasename) values (80, 'tempdb'); /* Max file size set */
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        select 223                                                                                                                   as checkid,
                               0                                                                                                                     as priority,
                               'Informational'                                                                                                       as findingsgroup,
                               'Some Checks Skipped'                                                                                                 as finding,
                               'https://docs.microsoft.com/en-us/azure/sql-database/sql-database-managed-instance-index'                             as url,
                               'Managed Instance detected, so we skipped some checks that are not currently possible, relevant, or practical there.' as details;
                    end;
                /* Azure Managed Instance skipped checks */

                /*
		That's the end of the SkipChecks stuff.
		The next several tables are used by various checks later.
		*/
                if OBJECT_ID('tempdb..#ConfigurationDefaults') is not null
                    drop table #configurationdefaults;
                create table #configurationdefaults
                (
                    name nvarchar(128),
                    defaultvalue bigint,
                    checkid int
                );

                if OBJECT_ID('tempdb..#Recompile') is not null
                    drop table #recompile;
                create table #recompile
                (
                    dbname varchar(200),
                    procname varchar(300),
                    recompileflag varchar(1),
                    spschema varchar(50)
                );

                if OBJECT_ID('tempdb..#DatabaseDefaults') is not null
                    drop table #databasedefaults;
                create table #databasedefaults
                (
                    name nvarchar(128),
                    defaultvalue nvarchar(200),
                    checkid int,
                    priority int,
                    finding varchar(200),
                    url varchar(200),
                    details nvarchar(4000)
                );

                if OBJECT_ID('tempdb..#DatabaseScopedConfigurationDefaults') is not null
                    drop table #databasescopedconfigurationdefaults;
                create table #databasescopedconfigurationdefaults
                (
                    id int identity (1,1),
                    configuration_id int,
                    [name] nvarchar(60),
                    default_value sql_variant,
                    default_value_for_secondary sql_variant,
                    checkid int,
                );

                if OBJECT_ID('tempdb..#DBCCs') is not null
                    drop table #dbccs;
                create table #dbccs
                (
                    id int identity (1, 1)
                        primary key,
                    parentobject varchar(255),
                    object varchar(255),
                    field varchar(255),
                    value varchar(255),
                    dbname nvarchar(128) null
                );

                if OBJECT_ID('tempdb..#LogInfo2012') is not null
                    drop table #loginfo2012;
                create table #loginfo2012
                (
                    recoveryunitid int,
                    fileid smallint,
                    filesize bigint,
                    startoffset bigint,
                    fseqno bigint,
                    [Status] tinyint,
                    parity tinyint,
                    createlsn numeric(38)
                );

                if OBJECT_ID('tempdb..#LogInfo') is not null
                    drop table #loginfo;
                create table #loginfo
                (
                    fileid smallint,
                    filesize bigint,
                    startoffset bigint,
                    fseqno bigint,
                    [Status] tinyint,
                    parity tinyint,
                    createlsn numeric(38)
                );

                if OBJECT_ID('tempdb..#partdb') is not null
                    drop table #partdb;
                create table #partdb
                (
                    dbname nvarchar(128),
                    objectname nvarchar(200),
                    type_desc nvarchar(128)
                );

                if OBJECT_ID('tempdb..#TraceStatus') is not null
                    drop table #tracestatus;
                create table #tracestatus
                (
                    traceflag varchar(10),
                    status bit,
                    global bit,
                    session bit
                );

                if OBJECT_ID('tempdb..#driveInfo') is not null
                    drop table #driveinfo;
                create table #driveinfo
                (
                    drive nvarchar,
                    size decimal(18, 2)
                );

                if OBJECT_ID('tempdb..#dm_exec_query_stats') is not null
                    drop table #dm_exec_query_stats;
                create table #dm_exec_query_stats
                (
                    [id] [int] not null
                        identity (1, 1),
                    [sql_handle] [varbinary](64) not null,
                    [statement_start_offset] [int] not null,
                    [statement_end_offset] [int] not null,
                    [plan_generation_num] [bigint] not null,
                    [plan_handle] [varbinary](64) not null,
                    [creation_time] [datetime] not null,
                    [last_execution_time] [datetime] not null,
                    [execution_count] [bigint] not null,
                    [total_worker_time] [bigint] not null,
                    [last_worker_time] [bigint] not null,
                    [min_worker_time] [bigint] not null,
                    [max_worker_time] [bigint] not null,
                    [total_physical_reads] [bigint] not null,
                    [last_physical_reads] [bigint] not null,
                    [min_physical_reads] [bigint] not null,
                    [max_physical_reads] [bigint] not null,
                    [total_logical_writes] [bigint] not null,
                    [last_logical_writes] [bigint] not null,
                    [min_logical_writes] [bigint] not null,
                    [max_logical_writes] [bigint] not null,
                    [total_logical_reads] [bigint] not null,
                    [last_logical_reads] [bigint] not null,
                    [min_logical_reads] [bigint] not null,
                    [max_logical_reads] [bigint] not null,
                    [total_clr_time] [bigint] not null,
                    [last_clr_time] [bigint] not null,
                    [min_clr_time] [bigint] not null,
                    [max_clr_time] [bigint] not null,
                    [total_elapsed_time] [bigint] not null,
                    [last_elapsed_time] [bigint] not null,
                    [min_elapsed_time] [bigint] not null,
                    [max_elapsed_time] [bigint] not null,
                    [query_hash] [binary](8) null,
                    [query_plan_hash] [binary](8) null,
                    [query_plan] [xml] null,
                    [query_plan_filtered] [nvarchar](max) null,
                    [text] [nvarchar](max) collate sql_latin1_general_cp1_ci_as
                        null,
                    [text_filtered] [nvarchar](max) collate sql_latin1_general_cp1_ci_as
                        null
                );

                if OBJECT_ID('tempdb..#ErrorLog') is not null
                    drop table #errorlog;
                create table #errorlog
                (
                    logdate datetime,
                    processinfo nvarchar(20),
                    [Text] nvarchar(1000)
                );

                if OBJECT_ID('tempdb..#fnTraceGettable') is not null
                    drop table #fntracegettable;
                create table #fntracegettable
                (
                    textdata nvarchar(4000),
                    databasename nvarchar(256),
                    eventclass int,
                    severity int,
                    starttime datetime,
                    endtime datetime,
                    duration bigint,
                    ntusername nvarchar(256),
                    ntdomainname nvarchar(256),
                    hostname nvarchar(256),
                    applicationname nvarchar(256),
                    loginname nvarchar(256),
                    dbusername nvarchar(256)
                );

                if OBJECT_ID('tempdb..#Instances') is not null
                    drop table #instances;
                create table #instances
                (
                    instance_number nvarchar(max),
                    instance_name nvarchar(max),
                    data_field nvarchar(max)
                );

                if OBJECT_ID('tempdb..#IgnorableWaits') is not null
                    drop table #ignorablewaits;
                create table #ignorablewaits
                (
                    wait_type nvarchar(60)
                );
                insert into #ignorablewaits values ('BROKER_EVENTHANDLER');
                insert into #ignorablewaits values ('BROKER_RECEIVE_WAITFOR');
                insert into #ignorablewaits values ('BROKER_TASK_STOP');
                insert into #ignorablewaits values ('BROKER_TO_FLUSH');
                insert into #ignorablewaits values ('BROKER_TRANSMITTER');
                insert into #ignorablewaits values ('CHECKPOINT_QUEUE');
                insert into #ignorablewaits values ('CLR_AUTO_EVENT');
                insert into #ignorablewaits values ('CLR_MANUAL_EVENT');
                insert into #ignorablewaits values ('CLR_SEMAPHORE');
                insert into #ignorablewaits values ('DBMIRROR_DBM_EVENT');
                insert into #ignorablewaits values ('DBMIRROR_DBM_MUTEX');
                insert into #ignorablewaits values ('DBMIRROR_EVENTS_QUEUE');
                insert into #ignorablewaits values ('DBMIRROR_WORKER_QUEUE');
                insert into #ignorablewaits values ('DBMIRRORING_CMD');
                insert into #ignorablewaits values ('DIRTY_PAGE_POLL');
                insert into #ignorablewaits values ('DISPATCHER_QUEUE_SEMAPHORE');
                insert into #ignorablewaits values ('FT_IFTS_SCHEDULER_IDLE_WAIT');
                insert into #ignorablewaits values ('FT_IFTSHC_MUTEX');
                insert into #ignorablewaits values ('HADR_CLUSAPI_CALL');
                insert into #ignorablewaits values ('HADR_FABRIC_CALLBACK');
                insert into #ignorablewaits values ('HADR_FILESTREAM_IOMGR_IOCOMPLETION');
                insert into #ignorablewaits values ('HADR_LOGCAPTURE_WAIT');
                insert into #ignorablewaits values ('HADR_NOTIFICATION_DEQUEUE');
                insert into #ignorablewaits values ('HADR_TIMER_TASK');
                insert into #ignorablewaits values ('HADR_WORK_QUEUE');
                insert into #ignorablewaits values ('LAZYWRITER_SLEEP');
                insert into #ignorablewaits values ('LOGMGR_QUEUE');
                insert into #ignorablewaits values ('ONDEMAND_TASK_QUEUE');
                insert into #ignorablewaits values ('PARALLEL_REDO_DRAIN_WORKER');
                insert into #ignorablewaits values ('PARALLEL_REDO_LOG_CACHE');
                insert into #ignorablewaits values ('PARALLEL_REDO_TRAN_LIST');
                insert into #ignorablewaits values ('PARALLEL_REDO_WORKER_SYNC');
                insert into #ignorablewaits values ('PARALLEL_REDO_WORKER_WAIT_WORK');
                insert into #ignorablewaits values ('PREEMPTIVE_HADR_LEASE_MECHANISM');
                insert into #ignorablewaits values ('PREEMPTIVE_SP_SERVER_DIAGNOSTICS');
                insert into #ignorablewaits values ('QDS_ASYNC_QUEUE');
                insert into #ignorablewaits values ('QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP');
                insert into #ignorablewaits values ('QDS_PERSIST_TASK_MAIN_LOOP_SLEEP');
                insert into #ignorablewaits values ('QDS_SHUTDOWN_QUEUE');
                insert into #ignorablewaits values ('REDO_THREAD_PENDING_WORK');
                insert into #ignorablewaits values ('REQUEST_FOR_DEADLOCK_SEARCH');
                insert into #ignorablewaits values ('SLEEP_SYSTEMTASK');
                insert into #ignorablewaits values ('SLEEP_TASK');
                insert into #ignorablewaits values ('SOS_WORK_DISPATCHER');
                insert into #ignorablewaits values ('SP_SERVER_DIAGNOSTICS_SLEEP');
                insert into #ignorablewaits values ('SQLTRACE_BUFFER_FLUSH');
                insert into #ignorablewaits values ('SQLTRACE_INCREMENTAL_FLUSH_SLEEP');
                insert into #ignorablewaits values ('UCS_SESSION_REGISTRATION');
                insert into #ignorablewaits values ('WAIT_XTP_OFFLINE_CKPT_NEW_LOG');
                insert into #ignorablewaits values ('WAITFOR');
                insert into #ignorablewaits values ('XE_DISPATCHER_WAIT');
                insert into #ignorablewaits values ('XE_LIVE_TARGET_TVF');
                insert into #ignorablewaits values ('XE_TIMER_EVENT');

                if @debug in (1, 2) raiserror ('Setting @MsSinceWaitsCleared', 0, 1) with nowait;

                select @mssincewaitscleared = DATEDIFF(minute, create_date, CURRENT_TIMESTAMP) * 60000.0
                from sys.databases
                where name = 'tempdb';

                /* Have they cleared wait stats? Using a 10% fudge factor */
                if @mssincewaitscleared * .9 > (select MAX(wait_time_ms)
                                                from sys.dm_os_wait_stats
                                                where wait_type in ('SP_SERVER_DIAGNOSTICS_SLEEP',
                                                                    'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
                                                                    'REQUEST_FOR_DEADLOCK_SEARCH',
                                                                    'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                                                                    'LAZYWRITER_SLEEP',
                                                                    'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                                                                    'DIRTY_PAGE_POLL', 'LOGMGR_QUEUE'))
                    begin

                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 185) with nowait;

                        set @mssincewaitscleared = (select MAX(wait_time_ms)
                                                    from sys.dm_os_wait_stats
                                                    where wait_type in ('SP_SERVER_DIAGNOSTICS_SLEEP',
                                                                        'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',
                                                                        'REQUEST_FOR_DEADLOCK_SEARCH',
                                                                        'HADR_FILESTREAM_IOMGR_IOCOMPLETION',
                                                                        'LAZYWRITER_SLEEP',
                                                                        'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',
                                                                        'DIRTY_PAGE_POLL', 'LOGMGR_QUEUE'));
                        if @mssincewaitscleared = 0 set @mssincewaitscleared = 1;
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        values (185,
                                240,
                                'Wait Stats',
                                'Wait Stats Have Been Cleared',
                                'https://BrentOzar.com/go/waits',
                                'Someone ran DBCC SQLPERF to clear sys.dm_os_wait_stats at approximately: '
                                    + CONVERT(nvarchar(100),
                                        DATEADD(minute, (-1. * (@mssincewaitscleared) / 1000. / 60.), GETDATE()), 120));
                    end;

                /* @CpuMsSinceWaitsCleared is used for waits stats calculations */

                if @debug in (1, 2) raiserror ('Setting @CpuMsSinceWaitsCleared', 0, 1) with nowait;

                select @cpumssincewaitscleared = @mssincewaitscleared * scheduler_count
                from sys.dm_os_sys_info;

                /* If we're outputting CSV or Markdown, don't bother checking the plan cache because we cannot export plans. */
                if @outputtype = 'CSV' or @outputtype = 'MARKDOWN'
                    set @checkprocedurecache = 0;

                /* If we're posting a question on Stack, include background info on the server */
                if @outputtype = 'MARKDOWN'
                    set @checkserverinfo = 1;

                /* Only run CheckUserDatabaseObjects if there are less than 50 databases. */
                if @bringthepain = 0 and 50 <= (select COUNT(*) from sys.databases) and @checkuserdatabaseobjects = 1
                    begin
                        set @checkuserdatabaseobjects = 0;
                        print 'Running sp_Blitz @CheckUserDatabaseObjects = 1 on a server with 50+ databases may cause temporary insanity for the server and/or user.';
                        print 'If you''re sure you want to do this, run again with the parameter @BringThePain = 1.';
                        insert into #blitzresults
                        (checkid,
                         priority,
                         findingsgroup,
                         finding,
                         url,
                         details)
                        select 201                                                                           as checkid,
                               0                                                                             as priority,
                               'Informational'                                                               as findingsgroup,
                               '@CheckUserDatabaseObjects Disabled'                                          as finding,
                               'https://www.BrentOzar.com/blitz/'                                            as url,
                               'If you want to check 50+ databases, you have to also use @BringThePain = 1.' as details;
                    end;

                /* Sanitize our inputs */
                select @outputservername = QUOTENAME(@outputservername),
                       @outputdatabasename = QUOTENAME(@outputdatabasename),
                       @outputschemaname = QUOTENAME(@outputschemaname),
                       @outputtablename = QUOTENAME(@outputtablename);

                /* Get the major and minor build numbers */

                if @debug in (1, 2) raiserror ('Getting version information.', 0, 1) with nowait;

                set @productversion = CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));
                select @productversionmajor = SUBSTRING(@productversion, 1, CHARINDEX('.', @productversion) + 1),
                       @productversionminor = PARSENAME(CONVERT(varchar(32), @productversion), 2);

                /*
		Whew! we're finally done with the setup, and we can start doing checks.
		First, let's make sure we're actually supposed to do checks on this server.
		The user could have passed in a SkipChecks table that specified to skip ALL
		checks on this server, so let's check for that:
		*/
                if ((SERVERPROPERTY('ServerName') not in (select servername
                                                          from #skipchecks
                                                          where databasename is null
                                                            and checkid is null))
                    or (@skipcheckstable is null)
                    )
                    begin

                        /*
				Our very first check! We'll put more comments in this one just to
				explain exactly how it works. First, we check to see if we're
				supposed to skip CheckID 1 (that's the check we're working on.)
				*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 1)
                            begin

                                /*
						Below, we check master.sys.databases looking for databases
						that haven't had a backup in the last week. If we find any,
						we insert them into #BlitzResults, the temp table that
						tracks our server's problems. Note that if the check does
						NOT find any problems, we don't save that. We're only
						saving the problems, not the successful checks.
						*/

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 1) with nowait;

                                if SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances need a special query */
                                    begin
                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 1                                                                 as checkid,
                                               d.[name]                                                          as databasename,
                                               1                                                                 as priority,
                                               'Backup'                                                          as findingsgroup,
                                               'Backups Not Performed Recently'                                  as finding,
                                               'https://BrentOzar.com/go/nobak'                                  as url,
                                               'Last backed up: '
                                                   + COALESCE(CAST(MAX(b.backup_finish_date) as varchar(25)),
                                                              'never')                                           as details
                                        from master.sys.databases d
                                                 left outer join msdb.dbo.backupset b
                                                                 on d.name collate sql_latin1_general_cp1_ci_as =
                                                                    b.database_name collate sql_latin1_general_cp1_ci_as
                                                                     and b.type = 'D'
                                                                     and
                                                                    b.server_name = SERVERPROPERTY('ServerName') /*Backupset ran on current server  */
                                        where d.database_id <> 2 /* Bonus points if you know what that means */
                                          and d.state not in (1, 6, 10) /* Not currently offline or restoring, like log shipping databases */
                                          and d.is_in_standby = 0 /* Not a log shipping target database */
                                          and d.source_database_id is null /* Excludes database snapshots */
                                          and d.name not in (select distinct databasename
                                                             from #skipchecks
                                                             where checkid is null
                                                                or checkid = 1)
                                            /*
										    The above NOT IN filters out the databases we're not supposed to check.
										    */
                                        group by d.name
                                        having MAX(b.backup_finish_date) <= DATEADD(dd,
                                                                                    -7, GETDATE())
                                            or MAX(b.backup_finish_date) is null;
                                    end;

                                else /* SERVERPROPERTY('EngineName') must be 8, Azure Managed Instances */
                                    begin
                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 1                                                                 as checkid,
                                               d.[name]                                                          as databasename,
                                               1                                                                 as priority,
                                               'Backup'                                                          as findingsgroup,
                                               'Backups Not Performed Recently'                                  as finding,
                                               'https://BrentOzar.com/go/nobak'                                  as url,
                                               'Last backed up: '
                                                   + COALESCE(CAST(MAX(b.backup_finish_date) as varchar(25)),
                                                              'never')                                           as details
                                        from master.sys.databases d
                                                 left outer join msdb.dbo.backupset b
                                                                 on d.name collate sql_latin1_general_cp1_ci_as =
                                                                    b.database_name collate sql_latin1_general_cp1_ci_as
                                                                     and b.type = 'D'
                                        where d.database_id <> 2 /* Bonus points if you know what that means */
                                          and d.state not in (1, 6, 10) /* Not currently offline or restoring, like log shipping databases */
                                          and d.is_in_standby = 0 /* Not a log shipping target database */
                                          and d.source_database_id is null /* Excludes database snapshots */
                                          and d.name not in (select distinct databasename
                                                             from #skipchecks
                                                             where checkid is null
                                                                or checkid = 1)
                                            /*
										    The above NOT IN filters out the databases we're not supposed to check.
										    */
                                        group by d.name
                                        having MAX(b.backup_finish_date) <= DATEADD(dd,
                                                                                    -7, GETDATE())
                                            or MAX(b.backup_finish_date) is null;
                                    end;


                                /*
						And there you have it. The rest of this stored procedure works the same
						way: it asks:
						- Should I skip this check?
						- If not, do I find problems?
						- Insert the results into #BlitzResults
						*/

                            end;

                        /*
				And that's the end of CheckID #1.

				CheckID #2 is a little simpler because it only involves one query, and it's
				more typical for queries that people contribute. But keep reading, because
				the next check gets more complex again.
				*/

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 2)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 2) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 2                                                        as checkid,
                                                d.name                                                   as databasename,
                                                1                                                        as priority,
                                                'Backup'                                                 as findingsgroup,
                                                'Full Recovery Model w/o Log Backups'                    as finding,
                                                'https://BrentOzar.com/go/biglogs'                       as url,
                                                ('The ' + CAST(CAST((select ((SUM([mf].[size]) * 8.) / 1024.)
                                                                     from sys.[master_files] as [mf]
                                                                     where [mf].[database_id] = d.[database_id]
                                                                       and [mf].[type_desc] = 'LOG') as decimal(18, 2)) as varchar(30)) +
                                                 'MB log file has not been backed up in the last week.') as details
                                from master.sys.databases d
                                where d.recovery_model in (1, 2)
                                  and d.database_id not in (2, 3)
                                  and d.source_database_id is null
                                  and d.state not in (1, 6, 10) /* Not currently offline or restoring, like log shipping databases */
                                  and d.is_in_standby = 0 /* Not a log shipping target database */
                                  and d.source_database_id is null /* Excludes database snapshots */
                                  and d.name not in (select distinct databasename
                                                     from #skipchecks
                                                     where checkid is null
                                                        or checkid = 2)
                                  and not EXISTS(select *
                                                 from msdb.dbo.backupset b
                                                 where d.name collate sql_latin1_general_cp1_ci_as =
                                                       b.database_name collate sql_latin1_general_cp1_ci_as
                                                   and b.type = 'L'
                                                   and b.backup_finish_date >= DATEADD(dd,
                                                                                       -7, GETDATE()));
                            end;

                        /*
				Next up, we've got CheckID 8. (These don't have to go in order.) This one
				won't work on SQL Server 2005 because it relies on a new DMV that didn't
				exist prior to SQL Server 2008. This means we have to check the SQL Server
				version first, then build a dynamic string with the query we want to run:
				*/

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 8)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 8) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
							(CheckID, Priority,
							FindingsGroup,
							Finding, URL,
							Details)
					  SELECT 8 AS CheckID,
					  230 AS Priority,
					  ''Security'' AS FindingsGroup,
					  ''Server Audits Running'' AS Finding,
					  ''https://BrentOzar.com/go/audits'' AS URL,
					  (''SQL Server built-in audit functionality is being used by server audit: '' + [name]) AS Details FROM sys.dm_server_audit_status  OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        /*
				But what if you need to run a query in every individual database?
				Hop down to the @CheckUserDatabaseObjects section.

				And that's the basic idea! You can read through the rest of the
				checks if you like - some more exciting stuff happens closer to the
				end of the stored proc, where we start doing things like checking
				the plan cache, but those aren't as cleanly commented.

				If you'd like to contribute your own check, use one of the check
				formats shown above and email it to Help@BrentOzar.com. You don't
				have to pick a CheckID or a link - we'll take care of that when we
				test and publish the code. Thanks!
				*/

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 93)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 93) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 93                                                as                                                          checkid,
                                       1                                                 as                                                          priority,
                                       'Backup'                                          as                                                          findingsgroup,
                                       'Backing Up to Same Drive Where Databases Reside' as                                                          finding,
                                       'https://BrentOzar.com/go/backup'                 as                                                          url,
                                       CAST(COUNT(1) as varchar(50)) + ' backups done on drive '
                                           + UPPER(LEFT(bmf.physical_device_name, 3))
                                           +
                                       ' in the last two weeks, where database files also live. This represents a serious risk if that array fails.' details
                                from msdb.dbo.backupmediafamily as bmf
                                         inner join msdb.dbo.backupset as bs on bmf.media_set_id = bs.media_set_id
                                    and bs.backup_start_date >= (DATEADD(dd,
                                                                         -14, GETDATE()))
                                    /* Filter out databases that were recently restored: */
                                         left outer join msdb.dbo.restorehistory rh
                                                         on bs.database_name = rh.destination_database_name and
                                                            rh.restore_date > DATEADD(dd, -14, GETDATE())
                                where UPPER(LEFT(bmf.physical_device_name, 3)) <> 'HTT'
                                  and bmf.physical_device_name not like '\\%'
                                  and -- GitHub Issue #2141
                                    @iswindowsoperatingsystem = 1
                                  and -- GitHub Issue #1995
                                        UPPER(LEFT(bmf.physical_device_name collate sql_latin1_general_cp1_ci_as, 3)) in
                                        (
                                            select distinct UPPER(
                                                                    LEFT(mf.physical_name collate sql_latin1_general_cp1_ci_as, 3))
                                            from sys.master_files as mf
                                            where mf.database_id <> 2)
                                  and rh.destination_database_name is null
                                group by UPPER(LEFT(bmf.physical_device_name, 3));
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 119)
                            and EXISTS(select *
                                       from sys.all_objects o
                                       where o.name = 'dm_database_encryption_keys')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 119) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, DatabaseName, URL, Details)
								SELECT 119 AS CheckID,
								1 AS Priority,
								''Backup'' AS FindingsGroup,
								''TDE Certificate Not Backed Up Recently'' AS Finding,
								db_name(dek.database_id) AS DatabaseName,
								''https://BrentOzar.com/go/tde'' AS URL,
								''The certificate '' + c.name + '' is used to encrypt database '' + db_name(dek.database_id) + ''. Last backup date: '' + COALESCE(CAST(c.pvt_key_last_backup_date AS VARCHAR(100)), ''Never'') AS Details
								FROM sys.certificates c INNER JOIN sys.dm_database_encryption_keys dek ON c.thumbprint = dek.encryptor_thumbprint
								WHERE pvt_key_last_backup_date IS NULL OR pvt_key_last_backup_date <= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 202)
                            and EXISTS(select *
                                       from sys.all_columns c
                                       where c.name = 'pvt_key_last_backup_date')
                            and EXISTS(select *
                                       from msdb.information_schema.columns c
                                       where c.table_name = 'backupset'
                                         and c.column_name = 'encryptor_thumbprint')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 202) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
								SELECT DISTINCT 202 AS CheckID,
								1 AS Priority,
								''Backup'' AS FindingsGroup,
								''Encryption Certificate Not Backed Up Recently'' AS Finding,
								''https://BrentOzar.com/go/tde'' AS URL,
								''The certificate '' + c.name + '' is used to encrypt database backups. Last backup date: '' + COALESCE(CAST(c.pvt_key_last_backup_date AS VARCHAR(100)), ''Never'') AS Details
								FROM sys.certificates c
                                INNER JOIN msdb.dbo.backupset bs ON c.thumbprint = bs.encryptor_thumbprint
                                WHERE pvt_key_last_backup_date IS NULL OR pvt_key_last_backup_date <= DATEADD(dd, -30, GETDATE()) OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 3)
                            begin
                                if DATEADD(dd, -60, GETDATE()) >
                                   (select top 1 backup_start_date from msdb.dbo.backupset order by backup_start_date)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 3) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select top 1 3                                                as checkid,
                                                     'msdb',
                                                     200                                              as priority,
                                                     'Backup'                                         as findingsgroup,
                                                     'MSDB Backup History Not Purged'                 as finding,
                                                     'https://BrentOzar.com/go/history'               as url,
                                                     ('Database backup history retained back to '
                                                         + CAST(bs.backup_start_date as varchar(20))) as details
                                        from msdb.dbo.backupset bs
                                                 left outer join msdb.dbo.restorehistory rh
                                                                 on bs.database_name = rh.destination_database_name
                                        where rh.destination_database_name is null
                                        order by bs.backup_start_date asc;
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 186)
                            begin
                                if DATEADD(dd, -2, GETDATE()) <
                                   (select top 1 backup_start_date from msdb.dbo.backupset order by backup_start_date)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 186) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select top 1 186                                              as checkid,
                                                     'msdb',
                                                     200                                              as priority,
                                                     'Backup'                                         as findingsgroup,
                                                     'MSDB Backup History Purged Too Frequently'      as finding,
                                                     'https://BrentOzar.com/go/history'               as url,
                                                     ('Database backup history only retained back to '
                                                         + CAST(bs.backup_start_date as varchar(20))) as details
                                        from msdb.dbo.backupset bs
                                        order by backup_start_date asc;
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 178)
                            and EXISTS(select *
                                       from msdb.dbo.backupset bs
                                       where bs.type = 'D'
                                         and bs.backup_size >= 50000000000 /* At least 50GB */
                                         and DATEDIFF(second, bs.backup_start_date, bs.backup_finish_date) <= 60 /* Backup took less than 60 seconds */
                                         and bs.backup_finish_date >= DATEADD(day, -14, GETDATE()) /* In the last 2 weeks */)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 178) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 178                                                                                                       as checkid,
                                       200                                                                                                       as priority,
                                       'Performance'                                                                                             as findingsgroup,
                                       'Snapshot Backups Occurring'                                                                              as finding,
                                       'https://BrentOzar.com/go/snaps'                                                                          as url,
                                       (CAST(COUNT(*) as varchar(20)) +
                                        ' snapshot-looking backups have occurred in the last two weeks, indicating that IO may be freezing up.') as details
                                from msdb.dbo.backupset bs
                                where bs.type = 'D'
                                  and bs.backup_size >= 50000000000 /* At least 50GB */
                                  and DATEDIFF(second, bs.backup_start_date, bs.backup_finish_date) <= 60 /* Backup took less than 60 seconds */
                                  and bs.backup_finish_date >= DATEADD(day, -14, GETDATE()); /* In the last 2 weeks */
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 4)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 4) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 4                                                                                                                                 as checkid,
                                       230                                                                                                                               as priority,
                                       'Security'                                                                                                                        as findingsgroup,
                                       'Sysadmins'                                                                                                                       as finding,
                                       'https://BrentOzar.com/go/sa'                                                                                                     as url,
                                       ('Login [' + l.name
                                           +
                                        '] is a sysadmin - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.') as details
                                from master.sys.syslogins l
                                where l.sysadmin = 1
                                  and l.name <> SUSER_SNAME(0x01)
                                  and l.denylogin = 0
                                  and l.name not like 'NT SERVICE\%'
                                  and l.name <> 'l_certSignSmDetach'; /* Added in SQL 2016 */
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where checkid = 2301)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 2301) with nowait;

                                insert into #invalidlogins
                                    exec sp_validatelogins;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 2301                                                                                                                as checkid,
                                       230                                                                                                                 as priority,
                                       'Security'                                                                                                          as findingsgroup,
                                       'Invalid login defined with Windows Authentication'                                                                 as finding,
                                       'https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-validatelogins-transact-sql' as url,
                                       ('Windows user or group ' + QUOTENAME(loginname) +
                                        ' is mapped to a SQL Server principal but no longer exists in the Windows environment.')                           as details
                                from #invalidlogins;
                            end;
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 5)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 5) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 5                                                                                                                                                                     as checkid,
                                       230                                                                                                                                                                   as priority,
                                       'Security'                                                                                                                                                            as findingsgroup,
                                       'Security Admins'                                                                                                                                                     as finding,
                                       'https://BrentOzar.com/go/sa'                                                                                                                                         as url,
                                       ('Login [' + l.name
                                           +
                                        '] is a security admin - meaning they can give themselves permission to do absolutely anything in SQL Server, including dropping databases or hiding their tracks.') as details
                                from master.sys.syslogins l
                                where l.securityadmin = 1
                                  and l.name <> SUSER_SNAME(0x01)
                                  and l.denylogin = 0;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 104)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 104) with nowait;

                                insert into #blitzresults
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 104                                                                                                                                                 as [CheckID],
                                       230                                                                                                                                                 as [Priority],
                                       'Security'                                                                                                                                          as [FindingsGroup],
                                       'Login Can Control Server'                                                                                                                          as [Finding],
                                       'https://BrentOzar.com/go/sa'                                                                                                                       as [URL],
                                       'Login [' + pri.[name]
                                           +
                                       '] has the CONTROL SERVER permission - meaning they can do absolutely anything in SQL Server, including dropping databases or hiding their tracks.' as [Details]
                                from sys.server_principals as pri
                                where pri.[principal_id] in (
                                    select p.[grantee_principal_id]
                                    from sys.server_permissions as p
                                    where p.[state] in ('G', 'W')
                                      and p.[class] = 100
                                      and p.[type] = 'CL')
                                  and pri.[name] not like '##%##';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 6)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 6) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 6                                                                                                                        as checkid,
                                       230                                                                                                                      as priority,
                                       'Security'                                                                                                               as findingsgroup,
                                       'Jobs Owned By Users'                                                                                                    as finding,
                                       'https://BrentOzar.com/go/owners'                                                                                        as url,
                                       ('Job [' + j.name + '] is owned by ['
                                           + SUSER_SNAME(j.owner_sid)
                                           +
                                        '] - meaning if their login is disabled or not available due to Active Directory problems, the job will stop working.') as details
                                from msdb.dbo.sysjobs j
                                where j.enabled = 1
                                  and SUSER_SNAME(j.owner_sid) <> SUSER_SNAME(0x01);
                            end;

                        /* --TOURSTOP06-- */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 7)
                            begin
                                /* --TOURSTOP02-- */

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 7) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 7                                                                                                                                                           as checkid,
                                       230                                                                                                                                                         as priority,
                                       'Security'                                                                                                                                                  as findingsgroup,
                                       'Stored Procedure Runs at Startup'                                                                                                                          as finding,
                                       'https://BrentOzar.com/go/startup'                                                                                                                          as url,
                                       ('Stored procedure [master].['
                                           + r.specific_schema + '].['
                                           + r.specific_name
                                           +
                                        '] runs automatically when SQL Server starts up.  Make sure you know exactly what this stored procedure is doing, because it could pose a security risk.') as details
                                from master.information_schema.routines r
                                where OBJECTPROPERTY(OBJECT_ID(routine_name),
                                                     'ExecIsStartup') = 1;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 10)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 10) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 10 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Resource Governor Enabled'' AS Finding,
					  ''https://BrentOzar.com/go/rg'' AS URL,
					  (''Resource Governor is enabled.  Queries may be throttled.  Make sure you understand how the Classifier Function is configured.'') AS Details FROM sys.resource_governor_configuration WHERE is_enabled = 1 OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 11)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 11) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
							(CheckID,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 11 AS CheckID,
					  100 AS Priority,
					  ''Performance'' AS FindingsGroup,
					  ''Server Triggers Enabled'' AS Finding,
					  ''https://BrentOzar.com/go/logontriggers/'' AS URL,
					  (''Server Trigger ['' + [name] ++ ''] is enabled.  Make sure you understand what that trigger is doing - the less work it does, the better.'') AS Details FROM sys.server_triggers WHERE is_disabled = 0 AND is_ms_shipped = 0  OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 12)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 12) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 12                                                                                 as checkid,
                                       [name]                                                                             as databasename,
                                       10                                                                                 as priority,
                                       'Performance'                                                                      as findingsgroup,
                                       'Auto-Close Enabled'                                                               as finding,
                                       'https://BrentOzar.com/go/autoclose'                                               as url,
                                       ('Database [' + [name]
                                           +
                                        '] has auto-close enabled.  This setting can dramatically decrease performance.') as details
                                from sys.databases
                                where is_auto_close_on = 1
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 12);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 13)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 13) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 13                                                                                  as checkid,
                                       [name]                                                                              as databasename,
                                       10                                                                                  as priority,
                                       'Performance'                                                                       as findingsgroup,
                                       'Auto-Shrink Enabled'                                                               as finding,
                                       'https://BrentOzar.com/go/autoshrink'                                               as url,
                                       ('Database [' + [name]
                                           +
                                        '] has auto-shrink enabled.  This setting can dramatically decrease performance.') as details
                                from sys.databases
                                where is_auto_shrink_on = 1
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 13);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 14)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 14) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 14 AS CheckID,
					  [name] as DatabaseName,
					  50 AS Priority,
					  ''Reliability'' AS FindingsGroup,
					  ''Page Verification Not Optimal'' AS Finding,
					  ''https://BrentOzar.com/go/torn'' AS URL,
					  (''Database ['' + [name] + ''] has '' + [page_verify_option_desc] + '' for page verification.  SQL Server may have a harder time recognizing and recovering from storage corruption.  Consider using CHECKSUM instead.'') COLLATE database_default AS Details
					  FROM sys.databases
					  WHERE page_verify_option < 2
					  AND name <> ''tempdb''
					  AND state <> 1 /* Restoring */
					  and name not in (select distinct DatabaseName from #SkipChecks WHERE CheckID IS NULL OR CheckID = 14) OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 15)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 15) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 15                                                                                                                                                                               as checkid,
                                       [name]                                                                                                                                                                           as databasename,
                                       110                                                                                                                                                                              as priority,
                                       'Performance'                                                                                                                                                                    as findingsgroup,
                                       'Auto-Create Stats Disabled'                                                                                                                                                     as finding,
                                       'https://BrentOzar.com/go/acs'                                                                                                                                                   as url,
                                       ('Database [' + [name]
                                           +
                                        '] has auto-create-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically create more, performance may suffer.') as details
                                from sys.databases
                                where is_auto_create_stats_on = 0
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 15);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 16)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 16) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 16                                                                                                                                                                               as checkid,
                                       [name]                                                                                                                                                                           as databasename,
                                       110                                                                                                                                                                              as priority,
                                       'Performance'                                                                                                                                                                    as findingsgroup,
                                       'Auto-Update Stats Disabled'                                                                                                                                                     as finding,
                                       'https://BrentOzar.com/go/aus'                                                                                                                                                   as url,
                                       ('Database [' + [name]
                                           +
                                        '] has auto-update-stats disabled.  SQL Server uses statistics to build better execution plans, and without the ability to automatically update them, performance may suffer.') as details
                                from sys.databases
                                where is_auto_update_stats_on = 0
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 16);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 17)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 17) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 17                                                                                                                                                                                                                                                                  as checkid,
                                       [name]                                                                                                                                                                                                                                                              as databasename,
                                       150                                                                                                                                                                                                                                                                 as priority,
                                       'Performance'                                                                                                                                                                                                                                                       as findingsgroup,
                                       'Stats Updated Asynchronously'                                                                                                                                                                                                                                      as finding,
                                       'https://BrentOzar.com/go/asyncstats'                                                                                                                                                                                                                               as url,
                                       ('Database [' + [name]
                                           +
                                        '] has auto-update-stats-async enabled.  When SQL Server gets a query for a table with out-of-date statistics, it will run the query with the stats it has - while updating stats to make later queries better. The initial run of the query may suffer, though.') as details
                                from sys.databases
                                where is_auto_update_stats_async_on = 1
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 17);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 20)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 20) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 20                                                                                                                                                                                                                                    as checkid,
                                       [name]                                                                                                                                                                                                                                as databasename,
                                       200                                                                                                                                                                                                                                   as priority,
                                       'Informational'                                                                                                                                                                                                                       as findingsgroup,
                                       'Date Correlation On'                                                                                                                                                                                                                 as finding,
                                       'https://BrentOzar.com/go/corr'                                                                                                                                                                                                       as url,
                                       ('Database [' + [name]
                                           +
                                        '] has date correlation enabled.  This is not a default setting, and it has some performance overhead.  It tells SQL Server that date fields in two tables are related, and SQL Server maintains statistics showing that relation.') as details
                                from sys.databases
                                where is_date_correlation_on = 1
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 20);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 21)
                            begin
                                /* --TOURSTOP04-- */
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 21) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
							(CheckID,
							DatabaseName,
							Priority,
							FindingsGroup,
							Finding,
							URL,
							Details)
					  SELECT 21 AS CheckID,
					  [name] as DatabaseName,
					  200 AS Priority,
					  ''Informational'' AS FindingsGroup,
					  ''Database Encrypted'' AS Finding,
					  ''https://BrentOzar.com/go/tde'' AS URL,
					  (''Database ['' + [name] + ''] has Transparent Data Encryption enabled.  Make absolutely sure you have backed up the certificate and private key, or else you will not be able to restore this database.'') AS Details
					  FROM sys.databases
					  WHERE is_encrypted = 1
					  and name not in (select distinct DatabaseName from #SkipChecks WHERE CheckID IS NULL OR CheckID = 21) OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        /*
				Believe it or not, SQL Server doesn't track the default values
				for sp_configure options! We'll make our own list here.
				*/

                        if @debug in (1, 2) raiserror ('Generating default configuration values', 0, 1) with nowait;

                        insert into #configurationdefaults
                        values ('access check cache bucket count', 0, 1001);
                        insert into #configurationdefaults
                        values ('access check cache quota', 0, 1002);
                        insert into #configurationdefaults
                        values ('Ad Hoc Distributed Queries', 0, 1003);
                        insert into #configurationdefaults
                        values ('affinity I/O mask', 0, 1004);
                        insert into #configurationdefaults
                        values ('affinity mask', 0, 1005);
                        insert into #configurationdefaults
                        values ('affinity64 mask', 0, 1066);
                        insert into #configurationdefaults
                        values ('affinity64 I/O mask', 0, 1067);
                        insert into #configurationdefaults
                        values ('Agent XPs', 0, 1071);
                        insert into #configurationdefaults
                        values ('allow updates', 0, 1007);
                        insert into #configurationdefaults
                        values ('awe enabled', 0, 1008);
                        insert into #configurationdefaults
                        values ('backup checksum default', 0, 1070);
                        insert into #configurationdefaults
                        values ('backup compression default', 0, 1073);
                        insert into #configurationdefaults
                        values ('blocked process threshold', 0, 1009);
                        insert into #configurationdefaults
                        values ('blocked process threshold (s)', 0, 1009);
                        insert into #configurationdefaults
                        values ('c2 audit mode', 0, 1010);
                        insert into #configurationdefaults
                        values ('clr enabled', 0, 1011);
                        insert into #configurationdefaults
                        values ('common criteria compliance enabled', 0, 1074);
                        insert into #configurationdefaults
                        values ('contained database authentication', 0, 1068);
                        insert into #configurationdefaults
                        values ('cost threshold for parallelism', 5, 1012);
                        insert into #configurationdefaults
                        values ('cross db ownership chaining', 0, 1013);
                        insert into #configurationdefaults
                        values ('cursor threshold', -1, 1014);
                        insert into #configurationdefaults
                        values ('Database Mail XPs', 0, 1072);
                        insert into #configurationdefaults
                        values ('default full-text language', 1033, 1016);
                        insert into #configurationdefaults
                        values ('default language', 0, 1017);
                        insert into #configurationdefaults
                        values ('default trace enabled', 1, 1018);
                        insert into #configurationdefaults
                        values ('disallow results from triggers', 0, 1019);
                        insert into #configurationdefaults
                        values ('EKM provider enabled', 0, 1075);
                        insert into #configurationdefaults
                        values ('filestream access level', 0, 1076);
                        insert into #configurationdefaults
                        values ('fill factor (%)', 0, 1020);
                        insert into #configurationdefaults
                        values ('ft crawl bandwidth (max)', 100, 1021);
                        insert into #configurationdefaults
                        values ('ft crawl bandwidth (min)', 0, 1022);
                        insert into #configurationdefaults
                        values ('ft notify bandwidth (max)', 100, 1023);
                        insert into #configurationdefaults
                        values ('ft notify bandwidth (min)', 0, 1024);
                        insert into #configurationdefaults
                        values ('index create memory (KB)', 0, 1025);
                        insert into #configurationdefaults
                        values ('in-doubt xact resolution', 0, 1026);
                        insert into #configurationdefaults
                        values ('lightweight pooling', 0, 1027);
                        insert into #configurationdefaults
                        values ('locks', 0, 1028);
                        insert into #configurationdefaults
                        values ('max degree of parallelism', 0, 1029);
                        insert into #configurationdefaults
                        values ('max full-text crawl range', 4, 1030);
                        insert into #configurationdefaults
                        values ('max server memory (MB)', 2147483647, 1031);
                        insert into #configurationdefaults
                        values ('max text repl size (B)', 65536, 1032);
                        insert into #configurationdefaults
                        values ('max worker threads', 0, 1033);
                        insert into #configurationdefaults
                        values ('media retention', 0, 1034);
                        insert into #configurationdefaults
                        values ('min memory per query (KB)', 1024, 1035);
                        /* Accepting both 0 and 16 below because both have been seen in the wild as defaults. */
                        if EXISTS(select *
                                  from sys.configurations
                                  where name = 'min server memory (MB)'
                                    and value_in_use in (0, 16))
                            insert into #configurationdefaults
                            select 'min server memory (MB)',
                                   CAST(value_in_use as bigint),
                                   1036
                            from sys.configurations
                            where name = 'min server memory (MB)';
                        else
                            insert into #configurationdefaults
                            values ('min server memory (MB)', 0, 1036);
                        insert into #configurationdefaults
                        values ('nested triggers', 1, 1037);
                        insert into #configurationdefaults
                        values ('network packet size (B)', 4096, 1038);
                        insert into #configurationdefaults
                        values ('Ole Automation Procedures', 0, 1039);
                        insert into #configurationdefaults
                        values ('open objects', 0, 1040);
                        insert into #configurationdefaults
                        values ('optimize for ad hoc workloads', 0, 1041);
                        insert into #configurationdefaults
                        values ('PH timeout (s)', 60, 1042);
                        insert into #configurationdefaults
                        values ('precompute rank', 0, 1043);
                        insert into #configurationdefaults
                        values ('priority boost', 0, 1044);
                        insert into #configurationdefaults
                        values ('query governor cost limit', 0, 1045);
                        insert into #configurationdefaults
                        values ('query wait (s)', -1, 1046);
                        insert into #configurationdefaults
                        values ('recovery interval (min)', 0, 1047);
                        insert into #configurationdefaults
                        values ('remote access', 1, 1048);
                        insert into #configurationdefaults
                        values ('remote admin connections', 0, 1049);
                        /* SQL Server 2012 changes a configuration default */
                        if @@VERSION like '%Microsoft SQL Server 2005%'
                            or @@VERSION like '%Microsoft SQL Server 2008%'
                            begin
                                insert into #configurationdefaults
                                values ('remote login timeout (s)', 20, 1069);
                            end;
                        else
                            begin
                                insert into #configurationdefaults
                                values ('remote login timeout (s)', 10, 1069);
                            end;
                        insert into #configurationdefaults
                        values ('remote proc trans', 0, 1050);
                        insert into #configurationdefaults
                        values ('remote query timeout (s)', 600, 1051);
                        insert into #configurationdefaults
                        values ('Replication XPs', 0, 1052);
                        insert into #configurationdefaults
                        values ('RPC parameter data validation', 0, 1053);
                        insert into #configurationdefaults
                        values ('scan for startup procs', 0, 1054);
                        insert into #configurationdefaults
                        values ('server trigger recursion', 1, 1055);
                        insert into #configurationdefaults
                        values ('set working set size', 0, 1056);
                        insert into #configurationdefaults
                        values ('show advanced options', 0, 1057);
                        insert into #configurationdefaults
                        values ('SMO and DMO XPs', 1, 1058);
                        insert into #configurationdefaults
                        values ('SQL Mail XPs', 0, 1059);
                        insert into #configurationdefaults
                        values ('transform noise words', 0, 1060);
                        insert into #configurationdefaults
                        values ('two digit year cutoff', 2049, 1061);
                        insert into #configurationdefaults
                        values ('user connections', 0, 1062);
                        insert into #configurationdefaults
                        values ('user options', 0, 1063);
                        insert into #configurationdefaults
                        values ('Web Assistant Procedures', 0, 1064);
                        insert into #configurationdefaults
                        values ('xp_cmdshell', 0, 1065);

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 22)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 22) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select cd.checkid,
                                       200                             as priority,
                                       'Non-Default Server Config'     as findingsgroup,
                                       cr.name                         as finding,
                                       'https://BrentOzar.com/go/conf' as url,
                                       ('This sp_configure option has been changed.  Its default value is '
                                           + COALESCE(CAST(cd.[DefaultValue] as varchar(100)),
                                                      '(unknown)')
                                           + ' and it has been set to '
                                           + CAST(cr.value_in_use as varchar(100))
                                           + '.')                      as details
                                from sys.configurations cr
                                         inner join #configurationdefaults cd on cd.name = cr.name
                                         left outer join #configurationdefaults cdused on cdused.name = cr.name
                                    and cdused.defaultvalue = cr.value_in_use
                                where cdused.name is null;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 190)
                            begin

                                if @debug in (1, 2)
                                    raiserror ('Setting @MinServerMemory and @MaxServerMemory', 0, 1) with nowait;

                                select @minservermemory = CAST(value_in_use as bigint)
                                from sys.configurations
                                where name = 'min server memory (MB)';
                                select @maxservermemory = CAST(value_in_use as bigint)
                                from sys.configurations
                                where name = 'max server memory (MB)';

                                if (@minservermemory = @maxservermemory)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 190) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        values (190,
                                                200,
                                                'Performance',
                                                'Non-Dynamic Memory',
                                                'https://BrentOzar.com/go/memory',
                                                'Minimum Server Memory setting is the same as the Maximum (both set to ' +
                                                CAST(@minservermemory as nvarchar(50)) +
                                                '). This will not allow dynamic memory. Please revise memory settings');
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 188)
                            begin

                                /* Let's set variables so that our query is still SARGable */

                                if @debug in (1, 2) raiserror ('Setting @Processors.', 0, 1) with nowait;

                                set @processors = (select cpu_count from sys.dm_os_sys_info);

                                if @debug in (1, 2) raiserror ('Setting @NUMANodes', 0, 1) with nowait;

                                set @numanodes = (select COUNT(1)
                                                  from sys.dm_os_performance_counters pc
                                                  where pc.object_name like '%Buffer Node%'
                                                    and counter_name = 'Page life expectancy');
                                /* If Cost Threshold for Parallelism is default then flag as a potential issue */
                                /* If MAXDOP is default and processors > 8 or NUMA nodes > 1 then flag as potential issue */

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 188) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 188                                 as checkid,
                                       200                                 as priority,
                                       'Performance'                       as findingsgroup,
                                       cr.name                             as finding,
                                       'https://BrentOzar.com/go/cxpacket' as url,
                                       ('Set to ' + CAST(cr.value_in_use as nvarchar(50)) +
                                        ', its default value. Changing this sp_configure setting may reduce CXPACKET waits.')
                                from sys.configurations cr
                                         inner join #configurationdefaults cd on cd.name = cr.name
                                    and cr.value_in_use = cd.defaultvalue
                                where cr.name = 'cost threshold for parallelism'
                                   or (cr.name = 'max degree of parallelism' and (@numanodes > 1 or @processors > 8));
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 24)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 24) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 24                                                                                                                                                as checkid,
                                                DB_NAME(database_id)                                                                                                                              as databasename,
                                                170                                                                                                                                               as priority,
                                                'File Configuration'                                                                                                                              as findingsgroup,
                                                'System Database on C Drive'                                                                                                                      as finding,
                                                'https://BrentOzar.com/go/cdrive'                                                                                                                 as url,
                                                ('The ' + DB_NAME(database_id)
                                                    +
                                                 ' database has a file on the C drive.  Putting system databases on the C drive runs the risk of crashing the server when it runs out of space.') as details
                                from sys.master_files
                                where UPPER(LEFT(physical_name, 1)) = 'C'
                                  and DB_NAME(database_id) in ('master',
                                                               'model', 'msdb');
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 25)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 25) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select top 1 25                                as checkid,
                                             'tempdb',
                                             20                                as priority,
                                             'File Configuration'              as findingsgroup,
                                             'TempDB on C Drive'               as finding,
                                             'https://BrentOzar.com/go/cdrive' as url,
                                             case
                                                 when growth > 0
                                                     then ('The tempdb database has files on the C drive.  TempDB frequently grows unpredictably, putting your server at risk of running out of C drive space and crashing hard.  C is also often much slower than other drives, so performance may be suffering.')
                                                 else ('The tempdb database has files on the C drive.  TempDB is not set to Autogrow, hopefully it is big enough.  C is also often much slower than other drives, so performance may be suffering.')
                                                 end                           as details
                                from sys.master_files
                                where UPPER(LEFT(physical_name, 1)) = 'C'
                                  and DB_NAME(database_id) = 'tempdb';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 26)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 26) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 26                                                                                                                                         as checkid,
                                                DB_NAME(database_id)                                                                                                                       as databasename,
                                                20                                                                                                                                         as priority,
                                                'Reliability'                                                                                                                              as findingsgroup,
                                                'User Databases on C Drive'                                                                                                                as finding,
                                                'https://BrentOzar.com/go/cdrive'                                                                                                          as url,
                                                ('The ' + DB_NAME(database_id)
                                                    +
                                                 ' database has a file on the C drive.  Putting databases on the C drive runs the risk of crashing the server when it runs out of space.') as details
                                from sys.master_files
                                where UPPER(LEFT(physical_name, 1)) = 'C'
                                  and DB_NAME(database_id) not in ('master',
                                                                   'model', 'msdb',
                                                                   'tempdb')
                                  and DB_NAME(database_id) not in (
                                    select distinct databasename
                                    from #skipchecks
                                    where checkid is null
                                       or checkid = 26);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 27)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 27) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 27                                                                                  as checkid,
                                       'master'                                                                            as databasename,
                                       200                                                                                 as priority,
                                       'Informational'                                                                     as findingsgroup,
                                       'Tables in the Master Database'                                                     as finding,
                                       'https://BrentOzar.com/go/mastuser'                                                 as url,
                                       ('The ' + name
                                           + ' table in the master database was created by end users on '
                                           + CAST(create_date as varchar(20))
                                           +
                                        '. Tables in the master database may not be restored in the event of a disaster.') as details
                                from master.sys.tables
                                where is_ms_shipped = 0
                                  and name not in ('CommandLog', 'SqlServerVersions', '$ndo$srvproperty');
                                /* That last one is the Dynamics NAV licensing table: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2426 */
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 28)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 28) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 28                                                                                as checkid,
                                       'msdb'                                                                            as databasename,
                                       200                                                                               as priority,
                                       'Informational'                                                                   as findingsgroup,
                                       'Tables in the MSDB Database'                                                     as finding,
                                       'https://BrentOzar.com/go/msdbuser'                                               as url,
                                       ('The ' + name
                                           + ' table in the msdb database was created by end users on '
                                           + CAST(create_date as varchar(20))
                                           +
                                        '. Tables in the msdb database may not be restored in the event of a disaster.') as details
                                from msdb.sys.tables
                                where is_ms_shipped = 0
                                  and name not like '%DTA_%';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 29)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 29) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 29                                                                                  as checkid,
                                       'model'                                                                             as databasename,
                                       200                                                                                 as priority,
                                       'Informational'                                                                     as findingsgroup,
                                       'Tables in the Model Database'                                                      as finding,
                                       'https://BrentOzar.com/go/model'                                                    as url,
                                       ('The ' + name
                                           + ' table in the model database was created by end users on '
                                           + CAST(create_date as varchar(20))
                                           +
                                        '. Tables in the model database are automatically copied into all new databases.') as details
                                from model.sys.tables
                                where is_ms_shipped = 0;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 30)
                            begin
                                if (select COUNT(*)
                                    from msdb.dbo.sysalerts
                                    where severity between 19 and 25
                                   ) < 7
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 30) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 30                                                                                                                                                                                           as checkid,
                                               200                                                                                                                                                                                          as priority,
                                               'Monitoring'                                                                                                                                                                                 as findingsgroup,
                                               'Not All Alerts Configured'                                                                                                                                                                  as finding,
                                               'https://BrentOzar.com/go/alert'                                                                                                                                                             as url,
                                               ('Not all SQL Server Agent alerts have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.') as details;
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 59)
                            begin
                                if EXISTS(select *
                                          from msdb.dbo.sysalerts
                                          where enabled = 1
                                            and COALESCE(has_notification, 0) = 0
                                            and (job_id is null or job_id = 0 x))
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 59) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 59                                                                                                                                                                                                                                                            as checkid,
                                               200                                                                                                                                                                                                                                                           as priority,
                                               'Monitoring'                                                                                                                                                                                                                                                  as findingsgroup,
                                               'Alerts Configured without Follow Up'                                                                                                                                                                                                                         as finding,
                                               'https://BrentOzar.com/go/alert'                                                                                                                                                                                                                              as url,
                                               ('SQL Server Agent alerts have been configured but they either do not notify anyone or else they do not take any action.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.') as details;

                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 96)
                            begin
                                if not EXISTS(select *
                                              from msdb.dbo.sysalerts
                                              where message_id in (823, 824, 825))
                                    begin
                                        ;

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 96) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 96                                                                                                                                                                                                    as checkid,
                                               200                                                                                                                                                                                                   as priority,
                                               'Monitoring'                                                                                                                                                                                          as findingsgroup,
                                               'No Alerts for Corruption'                                                                                                                                                                            as finding,
                                               'https://BrentOzar.com/go/alert'                                                                                                                                                                      as url,
                                               ('SQL Server Agent alerts do not exist for errors 823, 824, and 825.  These three errors can give you notification about early hardware failure. Enabling them can prevent you a lot of heartbreak.') as details;

                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 61)
                            begin
                                if not EXISTS(select *
                                              from msdb.dbo.sysalerts
                                              where severity between 19 and 25)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 61) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 61                                                                                                                                                                                                  as checkid,
                                               200                                                                                                                                                                                                 as priority,
                                               'Monitoring'                                                                                                                                                                                        as findingsgroup,
                                               'No Alerts for Sev 19-25'                                                                                                                                                                           as finding,
                                               'https://BrentOzar.com/go/alert'                                                                                                                                                                    as url,
                                               ('SQL Server Agent alerts do not exist for severity levels 19 through 25.  These are some very severe SQL Server errors. Knowing that these are happening may let you recover from errors faster.') as details;

                                    end;

                            end;

                        --check for disabled alerts
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 98)
                            begin
                                if EXISTS(select name
                                          from msdb.dbo.sysalerts
                                          where enabled = 0)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 98) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 98                               as checkid,
                                               200                              as priority,
                                               'Monitoring'                     as findingsgroup,
                                               'Alerts Disabled'                as finding,
                                               'https://BrentOzar.com/go/alert' as url,
                                               ('The following Alert is disabled, please review and enable if desired: '
                                                   + name)                      as details
                                        from msdb.dbo.sysalerts
                                        where enabled = 0;
                                    end;
                            end;

                        --check for alerts that do NOT include event descriptions in their outputs via email/pager/net-send
                        if not EXISTS(
                                select 1
                                from #skipchecks
                                where databasename is null
                                  and checkid = 219
                            )
                            begin
                                ;
                                if @debug in (1, 2)
                                    begin
                                        ;
                                        raiserror ('Running CheckId [%d].', 0, 1, 219) with nowait;
                                    end;

                                insert into #blitzresults ( checkid
                                                          , [Priority]
                                                          , findingsgroup
                                                          , finding
                                                          , [URL]
                                                          , details)
                                select 219                                                                                    as checkid
                                     , 200                                                                                    as [Priority]
                                     , 'Monitoring'                                                                           as findingsgroup
                                     , 'Alerts Without Event Descriptions'                                                    as finding
                                     , 'https://BrentOzar.com/go/alert'                                                       as [URL]
                                     , ('The following Alert is not including detailed event descriptions in its output messages: ' +
                                        QUOTENAME([name])
                                    +
                                        '. You can fix it by ticking the relevant boxes in its Properties --> Options page.') as details
                                from msdb.dbo.sysalerts
                                where [enabled] = 1
                                  and include_event_description = 0 --bitmask: 1 = email, 2 = pager, 4 = net send
                                ;
                            end;

                        --check whether we have NO ENABLED operators!
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 31)
                            begin
                                ;
                                if not EXISTS(select *
                                              from msdb.dbo.sysoperators
                                              where enabled = 1)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 31) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 31                                                                                                                                                                                                  as checkid,
                                               200                                                                                                                                                                                                 as priority,
                                               'Monitoring'                                                                                                                                                                                        as findingsgroup,
                                               'No Operators Configured/Enabled'                                                                                                                                                                   as finding,
                                               'https://BrentOzar.com/go/op'                                                                                                                                                                       as url,
                                               ('No SQL Server Agent operators (emails) have been configured.  This is a free, easy way to get notified of corruption, job failures, or major outages even before monitoring systems pick it up.') as details;

                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 34)
                            begin
                                if EXISTS(select *
                                          from sys.all_objects
                                          where name = 'dm_db_mirroring_auto_page_repair')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 34) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  34 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''https://BrentOzar.com/go/repair'' AS URL ,
		  ( ''Database mirroring has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_db_mirroring_auto_page_repair.'' ) AS Details
		  FROM (SELECT rp2.database_id, rp2.modification_time
			FROM sys.dm_db_mirroring_auto_page_repair rp2
			WHERE rp2.[database_id] not in (
			SELECT db2.[database_id]
			FROM sys.databases as db2
			WHERE db2.[state] = 1
			) ) as rp
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 89)
                            begin
                                if EXISTS(select *
                                          from sys.all_objects
                                          where name = 'dm_hadr_auto_page_repair')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 89) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  89 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''https://BrentOzar.com/go/repair'' AS URL ,
		  ( ''Availability Groups has automatically repaired at least one corrupt page in the last 30 days. For more information, query the DMV sys.dm_hadr_auto_page_repair.'' ) AS Details
		  FROM    sys.dm_hadr_auto_page_repair rp
		  INNER JOIN master.sys.databases db ON rp.database_id = db.database_id
		  WHERE   rp.modification_time >= DATEADD(dd, -30, GETDATE()) OPTION (RECOMPILE) ;';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 90)
                            begin
                                if EXISTS(select *
                                          from msdb.sys.all_objects
                                          where name = 'suspect_pages')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 90) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT
		  90 AS CheckID ,
		  db.name ,
		  1 AS Priority ,
		  ''Corruption'' AS FindingsGroup ,
		  ''Database Corruption Detected'' AS Finding ,
		  ''https://BrentOzar.com/go/repair'' AS URL ,
		  ( ''SQL Server has detected at least one corrupt page in the last 30 days. For more information, query the system table msdb.dbo.suspect_pages.'' ) AS Details
		  FROM    msdb.dbo.suspect_pages sp
		  INNER JOIN master.sys.databases db ON sp.database_id = db.database_id
		  WHERE   sp.last_update_date >= DATEADD(dd, -30, GETDATE())  OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 36)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 36) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 36                                                                                                                                                            as checkid,
                                                150                                                                                                                                                           as priority,
                                                'Performance'                                                                                                                                                 as findingsgroup,
                                                'Slow Storage Reads on Drive '
                                                    +
                                                UPPER(LEFT(mf.physical_name, 1))                                                                                                                              as finding,
                                                'https://BrentOzar.com/go/slow'                                                                                                                               as url,
                                                'Reads are averaging longer than 200ms for at least one database on this drive.  For specific database file speeds, run the query from the information link.' as details
                                from sys.dm_io_virtual_file_stats(null, null)
                                         as fs
                                         inner join sys.master_files as mf on fs.database_id = mf.database_id
                                    and fs.[file_id] = mf.[file_id]
                                where (io_stall_read_ms / (1.0 + num_of_reads)) > 200
                                  and num_of_reads > 100000;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 37)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 37) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 37                                                                                                                                                             as checkid,
                                                150                                                                                                                                                            as priority,
                                                'Performance'                                                                                                                                                  as findingsgroup,
                                                'Slow Storage Writes on Drive '
                                                    +
                                                UPPER(LEFT(mf.physical_name, 1))                                                                                                                               as finding,
                                                'https://BrentOzar.com/go/slow'                                                                                                                                as url,
                                                'Writes are averaging longer than 100ms for at least one database on this drive.  For specific database file speeds, run the query from the information link.' as details
                                from sys.dm_io_virtual_file_stats(null, null)
                                         as fs
                                         inner join sys.master_files as mf on fs.database_id = mf.database_id
                                    and fs.[file_id] = mf.[file_id]
                                where (io_stall_write_ms / (1.0
                                    + num_of_writes)) > 100
                                  and num_of_writes > 100000;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 40)
                            begin
                                if (select COUNT(*)
                                    from tempdb.sys.database_files
                                    where type_desc = 'ROWS'
                                   ) = 1
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 40) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        values (40,
                                                'tempdb',
                                                170,
                                                'File Configuration',
                                                'TempDB Only Has 1 Data File',
                                                'https://BrentOzar.com/go/tempdb',
                                                'TempDB is only configured with one data file.  More data files are usually required to alleviate SGAM contention.');
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 183)
                            begin

                                if (select COUNT(distinct [size])
                                    from tempdb.sys.database_files
                                    where type_desc = 'ROWS'
                                    having MAX((size * 8) / (1024. * 1024)) - MIN((size * 8) / (1024. * 1024)) > 1.
                                   ) <> 1
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 183) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        values (183,
                                                'tempdb',
                                                170,
                                                'File Configuration',
                                                'TempDB Unevenly Sized Data Files',
                                                'https://BrentOzar.com/go/tempdb',
                                                'TempDB data files are not configured with the same size.  Unevenly sized tempdb data files will result in unevenly sized workloads.');
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 44)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 44) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 44                                                                                                                                                                                                                                                                               as checkid,
                                       150                                                                                                                                                                                                                                                                              as priority,
                                       'Performance'                                                                                                                                                                                                                                                                    as findingsgroup,
                                       'Queries Forcing Order Hints'                                                                                                                                                                                                                                                    as finding,
                                       'https://BrentOzar.com/go/hints'                                                                                                                                                                                                                                                 as url,
                                       CAST(occurrence as varchar(10))
                                           +
                                       ' instances of order hinting have been recorded since restart.  This means queries are bossing the SQL Server optimizer around, and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' as details
                                from sys.dm_exec_query_optimizer_info
                                where counter = 'order hint'
                                  and occurrence > 1000;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 45)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 45) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 45                                                                                                                                                                                                                                                                              as checkid,
                                       150                                                                                                                                                                                                                                                                             as priority,
                                       'Performance'                                                                                                                                                                                                                                                                   as findingsgroup,
                                       'Queries Forcing Join Hints'                                                                                                                                                                                                                                                    as finding,
                                       'https://BrentOzar.com/go/hints'                                                                                                                                                                                                                                                as url,
                                       CAST(occurrence as varchar(10))
                                           +
                                       ' instances of join hinting have been recorded since restart.  This means queries are bossing the SQL Server optimizer around, and if they don''t know what they''re doing, this can cause more harm than good.  This can also explain why DBA tuning efforts aren''t working.' as details
                                from sys.dm_exec_query_optimizer_info
                                where counter = 'join hint'
                                  and occurrence > 1000;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 49)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 49) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 49                              as checkid,
                                                200                             as priority,
                                                'Informational'                 as findingsgroup,
                                                'Linked Server Configured'      as finding,
                                                'https://BrentOzar.com/go/link' as url,
                                                +case
                                                     when l.remote_name = 'sa'
                                                         then COALESCE(s.data_source, s.provider)
                                                         +
                                                              ' is configured as a linked server. Check its security configuration as it is connecting with sa, because any user who queries it will get admin-level permissions.'
                                                     else COALESCE(s.data_source, s.provider)
                                                         +
                                                          ' is configured as a linked server. Check its security configuration to make sure it isn''t connecting with SA or some other bone-headed administrative login, because any user who queries it might get admin-level permissions.'
                                                    end                         as details
                                from sys.servers s
                                         inner join sys.linked_logins l on s.server_id = l.server_id
                                where s.is_linked = 1;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 50)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 50) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		  SELECT  50 AS CheckID ,
		  100 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Max Memory Set Too High'' AS Finding ,
		  ''https://BrentOzar.com/go/max'' AS URL ,
		  ''SQL Server max memory is set to ''
			+ CAST(c.value_in_use AS VARCHAR(20))
			+ '' megabytes, but the server only has ''
			+ CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes.  SQL Server may drain the system dry of memory, and under certain conditions, this can cause Windows to swap to disk.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  INNER JOIN sys.configurations c ON c.name = ''max server memory (MB)''
		  WHERE   CAST(m.total_physical_memory_kb AS BIGINT) < ( CAST(c.value_in_use AS BIGINT) * 1024 ) OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 51)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 51) with nowait

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		  SELECT  51 AS CheckID ,
		  1 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Memory Dangerously Low'' AS Finding ,
		  ''https://BrentOzar.com/go/max'' AS URL ,
		  ''The server has '' + CAST(( CAST(m.total_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20)) + '' megabytes of physical memory, but only '' + CAST(( CAST(m.available_physical_memory_kb AS BIGINT) / 1024 ) AS VARCHAR(20))
			+ '' megabytes are available.  As the server runs out of memory, there is danger of swapping to disk, which will kill performance.'' AS Details
		  FROM    sys.dm_os_sys_memory m
		  WHERE   CAST(m.available_physical_memory_kb AS BIGINT) < 262144 OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 159)
                            begin
                                if @@VERSION not like '%Microsoft SQL Server 2000%'
                                    and @@VERSION not like '%Microsoft SQL Server 2005%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 159) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
		  SELECT DISTINCT 159 AS CheckID ,
		  1 AS Priority ,
		  ''Performance'' AS FindingsGroup ,
		  ''Memory Dangerously Low in NUMA Nodes'' AS Finding ,
		  ''https://BrentOzar.com/go/max'' AS URL ,
		  ''At least one NUMA node is reporting THREAD_RESOURCES_LOW in sys.dm_os_nodes and can no longer create threads.'' AS Details
		  FROM    sys.dm_os_nodes m
		  WHERE   node_state_desc LIKE ''%THREAD_RESOURCES_LOW%'' OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 53)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 53) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select top 1 53                              as checkid,
                                             200                             as priority,
                                             'Informational'                 as findingsgroup,
                                             'Cluster Node'                  as finding,
                                             'https://BrentOzar.com/go/node' as url,
                                             'This is a node in a cluster.'  as details
                                from sys.dm_os_cluster_nodes;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 55)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 55) with nowait;

                                if @usualdbowner is null
                                    set @usualdbowner = SUSER_SNAME(0x01);

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 55                                             as checkid,
                                       [name]                                         as databasename,
                                       230                                            as priority,
                                       'Security'                                     as findingsgroup,
                                       'Database Owner <> ' + @usualdbowner           as finding,
                                       'https://BrentOzar.com/go/owndb'               as url,
                                       ('Database name: ' + [name] + '   '
                                           + 'Owner name: ' + SUSER_SNAME(owner_sid)) as details
                                from sys.databases
                                where (((SUSER_SNAME(owner_sid) <> SUSER_SNAME(0x01)) and
                                        (name in (N'master', N'model', N'msdb', N'tempdb')))
                                    or ((SUSER_SNAME(owner_sid) <> @usualdbowner) and
                                        (name not in (N'master', N'model', N'msdb', N'tempdb')))
                                    )
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 55);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 213)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 213) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 213                                                                     as checkid,
                                       [name]                                                                  as databasename,
                                       230                                                                     as priority,
                                       'Security'                                                              as findingsgroup,
                                       'Database Owner is Unknown'                                             as finding,
                                       ''                                                                      as url,
                                       ('Database name: ' + [name] + '   '
                                           + 'Owner name: ' +
                                        ISNULL(SUSER_SNAME(owner_sid), '~~ UNKNOWN ~~'))                       as details
                                from sys.databases
                                where SUSER_SNAME(owner_sid) is null
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 213);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 57)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 57) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 57                                                                                                                                                   as checkid,
                                       230                                                                                                                                                  as priority,
                                       'Security'                                                                                                                                           as findingsgroup,
                                       'SQL Agent Job Runs at Startup'                                                                                                                      as finding,
                                       'https://BrentOzar.com/go/startup'                                                                                                                   as url,
                                       ('Job [' + j.name
                                           +
                                        '] runs automatically when SQL Server Agent starts up.  Make sure you know exactly what this job is doing, because it could pose a security risk.') as details
                                from msdb.dbo.sysschedules sched
                                         join msdb.dbo.sysjobschedules jsched on sched.schedule_id = jsched.schedule_id
                                         join msdb.dbo.sysjobs j on jsched.job_id = j.job_id
                                where sched.freq_type = 64
                                  and sched.enabled = 1;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 97)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 97) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 97                                                           as checkid,
                                       100                                                          as priority,
                                       'Performance'                                                as findingsgroup,
                                       'Unusual SQL Server Edition'                                 as finding,
                                       'https://BrentOzar.com/go/workgroup'                         as url,
                                       ('This server is using '
                                           + CAST(SERVERPROPERTY('edition') as varchar(100))
                                           + ', which is capped at low amounts of CPU and memory.') as details
                                where CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Standard%'
                                  and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Enterprise%'
                                  and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Data Center%'
                                  and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Developer%'
                                  and CAST(SERVERPROPERTY('edition') as varchar(100)) not like
                                      '%Business Intelligence%';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 154)
                            and SERVERPROPERTY('EngineEdition') <> 8
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 154) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 154                                                                                                                                                                                        as checkid,
                                       10                                                                                                                                                                                         as priority,
                                       'Performance'                                                                                                                                                                              as findingsgroup,
                                       '32-bit SQL Server Installed'                                                                                                                                                              as finding,
                                       'https://BrentOzar.com/go/32bit'                                                                                                                                                           as url,
                                       ('This server uses the 32-bit x86 binaries for SQL Server instead of the 64-bit x64 binaries. The amount of memory available for query workspace and execution plans is heavily limited.') as details
                                where CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%64%';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 62)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 62) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 62                                                                                                as checkid,
                                       [name]                                                                                            as databasename,
                                       200                                                                                               as priority,
                                       'Performance'                                                                                     as findingsgroup,
                                       'Old Compatibility Level'                                                                         as finding,
                                       'https://BrentOzar.com/go/compatlevel'                                                            as url,
                                       ('Database ' + [name]
                                           + ' is compatibility level '
                                           + CAST(compatibility_level as varchar(20))
                                           +
                                        ', which may cause unwanted results when trying to run queries that have newer T-SQL features.') as details
                                from sys.databases
                                where name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 62)
                                  and compatibility_level <= 90;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 94)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 94) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 94                                                              as checkid,
                                       200                                                             as [Priority],
                                       'Monitoring'                                                    as findingsgroup,
                                       'Agent Jobs Without Failure Emails'                             as finding,
                                       'https://BrentOzar.com/go/alerts'                               as url,
                                       'The job ' + [name]
                                           + ' has not been set up to notify an operator if it fails.' as details
                                from msdb.[dbo].[sysjobs] j
                                         inner join (select distinct [job_id]
                                                     from [msdb].[dbo].[sysjobschedules]
                                                     where next_run_date > 0
                                ) s on j.job_id = s.job_id
                                where j.enabled = 1
                                  and j.notify_email_operator_id = 0
                                  and j.notify_netsend_operator_id = 0
                                  and j.notify_page_operator_id = 0
                                  and j.category_id <> 100; /* Exclude SSRS category */
                            end;

                        if EXISTS(select 1
                                  from sys.configurations
                                  where name = 'remote admin connections'
                                    and value_in_use = 0)
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 100)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 100) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 100                            as checkid,
                                       50                             as priority,
                                       'Reliability'                  as findinggroup,
                                       'Remote DAC Disabled'          as finding,
                                       'https://BrentOzar.com/go/dac' as url,
                                       'Remote access to the Dedicated Admin Connection (DAC) is not enabled. The DAC can make remote troubleshooting much easier when SQL Server is unresponsive.';
                            end;

                        if EXISTS(select *
                                  from sys.dm_os_schedulers
                                  where is_online = 0)
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 101)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 101) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 101                                   as checkid,
                                       50                                    as priority,
                                       'Performance'                         as findinggroup,
                                       'CPU Schedulers Offline'              as finding,
                                       'https://BrentOzar.com/go/schedulers' as url,
                                       'Some CPU cores are not accessible to SQL Server due to affinity masking or licensing problems.';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 110)
                            and EXISTS(select * from master.sys.all_objects where name = 'dm_os_memory_nodes')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 110) with nowait;

                                set @stringtoexecute = 'IF EXISTS (SELECT  *
												FROM sys.dm_os_nodes n
												INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
												WHERE n.node_state_desc = ''OFFLINE'')
												INSERT  INTO #BlitzResults
														( CheckID ,
														  Priority ,
														  FindingsGroup ,
														  Finding ,
														  URL ,
														  Details
														)
														SELECT  110 AS CheckID ,
																50 AS Priority ,
																''Performance'' AS FindingGroup ,
																''Memory Nodes Offline'' AS Finding ,
																''https://BrentOzar.com/go/schedulers'' AS URL ,
																''Due to affinity masking or licensing problems, some of the memory may not be available.'' OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if EXISTS(select *
                                  from sys.databases
                                  where state > 1)
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 102)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 102) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 102                                       as checkid,
                                       [name],
                                       20                                        as priority,
                                       'Reliability'                             as findinggroup,
                                       'Unusual Database State: ' + [state_desc] as finding,
                                       'https://BrentOzar.com/go/repair'         as url,
                                       'This database may not be online.'
                                from sys.databases
                                where state > 1;
                            end;

                        if EXISTS(select *
                                  from master.sys.extended_procedures)
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 105)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 105) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 105                                    as checkid,
                                       'master',
                                       200                                    as priority,
                                       'Reliability'                          as findinggroup,
                                       'Extended Stored Procedures in Master' as finding,
                                       'https://BrentOzar.com/go/clr'         as url,
                                       'The [' + name
                                           +
                                       '] extended stored procedure is in the master database. CLR may be in use, and the master database now needs to be part of your backup/recovery planning.'
                                from master.sys.extended_procedures;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 107)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 107) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 107                                             as checkid,
                                       50                                              as priority,
                                       'Performance'                                   as findinggroup,
                                       'Poison Wait Detected: ' + wait_type            as finding,
                                       'https://BrentOzar.com/go/poison/#' + wait_type as url,
                                       CONVERT(varchar(10), (SUM([wait_time_ms]) / 1000) / 86400) + ':' +
                                       CONVERT(varchar(20), DATEADD(s, (SUM([wait_time_ms]) / 1000), 0), 108) +
                                       ' of this wait have been recorded. This wait often indicates killer performance problems.'
                                from sys.[dm_os_wait_stats]
                                where wait_type in
                                      ('IO_QUEUE_LIMIT', 'IO_RETRY', 'LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR',
                                       'PREEMPTIVE_DEBUG', 'RESMGR_THROTTLED', 'RESOURCE_SEMAPHORE',
                                       'RESOURCE_SEMAPHORE_QUERY_COMPILE', 'SE_REPL_CATCHUP_THROTTLE',
                                       'SE_REPL_COMMIT_ACK', 'SE_REPL_COMMIT_TURN', 'SE_REPL_ROLLBACK_ACK',
                                       'SE_REPL_SLOW_SECONDARY_THROTTLE', 'THREADPOOL')
                                group by wait_type
                                having SUM([wait_time_ms]) >
                                       (select 5000 * datediff(hh, create_date, CURRENT_TIMESTAMP) as hours_since_startup
                                        from sys.databases
                                        where name = 'tempdb')
                                   and SUM([wait_time_ms]) > 60000;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 121)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 121) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 121                                          as checkid,
                                       50                                           as priority,
                                       'Performance'                                as findinggroup,
                                       'Poison Wait Detected: Serializable Locking' as finding,
                                       'https://BrentOzar.com/go/serializable'      as url,
                                       CONVERT(varchar(10), (SUM([wait_time_ms]) / 1000) / 86400) + ':' +
                                       CONVERT(varchar(20), DATEADD(s, (SUM([wait_time_ms]) / 1000), 0), 108) +
                                       ' of LCK_M_R% waits have been recorded. This wait often indicates killer performance problems.'
                                from sys.[dm_os_wait_stats]
                                where wait_type in
                                      ('LCK_M_RS_S', 'LCK_M_RS_U', 'LCK_M_RIn_NL', 'LCK_M_RIn_S', 'LCK_M_RIn_U',
                                       'LCK_M_RIn_X', 'LCK_M_RX_S', 'LCK_M_RX_U', 'LCK_M_RX_X')
                                having SUM([wait_time_ms]) >
                                       (select 5000 * datediff(hh, create_date, CURRENT_TIMESTAMP) as hours_since_startup
                                        from sys.databases
                                        where name = 'tempdb')
                                   and SUM([wait_time_ms]) > 60000;
                            end;


                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 111)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 111) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 databasename,
                                 url,
                                 details)
                                select 111                                 as checkid,
                                       50                                  as priority,
                                       'Reliability'                       as findinggroup,
                                       'Possibly Broken Log Shipping'      as finding,
                                       d.[name],
                                       'https://BrentOzar.com/go/shipping' as url,
                                       d.[name] +
                                       ' is in a restoring state, but has not had a backup applied in the last two days. This is a possible indication of a broken transaction log shipping setup.'
                                from [master].sys.databases d
                                         inner join [master].sys.database_mirroring dm on d.database_id = dm.database_id
                                    and dm.mirroring_role is null
                                where (d.[state] = 1
                                    or (d.[state] = 0 and d.[is_in_standby] = 1))
                                  and not EXISTS(select *
                                                 from msdb.dbo.restorehistory rh
                                                          inner join msdb.dbo.backupset bs on rh.backup_set_id = bs.backup_set_id
                                                 where d.[name] collate sql_latin1_general_cp1_ci_as =
                                                       rh.destination_database_name collate sql_latin1_general_cp1_ci_as
                                                   and rh.restore_date >= DATEADD(dd, -2, GETDATE()));

                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 112)
                            and EXISTS(select * from master.sys.all_objects where name = 'change_tracking_databases')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 112) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									DatabaseName,
									URL,
									Details)
							  SELECT 112 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Change Tracking Enabled'' AS Finding,
							  d.[name],
							  ''https://BrentOzar.com/go/tracking'' AS URL,
							  ( d.[name] + '' has change tracking enabled. This is not a default setting, and it has some performance overhead. It keeps track of changes to rows in tables that have change tracking turned on.'' ) AS Details FROM sys.change_tracking_databases AS ctd INNER JOIN sys.databases AS d ON ctd.database_id = d.database_id OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 116)
                            and EXISTS(select * from msdb.sys.all_columns where name = 'compressed_backup_size')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 116) with nowait

                                set @stringtoexecute = 'INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
									SELECT  116 AS CheckID ,
											200 AS Priority ,
											''Informational'' AS FindingGroup ,
											''Backup Compression Default Off''  AS Finding ,
											''https://BrentOzar.com/go/backup'' AS URL ,
											''Uncompressed full backups have happened recently, and backup compression is not turned on at the server level. Backup compression is included with SQL Server 2008R2 & newer, even in Standard Edition. We recommend turning backup compression on by default so that ad-hoc backups will get compressed.''
											FROM sys.configurations
											WHERE configuration_id = 1579 AND CAST(value_in_use AS INT) = 0
                                            AND EXISTS (SELECT * FROM msdb.dbo.backupset WHERE backup_size = compressed_backup_size AND type = ''D'' AND backup_finish_date >= DATEADD(DD, -14, GETDATE())) OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 117)
                            and
                           EXISTS(select * from master.sys.all_objects where name = 'dm_exec_query_resource_semaphores')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 117) with nowait;

                                set @stringtoexecute = 'IF 0 < (SELECT SUM([forced_grant_count]) FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL)
								INSERT INTO #BlitzResults
									(CheckID,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
							  SELECT 117 AS CheckID,
							  100 AS Priority,
							  ''Performance'' AS FindingsGroup,
							  ''Memory Pressure Affecting Queries'' AS Finding,
							  ''https://BrentOzar.com/go/grants'' AS URL,
							  CAST(SUM(forced_grant_count) AS NVARCHAR(100)) + '' forced grants reported in the DMV sys.dm_exec_query_resource_semaphores, indicating memory pressure has affected query runtimes.''
							  FROM sys.dm_exec_query_resource_semaphores WHERE [forced_grant_count] IS NOT NULL OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 124)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 124) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 124,
                                       150,
                                       'Performance',
                                       'Deadlocks Happening Daily',
                                       'https://BrentOzar.com/go/deadlocks',
                                       CAST(CAST(p.cntr_value / @daysuptime as bigint) as nvarchar(100)) +
                                       ' average deadlocks per day. To find them, run sp_BlitzLock.' as details
                                from sys.dm_os_performance_counters p
                                         inner join sys.databases d on d.name = 'tempdb'
                                where RTRIM(p.counter_name) = 'Number of Deadlocks/sec'
                                  and RTRIM(p.instance_name) = '_Total'
                                  and p.cntr_value > 0
                                  and (1.0 * p.cntr_value / NULLIF(datediff(dd, create_date, CURRENT_TIMESTAMP), 0)) >
                                      10;
                            end;

                        if DATEADD(mi, -15, GETDATE()) <
                           (select top 1 creation_time from sys.dm_exec_query_stats order by creation_time)
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 125)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 125) with nowait;

                                declare @user_perm_sql nvarchar(max) = N'';
                                declare @user_perm_gb_out decimal(38, 2);

                                if @productversionmajor >= 11
                                    begin

                                        set @user_perm_sql += N'
									SELECT @user_perm_gb = CASE WHEN (pages_kb / 128.0 / 1024.) >= 2.
											THEN CONVERT(DECIMAL(38, 2), (pages_kb / 128.0 / 1024.))
											ELSE NULL
										   END
									FROM sys.dm_os_memory_clerks
									WHERE type = ''USERSTORE_TOKENPERM''
									AND    name = ''TokenAndPermUserStore''
								';

                                    end

                                if @productversionmajor < 11
                                    begin
                                        set @user_perm_sql += N'
									SELECT @user_perm_gb = CASE WHEN ((single_pages_kb + multi_pages_kb) / 1024.0 / 1024.) >= 2.
											THEN CONVERT(DECIMAL(38, 2), ((single_pages_kb + multi_pages_kb)  / 1024.0 / 1024.))
											ELSE NULL
										   END
									FROM sys.dm_os_memory_clerks
									WHERE type = ''USERSTORE_TOKENPERM''
									AND    name = ''TokenAndPermUserStore''
								';

                                    end

                                exec sys.sp_executesql @user_perm_sql,
                                     N'@user_perm_gb DECIMAL(38,2) OUTPUT',
                                     @user_perm_gb = @user_perm_gb_out output

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select top 1 125,
                                             10,
                                             'Performance',
                                             'Plan Cache Erased Recently',
                                             'https://BrentOzar.com/askbrent/plan-cache-erased-recently/',
                                             'The oldest query in the plan cache was created at ' +
                                             CAST(creation_time as nvarchar(50))
                                                 + case
                                                       when @user_perm_gb_out is null
                                                           then '. Someone ran DBCC FREEPROCCACHE, restarted SQL Server, or it is under horrific memory pressure.'
                                                       else '. You also have ' +
                                                            CONVERT(nvarchar(20), @user_perm_gb_out) +
                                                            ' GB of USERSTORE_TOKENPERM, which could indicate unusual memory consumption.'
                                                 end
                                from sys.dm_exec_query_stats with (nolock)
                                order by creation_time;
                            end;

                        if EXISTS(select * from sys.configurations where name = 'priority boost'
                                                                     and (value = 1 or value_in_use = 1))
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 126)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 126) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                values (126, 5, 'Reliability', 'Priority Boost Enabled',
                                        'https://BrentOzar.com/go/priorityboost/',
                                        'Priority Boost sounds awesome, but it can actually cause your SQL Server to crash.');
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 128)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin

                                if (@productversionmajor = 15 and @productversionminor < 2000) or
                                   (@productversionmajor = 14 and @productversionminor < 1000) or
                                   (@productversionmajor = 13 and @productversionminor < 5026) or
                                   (@productversionmajor = 12 and @productversionminor < 6024) or
                                   (@productversionmajor = 11 and @productversionminor < 7001) or
                                   (@productversionmajor = 10.5 /*AND @ProductVersionMinor < 6000*/) or
                                   (@productversionmajor = 10 /*AND @ProductVersionMinor < 6000*/) or
                                   (@productversionmajor = 9 /*AND @ProductVersionMinor <= 5000*/)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 128) with nowait;

                                        insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                        values (128, 20, 'Reliability', 'Unsupported Build of SQL Server',
                                                'https://BrentOzar.com/go/unsupported',
                                                'Version ' + CAST(@productversionmajor as varchar(100)) +
                                                case
                                                    when @productversionmajor >= 11 then
                                                            '.' + CAST(@productversionminor as varchar(100)) +
                                                            ' is no longer supported by Microsoft. You need to apply a service pack.'
                                                    else ' is no longer supported by Microsoft. You should be making plans to upgrade to a modern version of SQL Server.' end);
                                    end;

                            end;

                        /* Reliability - Dangerous Build of SQL Server (Corruption) */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 129)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin
                                if (@productversionmajor = 11 and @productversionminor >= 3000 and
                                    @productversionminor <= 3436) or
                                   (@productversionmajor = 11 and @productversionminor = 5058) or
                                   (@productversionmajor = 12 and @productversionminor >= 2000 and
                                    @productversionminor <= 2342)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 129) with nowait;

                                        insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                        values (129, 20, 'Reliability', 'Dangerous Build of SQL Server (Corruption)',
                                                'http://sqlperformance.com/2014/06/sql-indexes/hotfix-sql-2012-rebuilds',
                                                'There are dangerous known bugs with version ' +
                                                CAST(@productversionmajor as varchar(100)) + '.' +
                                                CAST(@productversionminor as varchar(100)) +
                                                '. Check the URL for details and apply the right service pack or hotfix.');
                                    end;

                            end;

                        /* Reliability - Dangerous Build of SQL Server (Security) */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 157)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin
                                if (@productversionmajor = 10 and @productversionminor >= 5500 and
                                    @productversionminor <= 5512) or
                                   (@productversionmajor = 10 and @productversionminor >= 5750 and
                                    @productversionminor <= 5867) or
                                   (@productversionmajor = 10.5 and @productversionminor >= 4000 and
                                    @productversionminor <= 4017) or
                                   (@productversionmajor = 10.5 and @productversionminor >= 4251 and
                                    @productversionminor <= 4319) or
                                   (@productversionmajor = 11 and @productversionminor >= 3000 and
                                    @productversionminor <= 3129) or
                                   (@productversionmajor = 11 and @productversionminor >= 3300 and
                                    @productversionminor <= 3447) or
                                   (@productversionmajor = 12 and @productversionminor >= 2000 and
                                    @productversionminor <= 2253) or
                                   (@productversionmajor = 12 and @productversionminor >= 2300 and
                                    @productversionminor <= 2370)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 157) with nowait;

                                        insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                        values (157, 20, 'Reliability', 'Dangerous Build of SQL Server (Security)',
                                                'https://technet.microsoft.com/en-us/library/security/MS14-044',
                                                'There are dangerous known bugs with version ' +
                                                CAST(@productversionmajor as varchar(100)) + '.' +
                                                CAST(@productversionminor as varchar(100)) +
                                                '. Check the URL for details and apply the right service pack or hotfix.');
                                    end;

                            end;

                        /* Check if SQL 2016 Standard Edition but not SP1 */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 189)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin
                                if (@productversionmajor = 13 and @productversionminor < 4001 and
                                    @@VERSION like '%Standard Edition%')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 189) with nowait;

                                        insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                        values (189, 100, 'Features', 'Missing Features',
                                                'https://blogs.msdn.microsoft.com/sqlreleaseservices/sql-server-2016-service-pack-1-sp1-released/',
                                                'SQL 2016 Standard Edition is being used but not Service Pack 1. Check the URL for a list of Enterprise Features that are included in Standard Edition as of SP1.');
                                    end;

                            end;

                        /* Check if SQL 2017 but not CU3 */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 216)
                            and SERVERPROPERTY('EngineEdition') <> 8 /* Azure Managed Instances */
                            begin
                                if (@productversionmajor = 14 and @productversionminor < 3015)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 216) with nowait;

                                        insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                        values (216, 100, 'Features', 'Missing Features',
                                                'https://support.microsoft.com/en-us/help/4041814',
                                                'SQL 2017 is being used but not Cumulative Update 3. We''d recommend patching to take advantage of increased analytics when running BlitzCache.');
                                    end;

                            end;

                        /* Cumulative Update Available */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 217)
                            and
                           SERVERPROPERTY('EngineEdition') not in (5, 8) /* Azure Managed Instances and Azure SQL DB*/
                            and EXISTS(select * from information_schema.tables where table_name = 'SqlServerVersions'
                                                                                 and table_type = 'BASE TABLE')
                            and not EXISTS(
                                    select * from #blitzresults where checkid in (128, 129, 157, 189, 216)) /* Other version checks */
                            begin
                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 217) with nowait;

                                insert into #blitzresults(checkid, priority, findingsgroup, finding, url, details)
                                select top 1 217,
                                             100,
                                             'Reliability',
                                             'Cumulative Update Available',
                                             COALESCE(v.url, 'https://SQLServerUpdates.com/'),
                                             v.minorversionname + ' was released on ' +
                                             CAST(CONVERT(datetime, v.releasedate, 112) as varchar(100))
                                from dbo.sqlserverversions v
                                where v.majorversionnumber = @productversionmajor
                                  and v.minorversionnumber > @productversionminor
                                order by v.minorversionnumber desc;
                            end;

                        /* Performance - High Memory Use for In-Memory OLTP (Hekaton) */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 145)
                            and EXISTS(select *
                                       from sys.all_objects o
                                       where o.name = 'dm_db_xtp_table_memory_stats')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 145) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        SELECT 145 AS CheckID,
			                        10 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''High Memory Use for In-Memory OLTP (Hekaton)'' AS Finding,
			                        ''https://BrentOzar.com/go/hekaton'' AS URL,
			                        CAST(CAST((SUM(mem.pages_kb / 1024.0) / CAST(value_in_use AS INT) * 100) AS INT) AS NVARCHAR(100)) + ''% of your '' + CAST(CAST((CAST(value_in_use AS DECIMAL(38,1)) / 1024) AS MONEY) AS NVARCHAR(100)) + ''GB of your max server memory is being used for in-memory OLTP tables (Hekaton). Microsoft recommends having 2X your Hekaton table space available in memory just for Hekaton, with a max of 250GB of in-memory data regardless of your server memory capacity.'' AS Details
			                        FROM sys.configurations c INNER JOIN sys.dm_os_memory_clerks mem ON mem.type = ''MEMORYCLERK_XTP''
                                    WHERE c.name = ''max server memory (MB)''
                                    GROUP BY c.value_in_use
                                    HAVING CAST(value_in_use AS DECIMAL(38,2)) * .25 < SUM(mem.pages_kb / 1024.0)
                                      OR SUM(mem.pages_kb / 1024.0) > 250000 OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* Performance - In-Memory OLTP (Hekaton) In Use */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 146)
                            and EXISTS(select *
                                       from sys.all_objects o
                                       where o.name = 'dm_db_xtp_table_memory_stats')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 146) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        SELECT 146 AS CheckID,
			                        200 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''In-Memory OLTP (Hekaton) In Use'' AS Finding,
			                        ''https://BrentOzar.com/go/hekaton'' AS URL,
			                        CAST(CAST((SUM(mem.pages_kb / 1024.0) / CAST(value_in_use AS INT) * 100) AS INT) AS NVARCHAR(100)) + ''% of your '' + CAST(CAST((CAST(value_in_use AS DECIMAL(38,1)) / 1024) AS MONEY) AS NVARCHAR(100)) + ''GB of your max server memory is being used for in-memory OLTP tables (Hekaton).'' AS Details
			                        FROM sys.configurations c INNER JOIN sys.dm_os_memory_clerks mem ON mem.type = ''MEMORYCLERK_XTP''
                                    WHERE c.name = ''max server memory (MB)''
                                    GROUP BY c.value_in_use
                                    HAVING SUM(mem.pages_kb / 1024.0) > 10 OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* In-Memory OLTP (Hekaton) - Transaction Errors */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 147)
                            and EXISTS(select *
                                       from sys.all_objects o
                                       where o.name = 'dm_xtp_transaction_stats')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 147) with nowait

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        SELECT 147 AS CheckID,
			                        100 AS Priority,
			                        ''In-Memory OLTP (Hekaton)'' AS FindingsGroup,
			                        ''Transaction Errors'' AS Finding,
			                        ''https://BrentOzar.com/go/hekaton'' AS URL,
			                        ''Since restart: '' + CAST(validation_failures AS NVARCHAR(100)) + '' validation failures, '' + CAST(dependencies_failed AS NVARCHAR(100)) + '' dependency failures, '' + CAST(write_conflicts AS NVARCHAR(100)) + '' write conflicts, '' + CAST(unique_constraint_violations AS NVARCHAR(100)) + '' unique constraint violations.'' AS Details
			                        FROM sys.dm_xtp_transaction_stats
                                    WHERE validation_failures <> 0
                                            OR dependencies_failed <> 0
                                            OR write_conflicts <> 0
                                            OR unique_constraint_violations <> 0 OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* Reliability - Database Files on Network File Shares */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 148)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 148) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 148                                                               as checkid,
                                                d.[name]                                                          as databasename,
                                                170                                                               as priority,
                                                'Reliability'                                                     as findingsgroup,
                                                'Database Files on Network File Shares'                           as finding,
                                                'https://BrentOzar.com/go/nas'                                    as url,
                                                ('Files for this database are on: ' + LEFT(mf.physical_name, 30)) as details
                                from sys.databases d
                                         inner join sys.master_files mf on d.database_id = mf.database_id
                                where mf.physical_name like '\\%'
                                  and d.name not in (select distinct databasename
                                                     from #skipchecks
                                                     where checkid is null
                                                        or checkid = 148);
                            end;

                        /* Reliability - Database Files Stored in Azure */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 149)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 149) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 149                                                               as checkid,
                                                d.[name]                                                          as databasename,
                                                170                                                               as priority,
                                                'Reliability'                                                     as findingsgroup,
                                                'Database Files Stored in Azure'                                  as finding,
                                                'https://BrentOzar.com/go/azurefiles'                             as url,
                                                ('Files for this database are on: ' + LEFT(mf.physical_name, 30)) as details
                                from sys.databases d
                                         inner join sys.master_files mf on d.database_id = mf.database_id
                                where mf.physical_name like 'http://%'
                                  and d.name not in (select distinct databasename
                                                     from #skipchecks
                                                     where checkid is null
                                                        or checkid = 149);
                            end;

                        /* Reliability - Errors Logged Recently in the Default Trace */

                        /* First, let's check that there aren't any issues with the trace files */
                        begin try

                            insert into #fntracegettable
                            (textdata,
                             databasename,
                             eventclass,
                             severity,
                             starttime,
                             endtime,
                             duration,
                             ntusername,
                             ntdomainname,
                             hostname,
                             applicationname,
                             loginname,
                             dbusername)
                            select top 20000 CONVERT(nvarchar(4000), t.textdata),
                                             t.databasename,
                                             t.eventclass,
                                             t.severity,
                                             t.starttime,
                                             t.endtime,
                                             t.duration,
                                             t.ntusername,
                                             t.ntdomainname,
                                             t.hostname,
                                             t.applicationname,
                                             t.loginname,
                                             t.dbusername
                            from sys.fn_trace_gettable(@base_tracefilename, DEFAULT) t
                            where (
                                    t.eventclass = 22
                                    and t.severity >= 17
                                    and t.starttime > DATEADD(dd, -30, GETDATE())
                                )
                               or (
                                    t.eventclass in (92, 93)
                                    and t.starttime > DATEADD(dd, -30, GETDATE())
                                    and t.duration > 15000000
                                )
                               or (
                                t.eventclass in (94, 95, 116)
                                )

                            set @tracefileissue = 0

                        end try
                        begin catch

                            set @tracefileissue = 1

                        end catch

                        if @tracefileissue = 1
                            begin
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 199)
                                    insert into #blitzresults
                                    (checkid,
                                     databasename,
                                     priority,
                                     findingsgroup,
                                     finding,
                                     url,
                                     details)
                                    select '199'                                      as checkid,
                                           ''                                         as databasename,
                                           50                                         as priority,
                                           'Reliability'                              as findingsgroup,
                                           'There Is An Error With The Default Trace' as finding,
                                           'https://BrentOzar.com/go/defaulttrace'    as url,
                                           'Somebody has been messing with your trace files. Check the files are present at ' +
                                           @base_tracefilename                        as details
                            end

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 150)
                            and @base_tracefilename is not null
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 150) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 150                                           as checkid,
                                                t.databasename,
                                                50                                            as priority,
                                                'Reliability'                                 as findingsgroup,
                                                'Errors Logged Recently in the Default Trace' as finding,
                                                'https://BrentOzar.com/go/defaulttrace'       as url,
                                                CAST(t.textdata as nvarchar(4000))            as details
                                from #fntracegettable t
                                where t.eventclass = 22
                                /* Removed these as they're unnecessary, we filter this when inserting data into #fnTraceGettable */
                                --AND t.Severity >= 17
                                --AND t.StartTime > DATEADD(dd, -30, GETDATE());
                            end;

                        /* Performance - File Growths Slow */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 151)
                            and @base_tracefilename is not null
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 151) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 151                                                                                                 as checkid,
                                                t.databasename,
                                                50                                                                                                  as priority,
                                                'Performance'                                                                                       as findingsgroup,
                                                'File Growths Slow'                                                                                 as finding,
                                                'https://BrentOzar.com/go/filegrowth'                                                               as url,
                                                CAST(COUNT(*) as nvarchar(100)) +
                                                ' growths took more than 15 seconds each. Consider setting file autogrowth to a smaller increment.' as details
                                from #fntracegettable t
                                where t.eventclass in (92, 93)
                                    /* Removed these as they're unnecessary, we filter this when inserting data into #fnTraceGettable */
                                      --AND t.StartTime > DATEADD(dd, -30, GETDATE())
                                      --AND t.Duration > 15000000
                                group by t.databasename
                                having COUNT(*) > 1;
                            end;

                        /* Performance - Many Plans for One Query */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 160)
                            and EXISTS(select * from sys.all_columns where name = 'query_hash')
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 160) with nowait;

                                set @stringtoexecute = N'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        SELECT TOP 1 160 AS CheckID,
			                        100 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''Many Plans for One Query'' AS Finding,
			                        ''https://BrentOzar.com/go/parameterization'' AS URL,
			                        CAST(COUNT(DISTINCT plan_handle) AS NVARCHAR(50)) + '' plans are present for a single query in the plan cache - meaning we probably have parameterization issues.'' AS Details
			                        FROM sys.dm_exec_query_stats qs
                                    CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) pa
                                    WHERE pa.attribute = ''dbid''
                                    GROUP BY qs.query_hash, pa.value
                                    HAVING COUNT(DISTINCT plan_handle) > ';

                                if 50 > (select COUNT(*) from sys.databases)
                                    set @stringtoexecute = @stringtoexecute + N' 50 ';
                                else
                                    select @stringtoexecute = @stringtoexecute + CAST(COUNT(*) * 2 as nvarchar(50))
                                    from sys.databases;

                                set @stringtoexecute = @stringtoexecute +
                                                       N' ORDER BY COUNT(DISTINCT plan_handle) DESC OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* Performance - High Number of Cached Plans */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 161)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 161) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        SELECT TOP 1 161 AS CheckID,
			                        100 AS Priority,
			                        ''Performance'' AS FindingsGroup,
			                        ''High Number of Cached Plans'' AS Finding,
			                        ''https://BrentOzar.com/go/planlimits'' AS URL,
			                        ''Your server configuration is limited to '' + CAST(ht.buckets_count * 4 AS VARCHAR(20)) + '' '' + ht.name + '', and you are currently caching '' + CAST(cc.entries_count AS VARCHAR(20)) + ''.'' AS Details
			                        FROM sys.dm_os_memory_cache_hash_tables ht
			                        INNER JOIN sys.dm_os_memory_cache_counters cc ON ht.name = cc.name AND ht.type = cc.type
			                        where ht.name IN ( ''SQL Plans'' , ''Object Plans'' , ''Bound Trees'' )
			                        AND cc.entries_count >= (3 * ht.buckets_count) OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* Performance - Too Much Free Memory */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 165)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 165) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 165,
                                       50,
                                       'Performance',
                                       'Too Much Free Memory',
                                       'https://BrentOzar.com/go/freememory',
                                       CAST((CAST(cfree.cntr_value as bigint) / 1024 / 1024) as nvarchar(100)) +
                                       N'GB of free memory inside SQL Server''s buffer pool, which is ' +
                                       CAST((CAST(ctotal.cntr_value as bigint) / 1024 / 1024) as nvarchar(100)) +
                                       N'GB. You would think lots of free memory would be good, but check out the URL for more information.' as details
                                from sys.dm_os_performance_counters cfree
                                         inner join sys.dm_os_performance_counters ctotal
                                                    on ctotal.object_name like N'%Memory Manager%'
                                                        and ctotal.counter_name =
                                                            N'Total Server Memory (KB)                                                                                                        '
                                where cfree.object_name like N'%Memory Manager%'
                                  and cfree.counter_name =
                                      N'Free Memory (KB)                                                                                                                '
                                  and CAST(ctotal.cntr_value as bigint) > 20480000000
                                  and CAST(ctotal.cntr_value as bigint) * .3 <= CAST(cfree.cntr_value as bigint)
                                  and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Standard%';

                            end;

                        /* Outdated sp_Blitz - sp_Blitz is Over 6 Months Old */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 155)
                            and DATEDIFF(mm, @versiondate, GETDATE()) > 6
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 155) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 155                                                                                                                                                   as checkid,
                                       0                                                                                                                                                     as priority,
                                       'Outdated sp_Blitz'                                                                                                                                   as findingsgroup,
                                       'sp_Blitz is Over 6 Months Old'                                                                                                                       as finding,
                                       'http://FirstResponderKit.org/'                                                                                                                       as url,
                                       'Some things get better with age, like fine wine and your T-SQL. However, sp_Blitz is not one of those things - time to go download the current one.' as details;
                            end;

                        /* Populate a list of database defaults. I'm doing this kind of oddly -
						    it reads like a lot of work, but this way it compiles & runs on all
						    versions of SQL Server.
						*/

                        if @debug in (1, 2) raiserror ('Generating database defaults.', 0, 1) with nowait;

                        insert into #databasedefaults
                        select 'is_supplemental_logging_enabled',
                               0,
                               131,
                               210,
                               'Supplemental Logging Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_supplemental_logging_enabled'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'snapshot_isolation_state',
                               0,
                               132,
                               210,
                               'Snapshot Isolation Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'snapshot_isolation_state'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_read_committed_snapshot_on',
                               case when SERVERPROPERTY('EngineEdition') = 5 then 1 else 0 end, /* RCSI is always enabled in Azure SQL DB per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */
                               133,
                               210,
                               case
                                   when SERVERPROPERTY('EngineEdition') = 5
                                       then 'Read Committed Snapshot Isolation Disabled'
                                   else 'Read Committed Snapshot Isolation Enabled' end,
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_read_committed_snapshot_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_auto_create_stats_incremental_on',
                               0,
                               134,
                               210,
                               'Auto Create Stats Incremental Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_auto_create_stats_incremental_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_ansi_null_default_on',
                               0,
                               135,
                               210,
                               'ANSI NULL Default Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_ansi_null_default_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_recursive_triggers_on',
                               0,
                               136,
                               210,
                               'Recursive Triggers Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_recursive_triggers_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_trustworthy_on',
                               0,
                               137,
                               210,
                               'Trustworthy Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_trustworthy_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_broker_enabled',
                               0,
                               230,
                               210,
                               'Broker Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_broker_enabled'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_honor_broker_priority_on',
                               0,
                               231,
                               210,
                               'Honor Broker Priority Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_honor_broker_priority_on'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_parameterization_forced',
                               0,
                               138,
                               210,
                               'Forced Parameterization Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_parameterization_forced'
                          and object_id = OBJECT_ID('sys.databases');
                        /* Not alerting for this since we actually want it and we have a separate check for it:
						INSERT INTO #DatabaseDefaults
						  SELECT 'is_query_store_on', 0, 139, 210, 'Query Store Enabled', 'https://BrentOzar.com/go/dbdefaults', NULL
						  FROM sys.all_columns
						  WHERE name = 'is_query_store_on' AND object_id = OBJECT_ID('sys.databases');
						*/
                        insert into #databasedefaults
                        select 'is_cdc_enabled',
                               0,
                               140,
                               210,
                               'Change Data Capture Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_cdc_enabled'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'containment',
                               0,
                               141,
                               210,
                               'Containment Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'containment'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'target_recovery_time_in_seconds',
                               0,
                               142,
                               210,
                               'Target Recovery Time Changed',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'target_recovery_time_in_seconds'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'delayed_durability',
                               0,
                               143,
                               210,
                               'Delayed Durability Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'delayed_durability'
                          and object_id = OBJECT_ID('sys.databases');
                        insert into #databasedefaults
                        select 'is_memory_optimized_elevate_to_snapshot_on',
                               0,
                               144,
                               210,
                               'Memory Optimized Enabled',
                               'https://BrentOzar.com/go/dbdefaults',
                               null
                        from sys.all_columns
                        where name = 'is_memory_optimized_elevate_to_snapshot_on'
                          and object_id = OBJECT_ID('sys.databases')
                          and SERVERPROPERTY('EngineEdition') <> 8; /* Hekaton is always enabled in Managed Instances per https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1919 */

                        declare databasedefaultsloop cursor for
                            select name, defaultvalue, checkid, priority, finding, url, details
                            from #databasedefaults;

                        open databasedefaultsloop;
                        fetch next from databasedefaultsloop into @currentname, @currentdefaultvalue, @currentcheckid, @currentpriority, @currentfinding, @currenturl, @currentdetails;
                        while @@FETCH_STATUS = 0
                            begin

                                if @debug in (1, 2)
                                    raiserror ('Running CheckId [%d].', 0, 1, @currentcheckid) with nowait;

                                /* Target Recovery Time (142) can be either 0 or 60 due to a number of bugs */
                                if @currentcheckid = 142
                                    set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
								   SELECT ' + CAST(@currentcheckid as nvarchar(200)) + ', d.[name], ' +
                                                           CAST(@currentpriority as nvarchar(200)) +
                                                           ', ''Non-Default Database Config'', ''' + @currentfinding +
                                                           ''',''' + @currenturl + ''',''' +
                                                           COALESCE(@currentdetails, 'This database setting is not the default.') + '''
									FROM sys.databases d
									WHERE d.database_id > 4 AND d.state <> 1 AND (d.[' + @currentname +
                                                           '] NOT IN (0, 60) OR d.[' + @currentname +
                                                           '] IS NULL) OPTION (RECOMPILE);';
                                else
                                    set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
								   SELECT ' + CAST(@currentcheckid as nvarchar(200)) + ', d.[name], ' +
                                                           CAST(@currentpriority as nvarchar(200)) +
                                                           ', ''Non-Default Database Config'', ''' + @currentfinding +
                                                           ''',''' + @currenturl + ''',''' +
                                                           COALESCE(@currentdetails, 'This database setting is not the default.') + '''
									FROM sys.databases d
									WHERE d.database_id > 4 AND d.state <> 1 AND (d.[' + @currentname + '] <> ' +
                                                           @currentdefaultvalue + ' OR d.[' + @currentname +
                                                           '] IS NULL) OPTION (RECOMPILE);';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                exec (@stringtoexecute);

                                fetch next from databasedefaultsloop into @currentname, @currentdefaultvalue, @currentcheckid, @currentpriority, @currentfinding, @currenturl, @currentdetails;
                            end;

                        close databasedefaultsloop;
                        deallocate databasedefaultsloop;


/*This checks to see if Agent is Offline*/
                        if @productversionmajor >= 10
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 167)
                            begin
                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_services')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 167) with nowait;

                                        insert into [#BlitzResults]
                                        ([CheckID],
                                         [Priority],
                                         [FindingsGroup],
                                         [Finding],
                                         [URL],
                                         [Details])

                                        select 167                          as [CheckID],
                                               250                          as [Priority],
                                               'Server Info'                as [FindingsGroup],
                                               'Agent is Currently Offline' as [Finding],
                                               ''                           as [URL],
                                               ('Oops! It looks like the ' + [servicename] + ' service is ' +
                                                [status_desc] + '. The startup type is ' + [startup_type_desc] + '.'
                                                   )                        as [Details]
                                        from [sys].[dm_server_services]
                                        where [status_desc] <> 'Running'
                                          and [servicename] like 'SQL Server Agent%'
                                          and CAST(SERVERPROPERTY('Edition') as varchar(1000)) not like '%xpress%';

                                    end;
                            end;

/*This checks to see if the Full Text thingy is offline*/
                        if @productversionmajor >= 10
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 168)
                            begin
                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_services')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 168) with nowait;

                                        insert into [#BlitzResults]
                                        ([CheckID],
                                         [Priority],
                                         [FindingsGroup],
                                         [Finding],
                                         [URL],
                                         [Details])

                                        select 168                                                     as [CheckID],
                                               250                                                     as [Priority],
                                               'Server Info'                                           as [FindingsGroup],
                                               'Full-text Filter Daemon Launcher is Currently Offline' as [Finding],
                                               ''                                                      as [URL],
                                               ('Oops! It looks like the ' + [servicename] + ' service is ' +
                                                [status_desc] + '. The startup type is ' + [startup_type_desc] + '.'
                                                   )                                                   as [Details]
                                        from [sys].[dm_server_services]
                                        where [status_desc] <> 'Running'
                                          and [servicename] like 'SQL Full-text Filter Daemon Launcher%';

                                    end;
                            end;

/*This checks which service account SQL Server is running as.*/
                        if @productversionmajor >= 10
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 169)
                            begin
                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_services')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 169) with nowait;

                                        insert into [#BlitzResults]
                                        ([CheckID],
                                         [Priority],
                                         [FindingsGroup],
                                         [Finding],
                                         [URL],
                                         [Details])

                                        select 169                                                 as [CheckID],
                                               250                                                 as [Priority],
                                               'Informational'                                     as [FindingsGroup],
                                               'SQL Server is running under an NT Service account' as [Finding],
                                               'https://BrentOzar.com/go/setup'                    as [URL],
                                               ('I''m running as ' + [service_account] +
                                                '. I wish I had an Active Directory service account instead.'
                                                   )                                               as [Details]
                                        from [sys].[dm_server_services]
                                        where [service_account] like 'NT Service%'
                                          and [servicename] like 'SQL Server%'
                                          and [servicename] not like 'SQL Server Agent%';

                                    end;
                            end;

/*This checks which service account SQL Agent is running as.*/
                        if @productversionmajor >= 10
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 170)
                            begin
                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_services')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 170) with nowait;

                                        insert into [#BlitzResults]
                                        ([CheckID],
                                         [Priority],
                                         [FindingsGroup],
                                         [Finding],
                                         [URL],
                                         [Details])

                                        select 170                                                       as [CheckID],
                                               250                                                       as [Priority],
                                               'Informational'                                           as [FindingsGroup],
                                               'SQL Server Agent is running under an NT Service account' as [Finding],
                                               'https://BrentOzar.com/go/setup'                          as [URL],
                                               ('I''m running as ' + [service_account] +
                                                '. I wish I had an Active Directory service account instead.'
                                                   )                                                     as [Details]
                                        from [sys].[dm_server_services]
                                        where [service_account] like 'NT Service%'
                                          and [servicename] like 'SQL Server Agent%';

                                    end;
                            end;

/*This checks that First Responder Kit is consistent.
It assumes that all the objects of the kit resides in the same database, the one in which this SP is stored
It also is ready to check for installation in another schema.
*/
                        if (
                            not EXISTS(
                                    select 1
                                    from #skipchecks
                                    where databasename is null
                                      and checkid = 226
                                )
                            )
                            begin

                                if @debug in (1, 2) raiserror ('Running check with id %d',0,1,2000);

                                set @spblitzfullname = QUOTENAME(DB_NAME()) + '.' +
                                                       QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID)) + '.' +
                                                       QUOTENAME(OBJECT_NAME(@@PROCID));
                                set @blitzisoutdatedcomparedtoothers = 0;
                                set @tsql = null;
                                set @versioncheckmodeexiststsql = null;
                                set @blitzprocdbname = DB_NAME();
                                set @execret = null;
                                set @innerexecret = null;
                                set @tmpcnt = null;

                                set @previouscomponentname = null;
                                set @previouscomponentfullpath = null;
                                set @currentstatementid = null;
                                set @currentcomponentschema = null;
                                set @currentcomponentname = null;
                                set @currentcomponenttype = null;
                                set @currentcomponentversiondate = null;
                                set @currentcomponentfullname = null;
                                set @currentcomponentmandatory = null;
                                set @maximumversiondate = null;

                                set @statementcheckname = null;
                                set @statementoutputscounter = null;
                                set @outputcounterexpectedvalue = null;
                                set @statementoutputsexecret = null;
                                set @statementoutputsdatetime = null;

                                set @currentcomponentmandatorycheckok = null;
                                set @currentcomponentversioncheckmodeok = null;

                                set @canexitloop = 0;
                                set @frkisconsistent = 0;


                                set @tsql = 'USE ' + QUOTENAME(@blitzprocdbname) + ';' + @crlf +
                                            'WITH FRKComponents (' + @crlf +
                                            '    ObjectName,' + @crlf +
                                            '    ObjectType,' + @crlf +
                                            '    MandatoryComponent' + @crlf +
                                            ')' + @crlf +
                                            'AS (' + @crlf +
                                            '    SELECT ''sp_AllNightLog'',''P'' ,0' + @crlf +
                                            '    UNION ALL' + @crlf +
                                            '    SELECT ''sp_AllNightLog_Setup'', ''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_Blitz'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzBackups'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzCache'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzFirst'',''P'',0' + @crlf +
                                            '    UNION ALL' + @crlf +
                                            '    SELECT ''sp_BlitzIndex'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzLock'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzQueryStore'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_BlitzWho'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_DatabaseRestore'',''P'',0' + @crlf +
                                            '    UNION ALL ' + @crlf +
                                            '    SELECT ''sp_ineachdb'',''P'',0' + @crlf +
                                            '    UNION ALL' + @crlf +
                                            '    SELECT ''SqlServerVersions'',''U'',0' + @crlf +
                                            ')' + @crlf +
                                            'INSERT INTO #FRKObjects (' + @crlf +
                                            '    DatabaseName,ObjectSchemaName,ObjectName, ObjectType,MandatoryComponent' +
                                            @crlf +
                                            ')' + @crlf +
                                            'SELECT DB_NAME(),SCHEMA_NAME(o.schema_id), c.ObjectName,c.ObjectType,c.MandatoryComponent' +
                                            @crlf +
                                            'FROM ' + @crlf +
                                            '    FRKComponents c' + @crlf +
                                            'LEFT JOIN ' + @crlf +
                                            '    sys.objects o' + @crlf +
                                            'ON c.ObjectName  = o.[name]' + @crlf +
                                            'AND c.ObjectType = o.[type]' + @crlf +
                                    --'WHERE o.schema_id IS NOT NULL' + @crlf +
                                            ';';

                                exec @execret = sp_executesql @tsql;

                                -- TODO: add check for statement success

                                -- TODO: based on SP requirements and presence (SchemaName is not null) ==> update MandatoryComponent column

                                -- Filling #StatementsToRun4FRKVersionCheck
                                insert into #statementstorun4frkversioncheck (checkname, statementtext, subjectname,
                                                                              subjectfullpath, statementoutputscounter,
                                                                              outputcounterexpectedvalue,
                                                                              statementoutputsexecret,
                                                                              statementoutputsdatetime)
                                select 'Mandatory',
                                       'SELECT @cnt = COUNT(*) FROM #FRKObjects WHERE ObjectSchemaName IS NULL AND ObjectName = ''' +
                                       objectname + ''' AND MandatoryComponent = 1;',
                                       objectname,
                                       QUOTENAME(databasename) + '.' + QUOTENAME(objectschemaname) + '.' +
                                       QUOTENAME(objectname),
                                       1,
                                       0,
                                       0,
                                       0
                                from #frkobjects
                                union all
                                select 'VersionCheckMode',
                                       'SELECT @cnt = COUNT(*) FROM ' +
                                       QUOTENAME(databasename) + '.sys.all_parameters ' +
                                       'where object_id = OBJECT_ID(''' + QUOTENAME(databasename) + '.' +
                                       QUOTENAME(objectschemaname) + '.' + QUOTENAME(objectname) +
                                       ''') AND [name] = ''@VersionCheckMode'';',
                                       objectname,
                                       QUOTENAME(databasename) + '.' + QUOTENAME(objectschemaname) + '.' +
                                       QUOTENAME(objectname),
                                       1,
                                       1,
                                       0,
                                       0
                                from #frkobjects
                                where objecttype = 'P'
                                  and objectschemaname is not null
                                union all
                                select 'VersionCheck',
                                       'EXEC @ExecRet = ' + QUOTENAME(databasename) + '.' +
                                       QUOTENAME(objectschemaname) + '.' + QUOTENAME(objectname) +
                                       ' @VersionCheckMode = 1 , @VersionDate = @ObjDate OUTPUT;',
                                       objectname,
                                       QUOTENAME(databasename) + '.' + QUOTENAME(objectschemaname) + '.' +
                                       QUOTENAME(objectname),
                                       0,
                                       0,
                                       1,
                                       1
                                from #frkobjects
                                where objecttype = 'P'
                                  and objectschemaname is not null;
                                if (@debug in (1, 2))
                                    begin
                                        select *
                                        from #statementstorun4frkversioncheck
                                        order by subjectname, subjectfullpath, statementid -- in case of schema change  ;
                                    end;


                                -- loop on queries...
                                while(@canexitloop = 0)
                                    begin
                                        set @currentstatementid = null;

                                        select top 1 @statementcheckname = checkname,
                                                     @currentstatementid = statementid,
                                                     @currentcomponentname = subjectname,
                                                     @currentcomponentfullname = subjectfullpath,
                                                     @tsql = statementtext,
                                                     @statementoutputscounter = statementoutputscounter,
                                                     @outputcounterexpectedvalue = outputcounterexpectedvalue,
                                                     @statementoutputsexecret = statementoutputsexecret,
                                                     @statementoutputsdatetime = statementoutputsdatetime
                                        from #statementstorun4frkversioncheck
                                        order by subjectname, subjectfullpath, statementid /* in case of schema change */
                                        ;

                                        -- loop exit condition
                                        if (@currentstatementid is null)
                                            begin
                                                break;
                                            end;

                                        if @debug in (1, 2) raiserror ('    Statement: %s',0,1,@tsql);

                                        -- we start a new component
                                        if (@previouscomponentname is null or
                                            (@previouscomponentname is not null and
                                             @previouscomponentname <> @currentcomponentname) or
                                            (@previouscomponentname is not null and
                                             @previouscomponentname = @currentcomponentname and
                                             @previouscomponentfullpath <> @currentcomponentfullname)
                                            )
                                            begin
                                                -- reset variables
                                                set @currentcomponentmandatorycheckok = 0;
                                                set @currentcomponentversioncheckmodeok = 0;
                                                set @previouscomponentname = @currentcomponentname;
                                                set @previouscomponentfullpath = @currentcomponentfullname;
                                            end;

                                        if (@statementcheckname not in
                                            ('Mandatory', 'VersionCheckMode', 'VersionCheck'))
                                            begin
                                                insert into #blitzresults(checkid,
                                                                          priority,
                                                                          findingsgroup,
                                                                          finding,
                                                                          url,
                                                                          details)
                                                select 226                                             as checkid,
                                                       253                                             as priority,
                                                       'First Responder Kit'                           as findingsgroup,
                                                       'Version Check Failed (code generator changed)' as finding,
                                                       'http://FirstResponderKit.org'                  as url,
                                                       'Download an updated First Responder Kit. Your version check failed because a change has been made to the version check code generator.' +
                                                       @crlf +
                                                       'Error: No handler for check with name "' +
                                                       ISNULL(@statementcheckname, '') + '"'           as details;

                                                -- we will stop the test because it's possible to get the same message for other components
                                                set @canexitloop = 1;
                                                continue;
                                            end;

                                        if (@statementcheckname = 'Mandatory')
                                            begin
                                                -- outputs counter
                                                exec @execret = sp_executesql @tsql, N'@cnt INT OUTPUT', @cnt = @tmpcnt output;

                                                if (@execret <> 0)
                                                    begin

                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                                            as checkid,
                                                               253                                            as priority,
                                                               'First Responder Kit'                          as findingsgroup,
                                                               'Version Check Failed (dynamic query failure)' as finding,
                                                               'http://FirstResponderKit.org'                 as url,
                                                               'Download an updated First Responder Kit. Your version check failed due to dynamic query failure.' +
                                                               @crlf +
                                                               'Error: following query failed at execution (check if component [' +
                                                               ISNULL(@currentcomponentname, @currentcomponentname) +
                                                               '] is mandatory and missing)' + @crlf +
                                                               @tsql                                          as details;

                                                        -- we will stop the test because it's possible to get the same message for other components
                                                        set @canexitloop = 1;
                                                        continue;
                                                    end;

                                                if (@tmpcnt <> @outputcounterexpectedvalue)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 227                                                                     as checkid,
                                                               253                                                                     as priority,
                                                               'First Responder Kit'                                                   as findingsgroup,
                                                               'Component Missing: ' + @currentcomponentname                           as finding,
                                                               'http://FirstResponderKit.org'                                          as url,
                                                               'Download an updated version of the First Responder Kit to install it.' as details;

                                                        -- as it's missing, no value for SubjectFullPath
                                                        delete
                                                        from #statementstorun4frkversioncheck
                                                        where subjectname = @currentcomponentname;
                                                        continue;
                                                    end;

                                                set @currentcomponentmandatorycheckok = 1;
                                            end;

                                        if (@statementcheckname = 'VersionCheckMode')
                                            begin
                                                if (@currentcomponentmandatorycheckok = 0)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                                                            as checkid,
                                                               253                                                            as priority,
                                                               'First Responder Kit'                                          as findingsgroup,
                                                               'Version Check Failed (unexpectedly modified checks ordering)' as finding,
                                                               'http://FirstResponderKit.org'                                 as url,
                                                               'Download an updated First Responder Kit. Version check failed because "Mandatory" check has not been completed before for current component' +
                                                               @crlf +
                                                               'Error: version check mode happenned before "Mandatory" check for component called "' +
                                                               @currentcomponentfullname + '"';

                                                        -- we will stop the test because it's possible to get the same message for other components
                                                        set @canexitloop = 1;
                                                        continue;
                                                    end;

                                                -- outputs counter
                                                exec @execret = sp_executesql @tsql, N'@cnt INT OUTPUT', @cnt = @tmpcnt output;

                                                if (@execret <> 0)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                                            as checkid,
                                                               253                                            as priority,
                                                               'First Responder Kit'                          as findingsgroup,
                                                               'Version Check Failed (dynamic query failure)' as finding,
                                                               'http://FirstResponderKit.org'                 as url,
                                                               'Download an updated First Responder Kit. Version check failed because a change has been made to the code generator.' +
                                                               @crlf +
                                                               'Error: following query failed at execution (check if component [' +
                                                               @currentcomponentfullname +
                                                               '] can run in VersionCheckMode)' + @crlf +
                                                               @tsql                                          as details;

                                                        -- we will stop the test because it's possible to get the same message for other components
                                                        set @canexitloop = 1;
                                                        continue;
                                                    end;

                                                if (@tmpcnt <> @outputcounterexpectedvalue)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 228                                                                                                                                            as checkid,
                                                               253                                                                                                                                            as priority,
                                                               'First Responder Kit'                                                                                                                          as findingsgroup,
                                                               'Component Outdated: ' + @currentcomponentfullname                                                                                             as finding,
                                                               'http://FirstResponderKit.org'                                                                                                                 as url,
                                                               'Download an updated First Responder Kit. Component ' +
                                                               @currentcomponentfullname +
                                                               ' is not at the minimum version required to run this procedure' +
                                                               @crlf +
                                                               'VersionCheckMode has been introduced in component version date after "20190320". This means its version is lower than or equal to that date.' as details;
                                                        ;

                                                        delete
                                                        from #statementstorun4frkversioncheck
                                                        where subjectfullpath = @currentcomponentfullname;
                                                        continue;
                                                    end;

                                                set @currentcomponentversioncheckmodeok = 1;
                                            end;

                                        if (@statementcheckname = 'VersionCheck')
                                            begin
                                                if (@currentcomponentmandatorycheckok = 0 or
                                                    @currentcomponentversioncheckmodeok = 0)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                                                            as checkid,
                                                               253                                                            as priority,
                                                               'First Responder Kit'                                          as findingsgroup,
                                                               'Version Check Failed (unexpectedly modified checks ordering)' as finding,
                                                               'http://FirstResponderKit.org'                                 as url,
                                                               'Download an updated First Responder Kit. Version check failed because "VersionCheckMode" check has not been completed before for component called "' +
                                                               @currentcomponentfullname + '"' + @crlf +
                                                               'Error: VersionCheck happenned before "VersionCheckMode" check for component called "' +
                                                               @currentcomponentfullname + '"';

                                                        -- we will stop the test because it's possible to get the same message for other components
                                                        set @canexitloop = 1;
                                                        continue;
                                                    end;

                                                exec @execret = sp_executesql @tsql,
                                                                N'@ExecRet INT OUTPUT, @ObjDate DATETIME OUTPUT',
                                                                @execret = @innerexecret output,
                                                                @objdate = @currentcomponentversiondate output;

                                                if (@execret <> 0)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                                            as checkid,
                                                               253                                            as priority,
                                                               'First Responder Kit'                          as findingsgroup,
                                                               'Version Check Failed (dynamic query failure)' as finding,
                                                               'http://FirstResponderKit.org'                 as url,
                                                               'Download an updated First Responder Kit. The version check failed because a change has been made to the code generator.' +
                                                               @crlf +
                                                               'Error: following query failed at execution (check if component [' +
                                                               @currentcomponentfullname +
                                                               '] is at the expected version)' + @crlf +
                                                               @tsql                                          as details;

                                                        -- we will stop the test because it's possible to get the same message for other components
                                                        set @canexitloop = 1;
                                                        continue;
                                                    end;


                                                if (@innerexecret <> 0)
                                                    begin
                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 226                             as checkid,
                                                               253                             as priority,
                                                               'First Responder Kit'           as findingsgroup,
                                                               'Version Check Failed (Failed dynamic SP call to ' +
                                                               @currentcomponentfullname + ')' as finding,
                                                               'http://FirstResponderKit.org'  as url,
                                                               'Download an updated First Responder Kit. Error: following query failed at execution (check if component [' +
                                                               @currentcomponentfullname +
                                                               '] is at the expected version)' + @crlf +
                                                               'Return code: ' + CONVERT(varchar(10), @innerexecret) +
                                                               @crlf +
                                                               'T-SQL Query: ' + @crlf +
                                                               @tsql                           as details;

                                                        -- advance to next component
                                                        delete
                                                        from #statementstorun4frkversioncheck
                                                        where subjectfullpath = @currentcomponentfullname;
                                                        continue;
                                                    end;

                                                if (@currentcomponentversiondate < @versiondate)
                                                    begin

                                                        insert into #blitzresults(checkid,
                                                                                  priority,
                                                                                  findingsgroup,
                                                                                  finding,
                                                                                  url,
                                                                                  details)
                                                        select 228                                                                                                                           as checkid,
                                                               253                                                                                                                           as priority,
                                                               'First Responder Kit'                                                                                                         as findingsgroup,
                                                               'Component Outdated: ' + @currentcomponentfullname                                                                            as finding,
                                                               'http://FirstResponderKit.org'                                                                                                as url,
                                                               'Download and install the latest First Responder Kit - you''re running some older code, and it doesn''t get better with age.' as details;

                                                        raiserror ('Component %s is outdated',10,1,@currentcomponentfullname);
                                                        -- advance to next component
                                                        delete
                                                        from #statementstorun4frkversioncheck
                                                        where subjectfullpath = @currentcomponentfullname;
                                                        continue;
                                                    end;

                                                else
                                                    if (@currentcomponentversiondate > @versiondate and
                                                        @blitzisoutdatedcomparedtoothers = 0)
                                                        begin
                                                            set @blitzisoutdatedcomparedtoothers = 1;
                                                            raiserror ('Procedure %s is outdated',10,1,@spblitzfullname);
                                                            if (@maximumversiondate is null or
                                                                @maximumversiondate < @currentcomponentversiondate)
                                                                begin
                                                                    set @maximumversiondate = @currentcomponentversiondate;
                                                                end;
                                                        end;
                                                /* Kept for debug purpose:
            ELSE
            BEGIN
                INSERT  INTO #BlitzResults(
                    CheckID ,
                    Priority ,
                    FindingsGroup ,
                    Finding ,
                    URL ,
                    Details
                )
                SELECT
                    2000 AS CheckID ,
                    250 AS Priority ,
                    'Informational' AS FindingsGroup ,
                    'First Responder kit component ' + @CurrentComponentFullName + ' is at the expected version' AS Finding ,
                    'https://www.BrentOzar.com/blitz/' AS URL ,
                    'Version date is: ' + CONVERT(VARCHAR(32),@CurrentComponentVersionDate,121) AS Details
                ;
            END;
            */
                                            end;

                                        -- could be performed differently to minimize computation
                                        delete
                                        from #statementstorun4frkversioncheck
                                        where statementid = @currentstatementid;
                                    end;
                            end;


/*This counts memory dumps and gives min and max date of in view*/
                        if @productversionmajor >= 10
                            and
                           not (@productversionmajor = 10.5 and @productversionminor < 4297) /* Skip due to crash bug: https://support.microsoft.com/en-us/help/2908087 */
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 171)
                            begin
                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_memory_dumps')
                                    begin
                                        if 5 <= (select COUNT(*)
                                                 from [sys].[dm_server_memory_dumps]
                                                 where [creation_time] >= DATEADD(year, -1, GETDATE()))
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 171) with nowait;

                                                insert into [#BlitzResults]
                                                ([CheckID],
                                                 [Priority],
                                                 [FindingsGroup],
                                                 [Finding],
                                                 [URL],
                                                 [Details])

                                                select 171                             as [CheckID],
                                                       20                              as [Priority],
                                                       'Reliability'                   as [FindingsGroup],
                                                       'Memory Dumps Have Occurred'    as [Finding],
                                                       'https://BrentOzar.com/go/dump' as [URL],
                                                       ('That ain''t good. I''ve had ' +
                                                        CAST(COUNT(*) as varchar(100)) + ' memory dumps between ' +
                                                        CAST(CAST(MIN([creation_time]) as datetime) as varchar(100)) +
                                                        ' and ' +
                                                        CAST(CAST(MAX([creation_time]) as datetime) as varchar(100)) +
                                                        '!'
                                                           )                           as [Details]
                                                from [sys].[dm_server_memory_dumps]
                                                where [creation_time] >= DATEADD(year, -1, GETDATE());

                                            end;
                                    end;
                            end;

/*Checks to see if you're on Developer or Evaluation*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 173)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 173) with nowait;

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 173                                  as [CheckID],
                                       200                                  as [Priority],
                                       'Licensing'                          as [FindingsGroup],
                                       'Non-Production License'             as [Finding],
                                       'https://BrentOzar.com/go/licensing' as [URL],
                                       ('We''re not the licensing police, but if this is supposed to be a production server, and you''re running ' +
                                        CAST(SERVERPROPERTY('edition') as varchar(100)) +
                                        ' the good folks at Microsoft might get upset with you. Better start counting those cores.'
                                           )                                as [Details]
                                where CAST(SERVERPROPERTY('edition') as varchar(100)) like '%Developer%'
                                   or CAST(SERVERPROPERTY('edition') as varchar(100)) like '%Evaluation%';

                            end;

/*Checks to see if Buffer Pool Extensions are in use*/
                        if @productversionmajor >= 12
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 174)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 174) with nowait;

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 174                              as [CheckID],
                                       200                              as [Priority],
                                       'Performance'                    as [FindingsGroup],
                                       'Buffer Pool Extensions Enabled' as [Finding],
                                       'https://BrentOzar.com/go/bpe'   as [URL],
                                       ('You have Buffer Pool Extensions enabled, and one lives here: ' +
                                        [path] +
                                        '. It''s currently ' +
                                        case
                                            when [current_size_in_kb] / 1024. / 1024. > 0
                                                then CAST([current_size_in_kb] / 1024. / 1024. as varchar(100))
                                                + ' GB'
                                            else CAST([current_size_in_kb] / 1024. as varchar(100))
                                                + ' MB'
                                            end +
                                        '. Did you know that BPEs only provide single threaded access 8KB (one page) at a time?'
                                           )                            as [Details]
                                from sys.dm_os_buffer_pool_extension_configuration
                                where [state_description] <> 'BUFFER POOL EXTENSION DISABLED';

                            end;

/*Check for too many tempdb files*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 175)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 175) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select distinct 175                                               as checkid,
                                                'TempDB'                                          as databasename,
                                                170                                               as priority,
                                                'File Configuration'                              as findingsgroup,
                                                'TempDB Has >16 Data Files'                       as finding,
                                                'https://BrentOzar.com/go/tempdb'                 as url,
                                                'Woah, Nelly! TempDB has ' + CAST(COUNT_BIG(*) as varchar(30)) +
                                                '. Did you forget to terminate a loop somewhere?' as details
                                from sys.[master_files] as [mf]
                                where [mf].[database_id] = 2
                                  and [mf].[type] = 0
                                having COUNT_BIG(*) > 16;
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 176)
                            begin

                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_xe_sessions')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 176) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select distinct 176                                                                 as checkid,
                                                        ''                                                                  as databasename,
                                                        200                                                                 as priority,
                                                        'Monitoring'                                                        as findingsgroup,
                                                        'Extended Events Hyperextension'                                    as finding,
                                                        'https://BrentOzar.com/go/xe'                                       as url,
                                                        'Hey big spender, you have ' +
                                                        CAST(COUNT_BIG(*) as varchar(30)) +
                                                        ' Extended Events sessions running. You sure you meant to do that?' as details
                                        from sys.dm_xe_sessions
                                        where [name] not in
                                              ('AlwaysOn_health',
                                               'system_health',
                                               'telemetry_xevents',
                                               'sp_server_diagnostics',
                                               'sp_server_diagnostics session',
                                               'hkenginexesession')
                                          and name not like '%$A%'
                                        having COUNT_BIG(*) >= 2;
                                    end;
                            end;

                        /*Harmful startup parameter*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 177)
                            begin

                                if EXISTS(select 1
                                          from sys.all_objects
                                          where name = 'dm_server_registry')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 177) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select distinct 177                                                                                                               as checkid,
                                                        ''                                                                                                                as databasename,
                                                        5                                                                                                                 as priority,
                                                        'Monitoring'                                                                                                      as findingsgroup,
                                                        'Disabled Internal Monitoring Features'                                                                           as finding,
                                                        'https://msdn.microsoft.com/en-us/library/ms190737.aspx'                                                          as url,
                                                        'You have -x as a startup parameter. You should head to the URL and read more about what it does to your system.' as details
                                        from [sys].[dm_server_registry] as [dsr]
                                        where [dsr].[registry_key] like N'%MSSQLServer\Parameters'
                                          and [dsr].[value_data] = '-x';;
                                    end;
                            end;


                        /* Reliability - Dangerous Third Party Modules - 179 */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 179)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 179) with nowait;

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 179                                                         as [CheckID],
                                       5                                                           as [Priority],
                                       'Reliability'                                               as [FindingsGroup],
                                       'Dangerous Third Party Modules'                             as [Finding],
                                       'https://support.microsoft.com/en-us/kb/2033238'            as [URL],
                                       (COALESCE(company, '') + ' - ' + COALESCE(description, '') + ' - ' +
                                        COALESCE(name, '') +
                                        ' - suspected dangerous third party module is installed.') as [Details]
                                from sys.dm_os_loaded_modules
                                where UPPER(name) like UPPER('%\ENTAPI.DLL') /* McAfee VirusScan Enterprise */
                                   or UPPER(name) like UPPER('%\HIPI.DLL')
                                   or UPPER(name) like UPPER('%\HcSQL.dll')
                                   or UPPER(name) like UPPER('%\HcApi.dll')
                                   or UPPER(name) like UPPER('%\HcThe.dll') /* McAfee Host Intrusion */
                                   or UPPER(name) like UPPER('%\SOPHOS_DETOURED.DLL')
                                   or UPPER(name) like UPPER('%\SOPHOS_DETOURED_x64.DLL')
                                   or UPPER(name) like UPPER('%\SWI_IFSLSP_64.dll')
                                   or UPPER(name) like UPPER('%\SOPHOS~%.dll') /* Sophos AV */
                                   or UPPER(name) like UPPER('%\PIOLEDB.DLL')
                                   or UPPER(name) like UPPER('%\PISDK.DLL'); /* OSISoft PI data access */

                            end;

                        /*Find shrink database tasks*/

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 180)
                            and
                           CONVERT(varchar(128), SERVERPROPERTY('productversion')) like '1%' /* Only run on 2008+ */
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 180) with nowait;

                                with xmlnamespaces ('www.microsoft.com/SqlServer/Dts' as [dts])
                                   , [maintenance_plan_steps] as (
                                    select [name]
                                   , [id] -- ID required to link maintenace plan with jobs and jobhistory (sp_Blitz Issue #776)
                                   , CAST(CAST([packagedata] as varbinary (max)) as xml) as [maintenance_plan_xml]
                                    from [msdb].[dbo].[sysssispackages]
                                    where [packagetype] = 6
                                    )
                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 180                                        as [CheckID],
                                       -- sp_Blitz Issue #776
                                       -- Job has history and was executed in the last 30 days
                                       case
                                           when (cast(datediff(dd, substring(cast(sjh.run_date as nvarchar(10)), 1, 4) +
                                                                   '-' +
                                                                   substring(cast(sjh.run_date as nvarchar(10)), 5, 2) +
                                                                   '-' +
                                                                   substring(cast(sjh.run_date as nvarchar(10)), 7, 2),
                                                               GETDATE()) as int) < 30) or
                                                (j.[enabled] = 1 and ssc.[enabled] = 1) then
                                               100
                                           else -- no job history (implicit) AND job not run in the past 30 days AND (Job disabled OR Job Schedule disabled)
                                               200
                                           end                                    as priority,
                                       'Performance'                              as [FindingsGroup],
                                       'Shrink Database Step In Maintenance Plan' as [Finding],
                                       'https://BrentOzar.com/go/autoshrink'      as [URL],
                                       'The maintenance plan ' + [mps].[name] +
                                       ' has a step to shrink databases in it. Shrinking databases is as outdated as maintenance plans.'
                                           + case
                                                 when COALESCE(ssc.name, '0') != '0'
                                                     then + ' (Schedule: [' + ssc.name + '])'
                                                 else + '' end                    as [Details]
                                from [maintenance_plan_steps] [mps]
                                         cross apply [maintenance_plan_xml].[nodes]('//dts:Executables/dts:Executable') [t]([c])
                                         join msdb.dbo.sysmaintplan_subplans as sms
                                              on mps.id = sms.plan_id
                                         join msdb.dbo.sysjobs j
                                              on sms.job_id = j.job_id
                                         left outer join msdb.dbo.sysjobsteps as step
                                                         on j.job_id = step.job_id
                                         left outer join msdb.dbo.sysjobschedules as sjsc
                                                         on j.job_id = sjsc.job_id
                                         left outer join msdb.dbo.sysschedules as ssc
                                                         on sjsc.schedule_id = ssc.schedule_id
                                                             and sjsc.job_id = j.job_id
                                         left outer join msdb.dbo.sysjobhistory as sjh
                                                         on j.job_id = sjh.job_id
                                                             and step.step_id = sjh.step_id
                                                             and sjh.run_date in (select max(sjh2.run_date)
                                                                                  from msdb.dbo.sysjobhistory as sjh2
                                                                                  where sjh2.job_id = j.job_id) -- get the latest entry date
                                                             and sjh.run_time in (select max(sjh3.run_time)
                                                                                  from msdb.dbo.sysjobhistory as sjh3
                                                                                  where sjh3.job_id = j.job_id
                                                                                    and sjh3.run_date = sjh.run_date) -- get the latest entry time
                                where [c].[value]('(@dts:ObjectName)', 'VARCHAR(128)') = 'Shrink Database Task';

                            end;

                        /*Find repetitive maintenance tasks*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 181)
                            and
                           CONVERT(varchar(128), SERVERPROPERTY('productversion')) like '1%' /* Only run on 2008+ */
                            begin
                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 181) with nowait;

                                with xmlnamespaces ('www.microsoft.com/SqlServer/Dts' as [dts])
                                   , [maintenance_plan_steps] as (
                                    select [name]
                                   , CAST(CAST([packagedata] as varbinary (max)) as xml) as [maintenance_plan_xml]
                                    from [msdb].[dbo].[sysssispackages]
                                    where [packagetype] = 6
                                    )
                                   , [maintenance_plan_table] as (
                                    select [mps].[name]
                                   , [c].[value]('(@dts:ObjectName)'
                                   , 'NVARCHAR(128)') as [step_name]
                                    from [maintenance_plan_steps] [mps]
                                    cross apply [maintenance_plan_xml].[nodes]('//dts:Executables/dts:Executable') [t]([c])
                                    )
                                   , [mp_steps_pretty] as (select distinct [m1].[name]
                                   , STUFF((select N', ' + [m2].[step_name] from [maintenance_plan_table] as [m2] where [m1].[name] = [m2].[name]
                                    for xml path (N''))
                                   , 1
                                   , 2
                                   , N'') as [maintenance_plan_steps]
                                    from [maintenance_plan_table] as [m1])

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 181                                                                                                     as [CheckID],
                                       100                                                                                                     as [Priority],
                                       'Performance'                                                                                           as [FindingsGroup],
                                       'Repetitive Steps In Maintenance Plans'                                                                 as [Finding],
                                       'https://ola.hallengren.com/'                                                                           as [URL],
                                       'The maintenance plan ' + [m].[name] +
                                       ' is doing repetitive work on indexes and statistics. Perhaps it''s time to try something more modern?' as [Details]
                                from [mp_steps_pretty] m
                                where m.[maintenance_plan_steps] like '%Rebuild%Reorganize%'
                                   or m.[maintenance_plan_steps] like '%Rebuild%Update%';

                            end;


                        /* Reliability - No Failover Cluster Nodes Available - 184 */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 184)
                            and CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)) not like '10%'
                            and CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)) not like '9%'
                            begin
                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 184) with nowait;

                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			                        							SELECT TOP 1
							  184 AS CheckID ,
							  20 AS Priority ,
							  ''Reliability'' AS FindingsGroup ,
							  ''No Failover Cluster Nodes Available'' AS Finding ,
							  ''https://BrentOzar.com/go/node'' AS URL ,
							  ''There are no failover cluster nodes available if the active node fails'' AS Details
							FROM (
							  SELECT SUM(CASE WHEN [status] = 0 AND [is_current_owner] = 0 THEN 1 ELSE 0 END) AS [available_nodes]
							  FROM sys.dm_os_cluster_nodes
							) a
							WHERE [available_nodes] < 1 OPTION (RECOMPILE)';

                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);
                            end;

                        /* Reliability - TempDB File Error */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 191)
                            and (select COUNT(*) from sys.master_files where database_id = 2) <>
                                (select COUNT(*) from tempdb.sys.database_files)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 191) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 191                                                                                                as [CheckID],
                                       50                                                                                                 as [Priority],
                                       'Reliability'                                                                                      as [FindingsGroup],
                                       'TempDB File Error'                                                                                as [Finding],
                                       'https://BrentOzar.com/go/tempdboops'                                                              as [URL],
                                       'Mismatch between the number of TempDB files in sys.master_files versus tempdb.sys.database_files' as [Details];
                            end;

/*Perf - Odd number of cores in a socket*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 198)
                            and EXISTS(select 1
                                       from sys.dm_os_schedulers
                                       where is_online = 1
                                         and scheduler_id < 255
                                         and parent_node_id < 64
                                       group by parent_node_id,
                                                is_online
                                       having (COUNT(cpu_id) + 2) % 2 = 1)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 198) with nowait

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 198                               as checkid,
                                       null                              as databasename,
                                       10                                as priority,
                                       'Performance'                     as findingsgroup,
                                       'CPU w/Odd Number of Cores'       as finding,
                                       'https://BrentOzar.com/go/oddity' as url,
                                       'Node ' + CONVERT(varchar(10), parent_node_id) + ' has ' +
                                       CONVERT(varchar(10), COUNT(cpu_id))
                                           + case
                                                 when COUNT(cpu_id) = 1
                                                     then ' core assigned to it. This is a really bad NUMA configuration.'
                                                 else ' cores assigned to it. This is a really bad NUMA configuration.'
                                           end                           as details
                                from sys.dm_os_schedulers
                                where is_online = 1
                                  and scheduler_id < 255
                                  and parent_node_id < 64
                                  and EXISTS(
                                        select 1
                                        from (select memory_node_id, SUM(online_scheduler_count) as schedulers
                                              from sys.dm_os_nodes
                                              where memory_node_id < 64
                                              group by memory_node_id) as nodes
                                        having MIN(nodes.schedulers) <> MAX(nodes.schedulers)
                                    )
                                group by parent_node_id,
                                         is_online
                                having (COUNT(cpu_id) + 2) % 2 = 1;

                            end;

                        /*Begin: checking default trace for odd DBCC activity*/

                        --Grab relevant event data
                        if @tracefileissue = 0
                            begin
                                select UPPER(
                                               REPLACE(
                                                       SUBSTRING(CONVERT(nvarchar(max), t.textdata), 0,
                                                                 ISNULL(
                                                                         NULLIF(
                                                                                 CHARINDEX('(', CONVERT(nvarchar(max), t.textdata)),
                                                                                 0),
                                                                         LEN(CONVERT(nvarchar(max), t.textdata)) + 1)) --This replaces everything up to an open paren, if one exists.
                                                   , SUBSTRING(CONVERT(nvarchar(max), t.textdata),
                                                               ISNULL(
                                                                       NULLIF(
                                                                               CHARINDEX(' WITH ', CONVERT(nvarchar(max), t.textdata))
                                                                           , 0),
                                                                       LEN(CONVERT(nvarchar(max), t.textdata)) + 1),
                                                               LEN(CONVERT(nvarchar(max), t.textdata)) + 1)
                                                   ,
                                                       '') --This replaces any optional WITH clause to a DBCC command, like tableresults.
                                           )                                                                   as [dbcc_event_trunc_upper],
                                       UPPER(
                                               REPLACE(
                                                       CONVERT(nvarchar(max), t.textdata), SUBSTRING(
                                                       CONVERT(nvarchar(max), t.textdata),
                                                       ISNULL(
                                                               NULLIF(
                                                                       CHARINDEX(' WITH ', CONVERT(nvarchar(max), t.textdata))
                                                                   , 0),
                                                               LEN(CONVERT(nvarchar(max), t.textdata)) + 1),
                                                       LEN(CONVERT(nvarchar(max), t.textdata)) + 1),
                                                       ''))                                                    as [dbcc_event_full_upper],
                                       MIN(t.starttime)
                                           over (partition by CONVERT(nvarchar(128), t.textdata))              as min_start_time,
                                       MAX(t.starttime)
                                           over (partition by CONVERT(nvarchar(128), t.textdata))              as max_start_time,
                                       t.ntusername                                                            as [nt_user_name],
                                       t.ntdomainname                                                          as [nt_domain_name],
                                       t.hostname                                                              as [host_name],
                                       t.applicationname                                                       as [application_name],
                                       t.loginname                                                                [login_name],
                                       t.dbusername                                                            as [db_user_name]
                                into #dbcc_events_from_trace
                                from #fntracegettable as t
                                where t.eventclass = 116
                                option (recompile)
                            end;

                        /*Overall count of DBCC events excluding silly stuff*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 203)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 203) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 203                                 as checkid,
                                       50                                  as priority,
                                       'DBCC Events'                       as findingsgroup,
                                       'Overall Events'                    as finding,
                                       'https://www.BrentOzar.com/go/dbcc' as url,
                                       CAST(COUNT(*) as nvarchar(100)) + ' DBCC events have taken place between ' +
                                       CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. This does not include CHECKDB and other usually benign DBCC events.'
                                                                           as details
                                from #dbcc_events_from_trace d
                                    /* This WHERE clause below looks horrible, but it's because users can run stuff like
			   DBCC     LOGINFO
			   with lots of spaces (or carriage returns, or comments) in between the DBCC and the
			   command they're trying to run. See Github issues 1062, 1074, 1075.
			*/
                                where d.dbcc_event_full_upper not like '%DBCC%ADDINSTANCE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%AUTOPILOT%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKALLOC%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKCATALOG%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKCONSTRAINTS%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKDB%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKFILEGROUP%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKIDENT%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKPRIMARYFILE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CHECKTABLE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%CLEANTABLE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%DBINFO%'
                                  and d.dbcc_event_full_upper not like '%DBCC%ERRORLOG%'
                                  and d.dbcc_event_full_upper not like '%DBCC%INCREMENTINSTANCE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%INPUTBUFFER%'
                                  and d.dbcc_event_full_upper not like '%DBCC%LOGINFO%'
                                  and d.dbcc_event_full_upper not like '%DBCC%OPENTRAN%'
                                  and d.dbcc_event_full_upper not like '%DBCC%SETINSTANCE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%SHOWFILESTATS%'
                                  and d.dbcc_event_full_upper not like '%DBCC%SHOW_STATISTICS%'
                                  and d.dbcc_event_full_upper not like '%DBCC%SQLPERF%NETSTATS%'
                                  and d.dbcc_event_full_upper not like '%DBCC%SQLPERF%LOGSPACE%'
                                  and d.dbcc_event_full_upper not like '%DBCC%TRACEON%'
                                  and d.dbcc_event_full_upper not like '%DBCC%TRACEOFF%'
                                  and d.dbcc_event_full_upper not like '%DBCC%TRACESTATUS%'
                                  and d.dbcc_event_full_upper not like '%DBCC%USEROPTIONS%'
                                  and d.application_name not like 'Critical Care(R) Collector'
                                  and d.application_name not like '%Red Gate Software Ltd SQL Prompt%'
                                  and d.application_name not like '%Spotlight Diagnostic Server%'
                                  and d.application_name not like '%SQL Diagnostic Manager%'
                                  and d.application_name not like 'SQL Server Checkup%'
                                  and d.application_name not like '%Sentry%'
                                  and d.application_name not like '%LiteSpeed%'
                                  and d.application_name not like '%SQL Monitor - Monitoring%'


                                having COUNT(*) > 0;

                            end;

                        /*Check for someone running drop clean buffers*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 207)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 207) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 207                                  as checkid,
                                       10                                   as priority,
                                       'Performance'                        as findingsgroup,
                                       'DBCC DROPCLEANBUFFERS Ran Recently' as finding,
                                       'https://www.BrentOzar.com/go/dbcc'  as url,
                                       'The user ' + COALESCE(d.nt_user_name, d.login_name) +
                                       ' has run DBCC DROPCLEANBUFFERS ' + CAST(COUNT(*) as nvarchar(100)) +
                                       ' times between ' + CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. If this is a production box, know that you''re clearing all data out of memory when this happens. What kind of monster would do that?'
                                                                            as details
                                from #dbcc_events_from_trace d
                                where d.dbcc_event_full_upper = N'DBCC DROPCLEANBUFFERS'
                                group by COALESCE(d.nt_user_name, d.login_name)
                                having COUNT(*) > 0;

                            end;

                        /*Check for someone running free proc cache*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 208)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 208) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 208                                 as checkid,
                                       10                                  as priority,
                                       'DBCC Events'                       as findingsgroup,
                                       'DBCC FREEPROCCACHE Ran Recently'   as finding,
                                       'https://www.BrentOzar.com/go/dbcc' as url,
                                       'The user ' + COALESCE(d.nt_user_name, d.login_name) +
                                       ' has run DBCC FREEPROCCACHE ' + CAST(COUNT(*) as nvarchar(100)) +
                                       ' times between ' + CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. This has bad idea jeans written all over its butt, like most other bad idea jeans.'
                                                                           as details
                                from #dbcc_events_from_trace d
                                where d.dbcc_event_full_upper = N'DBCC FREEPROCCACHE'
                                group by COALESCE(d.nt_user_name, d.login_name)
                                having COUNT(*) > 0;

                            end;

                        /*Check for someone clearing wait stats*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 205)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 205) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 205                                 as checkid,
                                       50                                  as priority,
                                       'Performance'                       as findingsgroup,
                                       'Wait Stats Cleared Recently'       as finding,
                                       'https://www.BrentOzar.com/go/dbcc' as url,
                                       'The user ' + COALESCE(d.nt_user_name, d.login_name) +
                                       ' has run DBCC SQLPERF(''SYS.DM_OS_WAIT_STATS'',CLEAR) ' +
                                       CAST(COUNT(*) as nvarchar(100)) + ' times between ' +
                                       CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. Why are you clearing wait stats? What are you hiding?'
                                                                           as details
                                from #dbcc_events_from_trace d
                                where d.dbcc_event_full_upper = N'DBCC SQLPERF(''SYS.DM_OS_WAIT_STATS'',CLEAR)'
                                group by COALESCE(d.nt_user_name, d.login_name)
                                having COUNT(*) > 0;

                            end;

                        /*Check for someone writing to pages. Yeah, right?*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 209)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 209) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])
                                select 209                                 as checkid,
                                       50                                  as priority,
                                       'Reliability'                       as findingsgroup,
                                       'DBCC WRITEPAGE Used Recently'      as finding,
                                       'https://www.BrentOzar.com/go/dbcc' as url,
                                       'The user ' + COALESCE(d.nt_user_name, d.login_name) +
                                       ' has run DBCC WRITEPAGE ' + CAST(COUNT(*) as nvarchar(100)) +
                                       ' times between ' + CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. So, uh, are they trying to fix corruption, or cause corruption?'
                                                                           as details
                                from #dbcc_events_from_trace d
                                where d.dbcc_event_trunc_upper = N'DBCC WRITEPAGE'
                                group by COALESCE(d.nt_user_name, d.login_name)
                                having COUNT(*) > 0;

                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 210)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 210) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 210                                 as checkid,
                                       10                                  as priority,
                                       'Performance'                       as findingsgroup,
                                       'DBCC SHRINK% Ran Recently'         as finding,
                                       'https://www.BrentOzar.com/go/dbcc' as url,
                                       'The user ' + COALESCE(d.nt_user_name, d.login_name) + ' has run file shrinks ' +
                                       CAST(COUNT(*) as nvarchar(100)) + ' times between ' +
                                       CONVERT(nvarchar(30), MIN(d.min_start_time)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(d.max_start_time)) +
                                       '. So, uh, are they trying cause bad performance on purpose?'
                                                                           as details
                                from #dbcc_events_from_trace d
                                where d.dbcc_event_trunc_upper like N'DBCC SHRINK%'
                                group by COALESCE(d.nt_user_name, d.login_name)
                                having COUNT(*) > 0;

                            end;

                        /*End: checking default trace for odd DBCC activity*/

                        /*Begin check for autoshrink events*/

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 206)
                            and @tracefileissue = 0
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 206) with nowait

                                insert into [#BlitzResults]
                                ([CheckID],
                                 [Priority],
                                 [FindingsGroup],
                                 [Finding],
                                 [URL],
                                 [Details])

                                select 206                        as checkid,
                                       10                         as priority,
                                       'Performance'              as findingsgroup,
                                       'Auto-Shrink Ran Recently' as finding,
                                       ''                         as url,
                                       N'The database ' + QUOTENAME(t.databasename) + N' has had '
                                           + CONVERT(nvarchar(10), COUNT(*))
                                           + N' auto shrink events between '
                                           + CONVERT(nvarchar(30), MIN(t.starttime)) + ' and ' +
                                       CONVERT(nvarchar(30), MAX(t.starttime))
                                           + ' that lasted on average '
                                           + CONVERT(nvarchar(10), AVG(DATEDIFF(second, t.starttime, t.endtime)))
                                           + ' seconds.'          as details
                                from #fntracegettable as t
                                where t.eventclass in (94, 95)
                                group by t.databasename
                                having AVG(DATEDIFF(second, t.starttime, t.endtime)) > 5;

                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 215)
                            and @tracefileissue = 0
                            and EXISTS(select * from sys.all_columns where name = 'database_id'
                                                                       and object_id = OBJECT_ID('sys.dm_exec_sessions'))
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 215) with nowait

                                set @stringtoexecute = 'INSERT    INTO [#BlitzResults]
									( [CheckID] ,
									  [Priority] ,
									  [FindingsGroup] ,
									  [Finding] ,
                                      [DatabaseName] ,
									  [URL] ,
									  [Details] )

								SELECT	215 AS CheckID ,
										100 AS Priority ,
										''Performance'' AS FindingsGroup ,
										''Implicit Transactions'' AS Finding ,
										DB_NAME(s.database_id) AS DatabaseName,
										''https://www.brentozar.com/go/ImplicitTransactions/'' AS URL ,
										N''The database '' +
										DB_NAME(s.database_id)
										+ '' has ''
										+ CONVERT(NVARCHAR(20), COUNT_BIG(*))
										+ '' open implicit transactions with an oldest begin time of ''
										+ CONVERT(NVARCHAR(30), MIN(tat.transaction_begin_time))
										+ '' Run sp_BlitzWho and check the is_implicit_transaction column to see the culprits.'' AS details
								FROM    sys.dm_tran_active_transactions AS tat
								LEFT JOIN sys.dm_tran_session_transactions AS tst
								ON tst.transaction_id = tat.transaction_id
								LEFT JOIN sys.dm_exec_sessions AS s
								ON s.session_id = tst.session_id
								WHERE tat.name = ''implicit_transaction''
								GROUP BY DB_NAME(s.database_id), transaction_type, transaction_state;';


                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                if @debug = 2 and @stringtoexecute is null
                                    print '@StringToExecute has gone NULL, for some reason.';

                                execute (@stringtoexecute);


                            end;


                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 221)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 221) with nowait;

                                with reboot_airhorn
                                         as
                                         (
                                             select create_date
                                             from sys.databases
                                             where database_id = 2
                                             union all
                                             select CAST(
                                                            DATEADD(second, (ms_ticks / 1000) * (-1), GETDATE()) as datetime)
                                             from sys.dm_os_sys_info
                                         )
                                insert
                                into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 221                                                   as checkid,
                                       10                                                    as priority,
                                       'Reliability'                                         as findingsgroup,
                                       'Server restarted in last 24 hours'                   as finding,
                                       ''                                                    as url,
                                       'Surprise! Your server was last restarted on: ' +
                                       CONVERT(varchar(30), MAX(reboot_airhorn.create_date)) as details
                                from reboot_airhorn
                                having MAX(reboot_airhorn.create_date) >= DATEADD(hour, -24, GETDATE());


                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 229)
                            and CAST(SERVERPROPERTY('Edition') as nvarchar(4000)) like '%Evaluation%'
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 229) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 229 as                                                                            checkid,
                                       1 as                                                                              priority,
                                       'Reliability' as                                                                  findingsgroup,
                                       'Evaluation Edition' as                                                           finding,
                                       'https://www.BrentOzar.com/go/workgroup' as                                       url,
                                       'This server will stop working on: ' + CAST(
                                               CONVERT(datetime, DATEADD(dd, 180, create_date), 102) as varchar(100)) as details
                                from sys.server_principals
                                where sid = 0x010100000000000512000000;

                            end;


                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 233)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 233) with nowait;


                                if EXISTS(select *
                                          from sys.all_columns
                                          where object_id = OBJECT_ID('sys.dm_os_memory_clerks') and name = 'pages_kb')
                                    begin
                                        /* SQL 2012+ version */
                                        set @stringtoexecute = N'
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
							SELECT 233 AS CheckID,
							       50 AS Priority,
							       ''Performance'' AS FindingsGroup,
							       ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
							       ''https://www.BrentOzar.com/go/userstore'' AS URL,
							       N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100))
								   		+ N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
								   AS details
							FROM sys.dm_os_memory_clerks
							HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / SUM(pages_kb) >= 0.1
							  AND SUM(pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
                                        exec sp_executesql @stringtoexecute;
                                    end
                                else
                                    begin
                                        /* Antiques Roadshow SQL 2008R2 - version */
                                        set @stringtoexecute = N'
							INSERT  INTO #BlitzResults
									( CheckID ,
									  Priority ,
									  FindingsGroup ,
									  Finding ,
									  URL ,
									  Details
									)
							SELECT 233 AS CheckID,
							       50 AS Priority,
							       ''Performance'' AS FindingsGroup,
							       ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
							       ''https://www.BrentOzar.com/go/userstore'' AS URL,
							       N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100))
								   		+ N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
								   AS details
							FROM sys.dm_os_memory_clerks
							HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / SUM(single_pages_kb + multi_pages_kb) >= 0.1
							  AND SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
                                        exec sp_executesql @stringtoexecute;
                                    end

                            end;


                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 234)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 234) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 databasename,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 234                                                                                                                                                                                                                     as checkid,
                                       100                                                                                                                                                                                                                     as priority,
                                       db_name(f.database_id)                                                                                                                                                                                                  as databasename,
                                       'Reliability'                                                                                                                                                                                                           as findingsgroup,
                                       'SQL Server Update May Fail'                                                                                                                                                                                            as finding,
                                       'https://desertdba.com/failovers-cant-serve-two-masters/'                                                                                                                                                               as url,
                                       'This database has a file with a logical name of ''master'', which can break SQL Server updates. Rename it in SSMS by right-clicking on the database, go into Properties, and rename the file. Takes effect instantly.' as details
                                from master.sys.master_files f
                                where (f.name = N'master')
                                  and f.database_id > 4
                                  and db_name(f.database_id) <> 'master'; /* Thanks Michaels3 for catching this */
                            end;


                        if @checkuserdatabaseobjects = 1
                            begin

                                if @debug in (1, 2)
                                    raiserror ('Starting @CheckUserDatabaseObjects section.', 0, 1) with nowait

                                /*
                        But what if you need to run a query in every individual database?
				        Check out CheckID 99 below. Yes, it uses sp_MSforeachdb, and no,
				        we're not happy about that. sp_MSforeachdb is known to have a lot
				        of issues, like skipping databases sometimes. However, this is the
				        only built-in option that we have. If you're writing your own code
				        for database maintenance, consider Aaron Bertrand's alternative:
				        http://www.mssqltips.com/sqlservertip/2201/making-a-more-reliable-and-flexible-spmsforeachdb/
				        We don't include that as part of sp_Blitz, of course, because
				        copying and distributing copyrighted code from others without their
				        written permission isn't a good idea.
				        */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 99)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 99) with nowait;

                                        exec dbo.sp_msforeachdb
                                             'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; IF EXISTS (SELECT * FROM  sys.tables WITH (NOLOCK) WHERE name = ''sysmergepublications'' ) IF EXISTS ( SELECT * FROM sysmergepublications WITH (NOLOCK) WHERE retention = 0)   INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 99, DB_NAME(), 110, ''Performance'', ''Infinite merge replication metadata retention period'', ''https://BrentOzar.com/go/merge'', (''The ['' + DB_NAME() + ''] database has merge replication metadata retention period set to infinite - this can be the case of significant performance issues.'')';
                                    end;
                                /*
				        Note that by using sp_MSforeachdb, we're running the query in all
				        databases. We're not checking #SkipChecks here for each database to
				        see if we should run the check in this database. That means we may
				        still run a skipped check if it involves sp_MSforeachdb. We just
				        don't output those results in the last step.
                        */

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 163)
                                    and EXISTS(
                                           select * from sys.all_objects where name = 'database_query_store_options')
                                    begin
                                        /* --TOURSTOP03-- */

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 163) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
                                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
			                            INSERT INTO #BlitzResults
			                            (CheckID,
			                            DatabaseName,
			                            Priority,
			                            FindingsGroup,
			                            Finding,
			                            URL,
			                            Details)
		                              SELECT TOP 1 163,
		                              N''?'',
		                              200,
		                              ''Performance'',
		                              ''Query Store Disabled'',
		                              ''https://BrentOzar.com/go/querystore'',
		                              (''The new SQL Server 2016 Query Store feature has not been enabled on this database.'')
		                              FROM [?].sys.database_query_store_options WHERE desired_state = 0
									  AND N''?'' NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''DWConfiguration'', ''DWDiagnostics'', ''DWQueue'', ''ReportServer'', ''ReportServerTempDB'') OPTION (RECOMPILE)';
                                    end;


                                if @productversionmajor >= 13 and @productversionminor < 2149 --CU1 has the fix in it
                                    and not EXISTS(select 1
                                                   from #skipchecks
                                                   where databasename is null
                                                     and checkid = 182)
                                    and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Enterprise%'
                                    and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Developer%'
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 182) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													URL,
													Details)
													SELECT TOP 1
													182,
													''Server'',
													20,
													''Reliability'',
													''Query Store Cleanup Disabled'',
													''https://BrentOzar.com/go/cleanup'',
													(''SQL 2016 RTM has a bug involving dumps that happen every time Query Store cleanup jobs run. This is fixed in CU1 and later: https://sqlserverupdates.com/sql-server-2016-updates/'')
													FROM    sys.databases AS d
													WHERE   d.is_query_store_on = 1 OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 41)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 41) with nowait;

                                        exec dbo.sp_msforeachdb 'use [?];
		                              SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                      INSERT INTO #BlitzResults
		                              (CheckID,
		                              DatabaseName,
		                              Priority,
		                              FindingsGroup,
		                              Finding,
		                              URL,
		                              Details)
		                              SELECT 41,
		                              N''?'',
		                              170,
		                              ''File Configuration'',
		                              ''Multiple Log Files on One Drive'',
		                              ''https://BrentOzar.com/go/manylogs'',
		                              (''The ['' + DB_NAME() + ''] database has multiple log files on the '' + LEFT(physical_name, 1) + '' drive. This is not a performance booster because log file access is sequential, not parallel.'')
		                              FROM [?].sys.database_files WHERE type_desc = ''LOG''
			                            AND N''?'' <> ''[tempdb]''
		                              GROUP BY LEFT(physical_name, 1)
		                              HAVING COUNT(*) > 1 OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 42)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 42) with nowait;

                                        exec dbo.sp_msforeachdb 'use [?];
			                            SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                        INSERT INTO #BlitzResults
			                            (CheckID,
			                            DatabaseName,
			                            Priority,
			                            FindingsGroup,
			                            Finding,
			                            URL,
			                            Details)
			                            SELECT DISTINCT 42,
			                            N''?'',
			                            170,
			                            ''File Configuration'',
			                            ''Uneven File Growth Settings in One Filegroup'',
			                            ''https://BrentOzar.com/go/grow'',
			                            (''The ['' + DB_NAME() + ''] database has multiple data files in one filegroup, but they are not all set up to grow in identical amounts.  This can lead to uneven file activity inside the filegroup.'')
			                            FROM [?].sys.database_files
			                            WHERE type_desc = ''ROWS''
			                            GROUP BY data_space_id
			                            HAVING COUNT(DISTINCT growth) > 1 OR COUNT(DISTINCT is_percent_growth) > 1 OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 82)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 82) with nowait;

                                        exec sp_MSforeachdb 'use [?];
		                                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                        INSERT INTO #BlitzResults
		                                (CheckID,
		                                DatabaseName,
		                                Priority,
		                                FindingsGroup,
		                                Finding,
		                                URL, Details)
		                                SELECT  DISTINCT 82 AS CheckID,
		                                N''?'' as DatabaseName,
		                                170 AS Priority,
		                                ''File Configuration'' AS FindingsGroup,
		                                ''File growth set to percent'',
		                                ''https://BrentOzar.com/go/percentgrowth'' AS URL,
		                                ''The ['' + DB_NAME() + ''] database file '' + f.physical_name + '' has grown to '' + CONVERT(NVARCHAR(10), CONVERT(NUMERIC(38, 2), (f.size / 128.) / 1024.)) + '' GB, and is using percent filegrowth settings. This can lead to slow performance during growths if Instant File Initialization is not enabled.''
		                                FROM    [?].sys.database_files f
		                                WHERE   is_percent_growth = 1 and size > 128000  OPTION (RECOMPILE);';
                                    end;

                                /* addition by Henrik Staun Poulsen, Stovi Software */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 158)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 158) with nowait;

                                        exec sp_MSforeachdb 'use [?];
		                                SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                        INSERT INTO #BlitzResults
		                                (CheckID,
		                                DatabaseName,
		                                Priority,
		                                FindingsGroup,
		                                Finding,
		                                URL, Details)
		                                SELECT  DISTINCT 158 AS CheckID,
		                                N''?'' as DatabaseName,
		                                170 AS Priority,
		                                ''File Configuration'' AS FindingsGroup,
		                                ''File growth set to 1MB'',
		                                ''https://BrentOzar.com/go/percentgrowth'' AS URL,
		                                ''The ['' + DB_NAME() + ''] database file '' + f.physical_name + '' is using 1MB filegrowth settings, but it has grown to '' + CAST((f.size * 8 / 1000000) AS NVARCHAR(10)) + '' GB. Time to up the growth amount.''
		                                FROM    [?].sys.database_files f
                                        WHERE is_percent_growth = 0 and growth=128 and size > 128000  OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 33)
                                    begin
                                        if @@VERSION not like '%Microsoft SQL Server 2000%'
                                            and @@VERSION not like '%Microsoft SQL Server 2005%'
                                            and @skipblockingchecks = 0
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 33) with nowait;

                                                exec dbo.sp_msforeachdb 'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                            INSERT INTO #BlitzResults
					                                (CheckID,
					                                DatabaseName,
					                                Priority,
					                                FindingsGroup,
					                                Finding,
					                                URL,
					                                Details)
		                                  SELECT DISTINCT 33,
		                                  db_name(),
		                                  200,
		                                  ''Licensing'',
		                                  ''Enterprise Edition Features In Use'',
		                                  ''https://BrentOzar.com/go/ee'',
		                                  (''The ['' + DB_NAME() + ''] database is using '' + feature_name + ''.  If this database is restored onto a Standard Edition server, the restore will fail on versions prior to 2016 SP1.'')
		                                  FROM [?].sys.dm_db_persisted_sku_features OPTION (RECOMPILE);';
                                            end;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 19)
                                    begin
                                        /* Method 1: Check sys.databases parameters */

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 19) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)

                                        select 19                                                                 as checkid,
                                               [name]                                                             as databasename,
                                               200                                                                as priority,
                                               'Informational'                                                    as findingsgroup,
                                               'Replication In Use'                                               as finding,
                                               'https://BrentOzar.com/go/repl'                                    as url,
                                               ('Database [' + [name]
                                                   +
                                                '] is a replication publisher, subscriber, or distributor.')      as details
                                        from sys.databases
                                        where name not in (select distinct databasename
                                                           from #skipchecks
                                                           where checkid is null
                                                              or checkid = 19)
                                            and is_published = 1
                                           or is_subscribed = 1
                                           or is_merge_published = 1
                                           or is_distributor = 1;

                                        /* Method B: check subscribers for MSreplication_objects tables */
                                        exec dbo.sp_msforeachdb 'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                    INSERT INTO #BlitzResults
										        (CheckID,
										        DatabaseName,
										        Priority,
										        FindingsGroup,
										        Finding,
										        URL,
										        Details)
							          SELECT DISTINCT 19,
							          db_name(),
							          200,
							          ''Informational'',
							          ''Replication In Use'',
							          ''https://BrentOzar.com/go/repl'',
							          (''['' + DB_NAME() + ''] has MSreplication_objects tables in it, indicating it is a replication subscriber.'')
							          FROM [?].sys.tables
							          WHERE name = ''dbo.MSreplication_objects'' AND ''?'' <> ''master'' OPTION (RECOMPILE)';

                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 32)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 32) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
			SELECT 32,
			N''?'',
			150,
			''Performance'',
			''Triggers on Tables'',
			''https://BrentOzar.com/go/trig'',
			(''The ['' + DB_NAME() + ''] database has '' + CAST(SUM(1) AS NVARCHAR(50)) + '' triggers.'')
			FROM [?].sys.triggers t INNER JOIN [?].sys.objects o ON t.parent_id = o.object_id
			INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id WHERE t.is_ms_shipped = 0 AND DB_NAME() != ''ReportServer''
			HAVING SUM(1) > 0 OPTION (RECOMPILE)';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 38)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 38) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
		  SELECT DISTINCT 38,
		  N''?'',
		  110,
		  ''Performance'',
		  ''Active Tables Without Clustered Indexes'',
		  ''https://BrentOzar.com/go/heaps'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that are being actively queried.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = N''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(NULLIF(ius.user_seeks,0), NULLIF(ius.user_scans,0), NULLIF(ius.user_lookups,0), NULLIF(ius.user_updates,0)) IS NOT NULL
		  AND sd.name <> ''tempdb'' AND sd.name <> ''DWDiagnostics'' AND o.is_ms_shipped = 0 AND o.type <> ''S'' OPTION (RECOMPILE)';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 164)
                                    and EXISTS(select * from sys.all_objects where name = 'fn_validate_plan_guide')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 164) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
		  SELECT DISTINCT 164,
		  N''?'',
		  20,
		  ''Reliability'',
		  ''Plan Guides Failing'',
		  ''https://BrentOzar.com/go/misguided'',
		  (''The ['' + DB_NAME() + ''] database has plan guides that are no longer valid, so the queries involved may be failing silently.'')
		  FROM [?].sys.plan_guides g CROSS APPLY fn_validate_plan_guide(g.plan_guide_id) OPTION (RECOMPILE)';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 39)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 39) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #BlitzResults
			(CheckID,
			DatabaseName,
			Priority,
			FindingsGroup,
			Finding,
			URL,
			Details)
		  SELECT DISTINCT 39,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Inactive Tables Without Clustered Indexes'',
		  ''https://BrentOzar.com/go/heaps'',
		  (''The ['' + DB_NAME() + ''] database has heaps - tables without a clustered index - that have not been queried since the last restart.  These may be backup tables carelessly left behind.'')
		  FROM [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id
		  INNER JOIN [?].sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
		  INNER JOIN sys.databases sd ON sd.name = N''?''
		  LEFT OUTER JOIN [?].sys.dm_db_index_usage_stats ius ON i.object_id = ius.object_id AND i.index_id = ius.index_id AND ius.database_id = sd.database_id
		  WHERE i.type_desc = ''HEAP'' AND COALESCE(NULLIF(ius.user_seeks,0), NULLIF(ius.user_scans,0), NULLIF(ius.user_lookups,0), NULLIF(ius.user_updates,0)) IS NULL
		  AND sd.name <> ''tempdb'' AND sd.name <> ''DWDiagnostics'' AND o.is_ms_shipped = 0 AND o.type <> ''S'' OPTION (RECOMPILE)';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 46)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 46) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 46,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Leftover Fake Indexes From Wizards'',
		  ''https://BrentOzar.com/go/hypo'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is a leftover hypothetical index from the Index Tuning Wizard or Database Tuning Advisor.  This index is not actually helping performance and should be removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_hypothetical = 1 OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 47)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 47) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 47,
		  N''?'',
		  100,
		  ''Performance'',
		  ''Indexes Disabled'',
		  ''https://BrentOzar.com/go/ixoff'',
		  (''The index ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is disabled.  This index is not actually helping performance and should either be enabled or removed.'')
		  from [?].sys.indexes i INNER JOIN [?].sys.objects o ON i.object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_disabled = 1 OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 48)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 48) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT DISTINCT 48,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Foreign Keys Not Trusted'',
		  ''https://BrentOzar.com/go/trust'',
		  (''The ['' + DB_NAME() + ''] database has foreign keys that were probably disabled, data was changed, and then the key was enabled again.  Simply enabling the key is not enough for the optimizer to use this key - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.foreign_keys i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0 AND N''?'' NOT IN (''master'', ''model'', ''msdb'', ''ReportServer'', ''ReportServerTempDB'') OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 56)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 56) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 56,
		  N''?'',
		  150,
		  ''Performance'',
		  ''Check Constraint Not Trusted'',
		  ''https://BrentOzar.com/go/trust'',
		  (''The check constraint ['' + DB_NAME() + ''].['' + s.name + ''].['' + o.name + ''].['' + i.name + ''] is not trusted - meaning, it was disabled, data was changed, and then the constraint was enabled again.  Simply enabling the constraint is not enough for the optimizer to use this constraint - we have to alter the table using the WITH CHECK CHECK CONSTRAINT parameter.'')
		  from [?].sys.check_constraints i INNER JOIN [?].sys.objects o ON i.parent_object_id = o.object_id
		  INNER JOIN [?].sys.schemas s ON o.schema_id = s.schema_id
		  WHERE i.is_not_trusted = 1 AND i.is_not_for_replication = 0 AND i.is_disabled = 0 OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 95)
                                    begin
                                        if @@VERSION not like '%Microsoft SQL Server 2000%'
                                            and @@VERSION not like '%Microsoft SQL Server 2005%'
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 95) with nowait;

                                                exec dbo.sp_msforeachdb 'USE [?];
			SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
            INSERT INTO #BlitzResults
				  (CheckID,
				  DatabaseName,
				  Priority,
				  FindingsGroup,
				  Finding,
				  URL,
				  Details)
			SELECT TOP 1 95 AS CheckID,
			N''?'' as DatabaseName,
			110 AS Priority,
			''Performance'' AS FindingsGroup,
			''Plan Guides Enabled'' AS Finding,
			''https://BrentOzar.com/go/guides'' AS URL,
			(''Database ['' + DB_NAME() + ''] has query plan guides so a query will always get a specific execution plan. If you are having trouble getting query performance to improve, it might be due to a frozen plan. Review the DMV sys.plan_guides to learn more about the plan guides in place on this server.'') AS Details
			FROM [?].sys.plan_guides WHERE is_disabled = 0 OPTION (RECOMPILE);';
                                            end;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 60)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 60) with nowait;

                                        exec sp_MSforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 60 AS CheckID,
		  N''?'' as DatabaseName,
		  100 AS Priority,
		  ''Performance'' AS FindingsGroup,
		  ''Fill Factor Changed'',
		  ''https://BrentOzar.com/go/fillfactor'' AS URL,
		  ''The ['' + DB_NAME() + ''] database has '' + CAST(SUM(1) AS NVARCHAR(50)) + '' objects with fill factor = '' + CAST(fill_factor AS NVARCHAR(5)) + ''%. This can cause memory and storage performance problems, but may also prevent page splits.''
		  FROM    [?].sys.indexes
		  WHERE   fill_factor <> 0 AND fill_factor < 80 AND is_disabled = 0 AND is_hypothetical = 0
		  GROUP BY fill_factor OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 78)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 78) with nowait;

                                        execute master.sys.sp_msforeachdb 'USE [?];
                                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                    INSERT INTO #Recompile
                                    SELECT DISTINCT DBName = DB_Name(), SPName = SO.name, SM.is_recompiled, ISR.SPECIFIC_SCHEMA
                                    FROM sys.sql_modules AS SM
                                    LEFT OUTER JOIN master.sys.databases AS sDB ON SM.object_id = DB_id()
                                    LEFT OUTER JOIN dbo.sysobjects AS SO ON SM.object_id = SO.id and type = ''P''
                                    LEFT OUTER JOIN INFORMATION_SCHEMA.ROUTINES AS ISR on ISR.Routine_Name = SO.name AND ISR.SPECIFIC_CATALOG = DB_Name()
                                    WHERE SM.is_recompiled=1  OPTION (RECOMPILE); /* oh the rich irony of recompile here */
                                    ';
                                        insert into #blitzresults
                                        (priority,
                                         findingsgroup,
                                         finding,
                                         databasename,
                                         url,
                                         details,
                                         checkid)
                                        select [Priority]    = '100',
                                               findingsgroup = 'Performance',
                                               finding       = 'Stored Procedure WITH RECOMPILE',
                                               databasename  = dbname,
                                               url           = 'https://BrentOzar.com/go/recompile',
                                               details       = '[' + dbname + '].[' + spschema + '].[' + procname +
                                                               '] has WITH RECOMPILE in the stored procedure code, which may cause increased CPU usage due to constant recompiles of the code.',
                                               checkid       = '78'
                                        from #recompile as tr
                                        where procname not like 'sp_AllNightLog%'
                                          and procname not like 'sp_AskBrent%'
                                          and procname not like 'sp_Blitz%';
                                        drop table #recompile;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 86)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 86) with nowait;

                                        exec dbo.sp_msforeachdb
                                             'USE [?]; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details) SELECT DISTINCT 86, DB_NAME(), 230, ''Security'', ''Elevated Permissions on a Database'', ''https://BrentOzar.com/go/elevated'', (''In ['' + DB_NAME() + ''], user ['' + u.name + '']  has the role ['' + g.name + ''].  This user can perform tasks beyond just reading and writing data.'') FROM (SELECT memberuid = convert(int, member_principal_id), groupuid = convert(int, role_principal_id) FROM [?].sys.database_role_members) m inner join [?].dbo.sysusers u on m.memberuid = u.uid inner join sysusers g on m.groupuid = g.uid where u.name <> ''dbo'' and g.name in (''db_owner'' , ''db_accessadmin'' , ''db_securityadmin'' , ''db_ddladmin'') OPTION (RECOMPILE);';
                                    end;

                                /*Check for non-aligned indexes in partioned databases*/

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 72)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 72) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
								SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                insert into #partdb(dbname, objectname, type_desc)
								SELECT distinct db_name(DB_ID()) as DBName,o.name Object_Name,ds.type_desc
								FROM sys.objects AS o JOIN sys.indexes AS i ON o.object_id = i.object_id
								JOIN sys.data_spaces ds on ds.data_space_id = i.data_space_id
								LEFT OUTER JOIN sys.dm_db_index_usage_stats AS s ON i.object_id = s.object_id AND i.index_id = s.index_id AND s.database_id = DB_ID()
								WHERE  o.type = ''u''
								 -- Clustered and Non-Clustered indexes
								AND i.type IN (1, 2)
								AND o.object_id in
								  (
									SELECT a.object_id from
									  (SELECT ob.object_id, ds.type_desc from sys.objects ob JOIN sys.indexes ind on ind.object_id = ob.object_id join sys.data_spaces ds on ds.data_space_id = ind.data_space_id
									  GROUP BY ob.object_id, ds.type_desc ) a group by a.object_id having COUNT (*) > 1
								  )  OPTION (RECOMPILE);';
                                        insert into #blitzresults
                                        (checkid,
                                         databasename,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select distinct 72                                                                                                    as checkid,
                                                        dbname                                                                                                as databasename,
                                                        100                                                                                                   as priority,
                                                        'Performance'                                                                                         as findingsgroup,
                                                        'The partitioned database ' + dbname
                                                            +
                                                        ' may have non-aligned indexes'                                                                       as finding,
                                                        'https://BrentOzar.com/go/aligned'                                                                    as url,
                                                        'Having non-aligned indexes on partitioned tables may cause inefficient query plans and CPU pressure' as details
                                        from #partdb
                                        where dbname is not null
                                          and dbname not in (select distinct databasename
                                                             from #skipchecks
                                                             where checkid is null
                                                                or checkid = 72);
                                        drop table #partdb;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 113)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 113) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
							  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                              INSERT INTO #BlitzResults
									(CheckID,
									DatabaseName,
									Priority,
									FindingsGroup,
									Finding,
									URL,
									Details)
							  SELECT DISTINCT 113,
							  N''?'',
							  50,
							  ''Reliability'',
							  ''Full Text Indexes Not Updating'',
							  ''https://BrentOzar.com/go/fulltext'',
							  (''At least one full text index in this database has not been crawled in the last week.'')
							  from [?].sys.fulltext_indexes i WHERE change_tracking_state_desc <> ''AUTO'' AND i.is_enabled = 1 AND i.crawl_end_date < DATEADD(dd, -7, GETDATE())  OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 115)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 115) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
		  SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
          INSERT INTO #BlitzResults
				(CheckID,
				DatabaseName,
				Priority,
				FindingsGroup,
				Finding,
				URL,
				Details)
		  SELECT 115,
		  N''?'',
		  110,
		  ''Performance'',
		  ''Parallelism Rocket Surgery'',
		  ''https://BrentOzar.com/go/makeparallel'',
		  (''['' + DB_NAME() + ''] has a make_parallel function, indicating that an advanced developer may be manhandling SQL Server into forcing queries to go parallel.'')
		  from [?].INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = ''make_parallel'' AND ROUTINE_TYPE = ''FUNCTION'' OPTION (RECOMPILE);';
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 122)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 122) with nowait;

                                        /* SQL Server 2012 and newer uses temporary stats for Availability Groups, and those show up as user-created */
                                        if EXISTS(select *
                                                  from sys.all_columns c
                                                           inner join sys.all_objects o on c.object_id = o.object_id
                                                  where c.name = 'is_temporary'
                                                    and o.name = 'stats')
                                            exec dbo.sp_msforeachdb 'USE [?];
												SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                                INSERT INTO #BlitzResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													URL,
													Details)
												SELECT TOP 1 122,
												N''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												''https://BrentOzar.com/go/userstats'',
												(''['' + DB_NAME() + ''] has '' + CAST(SUM(1) AS NVARCHAR(10)) + '' user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1 AND is_temporary = 0
                                                HAVING SUM(1) > 0  OPTION (RECOMPILE);';

                                        else
                                            exec dbo.sp_msforeachdb 'USE [?];
												SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                                INSERT INTO #BlitzResults
													(CheckID,
													DatabaseName,
													Priority,
													FindingsGroup,
													Finding,
													URL,
													Details)
												SELECT 122,
												N''?'',
												200,
												''Performance'',
												''User-Created Statistics In Place'',
												''https://BrentOzar.com/go/userstats'',
												(''['' + DB_NAME() + ''] has '' + CAST(SUM(1) AS NVARCHAR(10)) + '' user-created statistics. This indicates that someone is being a rocket scientist with the stats, and might actually be slowing things down, especially during stats updates.'')
												from [?].sys.stats WHERE user_created = 1
                                                HAVING SUM(1) > 0 OPTION (RECOMPILE);';

                                    end;
                                /* IF NOT EXISTS ( SELECT  1 */

                                /*Check for high VLF count: this will omit any database snapshots*/

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 69)
                                    begin
                                        if @productversionmajor >= 11
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d] (2012 version of Log Info).', 0, 1, 69) with nowait;

                                                exec sp_MSforeachdb N'USE [?];
		                                      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                              INSERT INTO #LogInfo2012
		                                      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		                                      IF    @@ROWCOUNT > 999
		                                      BEGIN
			                                    INSERT  INTO #BlitzResults
			                                    ( CheckID
			                                    ,DatabaseName
			                                    ,Priority
			                                    ,FindingsGroup
			                                    ,Finding
			                                    ,URL
			                                    ,Details)
			                                    SELECT      69
			                                    ,DB_NAME()
			                                    ,170
			                                    ,''File Configuration''
			                                    ,''High VLF Count''
			                                    ,''https://BrentOzar.com/go/vlf''
			                                    ,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			                                    FROM #LogInfo2012
			                                    WHERE EXISTS (SELECT name FROM master.sys.databases
					                                    WHERE source_database_id is null)  OPTION (RECOMPILE);
		                                      END
		                                    TRUNCATE TABLE #LogInfo2012;';
                                                drop table #loginfo2012;
                                            end;
                                        else
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d] (pre-2012 version of Log Info).', 0, 1, 69) with nowait;

                                                exec sp_MSforeachdb N'USE [?];
		                                      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                              INSERT INTO #LogInfo
		                                      EXEC sp_executesql N''DBCC LogInfo() WITH NO_INFOMSGS'';
		                                      IF    @@ROWCOUNT > 999
		                                      BEGIN
			                                    INSERT  INTO #BlitzResults
			                                    ( CheckID
			                                    ,DatabaseName
			                                    ,Priority
			                                    ,FindingsGroup
			                                    ,Finding
			                                    ,URL
			                                    ,Details)
			                                    SELECT      69
			                                    ,DB_NAME()
			                                    ,170
			                                    ,''File Configuration''
			                                    ,''High VLF Count''
			                                    ,''https://BrentOzar.com/go/vlf''
			                                    ,''The ['' + DB_NAME() + ''] database has '' +  CAST(COUNT(*) as VARCHAR(20)) + '' virtual log files (VLFs). This may be slowing down startup, restores, and even inserts/updates/deletes.''
			                                    FROM #LogInfo
			                                    WHERE EXISTS (SELECT name FROM master.sys.databases
			                                    WHERE source_database_id is null) OPTION (RECOMPILE);
		                                      END
		                                      TRUNCATE TABLE #LogInfo;';
                                                drop table #loginfo;
                                            end;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 80)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 80) with nowait;

                                        exec dbo.sp_msforeachdb 'USE [?];
                                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                    INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
                                    SELECT DISTINCT 80, DB_NAME(), 170, ''Reliability'', ''Max File Size Set'', ''https://BrentOzar.com/go/maxsize'',
                                    (''The ['' + DB_NAME() + ''] database file '' + df.name + '' has a max file size set to ''
                                        + CAST(CAST(df.max_size AS BIGINT) * 8 / 1024 AS VARCHAR(100))
                                        + ''MB. If it runs out of space, the database will stop working even though there may be drive space available.'')
                                    FROM sys.database_files df
                                    WHERE 0 = (SELECT is_read_only FROM sys.databases WHERE name = ''?'')
                                      AND df.max_size <> 268435456
                                      AND df.max_size <> -1
                                      AND df.type <> 2
                                      AND df.growth > 0
                                      AND df.name <> ''DWDiagnostics'' OPTION (RECOMPILE);';

                                        delete br
                                        from #blitzresults br
                                                 inner join #skipchecks sc on sc.checkid = 80 and br.databasename = sc.databasename;
                                    end;


                                /* Check if columnstore indexes are in use - for Github issue #615 */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 74) /* Trace flags */
                                    begin
                                        truncate table #temporarydatabaseresults;

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 74) with nowait;

                                        exec dbo.sp_msforeachdb
                                             'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; IF EXISTS(SELECT * FROM sys.indexes WHERE type IN (5,6)) INSERT INTO #TemporaryDatabaseResults (DatabaseName, Finding) VALUES (DB_NAME(), ''Yup'') OPTION (RECOMPILE);';
                                        if EXISTS(select * from #temporarydatabaseresults)
                                            set @columnstoreindexesinuse = 1;
                                    end;

                                /* Non-Default Database Scoped Config - Github issue #598 */
                                if EXISTS(select * from sys.all_objects where [name] = 'database_scoped_configurations')
                                    begin

                                        if @debug in (1, 2)
                                            raiserror ('Running CheckId [%d] through [%d].', 0, 1, 194, 197) with nowait;

                                        insert into #databasescopedconfigurationdefaults (configuration_id, [name],
                                                                                          default_value,
                                                                                          default_value_for_secondary,
                                                                                          checkid)
                                        select 1, 'MAXDOP', 0, null, 194
                                        union all
                                        select 2, 'LEGACY_CARDINALITY_ESTIMATION', 0, null, 195
                                        union all
                                        select 3, 'PARAMETER_SNIFFING', 1, null, 196
                                        union all
                                        select 4, 'QUERY_OPTIMIZER_HOTFIXES', 0, null, 197;
                                        exec dbo.sp_msforeachdb 'USE [?]; SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
									SELECT def1.CheckID, DB_NAME(), 210, ''Non-Default Database Scoped Config'', dsc.[name], ''https://BrentOzar.com/go/dbscope'', (''Set value: '' + COALESCE(CAST(dsc.value AS NVARCHAR(100)),''Empty'') + '' Default: '' + COALESCE(CAST(def1.default_value AS NVARCHAR(100)),''Empty'') + '' Set value for secondary: '' + COALESCE(CAST(dsc.value_for_secondary AS NVARCHAR(100)),''Empty'') + '' Default value for secondary: '' + COALESCE(CAST(def1.default_value_for_secondary AS NVARCHAR(100)),''Empty''))
									FROM [?].sys.database_scoped_configurations dsc
									INNER JOIN #DatabaseScopedConfigurationDefaults def1 ON dsc.configuration_id = def1.configuration_id
									LEFT OUTER JOIN #DatabaseScopedConfigurationDefaults def ON dsc.configuration_id = def.configuration_id AND (dsc.value = def.default_value OR dsc.value IS NULL) AND (dsc.value_for_secondary = def.default_value_for_secondary OR dsc.value_for_secondary IS NULL)
									LEFT OUTER JOIN #SkipChecks sk ON (sk.CheckID IS NULL OR def.CheckID = sk.CheckID) AND (sk.DatabaseName IS NULL OR sk.DatabaseName = DB_NAME())
									WHERE def.configuration_id IS NULL AND sk.CheckID IS NULL ORDER BY 1
									 OPTION (RECOMPILE);';
                                    end;

                                /* Check 218 - Show me the dodgy SET Options */
                                if not EXISTS(
                                        select 1
                                        from #skipchecks
                                        where databasename is null
                                          and checkid = 218
                                    )
                                    begin
                                        if @debug in (1, 2)
                                            begin
                                                raiserror ('Running CheckId [%d].',0,1,218) with nowait;
                                            end

                                        execute sp_MSforeachdb 'USE [?];
					SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
					SELECT 218 AS CheckID
						,''?'' AS DatabaseName
						,150 AS Priority
						,''Performance'' AS FindingsGroup
						,''Objects created with dangerous SET Options'' AS Finding
						,''https://BrentOzar.com/go/badset'' AS URL
						,''The '' + QUOTENAME(DB_NAME())
							+ '' database has '' + CONVERT(VARCHAR(20),COUNT(1))
							+ '' objects that were created with dangerous ANSI_NULL or QUOTED_IDENTIFIER options.''
							+ '' These objects can break when using filtered indexes, indexed views''
							+ '' and other advanced SQL features.'' AS Details
					FROM sys.sql_modules sm
					JOIN sys.objects o ON o.[object_id] = sm.[object_id]
						AND (
							sm.uses_ansi_nulls <> 1
							OR sm.uses_quoted_identifier <> 1
							)
						AND o.is_ms_shipped = 0
					HAVING COUNT(1) > 0;';
                                    end;
                                --of Check 218.

                                /* Check 225 - Reliability - Resumable Index Operation Paused */
                                if not EXISTS(
                                        select 1
                                        from #skipchecks
                                        where databasename is null
                                          and checkid = 225
                                    )
                                    and EXISTS(select * from sys.all_objects where name = 'index_resumable_operations')
                                    begin
                                        if @debug in (1, 2)
                                            begin
                                                raiserror ('Running CheckId [%d].',0,1,218) with nowait;
                                            end

                                        execute sp_MSforeachdb 'USE [?];
					SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
					SELECT 225 AS CheckID
						,''?'' AS DatabaseName
						,200 AS Priority
						,''Reliability'' AS FindingsGroup
						,''Resumable Index Operation Paused'' AS Finding
						,''https://BrentOzar.com/go/resumable'' AS URL
						,iro.state_desc + N'' since '' + CONVERT(NVARCHAR(50), last_pause_time, 120) + '', ''
                            + CAST(iro.percent_complete AS NVARCHAR(20)) + ''% complete: ''
                            + CAST(iro.sql_text AS NVARCHAR(1000)) AS Details
					FROM sys.index_resumable_operations iro
					JOIN sys.objects o ON iro.[object_id] = o.[object_id]
					WHERE iro.state <> 0;';
                                    end;
                                --of Check 225.

                                --/* Check 220 - Statistics Without Histograms */
                                --IF NOT EXISTS (
                                --		SELECT 1
                                --		FROM #SkipChecks
                                --		WHERE DatabaseName IS NULL
                                --			AND CheckID = 220
                                --		)
                                --             AND EXISTS (SELECT * FROM sys.all_objects WHERE name = 'dm_db_stats_histogram')
                                --BEGIN
                                --	IF @Debug IN (1,2)
                                --	BEGIN
                                --		RAISERROR ('Running CheckId [%d].',0,1,220) WITH NOWAIT;
                                --	END

                                --	EXECUTE sp_MSforeachdb 'USE [?];
                                --      SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                                --		INSERT INTO #BlitzResults (CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details)
                                --		SELECT 220 AS CheckID
                                --			,DB_NAME() AS DatabaseName
                                --			,110 AS Priority
                                --			,''Performance'' AS FindingsGroup
                                --			,''Statistics Without Histograms'' AS Finding
                                --			,''https://BrentOzar.com/go/brokenstats'' AS URL
                                --			,CAST(COUNT(DISTINCT o.object_id) AS VARCHAR(100)) + '' tables have statistics that have not been updated since the database was restored or upgraded,''
                                --				+ '' and have no data in their histogram. See the More Info URL for a script to update them. '' AS Details
                                --                   FROM sys.all_objects o
                                --                   INNER JOIN sys.stats s ON o.object_id = s.object_id AND s.has_filter = 0
                                --                   OUTER APPLY sys.dm_db_stats_histogram(o.object_id, s.stats_id) h
                                --                   WHERE o.is_ms_shipped = 0 AND o.type_desc = ''USER_TABLE''
                                --                     AND h.object_id IS NULL
                                --                     AND 0 < (SELECT SUM(row_count) FROM sys.dm_db_partition_stats ps WHERE ps.object_id = o.object_id)
                                --                     AND ''?'' NOT IN (''master'', ''model'', ''msdb'', ''tempdb'')
                                --                   HAVING COUNT(DISTINCT o.object_id) > 0;';
                                --END; --of Check 220.


                            end; /* IF @CheckUserDatabaseObjects = 1 */

                        if @checkprocedurecache = 1
                            begin

                                if @debug in (1, 2) raiserror ('Begin checking procedure cache', 0, 1) with nowait;

                                begin

                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 35)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 35) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details)
                                            select 35                                                                                                                                                                                                                                                        as checkid,
                                                   100                                                                                                                                                                                                                                                       as priority,
                                                   'Performance'                                                                                                                                                                                                                                             as findingsgroup,
                                                   'Single-Use Plans in Procedure Cache'                                                                                                                                                                                                                     as finding,
                                                   'https://BrentOzar.com/go/single'                                                                                                                                                                                                                         as url,
                                                   (CAST(COUNT(*) as varchar(10))
                                                       +
                                                    ' query plans are taking up memory in the procedure cache. This may be wasted memory if we cache plans for queries that never get called again. This may be a good use case for SQL Server 2008''s Optimize for Ad Hoc or for Forced Parameterization.') as details
                                            from sys.dm_exec_cached_plans as cp
                                            where cp.usecounts = 1
                                              and cp.objtype = 'Adhoc'
                                              and EXISTS(select 1
                                                         from sys.configurations
                                                         where name = 'optimize for ad hoc workloads'
                                                           and value_in_use = 0)
                                            having COUNT(*) > 1;
                                        end;

                                    /* Set up the cache tables. Different on 2005 since it doesn't support query_hash, query_plan_hash. */
                                    if @@VERSION like '%Microsoft SQL Server 2005%'
                                        begin
                                            if @checkprocedurecachefilter = 'CPU'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM sys.dm_exec_query_stats qs
			  ORDER BY qs.total_worker_time DESC)
			  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			  FROM queries qs
			  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'Reads'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'ExecCount'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'Duration'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM sys.dm_exec_query_stats qs
			ORDER BY qs.total_elapsed_time DESC)
			INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time])
			SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time]
			FROM queries qs
			LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
			WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                        end;
                                    if @productversionmajor >= 10
                                        begin
                                            if @checkprocedurecachefilter = 'CPU'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_worker_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'Reads'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_logical_reads DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'ExecCount'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.execution_count DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            if @checkprocedurecachefilter = 'Duration'
                                                or @checkprocedurecachefilter is null
                                                begin
                                                    set @stringtoexecute = 'WITH queries ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  AS (SELECT TOP 20 qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM sys.dm_exec_query_stats qs
		  ORDER BY qs.total_elapsed_time DESC)
		  INSERT INTO #dm_exec_query_stats ([sql_handle],[statement_start_offset],[statement_end_offset],[plan_generation_num],[plan_handle],[creation_time],[last_execution_time],[execution_count],[total_worker_time],[last_worker_time],[min_worker_time],[max_worker_time],[total_physical_reads],[last_physical_reads],[min_physical_reads],[max_physical_reads],[total_logical_writes],[last_logical_writes],[min_logical_writes],[max_logical_writes],[total_logical_reads],[last_logical_reads],[min_logical_reads],[max_logical_reads],[total_clr_time],[last_clr_time],[min_clr_time],[max_clr_time],[total_elapsed_time],[last_elapsed_time],[min_elapsed_time],[max_elapsed_time],[query_hash],[query_plan_hash])
		  SELECT qs.[sql_handle],qs.[statement_start_offset],qs.[statement_end_offset],qs.[plan_generation_num],qs.[plan_handle],qs.[creation_time],qs.[last_execution_time],qs.[execution_count],qs.[total_worker_time],qs.[last_worker_time],qs.[min_worker_time],qs.[max_worker_time],qs.[total_physical_reads],qs.[last_physical_reads],qs.[min_physical_reads],qs.[max_physical_reads],qs.[total_logical_writes],qs.[last_logical_writes],qs.[min_logical_writes],qs.[max_logical_writes],qs.[total_logical_reads],qs.[last_logical_reads],qs.[min_logical_reads],qs.[max_logical_reads],qs.[total_clr_time],qs.[last_clr_time],qs.[min_clr_time],qs.[max_clr_time],qs.[total_elapsed_time],qs.[last_elapsed_time],qs.[min_elapsed_time],qs.[max_elapsed_time],qs.[query_hash],qs.[query_plan_hash]
		  FROM queries qs
		  LEFT OUTER JOIN #dm_exec_query_stats qsCaught ON qs.sql_handle = qsCaught.sql_handle AND qs.plan_handle = qsCaught.plan_handle AND qs.statement_start_offset = qsCaught.statement_start_offset
		  WHERE qsCaught.sql_handle IS NULL OPTION (RECOMPILE);';
                                                    execute (@stringtoexecute);
                                                end;

                                            /* Populate the query_plan_filtered field. Only works in 2005SP2+, but we're just doing it in 2008 to be safe. */
                                            update #dm_exec_query_stats
                                            set query_plan_filtered = qp.query_plan
                                            from #dm_exec_query_stats qs
                                                     cross apply sys.dm_exec_text_query_plan(qs.plan_handle,
                                                                                             qs.statement_start_offset,
                                                                                             qs.statement_end_offset)
                                                as qp;

                                        end;

                                    /* Populate the additional query_plan, text, and text_filtered fields */
                                    update #dm_exec_query_stats
                                    set query_plan    = qp.query_plan,
                                        [text]        = st.[text],
                                        text_filtered = SUBSTRING(st.text,
                                                                  (qs.statement_start_offset
                                                                      / 2) + 1,
                                                                  ((case qs.statement_end_offset
                                                                        when -1
                                                                            then DATALENGTH(st.text)
                                                                        else qs.statement_end_offset
                                                                        end
                                                                      - qs.statement_start_offset)
                                                                      / 2) + 1)
                                    from #dm_exec_query_stats qs
                                             cross apply sys.dm_exec_sql_text(qs.sql_handle) as st
                                             cross apply sys.dm_exec_query_plan(qs.plan_handle)
                                        as qp;

                                    /* Dump instances of our own script. We're not trying to tune ourselves. */
                                    delete #dm_exec_query_stats
                                    where text like '%sp_Blitz%'
                                       or text like '%#BlitzResults%';

                                    /* Look for implicit conversions */

                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 63)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 63) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 63                                                                                                    as checkid,
                                                   120                                                                                                   as priority,
                                                   'Query Plans'                                                                                         as findingsgroup,
                                                   'Implicit Conversion'                                                                                 as finding,
                                                   'https://BrentOzar.com/go/implicit'                                                                   as url,
                                                   ('One of the top resource-intensive queries is comparing two fields that are not the same datatype.') as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like
                                                  '%CONVERT_IMPLICIT%'
                                              and COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like
                                                  '%PhysicalOp="Index Scan"%';
                                        end;

                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 64)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 64) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 64                                                                                                                 as checkid,
                                                   120                                                                                                                as priority,
                                                   'Query Plans'                                                                                                      as findingsgroup,
                                                   'Implicit Conversion Affecting Cardinality'                                                                        as finding,
                                                   'https://BrentOzar.com/go/implicit'                                                                                as url,
                                                   ('One of the top resource-intensive queries has an implicit conversion that is affecting cardinality estimation.') as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like
                                                  '%<PlanAffectingConvert ConvertIssue="Cardinality Estimate" Expression="CONVERT_IMPLICIT%';
                                        end;

                                    /* @cms4j, 29.11.2013: Look for RID or Key Lookups */
                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 118)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 118) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 118                                                                                                                      as checkid,
                                                   120                                                                                                                      as priority,
                                                   'Query Plans'                                                                                                            as findingsgroup,
                                                   'RID or Key Lookups'                                                                                                     as finding,
                                                   'https://BrentOzar.com/go/lookup'                                                                                        as url,
                                                   'One of the top resource-intensive queries contains RID or Key Lookups. Try to avoid them by creating covering indexes.' as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like '%Lookup="1"%';
                                        end;
                                    /* @cms4j, 29.11.2013: Look for RID or Key Lookups */

                                    /* Look for missing indexes */
                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 65)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 65) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 65                                                                                             as checkid,
                                                   120                                                                                            as priority,
                                                   'Query Plans'                                                                                  as findingsgroup,
                                                   'Missing Index'                                                                                as finding,
                                                   'https://BrentOzar.com/go/missingindex'                                                        as url,
                                                   ('One of the top resource-intensive queries may be dramatically improved by adding an index.') as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like
                                                  '%MissingIndexGroup%';
                                        end;

                                    /* Look for cursors */
                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 66)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 66) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 66                                                               as checkid,
                                                   120                                                              as priority,
                                                   'Query Plans'                                                    as findingsgroup,
                                                   'Cursor'                                                         as finding,
                                                   'https://BrentOzar.com/go/cursor'                                as url,
                                                   ('One of the top resource-intensive queries is using a cursor.') as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like '%<StmtCursor%';
                                        end;

                                    /* Look for scalar user-defined functions */

                                    if not EXISTS(select 1
                                                  from #skipchecks
                                                  where databasename is null
                                                    and checkid = 67)
                                        begin

                                            if @debug in (1, 2)
                                                raiserror ('Running CheckId [%d].', 0, 1, 67) with nowait;

                                            insert into #blitzresults
                                            (checkid,
                                             priority,
                                             findingsgroup,
                                             finding,
                                             url,
                                             details,
                                             queryplan,
                                             queryplanfiltered)
                                            select 67                                                                                                                  as checkid,
                                                   120                                                                                                                 as priority,
                                                   'Query Plans'                                                                                                       as findingsgroup,
                                                   'Scalar UDFs'                                                                                                       as finding,
                                                   'https://BrentOzar.com/go/functions'                                                                                as url,
                                                   ('One of the top resource-intensive queries is using a user-defined scalar function that may inhibit parallelism.') as details,
                                                   qs.query_plan,
                                                   qs.query_plan_filtered
                                            from #dm_exec_query_stats qs
                                            where COALESCE(qs.query_plan_filtered,
                                                           CAST(qs.query_plan as nvarchar(max))) like
                                                  '%<UserDefinedFunction%';
                                        end;

                                end; /* IF @CheckProcedureCache = 1 */
                            end;

                        /*Check to see if the HA endpoint account is set at the same as the SQL Server Service Account*/
                        if @productversionmajor >= 10
                            and not EXISTS(select 1
                                           from #skipchecks
                                           where databasename is null
                                             and checkid = 187)
                            if SERVERPROPERTY('IsHadrEnabled') = 1
                                begin

                                    if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 187) with nowait;

                                    insert into [#BlitzResults]
                                    ([CheckID],
                                     [Priority],
                                     [FindingsGroup],
                                     [Finding],
                                     [URL],
                                     [Details])
                                    select 187                               as [CheckID],
                                           230                               as [Priority],
                                           'Security'                        as [FindingsGroup],
                                           'Endpoints Owned by Users'        as [Finding],
                                           'https://BrentOzar.com/go/owners' as [URL],
                                           ('Endpoint ' + ep.[name] + ' is owned by ' + SUSER_NAME(ep.principal_id) +
                                            '. If the endpoint owner login is disabled or not available due to Active Directory problems, the high availability will stop working.'
                                               )                             as [Details]
                                    from sys.database_mirroring_endpoints ep
                                             left outer join sys.dm_server_services s
                                                             on SUSER_NAME(ep.principal_id) = s.service_account
                                    where s.service_account is null
                                      and ep.principal_id <> 1;
                                end;

                        /*Check for the last good DBCC CHECKDB date */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 68)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 68) with nowait;

                                exec sp_MSforeachdb N'USE [?];
                        SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
						INSERT #DBCCs
							(ParentObject,
							Object,
							Field,
							Value)
						EXEC (''DBCC DBInfo() With TableResults, NO_INFOMSGS'');
						UPDATE #DBCCs SET DbName = N''?'' WHERE DbName IS NULL OPTION (RECOMPILE);';

                                with db2
                                         as (select distinct field,
                                                             value,
                                                             dbname
                                             from #dbccs
                                                      inner join sys.databases d on #dbccs.dbname = d.name
                                             where field = 'dbi_dbccLastKnownGood'
                                               and d.create_date < DATEADD(dd, -14, GETDATE())
                                    )
                                insert
                                into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 68                                        as checkid,
                                       db2.dbname                                as databasename,
                                       1                                         as priority,
                                       'Reliability'                             as findingsgroup,
                                       'Last good DBCC CHECKDB over 2 weeks old' as finding,
                                       'https://BrentOzar.com/go/checkdb'        as url,
                                       'Last successful CHECKDB: '
                                           + case db2.value
                                                 when '1900-01-01 00:00:00.000'
                                                     then ' never.'
                                                 else db2.value
                                           end                                   as details
                                from db2
                                where db2.dbname <> 'tempdb'
                                  and db2.dbname not in (select distinct databasename
                                                         from #skipchecks
                                                         where checkid is null
                                                            or checkid = 68)
                                  and db2.dbname not in (select name
                                                         from sys.databases
                                                         where is_read_only = 1)
                                  and CONVERT(datetime, db2.value, 121) < DATEADD(dd,
                                                                                  -14,
                                                                                  CURRENT_TIMESTAMP);
                            end;

                        /*Verify that the servername is set */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 70)
                            begin
                                if @@SERVERNAME is null
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 70) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 70                                                                                                        as checkid,
                                               200                                                                                                       as priority,
                                               'Informational'                                                                                           as findingsgroup,
                                               '@@Servername Not Set'                                                                                    as finding,
                                               'https://BrentOzar.com/go/servername'                                                                     as url,
                                               '@@Servername variable is null. You can fix it by executing: "sp_addserver ''<LocalServerName>'', local"' as details;
                                    end;

                                if /* @@SERVERNAME IS set */
                                    (@@SERVERNAME is not null
                                        and
                                        /* not a named instance */
                                     CHARINDEX(CHAR(92), CAST(SERVERPROPERTY('ServerName') as nvarchar(128))) = 0
                                        and
                                        /* not clustered, when computername may be different than the servername */
                                     SERVERPROPERTY('IsClustered') = 0
                                        and
                                        /* @@SERVERNAME is different than the computer name */
                                     @@SERVERNAME <> CAST(
                                             ISNULL(SERVERPROPERTY('ComputerNamePhysicalNetBIOS'), @@SERVERNAME) as nvarchar(128)))
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 70) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 70                                                                                            as checkid,
                                               200                                                                                           as priority,
                                               'Configuration'                                                                               as findingsgroup,
                                               '@@Servername Not Correct'                                                                    as finding,
                                               'https://BrentOzar.com/go/servername'                                                         as url,
                                               'The @@Servername is different than the computer name, which may trigger certificate errors.' as details;
                                    end;

                            end;
                        /*Check to see if a failsafe operator has been configured*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 73)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 73) with nowait;

                                declare @alertinfo table
                                                   (
                                                       failsafeoperator nvarchar(255),
                                                       notificationmethod int,
                                                       forwardingserver nvarchar(255),
                                                       forwardingseverity int,
                                                       pagertotemplate nvarchar(255),
                                                       pagercctemplate nvarchar(255),
                                                       pagersubjecttemplate nvarchar(255),
                                                       pagersendsubjectonly nvarchar(255),
                                                       forwardalways int
                                                   );
                                insert into @alertinfo
                                    exec [master].[dbo].[sp_MSgetalertinfo] @includeaddresses = 0;
                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 73                                                                                                                                                         as checkid,
                                       200                                                                                                                                                        as priority,
                                       'Monitoring'                                                                                                                                               as findingsgroup,
                                       'No Failsafe Operator Configured'                                                                                                                          as finding,
                                       'https://BrentOzar.com/go/failsafe'                                                                                                                        as url,
                                       ('No failsafe operator is configured on this server.  This is a good idea just in-case there are issues with the [msdb] database that prevents alerting.') as details
                                from @alertinfo
                                where failsafeoperator is null;
                            end;

                        /*Identify globally enabled trace flags*/
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 74)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 74) with nowait;

                                insert into #tracestatus
                                    exec ( ' DBCC TRACESTATUS(-1) WITH NO_INFOMSGS'
                                        );
                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 74                                                      as checkid,
                                       200                                                     as priority,
                                       'Informational'                                         as findingsgroup,
                                       'TraceFlag On'                                          as finding,
                                       case
                                           when [T].[TraceFlag] = '834' and @columnstoreindexesinuse = 1
                                               then 'https://support.microsoft.com/en-us/kb/3210239'
                                           else 'https://www.BrentOzar.com/go/traceflags/' end as url,
                                       'Trace flag ' +
                                       case
                                           when [T].[TraceFlag] = '2330'
                                               then ' 2330 enabled globally. Using this trace Flag disables missing index requests!'
                                           when [T].[TraceFlag] = '1211'
                                               then ' 1211 enabled globally. Using this Trace Flag disables lock escalation when you least expect it. No Bueno!'
                                           when [T].[TraceFlag] = '1224'
                                               then ' 1224 enabled globally. Using this Trace Flag disables lock escalation based on the number of locks being taken. You shouldn''t have done that, Dave.'
                                           when [T].[TraceFlag] = '652'
                                               then ' 652 enabled globally. Using this Trace Flag disables pre-fetching during index scans. If you hate slow queries, you should turn that off.'
                                           when [T].[TraceFlag] = '661'
                                               then ' 661 enabled globally. Using this Trace Flag disables ghost record removal. Who you gonna call? No one, turn that thing off.'
                                           when [T].[TraceFlag] = '1806'
                                               then ' 1806 enabled globally. Using this Trace Flag disables Instant File Initialization. I question your sanity.'
                                           when [T].[TraceFlag] = '3505'
                                               then ' 3505 enabled globally. Using this Trace Flag disables Checkpoints. Probably not the wisest idea.'
                                           when [T].[TraceFlag] = '8649'
                                               then ' 8649 enabled globally. Using this Trace Flag drops cost threshold for parallelism down to 0. I hope this is a dev server.'
                                           when [T].[TraceFlag] = '834' and @columnstoreindexesinuse = 1
                                               then ' 834 is enabled globally. Using this Trace Flag with Columnstore Indexes is not a great idea.'
                                           when [T].[TraceFlag] = '8017' and
                                                (CAST(SERVERPROPERTY('Edition') as nvarchar(1000)) like N'%Express%')
                                               then ' 8017 is enabled globally, which is the default for express edition.'
                                           when [T].[TraceFlag] = '8017' and
                                                (CAST(SERVERPROPERTY('Edition') as nvarchar(1000)) not like
                                                 N'%Express%')
                                               then ' 8017 is enabled globally. Using this Trace Flag disables creation schedulers for all logical processors. Not good.'
                                           else [T].[TraceFlag] + ' is enabled globally.' end
                                                                                               as details
                                from #tracestatus t;
                            end;

                        /* High CMEMTHREAD waits that could need trace flag 8048.
               This check has to be run AFTER the globally enabled trace flag check,
               since it uses the #TraceStatus table to know if flags are enabled.
            */
                        if @productversionmajor >= 11 and not EXISTS(select 1
                                                                     from #skipchecks
                                                                     where databasename is null
                                                                       and checkid = 162)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 162) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 162                                       as checkid,
                                       50                                        as priority,
                                       'Performance'                             as findinggroup,
                                       'Poison Wait Detected: CMEMTHREAD & NUMA' as finding,
                                       'https://BrentOzar.com/go/poison'         as url,
                                       CONVERT(varchar(10), (MAX([wait_time_ms]) / 1000) / 86400) + ':' +
                                       CONVERT(varchar(20), DATEADD(s, (MAX([wait_time_ms]) / 1000), 0), 108) +
                                       ' of this wait have been recorded'
                                           + case
                                                 when ts.status = 1 then ' despite enabling trace flag 8048 already.'
                                                 else '. In servers with over 8 cores per NUMA node, when CMEMTHREAD waits are a bottleneck, trace flag 8048 may be needed.'
                                           end
                                from sys.dm_os_nodes n
                                         inner join sys.[dm_os_wait_stats] w on w.wait_type = 'CMEMTHREAD'
                                         left outer join #tracestatus ts on ts.traceflag = 8048 and ts.status = 1
                                where n.node_id = 0
                                  and n.online_scheduler_count >= 8
                                  and EXISTS(select *
                                             from sys.dm_os_nodes
                                             where node_id > 0 and node_state_desc not like '%DAC')
                                group by w.wait_type, ts.status
                                having SUM([wait_time_ms]) >
                                       (select 5000 * datediff(hh, create_date, CURRENT_TIMESTAMP) as hours_since_startup
                                        from sys.databases
                                        where name = 'tempdb')
                                   and SUM([wait_time_ms]) > 60000;
                            end;


                        /*Check for transaction log file larger than data file */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 75)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 75) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 75                                                                                                                                                                       as checkid,
                                       DB_NAME(a.database_id),
                                       50                                                                                                                                                                       as priority,
                                       'Reliability'                                                                                                                                                            as findingsgroup,
                                       'Transaction Log Larger than Data File'                                                                                                                                  as finding,
                                       'https://BrentOzar.com/go/biglog'                                                                                                                                        as url,
                                       'The database [' + DB_NAME(a.database_id)
                                           + '] has a ' + CAST((CAST(a.size as bigint) * 8 / 1000000) as nvarchar(20)) +
                                       ' GB transaction log file, larger than the total data file sizes. This may indicate that transaction log backups are not being performed or not performed often enough.' as details
                                from sys.master_files a
                                where a.type = 1
                                  and DB_NAME(a.database_id) not in (
                                    select distinct databasename
                                    from #skipchecks
                                    where checkid = 75
                                       or checkid is null)
                                  and a.size > 125000 /* Size is measured in pages here, so this gets us log files over 1GB. */
                                  and a.size > (select SUM(CAST(b.size as bigint))
                                                from sys.master_files b
                                                where a.database_id = b.database_id
                                                  and b.type = 0
                                )
                                  and a.database_id in (
                                    select database_id
                                    from sys.databases
                                    where source_database_id is null);
                            end;

                        /*Check for collation conflicts between user databases and tempdb */
                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 76)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 76) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 76                                                                                                                    as checkid,
                                       name                                                                                                                  as databasename,
                                       200                                                                                                                   as priority,
                                       'Informational'                                                                                                       as findingsgroup,
                                       'Collation is ' + collation_name                                                                                      as finding,
                                       'https://BrentOzar.com/go/collate'                                                                                    as url,
                                       'Collation differences between user databases and tempdb can cause conflicts especially when comparing string values' as details
                                from sys.databases
                                where name not in ('master', 'model', 'msdb')
                                  and name not like 'ReportServer%'
                                  and name not in (select distinct databasename
                                                   from #skipchecks
                                                   where checkid is null
                                                      or checkid = 76)
                                  and collation_name <> (select collation_name
                                                         from sys.databases
                                                         where name = 'tempdb'
                                );
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 77)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 77) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 databasename,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 77                                                                                                  as checkid,
                                       dsnap.[name]                                                                                        as databasename,
                                       50                                                                                                  as priority,
                                       'Reliability'                                                                                       as findingsgroup,
                                       'Database Snapshot Online'                                                                          as finding,
                                       'https://BrentOzar.com/go/snapshot'                                                                 as url,
                                       'Database [' + dsnap.[name]
                                           + '] is a snapshot of ['
                                           + doriginal.[name]
                                           +
                                       ']. Make sure you have enough drive space to maintain the snapshot as the original database grows.' as details
                                from sys.databases dsnap
                                         inner join sys.databases doriginal
                                                    on dsnap.source_database_id = doriginal.database_id
                                                        and dsnap.name not in (
                                                            select distinct databasename
                                                            from #skipchecks
                                                            where checkid = 77
                                                               or checkid is null);
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 79)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 79) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 79                                    as checkid,
                                       -- sp_Blitz Issue #776
                                       -- Job has history and was executed in the last 30 days OR Job is enabled AND Job Schedule is enabled
                                       case
                                           when (cast(datediff(dd, substring(cast(sjh.run_date as nvarchar(10)), 1, 4) +
                                                                   '-' +
                                                                   substring(cast(sjh.run_date as nvarchar(10)), 5, 2) +
                                                                   '-' +
                                                                   substring(cast(sjh.run_date as nvarchar(10)), 7, 2),
                                                               GETDATE()) as int) < 30) or
                                                (j.[enabled] = 1 and ssc.[enabled] = 1) then
                                               100
                                           else -- no job history (implicit) AND job not run in the past 30 days AND (Job disabled OR Job Schedule disabled)
                                               200
                                           end                               as priority,
                                       'Performance'                         as findingsgroup,
                                       'Shrink Database Job'                 as finding,
                                       'https://BrentOzar.com/go/autoshrink' as url,
                                       'In the [' + j.[name] + '] job, step ['
                                           + step.[step_name]
                                           +
                                       '] has SHRINKDATABASE or SHRINKFILE, which may be causing database fragmentation.'
                                           + case
                                                 when COALESCE(ssc.name, '0') != '0'
                                                     then + ' (Schedule: [' + ssc.name + '])'
                                                 else + '' end               as details
                                from msdb.dbo.sysjobs j
                                         inner join msdb.dbo.sysjobsteps step on j.job_id = step.job_id
                                         left outer join msdb.dbo.sysjobschedules as sjsc
                                                         on j.job_id = sjsc.job_id
                                         left outer join msdb.dbo.sysschedules as ssc
                                                         on sjsc.schedule_id = ssc.schedule_id
                                                             and sjsc.job_id = j.job_id
                                         left outer join msdb.dbo.sysjobhistory as sjh
                                                         on j.job_id = sjh.job_id
                                                             and step.step_id = sjh.step_id
                                                             and sjh.run_date in (select max(sjh2.run_date)
                                                                                  from msdb.dbo.sysjobhistory as sjh2
                                                                                  where sjh2.job_id = j.job_id) -- get the latest entry date
                                                             and sjh.run_time in (select max(sjh3.run_time)
                                                                                  from msdb.dbo.sysjobhistory as sjh3
                                                                                  where sjh3.job_id = j.job_id
                                                                                    and sjh3.run_date = sjh.run_date) -- get the latest entry time
                                where step.command like N'%SHRINKDATABASE%'
                                   or step.command like N'%SHRINKFILE%';
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 81)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 81) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select 81                                                                                                     as checkid,
                                       200                                                                                                    as priority,
                                       'Non-Active Server Config'                                                                             as findingsgroup,
                                       cr.name                                                                                                as finding,
                                       'https://www.BrentOzar.com/blitz/sp_configure/'                                                        as url,
                                       ('This sp_configure option isn''t running under its set value.  Its set value is '
                                           + CAST(cr.[value] as varchar(100))
                                           + ' and its running value is '
                                           + CAST(cr.value_in_use as varchar(100))
                                           +
                                        '. When someone does a RECONFIGURE or restarts the instance, this setting will start taking effect.') as details
                                from sys.configurations cr
                                where cr.value <> cr.value_in_use
                                  and not (cr.name = 'min server memory (MB)' and cr.value in (0, 16) and
                                           cr.value_in_use in (0, 16));
                            end;

                        if not EXISTS(select 1
                                      from #skipchecks
                                      where databasename is null
                                        and checkid = 123)
                            begin

                                if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 123) with nowait;

                                insert into #blitzresults
                                (checkid,
                                 priority,
                                 findingsgroup,
                                 finding,
                                 url,
                                 details)
                                select top 1 123                                                                                                                                  as checkid,
                                             200                                                                                                                                  as priority,
                                             'Informational'                                                                                                                      as findingsgroup,
                                             'Agent Jobs Starting Simultaneously'                                                                                                 as finding,
                                             'https://BrentOzar.com/go/busyagent/'                                                                                                as url,
                                             ('Multiple SQL Server Agent jobs are configured to start simultaneously. For detailed schedule listings, see the query in the URL.') as details
                                from msdb.dbo.sysjobactivity
                                where start_execution_date > DATEADD(dd, -14, GETDATE())
                                group by start_execution_date
                                having COUNT(*) > 1;
                            end;

                        if @checkserverinfo = 1
                            begin

                                /*This checks Windows version. It would be better if Microsoft gave everything a separate build number, but whatever.*/
                                if @productversionmajor >= 10
                                    and not EXISTS(select 1
                                                   from #skipchecks
                                                   where databasename is null
                                                     and checkid = 172)
                                    begin
                                        -- sys.dm_os_host_info includes both Windows and Linux info
                                        if EXISTS(select 1
                                                  from sys.all_objects
                                                  where name = 'dm_os_host_info')
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 172) with nowait;

                                                insert into [#BlitzResults]
                                                ([CheckID],
                                                 [Priority],
                                                 [FindingsGroup],
                                                 [Finding],
                                                 [URL],
                                                 [Details])

                                                select 172                        as [CheckID],
                                                       250                        as [Priority],
                                                       'Server Info'              as [FindingsGroup],
                                                       'Operating System Version' as [Finding],
                                                       (case
                                                            when @iswindowsoperatingsystem = 1
                                                                then 'https://en.wikipedia.org/wiki/List_of_Microsoft_Windows_versions'
                                                            else 'https://en.wikipedia.org/wiki/List_of_Linux_distributions'
                                                           end
                                                           )                      as [URL],
                                                       (case
                                                            when [ohi].[host_platform] = 'Linux' then
                                                                    'You''re running the ' +
                                                                    CAST([ohi].[host_distribution] as varchar(35)) +
                                                                    ' distribution of ' +
                                                                    CAST([ohi].[host_platform] as varchar(35)) +
                                                                    ', version ' +
                                                                    CAST([ohi].[host_release] as varchar(5))
                                                            when [ohi].[host_platform] = 'Windows' and [ohi].[host_release] = '5'
                                                                then 'You''re running Windows 2000, version ' +
                                                                     CAST([ohi].[host_release] as varchar(5))
                                                            when [ohi].[host_platform] = 'Windows' and [ohi].[host_release] > '5'
                                                                then 'You''re running ' +
                                                                     CAST([ohi].[host_distribution] as varchar(50)) +
                                                                     ', version ' +
                                                                     CAST([ohi].[host_release] as varchar(5))
                                                            else 'You''re running ' +
                                                                 CAST([ohi].[host_distribution] as varchar(35)) +
                                                                 ', version ' + CAST([ohi].[host_release] as varchar(5))
                                                           end
                                                           )                      as [Details]
                                                from [sys].[dm_os_host_info] [ohi];
                                            end;
                                        else
                                            begin
                                                -- Otherwise, stick with Windows-only detection

                                                if EXISTS(select 1
                                                          from sys.all_objects
                                                          where name = 'dm_os_windows_info')
                                                    begin

                                                        if @debug in (1, 2)
                                                            raiserror ('Running CheckId [%d].', 0, 1, 172) with nowait;

                                                        insert into [#BlitzResults]
                                                        ([CheckID],
                                                         [Priority],
                                                         [FindingsGroup],
                                                         [Finding],
                                                         [URL],
                                                         [Details])

                                                        select 172                                                                as [CheckID],
                                                               250                                                                as [Priority],
                                                               'Server Info'                                                      as [FindingsGroup],
                                                               'Windows Version'                                                  as [Finding],
                                                               'https://en.wikipedia.org/wiki/List_of_Microsoft_Windows_versions' as [URL],
                                                               (case
                                                                    when [owi].[windows_release] = '5' then
                                                                            'You''re running Windows 2000, version ' +
                                                                            CAST([owi].[windows_release] as varchar(5))
                                                                    when [owi].[windows_release] > '5' and [owi].[windows_release] < '6'
                                                                        then
                                                                            'You''re running Windows Server 2003/2003R2 era, version ' +
                                                                            CAST([owi].[windows_release] as varchar(5))
                                                                    when [owi].[windows_release] >= '6' and [owi].[windows_release] <= '6.1'
                                                                        then
                                                                            'You''re running Windows Server 2008/2008R2 era, version ' +
                                                                            CAST([owi].[windows_release] as varchar(5))
                                                                    when [owi].[windows_release] >= '6.2' and [owi].[windows_release] <= '6.3'
                                                                        then
                                                                            'You''re running Windows Server 2012/2012R2 era, version ' +
                                                                            CAST([owi].[windows_release] as varchar(5))
                                                                    when [owi].[windows_release] = '10.0' then
                                                                            'You''re running Windows Server 2016/2019 era, version ' +
                                                                            CAST([owi].[windows_release] as varchar(5))
                                                                    else 'You''re running Windows Server, version ' +
                                                                         CAST([owi].[windows_release] as varchar(5))
                                                                   end
                                                                   )                                                              as [Details]
                                                        from [sys].[dm_os_windows_info] [owi];

                                                    end;
                                            end;
                                    end;

/*
This check hits the dm_os_process_memory system view
to see if locked_page_allocations_kb is > 0,
which could indicate that locked pages in memory is enabled.
*/
                                if @productversionmajor >= 10 and not EXISTS(select 1
                                                                             from #skipchecks
                                                                             where databasename is null
                                                                               and checkid = 166)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 166) with nowait;

                                        insert into [#BlitzResults]
                                        ([CheckID],
                                         [Priority],
                                         [FindingsGroup],
                                         [Finding],
                                         [URL],
                                         [Details])
                                        select 166                                       as [CheckID],
                                               250                                       as [Priority],
                                               'Server Info'                             as [FindingsGroup],
                                               'Locked Pages In Memory Enabled'          as [Finding],
                                               'https://BrentOzar.com/go/lpim'           as [URL],
                                               ('You currently have '
                                                   + case
                                                         when [dopm].[locked_page_allocations_kb] / 1024. / 1024. > 0
                                                             then CAST(
                                                                          [dopm].[locked_page_allocations_kb] / 1024 / 1024 as varchar(100))
                                                             + ' GB'
                                                         else CAST([dopm].[locked_page_allocations_kb] / 1024 as varchar(100))
                                                             + ' MB'
                                                    end + ' of pages locked in memory.') as [Details]
                                        from [sys].[dm_os_process_memory] as [dopm]
                                        where [dopm].[locked_page_allocations_kb] > 0;
                                    end;

                                /* Server Info - Locked Pages In Memory Enabled - Check 166 - SQL Server 2016 SP1 and newer */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 166)
                                    and EXISTS(select *
                                               from sys.all_objects o
                                                        inner join sys.all_columns c on o.object_id = c.object_id
                                               where o.name = 'dm_os_sys_info'
                                                 and c.name = 'sql_memory_model')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 166) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  166 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Memory Model Unconventional'' AS Finding ,
			''https://BrentOzar.com/go/lpim'' AS URL ,
			''Memory Model: '' + CAST(sql_memory_model_desc AS NVARCHAR(100))
			FROM sys.dm_os_sys_info WHERE sql_memory_model <> 1 OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                /*
			Starting with SQL Server 2014 SP2, Instant File Initialization
			is logged in the SQL Server Error Log.
			*/
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 184)
                                       and (@productversionmajor >= 13) or
                                   (@productversionmajor = 12 and @productversionminor >= 5000)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 184) with nowait;

                                        insert into #errorlog
                                            exec sys.xp_readerrorlog 0, 1,
                                                 N'Database Instant File Initialization: enabled';

                                        if @@ROWCOUNT > 0
                                            insert into #blitzresults
                                            (checkid,
                                             [Priority],
                                             findingsgroup,
                                             finding,
                                             url,
                                             details)
                                            select 193                                   as [CheckID],
                                                   250                                   as [Priority],
                                                   'Server Info'                         as [FindingsGroup],
                                                   'Instant File Initialization Enabled' as [Finding],
                                                   'https://BrentOzar.com/go/instant'    as [URL],
                                                   'The service account has the Perform Volume Maintenance Tasks permission.';
                                    end;

                                /* Server Info - Instant File Initialization Not Enabled - Check 192 - SQL Server 2016 SP1 and newer */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 192)
                                    and EXISTS(select *
                                               from sys.all_objects o
                                                        inner join sys.all_columns c on o.object_id = c.object_id
                                               where o.name = 'dm_server_services'
                                                 and c.name = 'instant_file_initialization_enabled')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 192) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  192 AS CheckID ,
			50 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Instant File Initialization Not Enabled'' AS Finding ,
			''https://BrentOzar.com/go/instant'' AS URL ,
			''Consider enabling IFI for faster restores and data file growths.''
			FROM sys.dm_server_services WHERE instant_file_initialization_enabled <> ''Y'' AND filename LIKE ''%sqlservr.exe%'' OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 130)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 130) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 130                                   as checkid,
                                               250                                   as priority,
                                               'Server Info'                         as findingsgroup,
                                               'Server Name'                         as finding,
                                               'https://BrentOzar.com/go/servername' as url,
                                               @@SERVERNAME                          as details
                                        where @@SERVERNAME is not null;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 83)
                                    begin
                                        if EXISTS(select *
                                                  from sys.all_objects
                                                  where name = 'dm_server_services')
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 83) with nowait;

                                                -- DATETIMEOFFSET and DATETIME have different minimum values, so there's
                                                -- a small workaround here to force 1753-01-01 if the minimum is detected
                                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
				SELECT  83 AS CheckID ,
				250 AS Priority ,
				''Server Info'' AS FindingsGroup ,
				''Services'' AS Finding ,
				'''' AS URL ,
				N''Service: '' + servicename + N'' runs under service account '' + service_account + N''. Last startup time: '' + COALESCE(CAST(CASE WHEN YEAR(last_startup_time) <= 1753 THEN CAST(''17530101'' as datetime) ELSE CAST(last_startup_time AS DATETIME) END AS VARCHAR(50)), ''not shown.'') + ''. Startup type: '' + startup_type_desc + N'', currently '' + status_desc + ''.''
				FROM sys.dm_server_services OPTION (RECOMPILE);';

                                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                                if @debug = 2 and @stringtoexecute is null
                                                    print '@StringToExecute has gone NULL, for some reason.';

                                                execute (@stringtoexecute);
                                            end;
                                    end;

                                /* Check 84 - SQL Server 2012 */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 84)
                                    begin
                                        if EXISTS(select *
                                                  from sys.all_objects o
                                                           inner join sys.all_columns c on o.object_id = c.object_id
                                                  where o.name = 'dm_os_sys_info'
                                                    and c.name = 'physical_memory_kb')
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 84) with nowait;

                                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			'''' AS URL ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_kb / 1024.0 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info OPTION (RECOMPILE);';

                                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                                if @debug = 2 and @stringtoexecute is null
                                                    print '@StringToExecute has gone NULL, for some reason.';

                                                execute (@stringtoexecute);
                                            end;

                                        /* Check 84 - SQL Server 2008 */
                                        if EXISTS(select *
                                                  from sys.all_objects o
                                                           inner join sys.all_columns c on o.object_id = c.object_id
                                                  where o.name = 'dm_os_sys_info'
                                                    and c.name = 'physical_memory_in_bytes')
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 84) with nowait;

                                                set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
			SELECT  84 AS CheckID ,
			250 AS Priority ,
			''Server Info'' AS FindingsGroup ,
			''Hardware'' AS Finding ,
			'''' AS URL ,
			''Logical processors: '' + CAST(cpu_count AS VARCHAR(50)) + ''. Physical memory: '' + CAST( CAST(ROUND((physical_memory_in_bytes / 1024.0 / 1024 / 1024), 1) AS INT) AS VARCHAR(50)) + ''GB.''
			FROM sys.dm_os_sys_info OPTION (RECOMPILE);';

                                                if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                                if @debug = 2 and @stringtoexecute is null
                                                    print '@StringToExecute has gone NULL, for some reason.';

                                                execute (@stringtoexecute);
                                            end;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 85)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 85) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 85                   as checkid,
                                               250                  as priority,
                                               'Server Info'        as findingsgroup,
                                               'SQL Server Service' as finding,
                                               ''                   as url,
                                               N'Version: '
                                                   + CAST(SERVERPROPERTY('productversion') as nvarchar(100))
                                                   + N'. Patch Level: '
                                                   + CAST(SERVERPROPERTY('productlevel') as nvarchar(100))
                                                   + case
                                                         when SERVERPROPERTY('ProductUpdateLevel') is null
                                                             then N''
                                                         else N'. Cumulative Update: '
                                                             +
                                                              CAST(SERVERPROPERTY('ProductUpdateLevel') as nvarchar(100))
                                                   end
                                                   + N'. Edition: '
                                                   + CAST(SERVERPROPERTY('edition') as varchar(100))
                                                   + N'. Availability Groups Enabled: '
                                                   + CAST(COALESCE(SERVERPROPERTY('IsHadrEnabled'),
                                                                   0) as varchar(100))
                                                   + N'. Availability Groups Manager Status: '
                                                   + CAST(COALESCE(SERVERPROPERTY('HadrManagerStatus'),
                                                                   0) as varchar(100));
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 88)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 88) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 88                        as checkid,
                                               250                       as priority,
                                               'Server Info'             as findingsgroup,
                                               'SQL Server Last Restart' as finding,
                                               ''                        as url,
                                               CAST(create_date as varchar(100))
                                        from sys.databases
                                        where database_id = 2;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 91)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 91) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 91                    as checkid,
                                               250                   as priority,
                                               'Server Info'         as findingsgroup,
                                               'Server Last Restart' as finding,
                                               ''                    as url,
                                               CAST(
                                                       DATEADD(second, (ms_ticks / 1000) * (-1), GETDATE()) as nvarchar(25))
                                        from sys.dm_os_sys_info;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 92)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 92) with nowait;

                                        insert into #driveinfo
                                            (drive, size)
                                            exec master..xp_fixeddrives;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 92                            as checkid,
                                               250                           as priority,
                                               'Server Info'                 as findingsgroup,
                                               'Drive ' + i.drive + ' Space' as finding,
                                               ''                            as url,
                                               CAST(i.size as varchar(30))
                                                   + 'MB free on ' + i.drive
                                                   + ' drive'                as details
                                        from #driveinfo as i;
                                        drop table #driveinfo;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 103)
                                    and EXISTS(select *
                                               from sys.all_objects o
                                                        inner join sys.all_columns c on o.object_id = c.object_id
                                               where o.name = 'dm_os_sys_info'
                                                 and c.name = 'virtual_machine_type_desc')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 103) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
									SELECT 103 AS CheckID,
									250 AS Priority,
									''Server Info'' AS FindingsGroup,
									''Virtual Server'' AS Finding,
									''https://BrentOzar.com/go/virtual'' AS URL,
									''Type: ('' + virtual_machine_type_desc + '')'' AS Details
									FROM sys.dm_os_sys_info
									WHERE virtual_machine_type <> 0 OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 214)
                                    and EXISTS(select *
                                               from sys.all_objects o
                                                        inner join sys.all_columns c on o.object_id = c.object_id
                                               where o.name = 'dm_os_sys_info'
                                                 and c.name = 'container_type_desc')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 214) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
									SELECT 214 AS CheckID,
									250 AS Priority,
									''Server Info'' AS FindingsGroup,
									''Container'' AS Finding,
									''https://BrentOzar.com/go/virtual'' AS URL,
									''Type: ('' + container_type_desc + '')'' AS Details
									FROM sys.dm_os_sys_info
									WHERE container_type_desc <> ''NONE'' OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 114)
                                    and EXISTS(select *
                                               from sys.all_objects o
                                               where o.name = 'dm_os_memory_nodes')
                                    and EXISTS(select *
                                               from sys.all_objects o
                                                        inner join sys.all_columns c on o.object_id = c.object_id
                                               where o.name = 'dm_os_nodes'
                                                 and c.name = 'processor_group')
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 114) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
										SELECT  114 AS CheckID ,
												250 AS Priority ,
												''Server Info'' AS FindingsGroup ,
												''Hardware - NUMA Config'' AS Finding ,
												'''' AS URL ,
												''Node: '' + CAST(n.node_id AS NVARCHAR(10)) + '' State: '' + node_state_desc
												+ '' Online schedulers: '' + CAST(n.online_scheduler_count AS NVARCHAR(10)) + '' Offline schedulers: '' + CAST(oac.offline_schedulers AS VARCHAR(100)) + '' Processor Group: '' + CAST(n.processor_group AS NVARCHAR(10))
												+ '' Memory node: '' + CAST(n.memory_node_id AS NVARCHAR(10)) + '' Memory VAS Reserved GB: '' + CAST(CAST((m.virtual_address_space_reserved_kb / 1024.0 / 1024) AS INT) AS NVARCHAR(100))
										FROM sys.dm_os_nodes n
										INNER JOIN sys.dm_os_memory_nodes m ON n.memory_node_id = m.memory_node_id
										OUTER APPLY (SELECT
										COUNT(*) AS [offline_schedulers]
										FROM sys.dm_os_schedulers dos
										WHERE n.node_id = dos.parent_node_id
										AND dos.status = ''VISIBLE OFFLINE''
										) oac
										WHERE n.node_state_desc NOT LIKE ''%DAC%''
										ORDER BY n.node_id OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;


                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 211)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 211) with nowait;

                                        declare @outval varchar(36);
                                        /* Get power plan if set by group policy [Git Hub Issue #1620] */
                                        exec master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
                                             @key = 'SOFTWARE\Policies\Microsoft\Power\PowerSettings',
                                             @value_name = 'ActivePowerScheme',
                                             @value = @outval output;

                                        if @outval is null /* If power plan was not set by group policy, get local value [Git Hub Issue #1620]*/
                                            exec master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
                                                 @key = 'SYSTEM\CurrentControlSet\Control\Power\User\PowerSchemes',
                                                 @value_name = 'ActivePowerScheme',
                                                 @value = @outval output;

                                        declare @cpu_speed_mhz int,
                                            @cpu_speed_ghz decimal(18, 2);

                                        exec master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
                                             @key = 'HARDWARE\DESCRIPTION\System\CentralProcessor\0',
                                             @value_name = '~MHz',
                                             @value = @cpu_speed_mhz output;

                                        select @cpu_speed_ghz =
                                               CAST(CAST(@cpu_speed_mhz as decimal) / 1000 as decimal(18, 2));

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 211                                           as checkid,
                                               250                                           as priority,
                                               'Server Info'                                 as findingsgroup,
                                               'Power Plan'                                  as finding,
                                               'https://www.brentozar.com/blitz/power-mode/' as url,
                                               'Your server has '
                                                   + CAST(@cpu_speed_ghz as varchar(4))
                                                   + 'GHz CPUs, and is in '
                                                   + case @outval
                                                         when 'a1841308-3541-4fab-bc81-f71556f20b4a'
                                                             then 'power saving mode -- are you sure this is a production SQL Server?'
                                                         when '381b4222-f694-41f0-9685-ff5bb260df2e'
                                                             then 'balanced power mode -- Uh... you want your CPUs to run at full speed, right?'
                                                         when '8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c'
                                                             then 'high performance power mode'
                                                         when 'e9a42b02-d5df-448d-aa00-03f14749eb61'
                                                             then 'ultimate performance power mode'
                                                         else 'an unknown power mode.'
                                                   end                                       as details

                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 212)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 212) with nowait;

                                        insert into #instances (instance_number, instance_name, data_field)
                                            exec master.sys.xp_regread @rootkey = 'HKEY_LOCAL_MACHINE',
                                                 @key = 'SOFTWARE\Microsoft\Microsoft SQL Server',
                                                 @value_name = 'InstalledInstances'

                                        if (select COUNT(*) from #instances) > 1
                                            begin

                                                declare @instancecount nvarchar(max)
                                                select @instancecount = COUNT(*) from #instances

                                                insert into #blitzresults
                                                (checkid,
                                                 priority,
                                                 findingsgroup,
                                                 finding,
                                                 url,
                                                 details)
                                                select 212                                            as checkid,
                                                       250                                            as priority,
                                                       'Server Info'                                  as findingsgroup,
                                                       'Instance Stacking'                            as finding,
                                                       'https://www.brentozar.com/go/babygotstacked/' as url,
                                                       'Your Server has ' + @instancecount +
                                                       ' Instances of SQL Server installed. More than one is usually a bad idea. Read the URL for more info.'
                                            end;
                                    end;

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 106)
                                    and (select convert(int, value_in_use)
                                         from sys.configurations
                                         where name = 'default trace enabled') = 1
                                    and DATALENGTH(COALESCE(@base_tracefilename, '')) > DATALENGTH('.TRC')
                                    and @tracefileissue = 0
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 106) with nowait;

                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 106                              as checkid
                                             , 250                              as priority
                                             , 'Server Info'                    as findingsgroup
                                             , 'Default Trace Contents'         as finding
                                             , 'https://BrentOzar.com/go/trace' as url
                                             , 'The default trace holds ' +
                                               cast(DATEDIFF(hour, MIN(starttime), GETDATE()) as varchar(30)) +
                                               ' hours of data'
                                            + ' between ' + cast(Min(starttime) as varchar(30)) + ' and ' +
                                               cast(GETDATE() as varchar(30))
                                            + ('. The default trace files are located in: ' +
                                               left(@curr_tracefilename, len(@curr_tracefilename) - @indx)
                                                   )                            as details
                                        from ::fn_trace_gettable(@base_tracefilename, default)
                                        where eventclass between 65500 and 65600;
                                    end; /* CheckID 106 */

                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 152)
                                    begin
                                        if EXISTS(select *
                                                  from sys.dm_os_wait_stats ws
                                                           left outer join #ignorablewaits i on ws.wait_type = i.wait_type
                                                  where wait_time_ms > .1 * @cpumssincewaitscleared
                                                    and waiting_tasks_count > 0
                                                    and i.wait_type is null)
                                            begin
                                                /* Check for waits that have had more than 10% of the server's wait time */

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 152) with nowait;

                                                with os(wait_type, waiting_tasks_count, wait_time_ms, max_wait_time_ms,
                                                        signal_wait_time_ms)
                                                         as
                                                         (select ws.wait_type,
                                                                 waiting_tasks_count,
                                                                 wait_time_ms,
                                                                 max_wait_time_ms,
                                                                 signal_wait_time_ms
                                                          from sys.dm_os_wait_stats ws
                                                                   left outer join #ignorablewaits i on ws.wait_type = i.wait_type
                                                          where i.wait_type is null
                                                            and wait_time_ms > .1 * @cpumssincewaitscleared
                                                            and waiting_tasks_count > 0)
                                                insert
                                                into #blitzresults
                                                (checkid,
                                                 priority,
                                                 findingsgroup,
                                                 finding,
                                                 url,
                                                 details)
                                                select top 9           152                                                                 as checkid
                                                           ,           240                                                                 as priority
                                                           ,           'Wait Stats'                                                        as findingsgroup
                                                           ,           CAST(
                                                                               ROW_NUMBER() over (order by os.wait_time_ms desc) as nvarchar(10)) +
                                                                       N' - ' +
                                                                       os.wait_type                                                        as finding
                                                           ,           'https://www.sqlskills.com/help/waits/' + LOWER(os.wait_type) + '/' as url
                                                           , details = CAST(CAST(
                                                        SUM(os.wait_time_ms / 1000.0 / 60 / 60) over (partition by os.wait_type) as numeric(18, 1)) as nvarchar(20)) +
                                                                       N' hours of waits, ' +
                                                                       CAST(CAST(
                                                                                   (SUM(60.0 * os.wait_time_ms) over (partition by os.wait_type)) /
                                                                                   @mssincewaitscleared as numeric(18, 1)) as nvarchar(20)) +
                                                                       N' minutes average wait time per hour, ' +
                                                                           /* CAST(CAST(
														100.* SUM(os.wait_time_ms) OVER (PARTITION BY os.wait_type)
														/ (1. * SUM(os.wait_time_ms) OVER () )
														AS NUMERIC(18,1)) AS NVARCHAR(40)) + N'% of waits, ' + */
                                                                       CAST(CAST(
                                                                                   100. * SUM(os.signal_wait_time_ms) over (partition by os.wait_type)
                                                                                   / (1. * SUM(os.wait_time_ms) over ())
                                                                           as numeric(18, 1)) as nvarchar(40)) +
                                                                       N'% signal wait, ' +
                                                                       CAST(
                                                                               SUM(os.waiting_tasks_count) over (partition by os.wait_type) as nvarchar(40)) +
                                                                       N' waiting tasks, ' +
                                                                       CAST(case
                                                                                when SUM(os.waiting_tasks_count) over (partition by os.wait_type) > 0
                                                                                    then
                                                                                    CAST(
                                                                                                SUM(os.wait_time_ms) over (partition by os.wait_type)
                                                                                                /
                                                                                                (1. * SUM(os.waiting_tasks_count) over (partition by os.wait_type))
                                                                                        as numeric(18, 1))
                                                                                else 0 end as nvarchar(40)) +
                                                                       N' ms average wait time.'
                                                from os
                                                order by SUM(os.wait_time_ms / 1000.0 / 60 / 60) over (partition by os.wait_type) desc;
                                            end;
                                        /* IF EXISTS (SELECT * FROM sys.dm_os_wait_stats WHERE wait_time_ms > 0 AND waiting_tasks_count > 0) */

                                        /* If no waits were found, add a note about that */
                                        if not EXISTS(select *
                                                      from #blitzresults
                                                      where checkid in (107, 108, 109, 121, 152, 162))
                                            begin

                                                if @debug in (1, 2)
                                                    raiserror ('Running CheckId [%d].', 0, 1, 153) with nowait;

                                                insert into #blitzresults
                                                (checkid,
                                                 priority,
                                                 findingsgroup,
                                                 finding,
                                                 url,
                                                 details)
                                                values (153, 240, 'Wait Stats', 'No Significant Waits Detected',
                                                        'https://BrentOzar.com/go/waits',
                                                        'This server might be just sitting around idle, or someone may have cleared wait stats recently.');
                                            end;
                                    end;
                                /* CheckID 152 */

                                /* CheckID 222 - Server Info - Azure Managed Instance */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 222)
                                    and 4 = (select COUNT(*)
                                             from sys.all_objects o
                                                      inner join sys.all_columns c on o.object_id = c.object_id
                                             where o.name = 'dm_os_job_object'
                                               and c.name in ('cpu_rate', 'memory_limit_mb', 'process_memory_limit_mb',
                                                              'workingset_limit_mb'))
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 222) with nowait;

                                        set @stringtoexecute = 'INSERT INTO #BlitzResults (CheckID, Priority, FindingsGroup, Finding, URL, Details)
										SELECT  222 AS CheckID ,
												250 AS Priority ,
												''Server Info'' AS FindingsGroup ,
												''Azure Managed Instance'' AS Finding ,
												''https://www.BrenOzar.com/go/azurevm'' AS URL ,
												''cpu_rate: '' + CAST(COALESCE(cpu_rate, 0) AS VARCHAR(20)) +
												'', memory_limit_mb: '' + CAST(COALESCE(memory_limit_mb, 0) AS NVARCHAR(20)) +
												'', process_memory_limit_mb: '' + CAST(COALESCE(process_memory_limit_mb, 0) AS NVARCHAR(20)) +
												'', workingset_limit_mb: '' + CAST(COALESCE(workingset_limit_mb, 0) AS NVARCHAR(20))
										FROM sys.dm_os_job_object OPTION (RECOMPILE);';

                                        if @debug = 2 and @stringtoexecute is not null print @stringtoexecute;
                                        if @debug = 2 and @stringtoexecute is null
                                            print '@StringToExecute has gone NULL, for some reason.';

                                        execute (@stringtoexecute);
                                    end;

                                /* CheckID 224 - Performance - SSRS/SSAS/SSIS Installed */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 224)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 224) with nowait;

                                        if (select value_in_use from sys.configurations where [name] = 'xp_cmdshell') =
                                           1
                                            begin

                                                if OBJECT_ID('tempdb..#services') is not null drop table #services;
                                                create table #services
                                                (
                                                    cmdshell_output varchar(max)
                                                );

                                                insert into #services
                                                    exec xp_cmdshell 'net start'

                                                if EXISTS(select 1
                                                          from #services
                                                          where cmdshell_output like '%SQL Server Reporting Services%'
                                                             or cmdshell_output like '%SQL Server Integration Services%'
                                                             or cmdshell_output like '%SQL Server Analysis Services%')
                                                    begin
                                                        insert into #blitzresults
                                                        (checkid,
                                                         priority,
                                                         findingsgroup,
                                                         finding,
                                                         url,
                                                         details)
                                                        select 224                                                                                                                               as checkid
                                                             , 200                                                                                                                               as priority
                                                             , 'Performance'                                                                                                                     as findingsgroup
                                                             , 'SSAS/SSIS/SSRS Installed'                                                                                                        as finding
                                                             , 'https://www.BrentOzar.com/go/services'                                                                                           as url
                                                             , 'Did you know you have other SQL Server services installed on this box other than the engine? It can be a real performance pain.' as details

                                                    end;

                                            end;
                                    end;

                                /* CheckID 232 - Server Info - Data Size */
                                if not EXISTS(select 1
                                              from #skipchecks
                                              where databasename is null
                                                and checkid = 232)
                                    begin

                                        if @debug in (1, 2) raiserror ('Running CheckId [%d].', 0, 1, 232) with nowait;

                                        if OBJECT_ID('tempdb..#MasterFiles') is not null
                                            drop table #masterfiles;
                                        create table #masterfiles
                                        (
                                            database_id int,
                                            file_id int,
                                            type_desc nvarchar(50),
                                            name nvarchar(255),
                                            physical_name nvarchar(255),
                                            size bigint
                                        );
                                        /* Azure SQL Database doesn't have sys.master_files, so we have to build our own. */
                                        if ((SERVERPROPERTY('Edition')) = 'SQL Azure'
                                            and (OBJECT_ID('sys.master_files') is null))
                                            set @stringtoexecute =
                                                    'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT DB_ID(), file_id, type_desc, name, physical_name, size FROM sys.database_files;';
                                        else
                                            set @stringtoexecute =
                                                    'INSERT INTO #MasterFiles (database_id, file_id, type_desc, name, physical_name, size) SELECT database_id, file_id, type_desc, name, physical_name, size FROM sys.master_files;';
                                        exec (@stringtoexecute);


                                        insert into #blitzresults
                                        (checkid,
                                         priority,
                                         findingsgroup,
                                         finding,
                                         url,
                                         details)
                                        select 232                   as checkid
                                             , 250                   as priority
                                             , 'Server Info'         as findingsgroup
                                             , 'Data Size'           as finding
                                             , ''                    as url
                                             , CAST(COUNT(distinct database_id) as nvarchar(100)) + N' databases, ' +
                                               CAST(
                                                       CAST(SUM(CAST(size as bigint) * 8. / 1024. / 1024.) as money) as varchar(100)) +
                                               ' GB total file size' as details
                                        from #masterfiles
                                        where database_id > 4;

                                    end;


                            end; /* IF @CheckServerInfo = 1 */
                    end;
                /* IF ( ( SERVERPROPERTY('ServerName') NOT IN ( SELECT ServerName */

                /* Delete priorites they wanted to skip. */
                if @ignoreprioritiesabove is not null
                    delete #blitzresults
                    where [Priority] > @ignoreprioritiesabove
                      and checkid <> -1;

                if @ignoreprioritiesbelow is not null
                    delete #blitzresults
                    where [Priority] < @ignoreprioritiesbelow
                      and checkid <> -1;

                /* Delete checks they wanted to skip. */
                if @skipcheckstable is not null
                    begin
                        delete
                        from #blitzresults
                        where databasename in (select databasename
                                               from #skipchecks
                                               where checkid is null
                                                 and (servername is null or servername = SERVERPROPERTY('ServerName')));
                        delete
                        from #blitzresults
                        where checkid in (select checkid
                                          from #skipchecks
                                          where databasename is null
                                            and (servername is null or servername = SERVERPROPERTY('ServerName')));
                        delete r
                        from #blitzresults r
                                 inner join #skipchecks c on r.databasename = c.databasename and r.checkid = c.checkid
                            and (servername is null or servername = SERVERPROPERTY('ServerName'));
                    end;

                /* Add summary mode */
                if @summarymode > 0
                    begin
                        update #blitzresults
                        set finding = br.finding + ' (' + CAST(brtotals.recs as nvarchar(20)) + ')'
                        from #blitzresults br
                                 inner join (select findingsgroup, finding, priority, COUNT(*) as recs
                                             from #blitzresults
                                             group by findingsgroup, finding, priority) brtotals
                                            on br.findingsgroup = brtotals.findingsgroup and
                                               br.finding = brtotals.finding and br.priority = brtotals.priority
                        where brtotals.recs > 1;

                        delete br
                        from #blitzresults br
                        where EXISTS(select *
                                     from #blitzresults brlower
                                     where br.findingsgroup = brlower.findingsgroup
                                       and br.finding = brlower.finding
                                       and br.priority = brlower.priority
                                       and br.id > brlower.id);

                    end;

                /* Add credits for the nice folks who put so much time into building and maintaining this for free: */

                insert into #blitzresults
                (checkid,
                 priority,
                 findingsgroup,
                 finding,
                 url,
                 details)
                values (-1,
                        255,
                        'Thanks!',
                        'From Your Community Volunteers',
                        'http://FirstResponderKit.org',
                        'We hope you found this tool useful.');

                insert into #blitzresults
                (checkid,
                 priority,
                 findingsgroup,
                 finding,
                 url,
                 details)
                values (-1,
                        0,
                        'sp_Blitz ' + CAST(CONVERT(datetime, @versiondate, 102) as varchar(100)),
                        'SQL Server First Responder Kit',
                        'http://FirstResponderKit.org/',
                        'To get help or add your own contributions, join us at http://FirstResponderKit.org.');

                insert into #blitzresults
                (checkid,
                 priority,
                 findingsgroup,
                 finding,
                 url,
                 details)
                select 156,
                       254,
                       'Rundate',
                       GETDATE(),
                       'http://FirstResponderKit.org/',
                       'Captain''s log: stardate something and something...';

                if @emailrecipients is not null
                    begin

                        if @debug in (1, 2) raiserror ('Sending an email.', 0, 1) with nowait;

                        /* Database mail won't work off a local temp table. I'm not happy about this hacky workaround either. */
                        if (OBJECT_ID('tempdb..##BlitzResults', 'U') is not null) drop table ##blitzresults;
                        select * into ##blitzresults from #blitzresults;
                        set @query_result_separator = char(9);
                        set @stringtoexecute =
                                'SET NOCOUNT ON;SELECT [Priority] , [FindingsGroup] , [Finding] , [DatabaseName] , [URL] ,  [Details] , CheckID FROM ##BlitzResults ORDER BY Priority , FindingsGroup, Finding, Details; SET NOCOUNT OFF;';
                        set @emailsubject = 'sp_Blitz Results for ' + @@SERVERNAME;
                        set @emailbody = 'sp_Blitz ' + CAST(CONVERT(datetime, @versiondate, 102) as varchar(100)) +
                                         '. http://FirstResponderKit.org';
                        if @emailprofile is null
                            exec msdb.dbo.sp_send_dbmail
                                 @recipients = @emailrecipients,
                                 @subject = @emailsubject,
                                 @body = @emailbody,
                                 @query_attachment_filename = 'sp_Blitz-Results.csv',
                                 @attach_query_result_as_file = 1,
                                 @query_result_header = 1,
                                 @query_result_width = 32767,
                                 @append_query_error = 1,
                                 @query_result_no_padding = 1,
                                 @query_result_separator = @query_result_separator,
                                 @query = @stringtoexecute;
                        else
                            exec msdb.dbo.sp_send_dbmail
                                 @profile_name = @emailprofile,
                                 @recipients = @emailrecipients,
                                 @subject = @emailsubject,
                                 @body = @emailbody,
                                 @query_attachment_filename = 'sp_Blitz-Results.csv',
                                 @attach_query_result_as_file = 1,
                                 @query_result_header = 1,
                                 @query_result_width = 32767,
                                 @append_query_error = 1,
                                 @query_result_no_padding = 1,
                                 @query_result_separator = @query_result_separator,
                                 @query = @stringtoexecute;
                        if (OBJECT_ID('tempdb..##BlitzResults', 'U') is not null) drop table ##blitzresults;
                    end;

                /* Checks if @OutputServerName is populated with a valid linked server, and that the database name specified is valid */
                declare @validoutputserver bit;
                declare @validoutputlocation bit;
                declare @linkedserverdbcheck nvarchar(2000);
                declare @validlinkedserverdb int;
                declare @tmpdbchk table
                                  (
                                      cnt int
                                  );
                if @outputservername is not null
                    begin

                        if @debug in (1, 2) raiserror ('Outputting to a remote server.', 0, 1) with nowait;

                        if EXISTS(select server_id from sys.servers where QUOTENAME([name]) = @outputservername)
                            begin
                                set @linkedserverdbcheck = 'SELECT 1 WHERE EXISTS (SELECT * FROM ' + @outputservername +
                                                           '.master.sys.databases WHERE QUOTENAME([name]) = ''' +
                                                           @outputdatabasename + ''')';
                                insert into @tmpdbchk exec sys.sp_executesql @linkedserverdbcheck;
                                set @validlinkedserverdb = (select COUNT(*) from @tmpdbchk);
                                if (@validlinkedserverdb > 0)
                                    begin
                                        set @validoutputserver = 1;
                                        set @validoutputlocation = 1;
                                    end;
                                else
                                    raiserror ('The specified database was not found on the output server', 16, 0);
                            end;
                        else
                            begin
                                raiserror ('The specified output server was not found', 16, 0);
                            end;
                    end;
                else
                    begin
                        if @outputdatabasename is not null
                            and @outputschemaname is not null
                            and @outputtablename is not null
                            and EXISTS(select *
                                       from sys.databases
                                       where QUOTENAME([name]) = @outputdatabasename)
                            begin
                                set @validoutputlocation = 1;
                            end;
                        else
                            if @outputdatabasename is not null
                                and @outputschemaname is not null
                                and @outputtablename is not null
                                and not EXISTS(select *
                                               from sys.databases
                                               where QUOTENAME([name]) = @outputdatabasename)
                                begin
                                    raiserror ('The specified output database was not found on this server', 16, 0);
                                end;
                            else
                                begin
                                    set @validoutputlocation = 0;
                                end;
                    end;

                /* @OutputTableName lets us export the results to a permanent table */
                if @validoutputlocation = 1
                    begin
                        set @stringtoexecute = 'USE '
                            + @outputdatabasename
                            + '; IF EXISTS(SELECT * FROM '
                            + @outputdatabasename
                            + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                            + @outputschemaname
                            + ''') AND NOT EXISTS (SELECT * FROM '
                            + @outputdatabasename
                            + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
                            + @outputschemaname + ''' AND QUOTENAME(TABLE_NAME) = '''
                            + @outputtablename + ''') CREATE TABLE '
                            + @outputschemaname + '.'
                            + @outputtablename
                            + ' (ID INT IDENTITY(1,1) NOT NULL,
								ServerName NVARCHAR(128),
								CheckDate DATETIMEOFFSET,
								Priority TINYINT ,
								FindingsGroup VARCHAR(50) ,
								Finding VARCHAR(200) ,
								DatabaseName NVARCHAR(128),
								URL VARCHAR(200) ,
								Details NVARCHAR(4000) ,
								QueryPlan [XML] NULL ,
								QueryPlanFiltered [NVARCHAR](MAX) NULL,
								CheckID INT ,
								CONSTRAINT [PK_' + CAST(NEWID() as char(36)) + '] PRIMARY KEY CLUSTERED (ID ASC));';
                        if @validoutputserver = 1
                            begin
                                set @stringtoexecute = REPLACE(@stringtoexecute, '''' + @outputschemaname + '''',
                                                               '''''' + @outputschemaname + '''''');
                                set @stringtoexecute = REPLACE(@stringtoexecute, '''' + @outputtablename + '''',
                                                               '''''' + @outputtablename + '''''');
                                set @stringtoexecute = REPLACE(@stringtoexecute, '[XML]', '[NVARCHAR](MAX)');
                                exec ('EXEC('''+@stringtoexecute+''') AT ' + @outputservername);
                            end;
                        else
                            begin
                                exec (@stringtoexecute);
                            end;
                        if @validoutputserver = 1
                            begin
                                set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                    + @outputservername + '.'
                                    + @outputdatabasename
                                    + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                    + @outputschemaname + ''') INSERT '
                                    + @outputservername + '.'
                                    + @outputdatabasename + '.'
                                    + @outputschemaname + '.'
                                    + @outputtablename
                                    +
                                                       ' (ServerName, CheckDate, CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered) SELECT '''
                                    + CAST(SERVERPROPERTY('ServerName') as nvarchar(128))
                                    +
                                                       ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, CAST(QueryPlan AS NVARCHAR(MAX)), QueryPlanFiltered FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';

                                exec (@stringtoexecute);
                            end;
                        else
                            begin
                                set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                    + @outputdatabasename
                                    + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                    + @outputschemaname + ''') INSERT '
                                    + @outputdatabasename + '.'
                                    + @outputschemaname + '.'
                                    + @outputtablename
                                    +
                                                       ' (ServerName, CheckDate, CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered) SELECT '''
                                    + CAST(SERVERPROPERTY('ServerName') as nvarchar(128))
                                    +
                                                       ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';

                                exec (@stringtoexecute);
                            end;
                    end;
                else
                    if (SUBSTRING(@outputtablename, 2, 2) = '##')
                        begin
                            if @validoutputserver = 1
                                begin
                                    raiserror ('Due to the nature of temporary tables, outputting to a linked server requires a permanent table.', 16, 0);
                                end;
                            else
                                begin
                                    set @stringtoexecute = N' IF (OBJECT_ID(''tempdb..'
                                        + @outputtablename
                                        + ''') IS NOT NULL) DROP TABLE ' + @outputtablename + ';'
                                        + 'CREATE TABLE '
                                        + @outputtablename
                                        + ' (ID INT IDENTITY(1,1) NOT NULL,
										ServerName NVARCHAR(128),
										CheckDate DATETIMEOFFSET,
										Priority TINYINT ,
										FindingsGroup VARCHAR(50) ,
										Finding VARCHAR(200) ,
										DatabaseName NVARCHAR(128),
										URL VARCHAR(200) ,
										Details NVARCHAR(4000) ,
										QueryPlan [XML] NULL ,
										QueryPlanFiltered [NVARCHAR](MAX) NULL,
										CheckID INT ,
										CONSTRAINT [PK_' + CAST(NEWID() as char(36)) +
                                                           '] PRIMARY KEY CLUSTERED (ID ASC));'
                                        + ' INSERT '
                                        + @outputtablename
                                        +
                                                           ' (ServerName, CheckDate, CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered) SELECT '''
                                        + CAST(SERVERPROPERTY('ServerName') as nvarchar(128))
                                        +
                                                           ''', SYSDATETIMEOFFSET(), CheckID, DatabaseName, Priority, FindingsGroup, Finding, URL, Details, QueryPlan, QueryPlanFiltered FROM #BlitzResults ORDER BY Priority , FindingsGroup , Finding , Details';

                                    exec (@stringtoexecute);
                                end;
                        end;
                    else
                        if (SUBSTRING(@outputtablename, 2, 1) = '#')
                            begin
                                raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                            end;

                declare @separator as varchar(1);
                if @outputtype = 'RSV'
                    set @separator = CHAR(31);
                else
                    set @separator = ',';

                if @outputtype = 'COUNT'
                    begin
                        select COUNT(*) as warnings
                        from #blitzresults;
                    end;
                else
                    if @outputtype in ('CSV', 'RSV')
                        begin

                            select result = CAST([Priority] as nvarchar(100))
                                + @separator + CAST(checkid as nvarchar(100))
                                + @separator + COALESCE([FindingsGroup],
                                                        '(N/A)') + @separator
                                + COALESCE([Finding], '(N/A)') + @separator
                                + COALESCE(databasename, '(N/A)') + @separator
                                + COALESCE([URL], '(N/A)') + @separator
                                + COALESCE([Details], '(N/A)')
                            from #blitzresults
                            order by priority,
                                     findingsgroup,
                                     finding,
                                     databasename,
                                     details;
                        end;
                    else
                        if @outputxmlasnvarchar = 1 and @outputtype <> 'NONE'
                            begin
                                select [Priority],
                                       [FindingsGroup],
                                       [Finding],
                                       [DatabaseName],
                                       [URL],
                                       [Details],
                                       CAST([QueryPlan] as nvarchar(max)) as queryplan,
                                       [QueryPlanFiltered],
                                       checkid
                                from #blitzresults
                                order by priority,
                                         findingsgroup,
                                         finding,
                                         databasename,
                                         details;
                            end;
                        else
                            if @outputtype = 'MARKDOWN'
                                begin
                                    with results as (select row_number()
                                                                    over (order by priority, findingsgroup, finding, databasename, details) as rownum,
                                                            *
                                                     from #blitzresults
                                                     where priority > 0
                                                       and priority < 255
                                                       and findingsgroup is not null
                                                       and finding is not null
                                                       and findingsgroup <> 'Security' /* Specifically excluding security checks for public exports */)
                                    select case
                                               when r.priority <> COALESCE(rprior.priority, 0) or
                                                    r.findingsgroup <> rprior.findingsgroup then @crlf +
                                                                                                 N'**Priority ' +
                                                                                                 CAST(COALESCE(r.priority, N'') as nvarchar(5)) +
                                                                                                 N': ' +
                                                                                                 COALESCE(r.findingsgroup, N'') +
                                                                                                 N'**:' + @crlf + @crlf
                                               else N''
                                               end
                                               + case
                                                     when r.finding <> COALESCE(rprior.finding, N'') and
                                                          r.finding <> rnext.finding then N'- ' +
                                                                                          COALESCE(r.finding, N'') +
                                                                                          N' ' +
                                                                                          COALESCE(r.databasename, N'') +
                                                                                          N' - ' +
                                                                                          COALESCE(r.details, N'') +
                                                                                          @crlf
                                                     when r.finding <> COALESCE(rprior.finding, N'') and
                                                          r.finding = rnext.finding and r.details = rnext.details then
                                                             N'- ' + COALESCE(r.finding, N'') + N' - ' +
                                                             COALESCE(r.details, N'') + @crlf + @crlf + N'    * ' +
                                                             COALESCE(r.databasename, N'') + @crlf
                                                     when r.finding <> COALESCE(rprior.finding, N'') and
                                                          r.finding = rnext.finding then N'- ' +
                                                                                         COALESCE(r.finding, N'') +
                                                                                         @crlf + case
                                                                                                     when r.databasename is null
                                                                                                         then N''
                                                                                                     else N'    * ' + COALESCE(r.databasename, N'') end +
                                                                                         case
                                                                                             when r.details <> rprior.details
                                                                                                 then N' - ' + COALESCE(r.details, N'') + @crlf
                                                                                             else '' end
                                                     else case
                                                              when r.databasename is null then N''
                                                              else N'    * ' + COALESCE(r.databasename, N'') end + case
                                                                                                                       when r.details <> rprior.details
                                                                                                                           then N' - ' + COALESCE(r.details, N'') + @crlf
                                                                                                                       else N'' + @crlf end
                                               end + @crlf
                                    from results r
                                             left outer join results rprior on r.rownum = rprior.rownum + 1
                                             left outer join results rnext on r.rownum = rnext.rownum - 1
                                    order by r.rownum
                                    for xml path(N'');
                                end;
                            else
                                if @outputtype = 'XML'
                                    begin
                                        /* --TOURSTOP05-- */
                                        select [Priority],
                                               [FindingsGroup],
                                               [Finding],
                                               [DatabaseName],
                                               [URL],
                                               [Details],
                                               [QueryPlanFiltered],
                                               checkid
                                        from #blitzresults
                                        order by priority,
                                                 findingsgroup,
                                                 finding,
                                                 databasename,
                                                 details
                                        for xml path('Result'), root('sp_Blitz_Output');
                                    end;
                                else
                                    if @outputtype <> 'NONE'
                                        begin
                                            /* --TOURSTOP05-- */
                                            select [Priority],
                                                   [FindingsGroup],
                                                   [Finding],
                                                   [DatabaseName],
                                                   [URL],
                                                   [Details],
                                                   [QueryPlan],
                                                   [QueryPlanFiltered],
                                                   checkid
                                            from #blitzresults
                                            order by priority,
                                                     findingsgroup,
                                                     finding,
                                                     databasename,
                                                     details;
                                        end;

                drop table #blitzresults;

                if @outputprocedurecache = 1
                    and @checkprocedurecache = 1
                    select top 20 total_worker_time / execution_count   as avgcpu,
                                  total_worker_time                     as totalcpu,
                                  CAST(ROUND(100.00 * total_worker_time
                                                 / (select SUM(total_worker_time)
                                                    from sys.dm_exec_query_stats
                                             ), 2) as money)            as percentcpu,
                                  total_elapsed_time / execution_count  as avgduration,
                                  total_elapsed_time                    as totalduration,
                                  CAST(ROUND(100.00 * total_elapsed_time
                                                 / (select SUM(total_elapsed_time)
                                                    from sys.dm_exec_query_stats
                                             ), 2) as money)            as percentduration,
                                  total_logical_reads / execution_count as avgreads,
                                  total_logical_reads                   as totalreads,
                                  CAST(ROUND(100.00 * total_logical_reads
                                                 / (select SUM(total_logical_reads)
                                                    from sys.dm_exec_query_stats
                                             ), 2) as money)            as percentreads,
                                  execution_count,
                                  CAST(ROUND(100.00 * execution_count
                                                 / (select SUM(execution_count)
                                                    from sys.dm_exec_query_stats
                                             ), 2) as money)            as percentexecutions,
                                  case
                                      when DATEDIFF(mi, creation_time,
                                                    qs.last_execution_time) = 0 then 0
                                      else CAST((1.00 * execution_count / DATEDIFF(mi,
                                                                                   creation_time,
                                                                                   qs.last_execution_time)) as money)
                                      end                               as executions_per_minute,
                                  qs.creation_time                      as plan_creation_time,
                                  qs.last_execution_time,
                                  text,
                                  text_filtered,
                                  query_plan,
                                  query_plan_filtered,
                                  sql_handle,
                                  query_hash,
                                  plan_handle,
                                  query_plan_hash
                    from #dm_exec_query_stats qs
                    order by case UPPER(@checkprocedurecachefilter)
                                 when 'CPU' then total_worker_time
                                 when 'READS' then total_logical_reads
                                 when 'EXECCOUNT' then execution_count
                                 when 'DURATION' then total_elapsed_time
                                 else total_worker_time
                                 end desc;

            end; /* ELSE -- IF @OutputType = 'SCHEMA' */

/*
	   Cleanups - drop temporary tables that have been created by this SP.
    */

    if (OBJECT_ID('tempdb..#InvalidLogins') is not null)
        begin
            exec sp_executesql N'DROP TABLE #InvalidLogins;';
        end;

    /*
	Reset the Nmumeric_RoundAbort session state back to enabled if it was disabled earlier.
	See Github issue #2302 for more info.
	*/
    if @needtoturnnumericroundabortbackon = 1
        set numeric_roundabort on;

    set nocount off;
go

/*
--Sample execution call with the most common parameters:
EXEC [dbo].[sp_Blitz] 
    @CheckUserDatabaseObjects = 1 ,
    @CheckProcedureCache = 0 ,
    @OutputType = 'TABLE' ,
    @OutputProcedureCache = 0 ,
    @CheckProcedureCacheFilter = NULL,
    @CheckServerInfo = 1
*/
