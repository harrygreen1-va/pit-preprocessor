if OBJECT_ID('dbo.sp_BlitzFirst') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzFirst AS RETURN 0;');
go


alter procedure [dbo].[sp_BlitzFirst] @logmessage nvarchar(4000) = null,
                                      @help tinyint = 0,
                                      @asof datetimeoffset = null,
                                      @expertmode tinyint = 0,
                                      @seconds int = 5,
                                      @outputtype varchar(20) = 'TABLE',
                                      @outputservername nvarchar(256) = null,
                                      @outputdatabasename nvarchar(256) = null,
                                      @outputschemaname nvarchar(256) = null,
                                      @outputtablename nvarchar(256) = null,
                                      @outputtablenamefilestats nvarchar(256) = null,
                                      @outputtablenameperfmonstats nvarchar(256) = null,
                                      @outputtablenamewaitstats nvarchar(256) = null,
                                      @outputtablenameblitzcache nvarchar(256) = null,
                                      @outputtablenameblitzwho nvarchar(256) = null,
                                      @outputtableretentiondays tinyint = 7,
                                      @outputxmlasnvarchar tinyint = 0,
                                      @filterplansbydatabase varchar(max) = null,
                                      @checkprocedurecache tinyint = 0,
                                      @checkserverinfo tinyint = 1,
                                      @filelatencythresholdms int = 100,
                                      @sincestartup tinyint = 0,
                                      @showsleepingspids tinyint = 0,
                                      @blitzcacheskipanalysis bit = 1,
                                      @logmessagecheckid int = 38,
                                      @logmessagepriority tinyint = 1,
                                      @logmessagefindingsgroup varchar(50) = 'Logged Message',
                                      @logmessagefinding varchar(200) = 'Logged from sp_BlitzFirst',
                                      @logmessageurl varchar(200) = '',
                                      @logmessagecheckdate datetimeoffset = null,
                                      @debug bit = 0,
                                      @version varchar(30) = null output,
                                      @versiondate datetime = null output,
                                      @versioncheckmode bit = 0
    with execute as caller , recompile
as
begin
    set nocount on;
    set transaction isolation level read uncommitted;

    select @version = '7.97', @versiondate = '20200712';

    if (@versioncheckmode = 1)
        begin
            return;
        end;

    if @help = 1
        print '
sp_BlitzFirst from http://FirstResponderKit.org

This script gives you a prioritized list of why your SQL Server is slow right now.

This is not an overall health check - for that, check out sp_Blitz.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000. It
   may work just fine on 2005, and if it does, hug your parents. Just don''t
   file support issues if it breaks.
 - If a temp table called #CustomPerfmonCounters exists for any other session,
   but not our session, this stored proc will fail with an error saying the
   temp table #CustomPerfmonCounters does not exist.
 - @OutputServerName is not functional yet.
 - If @OutputDatabaseName, SchemaName, TableName, etc are quoted with brackets,
   the write to table may silently fail. Look, I never said I was good at this.

Unknown limitations of this version:
 - None. Like Zombo.com, the only limit is yourself.

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

';


    raiserror ('Setting up configuration variables',10,1) with nowait;
    declare @stringtoexecute nvarchar(max),
        @parmdefinitions nvarchar(4000),
        @parm1 nvarchar(4000),
        @oursessionid int,
        @linefeed nvarchar(10),
        @stockwarningheader nvarchar(max) = N'',
        @stockwarningfooter nvarchar(max) = N'',
        @stockdetailsheader nvarchar(max) = N'',
        @stockdetailsfooter nvarchar(max) = N'',
        @startsampletime datetimeoffset,
        @finishsampletime datetimeoffset,
        @finishsampletimewaitfor datetime,
        @asof1 datetimeoffset,
        @asof2 datetimeoffset,
        @servicename sysname,
        @outputtablenamefilestats_view nvarchar(256),
        @outputtablenameperfmonstats_view nvarchar(256),
        @outputtablenameperfmonstatsactuals_view nvarchar(256),
        @outputtablenamewaitstats_view nvarchar(256),
        @outputtablenamewaitstats_categories nvarchar(256),
        @outputtablecleanupdate date,
        @objectfullname nvarchar(2000),
        @blitzwho nvarchar(max) = N'EXEC dbo.sp_BlitzWho @ShowSleepingSPIDs = ' +
                                  CONVERT(nvarchar(1), @showsleepingspids) + N';',
        @blitzcacheminutesback int,
        @unquotedoutputservername nvarchar(256) = @outputservername ,
        @unquotedoutputdatabasename nvarchar(256) = @outputdatabasename ,
        @unquotedoutputschemaname nvarchar(256) = @outputschemaname ,
        @localservername nvarchar(128) = CAST(SERVERPROPERTY('ServerName') as nvarchar(128)),
        @dm_exec_query_statistics_xml bit = 0;

/* Sanitize our inputs */
    select @outputtablenamefilestats_view = QUOTENAME(@outputtablenamefilestats + '_Deltas'),
           @outputtablenameperfmonstats_view = QUOTENAME(@outputtablenameperfmonstats + '_Deltas'),
           @outputtablenameperfmonstatsactuals_view = QUOTENAME(@outputtablenameperfmonstats + '_Actuals'),
           @outputtablenamewaitstats_view = QUOTENAME(@outputtablenamewaitstats + '_Deltas'),
           @outputtablenamewaitstats_categories = QUOTENAME(@outputtablenamewaitstats + '_Categories');

    select @outputdatabasename = QUOTENAME(@outputdatabasename),
           @outputschemaname = QUOTENAME(@outputschemaname),
           @outputtablename = QUOTENAME(@outputtablename),
           @outputtablenamefilestats = QUOTENAME(@outputtablenamefilestats),
           @outputtablenameperfmonstats = QUOTENAME(@outputtablenameperfmonstats),
           @outputtablenamewaitstats = QUOTENAME(@outputtablenamewaitstats),
           @outputtablecleanupdate = CAST((DATEADD(day, -1 * @outputtableretentiondays, GETDATE())) as date),
        /* @OutputTableNameBlitzCache = QUOTENAME(@OutputTableNameBlitzCache),  We purposely don't sanitize this because sp_BlitzCache will */
        /* @OutputTableNameBlitzWho = QUOTENAME(@OutputTableNameBlitzWho),  We purposely don't sanitize this because sp_BlitzWho will */
           @linefeed = CHAR(13) + CHAR(10),
           @oursessionid = @@SPID,
           @outputtype = UPPER(@outputtype);

    if (@outputtype = 'NONE' and (@outputtablename is null or @outputschemaname is null or @outputdatabasename is null))
        begin
            raiserror ('This procedure should be called with a value for all @Output* parameters, as @OutputType is set to NONE',12,1);
            return;
        end;

    if UPPER(@outputtype) like 'TOP 10%' set @outputtype = 'Top10';
    if @outputtype = 'Top10' set @sincestartup = 1;

    if @logmessage is not null
        begin

            raiserror ('Saving LogMessage to table',10,1) with nowait;

            /* Try to set the output table parameters if they don't exist */
            if @outputschemaname is null and @outputtablename is null and @outputdatabasename is null
                begin
                    set @outputschemaname = N'[dbo]';
                    set @outputtablename = N'[BlitzFirst]';

                    /* Look for the table in the current database */
                    select top 1 @outputdatabasename = QUOTENAME(table_catalog)
                    from information_schema.tables
                    where table_schema = 'dbo'
                      and table_name = 'BlitzFirst';

                    if @outputdatabasename is null and EXISTS(select * from sys.databases where name = 'DBAtools')
                        set @outputdatabasename = '[DBAtools]';

                end;

            if @outputdatabasename is null or @outputschemaname is null or @outputtablename is null
                or not EXISTS(select *
                              from sys.databases
                              where QUOTENAME([name]) = @outputdatabasename)
                begin
                    raiserror ('We have a hard time logging a message without a valid @OutputDatabaseName, @OutputSchemaName, and @OutputTableName to log it to.', 0, 1) with nowait;
                    return;
                end;
            if @logmessagecheckdate is null
                set @logmessagecheckdate = SYSDATETIMEOFFSET();
            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                + @outputdatabasename
                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                + @outputschemaname + ''') INSERT '
                + @outputdatabasename + '.'
                + @outputschemaname + '.'
                + @outputtablename
                + ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, Details, URL) VALUES( '
                +
                                   ' @SrvName, @LogMessageCheckDate, @LogMessageCheckID, @LogMessagePriority, @LogMessageFindingsGroup, @LogMessageFinding, @LogMessage, @LogMessageURL)';

            execute sp_executesql @stringtoexecute,
                    N'@SrvName NVARCHAR(128), @LogMessageCheckID INT, @LogMessagePriority TINYINT, @LogMessageFindingsGroup VARCHAR(50), @LogMessageFinding VARCHAR(200), @LogMessage NVARCHAR(4000), @LogMessageCheckDate DATETIMEOFFSET, @LogMessageURL VARCHAR(200)',
                    @localservername, @logmessagecheckid, @logmessagepriority, @logmessagefindingsgroup,
                    @logmessagefinding, @logmessage, @logmessagecheckdate, @logmessageurl;

            raiserror ('LogMessage saved to table. We have made a note of your activity. Keep up the good work.',10,1) with nowait;

            return;
        end;

    if @sincestartup = 1
        select @seconds = 0, @expertmode = 1;


    if @outputtype = 'SCHEMA'
        begin
            select fieldlist = '[Priority] TINYINT, [FindingsGroup] VARCHAR(50), [Finding] VARCHAR(200), [URL] VARCHAR(200), [Details] NVARCHAR(4000), [HowToStopIt] NVARCHAR(MAX), [QueryPlan] XML, [QueryText] NVARCHAR(MAX)';

        end;
    else
        if @asof is not null and @outputdatabasename is not null and @outputschemaname is not null and
           @outputtablename is not null
            begin
                /* They want to look into the past. */
                set @asof1 = DATEADD(mi, -15, @asof);
                set @asof2 = DATEADD(mi, +15, @asof);

                set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                    + @outputdatabasename
                    + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                    + @outputschemaname +
                                       ''') SELECT CheckDate, [Priority], [FindingsGroup], [Finding], [URL], CAST([Details] AS [XML]) AS Details,'
                    +
                                       '[HowToStopIt], [CheckID], [StartTime], [LoginName], [NTUserName], [OriginalLoginName], [ProgramName], [HostName], [DatabaseID],'
                    + '[DatabaseName], [OpenTransactionCount], [QueryPlan], [QueryText] FROM '
                    + @outputdatabasename + '.'
                    + @outputschemaname + '.'
                    + @outputtablename
                    + ' WHERE CheckDate >= @AsOf1'
                    + ' AND CheckDate <= @AsOf2'
                    + ' /*ORDER BY CheckDate, Priority , FindingsGroup , Finding , Details*/;';
                exec sp_executesql @stringtoexecute,
                     N'@AsOf1 DATETIMEOFFSET, @AsOf2 DATETIMEOFFSET',
                     @asof1, @asof2


            end; /* IF @AsOf IS NOT NULL AND @OutputDatabaseName IS NOT NULL AND @OutputSchemaName IS NOT NULL AND @OutputTableName IS NOT NULL */
        else
            if @logmessage is null /* IF @OutputType = 'SCHEMA' */
                begin
                    /* What's running right now? This is the first and last result set. */
                    if @sincestartup = 0 and @seconds > 0 and @expertmode = 1 and @outputtype <> 'NONE'
                        begin
                            if OBJECT_ID('master.dbo.sp_BlitzWho') is null and OBJECT_ID('dbo.sp_BlitzWho') is null
                                begin
                                    print N'sp_BlitzWho is not installed in the current database_files.  You can get a copy from http://FirstResponderKit.org';
                                end;
                            else
                                begin
                                    exec (@blitzwho);
                                end;
                        end;
                    /* IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'   -   What's running right now? This is the first and last result set. */

                    /* Set start/finish times AFTER sp_BlitzWho runs. For more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2244 */
                    if @seconds = 0 and SERVERPROPERTY('Edition') = 'SQL Azure'
                        with waittimes as (
                            select wait_type,
                                   wait_time_ms,
                                   NTILE(3) over (order by wait_time_ms) as grouper
                            from sys.dm_os_wait_stats w
                            where wait_type in
                                  ('DIRTY_PAGE_POLL', 'HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'LAZYWRITER_SLEEP',
                                   'LOGMGR_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT')
                        )
                        select @startsampletime = DATEADD(mi, AVG(-wait_time_ms / 1000 / 60), SYSDATETIMEOFFSET()),
                               @finishsampletime = SYSDATETIMEOFFSET()
                        from waittimes
                        where grouper = 2;
                    else
                        if @seconds = 0 and SERVERPROPERTY('Edition') <> 'SQL Azure'
                            select @startsampletime =
                                   DATEADD(minute, DATEDIFF(minute, GETDATE(), GETUTCDATE()), create_date),
                                   @finishsampletime = SYSDATETIMEOFFSET()
                            from sys.databases
                            where database_id = 2;
                        else
                            select @startsampletime = SYSDATETIMEOFFSET(),
                                   @finishsampletime = DATEADD(ss, @seconds, SYSDATETIMEOFFSET()),
                                   @finishsampletimewaitfor = DATEADD(ss, @seconds, GETDATE());


                    raiserror ('Now starting diagnostic analysis',10,1) with nowait;

                    /*
    We start by creating #BlitzFirstResults. It's a temp table that will store
    the results from our checks. Throughout the rest of this stored procedure,
    we're running a series of checks looking for dangerous things inside the SQL
    Server. When we find a problem, we insert rows into the temp table. At the
    end, we return these results to the end user.

    #BlitzFirstResults has a CheckID field, but there's no Check table. As we do
    checks, we insert data into this table, and we manually put in the CheckID.
    We (Brent Ozar Unlimited) maintain a list of the checks by ID#. You can
    download that from http://FirstResponderKit.org if you want to build
    a tool that relies on the output of sp_BlitzFirst.
    */


                    if OBJECT_ID('tempdb..#BlitzFirstResults') is not null
                        drop table #blitzfirstresults;
                    create table #blitzfirstresults
                    (
                        id int identity (1, 1) primary key clustered,
                        checkid int not null,
                        priority tinyint not null,
                        findingsgroup varchar(50) not null,
                        finding varchar(200) not null,
                        url varchar(200) null,
                        details nvarchar(max) null,
                        howtostopit nvarchar(max) null,
                        queryplan [XML] null,
                        querytext nvarchar(max) null,
                        starttime datetimeoffset null,
                        loginname nvarchar(128) null,
                        ntusername nvarchar(128) null,
                        originalloginname nvarchar(128) null,
                        programname nvarchar(128) null,
                        hostname nvarchar(128) null,
                        databaseid int null,
                        databasename nvarchar(128) null,
                        opentransactioncount int null,
                        querystatsnowid int null,
                        querystatsfirstid int null,
                        planhandle varbinary(64) null,
                        detailsint int null,
                        queryhash binary(8)
                    );

                    if OBJECT_ID('tempdb..#WaitStats') is not null
                        drop table #waitstats;
                    create table #waitstats
                    (
                        pass tinyint not null,
                        wait_type nvarchar(60),
                        wait_time_ms bigint,
                        signal_wait_time_ms bigint,
                        waiting_tasks_count bigint,
                        sampletime datetimeoffset
                    );

                    if OBJECT_ID('tempdb..#FileStats') is not null
                        drop table #filestats;
                    create table #filestats
                    (
                        id int identity (1, 1) primary key clustered,
                        pass tinyint not null,
                        sampletime datetimeoffset not null,
                        databaseid int not null,
                        fileid int not null,
                        databasename nvarchar(256),
                        filelogicalname nvarchar(256),
                        typedesc nvarchar(60),
                        sizeondiskmb bigint,
                        io_stall_read_ms bigint,
                        num_of_reads bigint,
                        bytes_read bigint,
                        io_stall_write_ms bigint,
                        num_of_writes bigint,
                        bytes_written bigint,
                        physicalname nvarchar(520),
                        avg_stall_read_ms int,
                        avg_stall_write_ms int
                    );

                    if OBJECT_ID('tempdb..#QueryStats') is not null
                        drop table #querystats;
                    create table #querystats
                    (
                        id int identity (1, 1) primary key clustered,
                        pass int not null,
                        sampletime datetimeoffset not null,
                        [sql_handle] varbinary(64),
                        statement_start_offset int,
                        statement_end_offset int,
                        plan_generation_num bigint,
                        plan_handle varbinary(64),
                        execution_count bigint,
                        total_worker_time bigint,
                        total_physical_reads bigint,
                        total_logical_writes bigint,
                        total_logical_reads bigint,
                        total_clr_time bigint,
                        total_elapsed_time bigint,
                        creation_time datetimeoffset,
                        query_hash binary(8),
                        query_plan_hash binary(8),
                        points tinyint
                    );

                    if OBJECT_ID('tempdb..#PerfmonStats') is not null
                        drop table #perfmonstats;
                    create table #perfmonstats
                    (
                        id int identity (1, 1) primary key clustered,
                        pass tinyint not null,
                        sampletime datetimeoffset not null,
                        [object_name] nvarchar(128) not null,
                        [counter_name] nvarchar(128) not null,
                        [instance_name] nvarchar(128) null,
                        [cntr_value] bigint null,
                        [cntr_type] int not null,
                        [value_delta] bigint null,
                        [value_per_second] decimal(18, 2) null
                    );

                    if OBJECT_ID('tempdb..#PerfmonCounters') is not null
                        drop table #perfmoncounters;
                    create table #perfmoncounters
                    (
                        id int identity (1, 1) primary key clustered,
                        [object_name] nvarchar(128) not null,
                        [counter_name] nvarchar(128) not null,
                        [instance_name] nvarchar(128) null
                    );

                    if OBJECT_ID('tempdb..#FilterPlansByDatabase') is not null
                        drop table #filterplansbydatabase;
                    create table #filterplansbydatabase
                    (
                        databaseid int primary key clustered
                    );

                    if OBJECT_ID('tempdb..##WaitCategories') is null
                        begin
                            /* We reuse this one by default rather than recreate it every time. */
                            create table ##waitcategories
                            (
                                waittype nvarchar(60) primary key clustered,
                                waitcategory nvarchar(128) not null,
                                ignorable bit default 0
                            );
                        end; /* IF OBJECT_ID('tempdb..##WaitCategories') IS NULL */

                    if OBJECT_ID('tempdb..#checkversion') is not null
                        drop table #checkversion;
                    create table #checkversion
                    (
                        version nvarchar(128),
                        common_version as SUBSTRING(version, 1, CHARINDEX('.', version) + 1),
                        major as PARSENAME(CONVERT(varchar(32), version), 4),
                        minor as PARSENAME(CONVERT(varchar(32), version), 3),
                        build as PARSENAME(CONVERT(varchar(32), version), 2),
                        revision as PARSENAME(CONVERT(varchar(32), version), 1)
                    );

                    if 527 <> (select COALESCE(SUM(1), 0) from ##waitcategories)
                        begin
                            truncate table ##waitcategories;
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('ASYNC_IO_COMPLETION', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('ASYNC_NETWORK_IO', 'Network IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BACKUPIO', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_CONNECTION_RECEIVE_TASK', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_DISPATCHER', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_ENDPOINT_STATE_MUTEX', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_EVENTHANDLER', 'Service Broker', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_FORWARDER', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_INIT', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_MASTERSTART', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_RECEIVE_WAITFOR', 'User Wait', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_REGISTERALLENDPOINTS', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_SERVICE', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_SHUTDOWN', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_START', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TASK_SHUTDOWN', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TASK_STOP', 'Service Broker', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TASK_SUBMIT', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TO_FLUSH', 'Service Broker', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TRANSMISSION_OBJECT', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TRANSMISSION_TABLE', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TRANSMISSION_WORK', 'Service Broker', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('BROKER_TRANSMITTER', 'Service Broker', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CHECKPOINT_QUEUE', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CHKPT', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_AUTO_EVENT', 'SQL CLR', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_CRST', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_JOIN', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_MANUAL_EVENT', 'SQL CLR', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_MEMORY_SPY', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_MONITOR', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_RWLOCK_READER', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_RWLOCK_WRITER', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_SEMAPHORE', 'SQL CLR', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLR_TASK_START', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CLRHOST_STATE_ACCESS', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CMEMPARTITIONED', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CMEMTHREAD', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CXPACKET', 'Parallelism', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('CXCONSUMER', 'Parallelism', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRROR_DBM_EVENT', 'Mirroring', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRROR_DBM_MUTEX', 'Mirroring', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRROR_EVENTS_QUEUE', 'Mirroring', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRROR_SEND', 'Mirroring', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRROR_WORKER_QUEUE', 'Mirroring', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DBMIRRORING_CMD', 'Mirroring', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DIRTY_PAGE_POLL', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DIRTY_PAGE_TABLE_LOCK', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DISPATCHER_QUEUE_SEMAPHORE', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DPT_ENTRY_LOCK', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC_ABORT_REQUEST', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC_RESOLVE', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC_STATE', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC_TMDOWN_REQUEST', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTC_WAITFOR_OUTCOME', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCNEW_ENLIST', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCNEW_PREPARE', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCNEW_RECOVERY', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCNEW_TM', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCNEW_TRANSACTION_ENLISTMENT', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('DTCPNTSYNC', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('EE_PMOLOCK', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('EXCHANGE', 'Parallelism', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('EXTERNAL_SCRIPT_NETWORK_IOF', 'Network IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FCB_REPLICA_READ', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FCB_REPLICA_WRITE', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_COMPROWSET_RWLOCK', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_IFTS_RWLOCK', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_IFTS_SCHEDULER_IDLE_WAIT', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_IFTSHC_MUTEX', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_IFTSISM_MUTEX', 'Full Text Search', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_MASTER_MERGE', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_MASTER_MERGE_COORDINATOR', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_METADATA_MUTEX', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_PROPERTYLIST_CACHE', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FT_RESTART_CRAWL', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('FULLTEXT GATHERER', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_AG_MUTEX', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_AR_CRITICAL_SECTION_ENTRY', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_AR_MANAGER_MUTEX', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_AR_UNLOAD_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_ARCONTROLLER_NOTIFICATIONS_SUBSCRIBER_LIST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_BACKUP_BULK_LOCK', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_BACKUP_QUEUE', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_CLUSAPI_CALL', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_COMPRESSED_CACHE_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_CONNECTIVITY_INFO', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DATABASE_FLOW_CONTROL', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DATABASE_VERSIONING_STATE', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DATABASE_WAIT_FOR_RECOVERY', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DATABASE_WAIT_FOR_RESTART', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DATABASE_WAIT_FOR_TRANSITION_TO_VERSIONING', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DB_COMMAND', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DB_OP_COMPLETION_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DB_OP_START_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DBR_SUBSCRIBER', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DBR_SUBSCRIBER_FILTER_LIST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DBSEEDING', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DBSEEDING_LIST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_DBSTATECHANGE_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FABRIC_CALLBACK', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_BLOCK_FLUSH', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_FILE_CLOSE', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_FILE_REQUEST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_IOMGR', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_IOMGR_IOCOMPLETION', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_MANAGER', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_FILESTREAM_PREPROC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_GROUP_COMMIT', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_LOGCAPTURE_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_LOGCAPTURE_WAIT', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_LOGPROGRESS_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_NOTIFICATION_DEQUEUE', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_NOTIFICATION_WORKER_EXCLUSIVE_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_NOTIFICATION_WORKER_STARTUP_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_NOTIFICATION_WORKER_TERMINATION_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_PARTNER_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_READ_ALL_NETWORKS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_RECOVERY_WAIT_FOR_CONNECTION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_RECOVERY_WAIT_FOR_UNDO', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_REPLICAINFO_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_CANCELLATION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_FILE_LIST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_LIMIT_BACKUPS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_SYNC_COMPLETION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_TIMEOUT_TASK', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SEEDING_WAIT_FOR_COMPLETION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SYNC_COMMIT', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_SYNCHRONIZING_THROTTLE', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TDS_LISTENER_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TDS_LISTENER_SYNC_PROCESSING', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_THROTTLE_LOG_RATE_GOVERNOR', 'Log Rate Governor', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TIMER_TASK', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TRANSPORT_DBRLIST', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TRANSPORT_FLOW_CONTROL', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_TRANSPORT_SESSION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_WORK_POOL', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_WORK_QUEUE', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('HADR_XRF_STACK_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('INSTANCE_LOG_RATE_GOVERNOR', 'Log Rate Governor', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('IO_COMPLETION', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('IO_QUEUE_LIMIT', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('IO_RETRY', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_DT', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_EX', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_KP', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_NL', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_SH', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LATCH_UP', 'Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LAZYWRITER_SLEEP', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_BU', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_BU_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_BU_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IS_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IS_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IU', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IU_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IU_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IX', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IX_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_IX_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_NL', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_NL_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_NL_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_S', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_S_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_S_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_U', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_U_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_U_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_X', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_X_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RIn_X_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_S', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_S_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_S_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_U', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_U_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RS_U_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_S', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_S_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_S_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_U', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_U_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_U_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_X', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_X_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_RX_X_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_S', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_S_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_S_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_M', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_M_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_M_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_S', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_S_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SCH_S_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIU', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIU_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIU_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIX', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIX_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_SIX_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_U', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_U_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_U_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_UIX', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_UIX_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_UIX_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_X', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_X_ABORT_BLOCKERS', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LCK_M_X_LOW_PRIORITY', 'Lock', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOG_RATE_GOVERNOR', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGBUFFER', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGMGR', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGMGR_FLUSH', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGMGR_PMM_LOG', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGMGR_QUEUE', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('LOGMGR_RESERVE_APPEND', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('MEMORY_ALLOCATION_EXT', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('MEMORY_GRANT_UPDATE', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('MSQL_XACT_MGR_MUTEX', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('MSQL_XACT_MUTEX', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('MSSEARCH', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('NET_WAITFOR_PACKET', 'Network IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('ONDEMAND_TASK_QUEUE', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_DT', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_EX', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_KP', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_NL', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_SH', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGEIOLATCH_UP', 'Buffer IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_DT', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_EX', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_KP', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_NL', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_SH', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PAGELATCH_UP', 'Buffer Latch', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_DRAIN_WORKER', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_FLOW_CONTROL', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_LOG_CACHE', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_TRAN_LIST', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_TRAN_TURN', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_WORKER_SYNC', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PARALLEL_REDO_WORKER_WAIT_WORK', 'Replication', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('POOL_LOG_RATE_GOVERNOR', 'Log Rate Governor', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_ABR', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CLOSEBACKUPMEDIA', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CLOSEBACKUPTAPE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CLOSEBACKUPVDIDEVICE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CLUSAPI_CLUSTERRESOURCECONTROL', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_COCREATEINSTANCE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_COGETCLASSOBJECT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_CREATEACCESSOR', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_DELETEROWS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_GETCOMMANDTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_GETDATA', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_GETNEXTROWS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_GETRESULT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_GETROWSBYBOOKMARK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBFLUSH', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBLOCKREGION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBREADAT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBSETSIZE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBSTAT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBUNLOCKREGION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_LBWRITEAT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_QUERYINTERFACE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_RELEASE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_RELEASEACCESSOR', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_RELEASEROWS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_RELEASESESSION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_RESTARTPOSITION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_SEQSTRMREAD', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_SEQSTRMREADANDWRITE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_SETDATAFAILURE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_SETPARAMETERINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_SETPARAMETERPROPERTIES', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMLOCKREGION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMSEEKANDREAD', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMSEEKANDWRITE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMSETSIZE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMSTAT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_COM_STRMUNLOCKREGION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CONSOLEWRITE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_CREATEPARAM', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DEBUG', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSADDLINK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSLINKEXISTCHECK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSLINKHEALTHCHECK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSREMOVELINK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSREMOVEROOT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSROOTFOLDERCHECK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSROOTINIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DFSROOTSHARECHECK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_ABORT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_ABORTREQUESTDONE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_BEGINTRANSACTION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_COMMITREQUESTDONE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_ENLIST', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_DTC_PREPAREREQUESTDONE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_FILESIZEGET', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_FSAOLEDB_ABORTTRANSACTION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_FSAOLEDB_COMMITTRANSACTION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_FSAOLEDB_STARTTRANSACTION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_FSRECOVER_UNCONDITIONALUNDO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_GETRMINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_HADR_LEASE_MECHANISM', 'Preemptive', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_HTTP_EVENT_WAIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_HTTP_REQUEST', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_LOCKMONITOR', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_MSS_RELEASE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_ODBCOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLE_UNINIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_ABORTORCOMMITTRAN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_ABORTTRAN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_GETDATASOURCE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_GETLITERALINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_GETPROPERTIES', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_GETPROPERTYINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_GETSCHEMALOCK', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_JOINTRANSACTION', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_RELEASE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDB_SETPROPERTIES', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OLEDBOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_ACCEPTSECURITYCONTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_ACQUIRECREDENTIALSHANDLE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_AUTHENTICATIONOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_AUTHORIZATIONOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_AUTHZGETINFORMATIONFROMCONTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_AUTHZINITIALIZECONTEXTFROMSID', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_AUTHZINITIALIZERESOURCEMANAGER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_BACKUPREAD', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CLOSEHANDLE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CLUSTEROPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_COMOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_COMPLETEAUTHTOKEN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_COPYFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CREATEDIRECTORY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CREATEFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CRYPTACQUIRECONTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CRYPTIMPORTKEY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_CRYPTOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DECRYPTMESSAGE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DELETEFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DELETESECURITYCONTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DEVICEIOCONTROL', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DEVICEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DIRSVC_NETWORKOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DISCONNECTNAMEDPIPE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DOMAINSERVICESOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DSGETDCNAME', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_DTCOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_ENCRYPTMESSAGE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FILEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FINDFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FLUSHFILEBUFFERS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FORMATMESSAGE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FREECREDENTIALSHANDLE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_FREELIBRARY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GENERICOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETADDRINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETCOMPRESSEDFILESIZE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETDISKFREESPACE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETFILEATTRIBUTES', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETFILESIZE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETFINALFILEPATHBYHANDLE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETLONGPATHNAME', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETPROCADDRESS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETVOLUMENAMEFORVOLUMEMOUNTPOINT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_GETVOLUMEPATHNAME', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_INITIALIZESECURITYCONTEXT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_LIBRARYOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_LOADLIBRARY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_LOGONUSER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_LOOKUPACCOUNTSID', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_MESSAGEQUEUEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_MOVEFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETGROUPGETUSERS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETLOCALGROUPGETMEMBERS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETUSERGETGROUPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETUSERGETLOCALGROUPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETUSERMODALSGET', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_NETVALIDATEPASSWORDPOLICYFREE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_OPENDIRECTORY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_PDH_WMI_INIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_PIPEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_PROCESSOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_QUERYCONTEXTATTRIBUTES', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_QUERYREGISTRY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_QUERYSECURITYCONTEXTTOKEN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_REMOVEDIRECTORY', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_REPORTEVENT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_REVERTTOSELF', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_RSFXDEVICEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SECURITYOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SERVICEOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SETENDOFFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SETFILEPOINTER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SETFILEVALIDDATA', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SETNAMEDSECURITYINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SQLCLROPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_SQMLAUNCH', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_VERIFYSIGNATURE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_VERIFYTRUST', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_VSSOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_WAITFORSINGLEOBJECT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_WINSOCKOPS', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_WRITEFILE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_WRITEFILEGATHER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_OS_WSASETLASTERROR', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_REENLIST', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_RESIZELOG', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_ROLLFORWARDREDO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_ROLLFORWARDUNDO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SB_STOPENDPOINT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SERVER_STARTUP', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SETRMINFO', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SHAREDMEM_GETDATA', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SNIOPEN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SOSHOST', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SOSTESTING', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_SP_SERVER_DIAGNOSTICS', 'Preemptive', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_STARTRM', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_STREAMFCB_CHECKPOINT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_STREAMFCB_RECOVER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_STRESSDRIVER', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_TESTING', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_TRANSIMPORT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_UNMARSHALPROPAGATIONTOKEN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_VSS_CREATESNAPSHOT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_VSS_CREATEVOLUMESNAPSHOT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_CALLBACKEXECUTE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_CX_FILE_OPEN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_CX_HTTP_CALL', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_DISPATCHER', 'Preemptive', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_ENGINEINIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_GETTARGETSTATE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_SESSIONCOMMIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_TARGETFINALIZE', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_TARGETINIT', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XE_TIMERRUN', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PREEMPTIVE_XETESTING', 'Preemptive', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_ACTION_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_CHANGE_NOTIFIER_TERMINATION_SYNC', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_CLUSTER_INTEGRATION', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_FAILOVER_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_JOIN', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_OFFLINE_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_ONLINE_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_POST_ONLINE_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_SERVER_READY_CONNECTIONS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADR_WORKITEM_COMPLETED', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_HADRSIM', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC', 'Full Text Search', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('QDS_ASYNC_QUEUE', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('QDS_SHUTDOWN_QUEUE', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('QUERY_TRACEOUT', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REDO_THREAD_PENDING_WORK', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_CACHE_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_HISTORYCACHE_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_SCHEMA_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_TRANFSINFO_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_TRANHASHTABLE_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPL_TRANTEXTINFO_ACCESS', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REPLICA_WRITES', 'Replication', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('REQUEST_FOR_DEADLOCK_SEARCH', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('RESERVED_MEMORY_ALLOCATION_EXT', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('RESOURCE_SEMAPHORE', 'Memory', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('RESOURCE_SEMAPHORE_QUERY_COMPILE', 'Compilation', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_BPOOL_FLUSH', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_BUFFERPOOL_HELPLW', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_DBSTARTUP', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_DCOMSTARTUP', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_MASTERDBREADY', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_MASTERMDREADY', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_MASTERUPGRADED', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_MEMORYPOOL_ALLOCATEPAGES', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_MSDBSTARTUP', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_RETRY_VIRTUALALLOC', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_SYSTEMTASK', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_TASK', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_TEMPDBSTARTUP', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SLEEP_WORKSPACE_ALLOCATEPAGE', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SOS_SCHEDULER_YIELD', 'CPU', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SOS_WORK_DISPATCHER', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SP_SERVER_DIAGNOSTICS_SLEEP', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLCLR_APPDOMAIN', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLCLR_ASSEMBLY', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLCLR_DEADLOCK_DETECTION', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLCLR_QUANTUM_PUNISHMENT', 'SQL CLR', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_BUFFER_FLUSH', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_FILE_BUFFER', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_FILE_READ_IO_COMPLETION', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_FILE_WRITE_IO_COMPLETION', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_PENDING_BUFFER_WRITERS', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_SHUTDOWN', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('SQLTRACE_WAIT_ENTRIES', 'Idle', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('THREADPOOL', 'Worker Thread', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRACE_EVTNOTIF', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRACEWRITE', 'Tracing', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_DT', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_EX', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_KP', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_NL', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_SH', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRAN_MARKLATCH_UP', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('TRANSACTION_MUTEX', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('UCS_SESSION_REGISTRATION', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('WAIT_FOR_RESULTS', 'User Wait', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('WAIT_XTP_OFFLINE_CKPT_NEW_LOG', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('WAITFOR', 'User Wait', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('WRITE_COMPLETION', 'Other Disk IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('WRITELOG', 'Tran Log IO', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XACT_OWN_TRANSACTION', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XACT_RECLAIM_SESSION', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XACTLOCKINFO', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XACTWORKSPACE_MUTEX', 'Transaction', 0);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XE_DISPATCHER_WAIT', 'Idle', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XE_LIVE_TARGET_TVF', 'Other', 1);
                            insert into ##waitcategories(waittype, waitcategory, ignorable)
                            values ('XE_TIMER_EVENT', 'Idle', 1);
                        end; /* IF SELECT SUM(1) FROM ##WaitCategories <> 527 */


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

                    if @filterplansbydatabase is not null
                        begin
                            if UPPER(LEFT(@filterplansbydatabase, 4)) = 'USER'
                                begin
                                    insert into #filterplansbydatabase (databaseid)
                                    select database_id
                                    from sys.databases
                                    where [name] not in ('master', 'model', 'msdb', 'tempdb');
                                end;
                            else
                                begin
                                    set @filterplansbydatabase = @filterplansbydatabase + ',';
                                    with a as
                                             (
                                                 select CAST(1 as bigint)                      f,
                                                        CHARINDEX(',', @filterplansbydatabase) t,
                                                        1                                      seq
                                                 union all
                                                 select t + 1, CHARINDEX(',', @filterplansbydatabase, t + 1), seq + 1
                                                 from a
                                                 where CHARINDEX(',', @filterplansbydatabase, t + 1) > 0
                                             )
                                    insert
                                    #filterplansbydatabase
                                    (
                                    databaseid
                                    )
                                    select distinct db.database_id
                                    from a
                                             inner join sys.databases db
                                                        on LTRIM(RTRIM(SUBSTRING(@filterplansbydatabase, a.f, a.t - a.f))) =
                                                           db.name
                                    where SUBSTRING(@filterplansbydatabase, f, t - f) is not null
                                    option (maxrecursion 0);
                                end;
                        end;

                    if OBJECT_ID('tempdb..#ReadableDBs') is not null
                        drop table #readabledbs;
                    create table #readabledbs
                    (
                        database_id int
                    );

                    if EXISTS(select * from sys.all_objects o where o.name = 'dm_hadr_database_replica_states')
                        begin
                            raiserror ('Checking for Read intent databases to exclude',0,0) with nowait;

                            set @stringtoexecute =
                                    'INSERT INTO #ReadableDBs (database_id) SELECT DBs.database_id FROM sys.databases DBs INNER JOIN sys.availability_replicas Replicas ON DBs.replica_id = Replicas.replica_id WHERE replica_server_name NOT IN (SELECT DISTINCT primary_replica FROM sys.dm_hadr_availability_group_states States) AND Replicas.secondary_role_allow_connections_desc = ''READ_ONLY'' AND replica_server_name = @@SERVERNAME;';
                            exec (@stringtoexecute);

                        end

                    declare @v decimal(6, 2),
                        @build int,
                        @memgrantsortsupported bit = 1;

                    raiserror (N'Determining SQL Server version.',0,1) with nowait;

                    insert into #checkversion (version)
                    select CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))
                    option (recompile);


                    select @v = common_version,
                           @build = build
                    from #checkversion
                    option (recompile);

                    if (@v < 11)
                        or (@v = 11 and @build < 6020)
                        or (@v = 12 and @build < 5000)
                        or (@v = 13 and @build < 1601)
                        set @memgrantsortsupported = 0;

                    if EXISTS(select * from sys.all_objects where name = 'dm_exec_query_statistics_xml')
                        and
                       ((@v = 13 and @build >= 5337) /* This DMF causes assertion errors: https://support.microsoft.com/en-us/help/4490136/fix-assertion-error-occurs-when-you-use-sys-dm-exec-query-statistics-x */
                           or (@v = 14 and @build >= 3162)
                           or (@v >= 15)
                           or (@v <= 12)) /* Azure */
                        set @dm_exec_query_statistics_xml = 1;


                    set @stockwarningheader = '<?ClickToSeeCommmand -- ' + @linefeed + @linefeed
                        + 'WARNING: Running this command may result in data loss or an outage.' + @linefeed
                        + 'This tool is meant as a shortcut to help generate scripts for DBAs.' + @linefeed
                        + 'It is not a substitute for database training and experience.' + @linefeed
                        + 'Now, having said that, here''s the details:' + @linefeed + @linefeed;

                    select @stockwarningfooter = @stockwarningfooter + @linefeed + @linefeed + '-- ?>',
                           @stockdetailsheader = @stockdetailsheader + '<?ClickToSeeDetails -- ' + @linefeed,
                           @stockdetailsfooter = @stockdetailsfooter + @linefeed + ' -- ?>';

                    /* Get the instance name to use as a Perfmon counter prefix. */
                    if CAST(SERVERPROPERTY('edition') as varchar(100)) = 'SQL Azure'
                        select top 1 @servicename = LEFT(object_name, (CHARINDEX(':', object_name) - 1))
                        from sys.dm_os_performance_counters;
                    else
                        begin
                            set @stringtoexecute =
                                    'INSERT INTO #PerfmonStats(object_name, Pass, SampleTime, counter_name, cntr_type) SELECT CASE WHEN @@SERVICENAME = ''MSSQLSERVER'' THEN ''SQLServer'' ELSE ''MSSQL$'' + @@SERVICENAME END, 0, SYSDATETIMEOFFSET(), ''stuffing'', 0 ;';
                            exec (@stringtoexecute);
                            select @servicename = object_name from #perfmonstats;
                            delete #perfmonstats;
                        end;

                    /* Build a list of queries that were run in the last 10 seconds.
       We're looking for the death-by-a-thousand-small-cuts scenario
       where a query is constantly running, and it doesn't have that
       big of an impact individually, but it has a ton of impact
       overall. We're going to build this list, and then after we
       finish our @Seconds sample, we'll compare our plan cache to
       this list to see what ran the most. */

                    /* Populate #QueryStats. SQL 2005 doesn't have query hash or query plan hash. */
                    if @checkprocedurecache = 1
                        begin
                            raiserror ('@CheckProcedureCache = 1, capturing first pass of plan cache',10,1) with nowait;
                            if @@VERSION like 'Microsoft SQL Server 2005%'
                                begin
                                    if @filterplansbydatabase is null
                                        begin
                                            set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()));';
                                        end;
                                    else
                                        begin
                                            set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
												CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
												INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()))
												AND attr.attribute = ''dbid'';';
                                        end;
                                end;
                            else
                                begin
                                    if @filterplansbydatabase is null
                                        begin
                                            set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()));';
                                        end;
                                    else
                                        begin
                                            set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 1 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
											INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= (DATEADD(ss, -10, SYSDATETIMEOFFSET()))
												AND attr.attribute = ''dbid'';';
                                        end;
                                end;
                            exec (@stringtoexecute);

                            /* Get the totals for the entire plan cache */
                            insert into #querystats (pass, sampletime, execution_count, total_worker_time,
                                                     total_physical_reads, total_logical_writes, total_logical_reads,
                                                     total_clr_time, total_elapsed_time, creation_time)
                            select -1 as pass,
                                   SYSDATETIMEOFFSET(),
                                   SUM(execution_count),
                                   SUM(total_worker_time),
                                   SUM(total_physical_reads),
                                   SUM(total_logical_writes),
                                   SUM(total_logical_reads),
                                   SUM(total_clr_time),
                                   SUM(total_elapsed_time),
                                   MIN(creation_time)
                            from sys.dm_exec_query_stats qs;
                        end; /*IF @CheckProcedureCache = 1 */


                    if EXISTS(select *
                              from tempdb.sys.all_objects obj
                                       inner join tempdb.sys.all_columns col1
                                                  on obj.object_id = col1.object_id and col1.name = 'object_name'
                                       inner join tempdb.sys.all_columns col2
                                                  on obj.object_id = col2.object_id and col2.name = 'counter_name'
                                       inner join tempdb.sys.all_columns col3
                                                  on obj.object_id = col3.object_id and col3.name = 'instance_name'
                              where obj.name like '%CustomPerfmonCounters%')
                        begin
                            set @stringtoexecute =
                                    'INSERT INTO #PerfmonCounters ([object_name],[counter_name],[instance_name]) SELECT [object_name],[counter_name],[instance_name] FROM #CustomPerfmonCounters';
                            exec (@stringtoexecute);
                        end;
                    else
                        begin
                            /* Add our default Perfmon counters */
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Forwarded Records/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Page compression attempts/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Page Splits/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Skipped Ghosted Records/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Table Lock Escalations/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Worktables Created/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Group', 'Active Hadr Threads', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Bytes Received from Replica/sec',
                                    '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Bytes Sent to Replica/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Bytes Sent to Transport/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Flow Control Time (ms/sec)', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Flow Control/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Resent Messages/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Availability Replica', 'Sends to Replica/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Page life expectancy', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Page reads/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Page writes/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Readahead pages/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Target pages', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Total pages', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', '', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Active Transactions', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Database Flow Control Delay', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Database Flow Controls/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Group Commit Time', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Group Commits/Sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Log Apply Pending Queue', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Log Apply Ready Queue', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Log Compression Cache misses/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Log remaining for undo', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Log Send Queue', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Recovery Queue', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Redo blocked/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Redo Bytes Remaining', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Database Replica', 'Redone Bytes/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Log Bytes Flushed/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Log Growths', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Log Pool LogWriter Pushes/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Log Shrinks', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Transactions/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'Write Transactions/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Databases', 'XTP Memory Used (KB)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Exec Statistics', 'Distributed Query', 'Execs in progress');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Exec Statistics', 'DTC calls', 'Execs in progress');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Exec Statistics', 'Extended Procedures', 'Execs in progress');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Exec Statistics', 'OLEDB calls', 'Execs in progress');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'Active Temp Tables', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'Logins/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'Logouts/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'Mars Deadlocks', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'Processes blocked', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Number of Deadlocks/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Memory Manager', 'Memory Grants Pending', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Errors', 'Errors/sec', '_Total');
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Batch Requests/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Forced Parameterizations/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Guided plan executions/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'SQL Attention rate', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'SQL Compilations/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'SQL Re-Compilations/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Workload Group Stats', 'Query optimizations/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Workload Group Stats', 'Suboptimal plans/sec', null);
                            /* Below counters added by Jefferson Elias */
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Worktables From Cache Base', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Worktables From Cache Ratio', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Database pages', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Free pages', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Stolen pages', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Memory Manager', 'Granted Workspace Memory (KB)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Memory Manager', 'Maximum Workspace Memory (KB)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Memory Manager', 'Target Server Memory (KB)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Memory Manager', 'Total Server Memory (KB)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Buffer cache hit ratio', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Buffer cache hit ratio base', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Checkpoint pages/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Free list stalls/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Lazy writes/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Auto-Param Attempts/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Failed Auto-Params/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Safe Auto-Params/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':SQL Statistics', 'Unsafe Auto-Params/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Workfiles Created/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':General Statistics', 'User Connections', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Latches', 'Average Latch Wait Time (ms)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Latches', 'Average Latch Wait Time Base', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Latches', 'Latch Waits/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Latches', 'Total Latch Wait Time (ms)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Average Wait Time (ms)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Average Wait Time Base', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Lock Requests/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Lock Timeouts/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Lock Wait Time (ms)', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Locks', 'Lock Waits/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Transactions', 'Longest Transaction Running Time', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Full Scans/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Access Methods', 'Index Searches/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Buffer Manager', 'Page lookups/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values (@servicename + ':Cursor Manager by Type', 'Active cursors', null);
                            /* Below counters are for In-Memory OLTP (Hekaton), which have a different naming convention.
           And yes, they actually hard-coded the version numbers into the counters, and SQL 2019 still says 2017, oddly.
           For why, see: https://connect.microsoft.com/SQLServer/feedback/details/817216/xtp-perfmon-counters-should-appear-under-sql-server-perfmon-counter-group
        */
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Cursors', 'Expired rows removed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Cursors', 'Expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Garbage Collection', 'Rows processed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP IO Governor', 'Io Issued/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Phantom Processor', 'Phantom expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Phantom Processor', 'Phantom rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Transaction Log', 'Log bytes written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Transaction Log', 'Log records written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Transactions', 'Transactions aborted by user/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Transactions', 'Transactions aborted/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2014 XTP Transactions', 'Transactions created/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Cursors', 'Expired rows removed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Cursors', 'Expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Garbage Collection', 'Rows processed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP IO Governor', 'Io Issued/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Phantom Processor', 'Phantom expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Phantom Processor', 'Phantom rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Transaction Log', 'Log bytes written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Transaction Log', 'Log records written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Transactions', 'Transactions aborted by user/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Transactions', 'Transactions aborted/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2016 XTP Transactions', 'Transactions created/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Cursors', 'Expired rows removed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Cursors', 'Expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Garbage Collection', 'Rows processed/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP IO Governor', 'Io Issued/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Phantom Processor', 'Phantom expired rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Phantom Processor', 'Phantom rows touched/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Transaction Log', 'Log bytes written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Transaction Log', 'Log records written/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Transactions', 'Transactions aborted by user/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Transactions', 'Transactions aborted/sec', null);
                            insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                            values ('SQL Server 2017 XTP Transactions', 'Transactions created/sec', null);
                        end;

                    /* Populate #FileStats, #PerfmonStats, #WaitStats with DMV data.
        After we finish doing our checks, we'll take another sample and compare them. */
                    raiserror ('Capturing first pass of wait stats, perfmon counters, file stats',10,1) with nowait;
                    insert #waitstats(pass, sampletime, wait_type, wait_time_ms, signal_wait_time_ms,
                                      waiting_tasks_count)
                    select x.pass,
                           x.sampletime,
                           x.wait_type,
                           SUM(x.sum_wait_time_ms)        as sum_wait_time_ms,
                           SUM(x.sum_signal_wait_time_ms) as sum_signal_wait_time_ms,
                           SUM(x.sum_waiting_tasks)       as sum_waiting_tasks
                    from (
                             select 1                                                                       as pass,
                                    case @seconds when 0 then @startsampletime else SYSDATETIMEOFFSET() end as sampletime,
                                    owt.wait_type,
                                    case @seconds
                                        when 0 then 0
                                        else SUM(owt.wait_duration_ms) over (partition by owt.wait_type, owt.session_id)
                                            -
                                             case when @seconds = 0 then 0 else (@seconds * 1000) end end   as sum_wait_time_ms,
                                    0                                                                       as sum_signal_wait_time_ms,
                                    0                                                                       as sum_waiting_tasks
                             from sys.dm_os_waiting_tasks owt
                             where owt.session_id > 50
                               and owt.wait_duration_ms >= case @seconds when 0 then 0 else @seconds * 1000 end
                             union all
                             select 1                                                                          as pass,
                                    case @seconds when 0 then @startsampletime else SYSDATETIMEOFFSET() end    as sampletime,
                                    os.wait_type,
                                    case @seconds
                                        when 0 then 0
                                        else SUM(os.wait_time_ms) over (partition by os.wait_type) end         as sum_wait_time_ms,
                                    case @seconds
                                        when 0 then 0
                                        else SUM(os.signal_wait_time_ms) over (partition by os.wait_type ) end as sum_signal_wait_time_ms,
                                    case @seconds
                                        when 0 then 0
                                        else SUM(os.waiting_tasks_count) over (partition by os.wait_type) end  as sum_waiting_tasks
                             from sys.dm_os_wait_stats os
                         ) x
                    where EXISTS
                              (
                                  select 1 / 0
                                  from ##waitcategories as wc
                                  where wc.waittype = x.wait_type
                                    and wc.ignorable = 0
                              )
                    group by x.pass, x.sampletime, x.wait_type
                    order by sum_wait_time_ms desc;


                    insert into #filestats (pass, sampletime, databaseid, fileid, databasename, filelogicalname,
                                            sizeondiskmb, io_stall_read_ms,
                                            num_of_reads, [bytes_read], io_stall_write_ms, num_of_writes,
                                            [bytes_written], physicalname, typedesc)
                    select 1                                                                          as pass,
                           case @seconds when 0 then @startsampletime else SYSDATETIMEOFFSET() end    as sampletime,
                           mf.[database_id],
                           mf.[file_id],
                           DB_NAME(vfs.database_id)                                                   as [db_name],
                           mf.name + N' [' + mf.type_desc collate sql_latin1_general_cp1_ci_as +
                           N']'                                                                       as file_logical_name,
                           CAST(((vfs.size_on_disk_bytes / 1024.0) / 1024.0) as int)                  as size_on_disk_mb,
                           case @seconds when 0 then 0 else vfs.io_stall_read_ms end,
                           case @seconds when 0 then 0 else vfs.num_of_reads end,
                           case @seconds when 0 then 0 else vfs.[num_of_bytes_read] end,
                           case @seconds when 0 then 0 else vfs.io_stall_write_ms end,
                           case @seconds when 0 then 0 else vfs.num_of_writes end,
                           case @seconds when 0 then 0 else vfs.[num_of_bytes_written] end,
                           mf.physical_name,
                           mf.type_desc
                    from sys.dm_io_virtual_file_stats(null, null) as vfs
                             inner join #masterfiles as mf on vfs.file_id = mf.file_id
                        and vfs.database_id = mf.database_id
                    where vfs.num_of_reads > 0
                       or vfs.num_of_writes > 0;

                    insert into #perfmonstats (pass, sampletime, [object_name], [counter_name], [instance_name],
                                               [cntr_value], [cntr_type])
                    select 1                                                                       as pass,
                           case @seconds when 0 then @startsampletime else SYSDATETIMEOFFSET() end as sampletime,
                           RTRIM(dmv.object_name),
                           RTRIM(dmv.counter_name),
                           RTRIM(dmv.instance_name),
                           case @seconds when 0 then 0 else dmv.cntr_value end,
                           dmv.cntr_type
                    from #perfmoncounters counters
                             inner join sys.dm_os_performance_counters dmv
                                        on counters.counter_name collate sql_latin1_general_cp1_ci_as =
                                           RTRIM(dmv.counter_name) collate sql_latin1_general_cp1_ci_as
                                            and counters.[object_name] collate sql_latin1_general_cp1_ci_as =
                                                RTRIM(dmv.[object_name]) collate sql_latin1_general_cp1_ci_as
                                            and (counters.[instance_name] is null or
                                                 counters.[instance_name] collate sql_latin1_general_cp1_ci_as =
                                                 RTRIM(dmv.[instance_name]) collate sql_latin1_general_cp1_ci_as);

                    /* If they want to run sp_BlitzWho and export to table, go for it. */
                    if @outputtablenameblitzwho is not null
                        and @outputdatabasename is not null
                        and @outputschemaname is not null
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
                        begin
                            raiserror ('Logging sp_BlitzWho to table',10,1) with nowait;
                            exec sp_BlitzWho @outputdatabasename = @unquotedoutputdatabasename,
                                 @outputschemaname = @unquotedoutputschemaname,
                                 @outputtablename = @outputtablenameblitzwho, @checkdateoverride = @startsampletime;
                        end

                    raiserror ('Beginning investigatory queries',10,1) with nowait;


                    /* Maintenance Tasks Running - Backup Running - CheckID 1 */
                    if @seconds > 0
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, queryplan, starttime, loginname, ntusername,
                                                        programname, hostname, databaseid, databasename,
                                                        opentransactioncount, queryhash)
                        select 1                                                   as checkid,
                               1                                                   as priority,
                               'Maintenance Tasks Running'                         as findinggroup,
                               'Backup Running'                                    as finding,
                               'http://www.BrentOzar.com/askbrent/backups/'        as url,
                               'Backup of ' + DB_NAME(db.resource_database_id) + ' database (' +
                               (select CAST(CAST(SUM(size * 8.0 / 1024 / 1024) as bigint) as nvarchar)
                                from #masterfiles
                                where database_id = db.resource_database_id) + 'GB) ' + @linefeed
                                   + CAST(r.percent_complete as nvarchar(100)) + '% complete, has been running since ' +
                               CAST(r.start_time as nvarchar(100)) + '. ' + @linefeed
                                   + case
                                         when COALESCE(s.nt_user_name, s.login_name) is not null
                                             then (' Login: ' + COALESCE(s.nt_user_name, s.login_name) + ' ')
                                         else '' end                               as details,
                               'KILL ' + CAST(r.session_id as nvarchar(100)) + ';' as howtostopit,
                               pl.query_plan                                       as queryplan,
                               r.start_time                                        as starttime,
                               s.login_name                                        as loginname,
                               s.nt_user_name                                      as ntusername,
                               s.[program_name]                                    as programname,
                               s.[host_name]                                       as hostname,
                               db.[resource_database_id]                           as databaseid,
                               DB_NAME(db.resource_database_id)                    as databasename,
                               0                                                   as opentransactioncount,
                               r.query_hash
                        from sys.dm_exec_requests r
                                 inner join sys.dm_exec_connections c on r.session_id = c.session_id
                                 inner join sys.dm_exec_sessions s on r.session_id = s.session_id
                                 inner join (
                            select distinct request_session_id, resource_database_id
                            from sys.dm_tran_locks
                            where resource_type = N'DATABASE'
                              and request_mode = N'S'
                              and request_status = N'GRANT'
                              and request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') as db
                                            on s.session_id = db.request_session_id
                                 cross apply sys.dm_exec_query_plan(r.plan_handle) pl
                        where r.command like 'BACKUP%'
                          and r.start_time <= DATEADD(minute, -5, GETDATE())
                          and r.database_id not in (select database_id from #readabledbs);

                    /* If there's a backup running, add details explaining how long full backup has been taking in the last month. */
                    if @seconds > 0 and CAST(SERVERPROPERTY('edition') as varchar(100)) <> 'SQL Azure'
                        begin
                            set @stringtoexecute =
                                    'UPDATE #BlitzFirstResults SET Details = Details + '' Over the last 60 days, the full backup usually takes '' + CAST((SELECT AVG(DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date)) FROM msdb.dbo.backupset bs WHERE abr.DatabaseName = bs.database_name AND bs.type = ''D'' AND bs.backup_start_date > DATEADD(dd, -60, SYSDATETIMEOFFSET()) AND bs.backup_finish_date IS NOT NULL) AS NVARCHAR(100)) + '' minutes.'' FROM #BlitzFirstResults abr WHERE abr.CheckID = 1 AND EXISTS (SELECT * FROM msdb.dbo.backupset bs WHERE bs.type = ''D'' AND bs.backup_start_date > DATEADD(dd, -60, SYSDATETIMEOFFSET()) AND bs.backup_finish_date IS NOT NULL AND abr.DatabaseName = bs.database_name AND DATEDIFF(mi, bs.backup_start_date, bs.backup_finish_date) > 1)';
                            exec (@stringtoexecute);
                        end;


                    /* Maintenance Tasks Running - DBCC CHECK* Running - CheckID 2 */
                    if @seconds > 0 and EXISTS(select * from sys.dm_exec_requests where command like 'DBCC%')
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, queryplan, starttime, loginname, ntusername,
                                                        programname, hostname, databaseid, databasename,
                                                        opentransactioncount, queryhash)
                        select 2                                                   as checkid,
                               1                                                   as priority,
                               'Maintenance Tasks Running'                         as findinggroup,
                               'DBCC CHECK* Running'                               as finding,
                               'http://www.BrentOzar.com/askbrent/dbcc/'           as url,
                               'Corruption check of ' + DB_NAME(db.resource_database_id) + ' database (' +
                               (select CAST(CAST(SUM(size * 8.0 / 1024 / 1024) as bigint) as nvarchar)
                                from #masterfiles
                                where database_id = db.resource_database_id) + 'GB) has been running since ' +
                               CAST(r.start_time as nvarchar(100)) + '. '          as details,
                               'KILL ' + CAST(r.session_id as nvarchar(100)) + ';' as howtostopit,
                               pl.query_plan                                       as queryplan,
                               r.start_time                                        as starttime,
                               s.login_name                                        as loginname,
                               s.nt_user_name                                      as ntusername,
                               s.[program_name]                                    as programname,
                               s.[host_name]                                       as hostname,
                               db.[resource_database_id]                           as databaseid,
                               DB_NAME(db.resource_database_id)                    as databasename,
                               0                                                   as opentransactioncount,
                               r.query_hash
                        from sys.dm_exec_requests r
                                 inner join sys.dm_exec_connections c on r.session_id = c.session_id
                                 inner join sys.dm_exec_sessions s on r.session_id = s.session_id
                                 inner join (select distinct l.request_session_id, l.resource_database_id
                                             from sys.dm_tran_locks l
                                                      inner join sys.databases d on l.resource_database_id = d.database_id
                                             where l.resource_type = N'DATABASE'
                                               and l.request_mode = N'S'
                                               and l.request_status = N'GRANT'
                                               and l.request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') as db
                                            on s.session_id = db.request_session_id
                                 cross apply sys.dm_exec_query_plan(r.plan_handle) pl
                                 cross apply sys.dm_exec_sql_text(r.sql_handle) as t
                        where r.command like 'DBCC%'
                          and CAST(t.text as nvarchar(4000)) not like '%dm_db_index_physical_stats%'
                          and CAST(t.text as nvarchar(4000)) not like '%ALTER INDEX%'
                          and CAST(t.text as nvarchar(4000)) not like '%fileproperty%'
                          and r.database_id not in (select database_id from #readabledbs);


                    /* Maintenance Tasks Running - Restore Running - CheckID 3 */
                    if @seconds > 0
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, queryplan, starttime, loginname, ntusername,
                                                        programname, hostname, databaseid, databasename,
                                                        opentransactioncount, queryhash)
                        select 3                                                   as checkid,
                               1                                                   as priority,
                               'Maintenance Tasks Running'                         as findinggroup,
                               'Restore Running'                                   as finding,
                               'http://www.BrentOzar.com/askbrent/backups/'        as url,
                               'Restore of ' + DB_NAME(db.resource_database_id) + ' database (' +
                               (select CAST(CAST(SUM(size * 8.0 / 1024 / 1024) as bigint) as nvarchar)
                                from #masterfiles
                                where database_id = db.resource_database_id) + 'GB) is ' +
                               CAST(r.percent_complete as nvarchar(100)) + '% complete, has been running since ' +
                               CAST(r.start_time as nvarchar(100)) + '. '          as details,
                               'KILL ' + CAST(r.session_id as nvarchar(100)) + ';' as howtostopit,
                               pl.query_plan                                       as queryplan,
                               r.start_time                                        as starttime,
                               s.login_name                                        as loginname,
                               s.nt_user_name                                      as ntusername,
                               s.[program_name]                                    as programname,
                               s.[host_name]                                       as hostname,
                               db.[resource_database_id]                           as databaseid,
                               DB_NAME(db.resource_database_id)                    as databasename,
                               0                                                   as opentransactioncount,
                               r.query_hash
                        from sys.dm_exec_requests r
                                 inner join sys.dm_exec_connections c on r.session_id = c.session_id
                                 inner join sys.dm_exec_sessions s on r.session_id = s.session_id
                                 inner join (
                            select distinct request_session_id, resource_database_id
                            from sys.dm_tran_locks
                            where resource_type = N'DATABASE'
                              and request_mode = N'S'
                              and request_status = N'GRANT') as db on s.session_id = db.request_session_id
                                 cross apply sys.dm_exec_query_plan(r.plan_handle) pl
                        where r.command like 'RESTORE%'
                          and s.program_name <> 'SQL Server Log Shipping'
                          and r.database_id not in (select database_id from #readabledbs);


                    /* SQL Server Internal Maintenance - Database File Growing - CheckID 4 */
                    if @seconds > 0
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, queryplan, starttime, loginname, ntusername,
                                                        programname, hostname, databaseid, databasename,
                                                        opentransactioncount)
                        select 4                                                                                                                                   as checkid,
                               1                                                                                                                                   as priority,
                               'SQL Server Internal Maintenance'                                                                                                   as findinggroup,
                               'Database File Growing'                                                                                                             as finding,
                               'http://www.BrentOzar.com/go/instant'                                                                                               as url,
                               'SQL Server is waiting for Windows to provide storage space for a database restore, a data file growth, or a log file growth. This task has been running since ' +
                               CAST(r.start_time as nvarchar(100)) + '.' + @linefeed +
                               'Check the query plan (expert mode) to identify the database involved.'                                                             as details,
                               'Unfortunately, you can''t stop this, but you can prevent it next time. Check out http://www.BrentOzar.com/go/instant for details.' as howtostopit,
                               pl.query_plan                                                                                                                       as queryplan,
                               r.start_time                                                                                                                        as starttime,
                               s.login_name                                                                                                                        as loginname,
                               s.nt_user_name                                                                                                                      as ntusername,
                               s.[program_name]                                                                                                                    as programname,
                               s.[host_name]                                                                                                                       as hostname,
                               null                                                                                                                                as databaseid,
                               null                                                                                                                                as databasename,
                               0                                                                                                                                   as opentransactioncount
                        from sys.dm_os_waiting_tasks t
                                 inner join sys.dm_exec_connections c on t.session_id = c.session_id
                                 inner join sys.dm_exec_requests r on t.session_id = r.session_id
                                 inner join sys.dm_exec_sessions s on r.session_id = s.session_id
                                 cross apply sys.dm_exec_query_plan(r.plan_handle) pl
                        where t.wait_type = 'PREEMPTIVE_OS_WRITEFILEGATHER'
                          and r.database_id not in (select database_id from #readabledbs);


                    /* Query Problems - Long-Running Query Blocking Others - CheckID 5 */
                    if SERVERPROPERTY('Edition') <> 'SQL Azure' and @seconds > 0 and EXISTS(
                            select * from sys.dm_os_waiting_tasks where wait_type like 'LCK%'
                                                                    and wait_duration_ms > 30000)
                        begin
                            set @stringtoexecute = N'INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, QueryHash)
            SELECT 5 AS CheckID,
                1 AS Priority,
                ''Query Problems'' AS FindingGroup,
                ''Long-Running Query Blocking Others'' AS Finding,
                ''http://www.BrentOzar.com/go/blocking'' AS URL,
                ''Query in '' + COALESCE(DB_NAME(COALESCE((SELECT TOP 1 dbid FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 t.dbid FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) t WHERE spBlocker.spid = tBlocked.blocking_session_id))), ''(Unknown)'') + '' has a last request start time of '' + CAST(s.last_request_start_time AS NVARCHAR(100)) + ''. Query follows: '
                                + @linefeed + @linefeed +
                                                   '''+ CAST(COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id), '''') AS NVARCHAR(2000)) AS Details,
                ''KILL '' + CAST(tBlocked.blocking_session_id AS NVARCHAR(100)) + '';'' AS HowToStopIt,
                (SELECT TOP 1 query_plan FROM sys.dm_exec_query_plan(r.plan_handle)) AS QueryPlan,
                COALESCE((SELECT TOP 1 [text] FROM sys.dm_exec_sql_text(r.sql_handle)),
                    (SELECT TOP 1 [text] FROM master..sysprocesses spBlocker CROSS APPLY sys.dm_exec_sql_text(spBlocker.sql_handle) WHERE spBlocker.spid = tBlocked.blocking_session_id)) AS QueryText,
                r.start_time AS StartTime,
                s.login_name AS LoginName,
                s.nt_user_name AS NTUserName,
                s.[program_name] AS ProgramName,
                s.[host_name] AS HostName,
                r.[database_id] AS DatabaseID,
                DB_NAME(r.database_id) AS DatabaseName,
                0 AS OpenTransactionCount,
                r.query_hash
            FROM sys.dm_os_waiting_tasks tBlocked
	        INNER JOIN sys.dm_exec_sessions s ON tBlocked.blocking_session_id = s.session_id
            LEFT OUTER JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
            INNER JOIN sys.dm_exec_connections c ON s.session_id = c.session_id
            WHERE tBlocked.wait_type LIKE ''LCK%'' AND tBlocked.wait_duration_ms > 30000
              /* And the blocking session ID is not blocked by anyone else: */
              AND NOT EXISTS(SELECT * FROM sys.dm_os_waiting_tasks tBlocking WHERE s.session_id = tBlocking.session_id AND tBlocking.session_id <> tBlocking.blocking_session_id AND tBlocking.blocking_session_id IS NOT NULL)
			  AND r.database_id NOT IN (SELECT database_id FROM #ReadableDBs);';
                            execute sp_executesql @stringtoexecute;
                        end;

                    /* Query Problems - Plan Cache Erased Recently */
                    if DATEADD(mi, -15, SYSDATETIME()) <
                       (select top 1 creation_time from sys.dm_exec_query_stats order by creation_time)
                        begin
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                            select top 1 7                                                                      as checkid,
                                         50                                                                     as priority,
                                         'Query Problems'                                                       as findinggroup,
                                         'Plan Cache Erased Recently'                                           as finding,
                                         'http://www.BrentOzar.com/askbrent/plan-cache-erased-recently/'        as url,
                                         'The oldest query in the plan cache was created at ' +
                                         CAST(creation_time as nvarchar(50)) + '. ' + @linefeed + @linefeed
                                             + 'This indicates that someone ran DBCC FREEPROCCACHE at that time,' +
                                         @linefeed
                                             + 'Giving SQL Server temporary amnesia. Now, as queries come in,' +
                                         @linefeed
                                             + 'SQL Server has to use a lot of CPU power in order to build execution' +
                                         @linefeed
                                             +
                                         'plans and put them in cache again. This causes high CPU loads.'       as details,
                                         'Find who did that, and stop them from doing it again.'                as howtostopit
                            from sys.dm_exec_query_stats
                            order by creation_time;
                        end;


                    /* Query Problems - Sleeping Query with Open Transactions - CheckID 8 */
                    if @seconds > 0
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, starttime, loginname, ntusername, programname,
                                                        hostname, databaseid, databasename, querytext,
                                                        opentransactioncount)
                        select 8                                                                          as checkid,
                               50                                                                         as priority,
                               'Query Problems'                                                           as findinggroup,
                               'Sleeping Query with Open Transactions'                                    as finding,
                               'http://www.brentozar.com/askbrent/sleeping-query-with-open-transactions/' as url,
                               'Database: ' + DB_NAME(db.resource_database_id) + @linefeed + 'Host: ' + s.[host_name] +
                               @linefeed + 'Program: ' + s.[program_name] + @linefeed +
                               'Asleep with open transactions and locks since ' +
                               CAST(s.last_request_end_time as nvarchar(100)) + '. '                      as details,
                               'KILL ' + CAST(s.session_id as nvarchar(100)) + ';'                        as howtostopit,
                               s.last_request_start_time                                                  as starttime,
                               s.login_name                                                               as loginname,
                               s.nt_user_name                                                             as ntusername,
                               s.[program_name]                                                           as programname,
                               s.[host_name]                                                              as hostname,
                               db.[resource_database_id]                                                  as databaseid,
                               DB_NAME(db.resource_database_id)                                           as databasename,
                               (select top 1 [text] from sys.dm_exec_sql_text(c.most_recent_sql_handle))  as querytext,
                               sessions_with_transactions.open_transaction_count                          as opentransactioncount
                        from (select session_id, SUM(open_transaction_count) as open_transaction_count
                              from sys.dm_exec_requests
                              where open_transaction_count > 0
                              group by session_id) as sessions_with_transactions
                                 inner join sys.dm_exec_sessions s
                                            on sessions_with_transactions.session_id = s.session_id
                                 inner join sys.dm_exec_connections c on s.session_id = c.session_id
                                 inner join (
                            select distinct request_session_id, resource_database_id
                            from sys.dm_tran_locks
                            where resource_type = N'DATABASE'
                              and request_mode = N'S'
                              and request_status = N'GRANT'
                              and request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') as db
                                            on s.session_id = db.request_session_id
                        where s.status = 'sleeping'
                          and s.last_request_end_time < DATEADD(ss, -10, SYSDATETIME())
                          and EXISTS(select *
                                     from sys.dm_tran_locks
                                     where request_session_id = s.session_id
                                       and not (resource_type = N'DATABASE' and request_mode = N'S' and
                                                request_status = N'GRANT' and
                                                request_owner_type = N'SHARED_TRANSACTION_WORKSPACE'));


                    /*Query Problems - Clients using implicit transactions */
                    if @seconds > 0
                        and (@@VERSION not like 'Microsoft SQL Server 2005%'
                            and @@VERSION not like 'Microsoft SQL Server 2008%'
                            and @@VERSION not like 'Microsoft SQL Server 2008 R2%')
                        begin
                            set @stringtoexecute = N'INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount)
		SELECT  37 AS CheckId,
		        50 AS Priority,
		        ''Query Problems'' AS FindingsGroup,
		        ''Implicit Transactions'',
		        ''https://www.brentozar.com/go/ImplicitTransactions/'' AS URL,
		        ''Database: '' + DB_NAME(s.database_id)  + '' '' + CHAR(13) + CHAR(10) +
				''Host: '' + s.[host_name]  + '' '' + CHAR(13) + CHAR(10) +
				''Program: '' + s.[program_name]  + '' '' + CHAR(13) + CHAR(10) +
				CONVERT(NVARCHAR(10), s.open_transaction_count) +
				'' open transactions since: '' +
				CONVERT(NVARCHAR(30), tat.transaction_begin_time) + ''. ''
					AS Details,
				''Run sp_BlitzWho and check the is_implicit_transaction column to spot the culprits.
If one of them is a lead blocker, consider killing that query.'' AS HowToStopit,
		        tat.transaction_begin_time,
		        s.login_name,
		        s.nt_user_name,
		        s.program_name,
		        s.host_name,
		        s.database_id,
		        DB_NAME(s.database_id) AS DatabaseName,
		        NULL AS Querytext,
		        s.open_transaction_count AS OpenTransactionCount
		FROM    sys.dm_tran_active_transactions AS tat
		LEFT JOIN sys.dm_tran_session_transactions AS tst
		ON tst.transaction_id = tat.transaction_id
		LEFT JOIN sys.dm_exec_sessions AS s
		ON s.session_id = tst.session_id
		WHERE tat.name = ''implicit_transaction'';
		'
                            execute sp_executesql @stringtoexecute;
                        end;

                    /* Query Problems - Query Rolling Back - CheckID 9 */
                    if @seconds > 0
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                        howtostopit, starttime, loginname, ntusername, programname,
                                                        hostname, databaseid, databasename, querytext, queryhash)
                        select 9                                                                                                                                              as checkid,
                               1                                                                                                                                              as priority,
                               'Query Problems'                                                                                                                               as findinggroup,
                               'Query Rolling Back'                                                                                                                           as finding,
                               'http://www.BrentOzar.com/askbrent/rollback/'                                                                                                  as url,
                               'Rollback started at ' + CAST(r.start_time as nvarchar(100)) + ', is ' +
                               CAST(r.percent_complete as nvarchar(100)) +
                               '% complete.'                                                                                                                                  as details,
                               'Unfortunately, you can''t stop this. Whatever you do, don''t restart the server in an attempt to fix it - SQL Server will keep rolling back.' as howtostopit,
                               r.start_time                                                                                                                                   as starttime,
                               s.login_name                                                                                                                                   as loginname,
                               s.nt_user_name                                                                                                                                 as ntusername,
                               s.[program_name]                                                                                                                               as programname,
                               s.[host_name]                                                                                                                                  as hostname,
                               db.[resource_database_id]                                                                                                                      as databaseid,
                               DB_NAME(db.resource_database_id)                                                                                                               as databasename,
                               (select top 1 [text]
                                from sys.dm_exec_sql_text(c.most_recent_sql_handle))                                                                                          as querytext,
                               r.query_hash
                        from sys.dm_exec_sessions s
                                 inner join sys.dm_exec_connections c on s.session_id = c.session_id
                                 inner join sys.dm_exec_requests r on s.session_id = r.session_id
                                 left outer join (
                            select distinct request_session_id, resource_database_id
                            from sys.dm_tran_locks
                            where resource_type = N'DATABASE'
                              and request_mode = N'S'
                              and request_status = N'GRANT'
                              and request_owner_type = N'SHARED_TRANSACTION_WORKSPACE') as db
                                                 on s.session_id = db.request_session_id
                        where r.status = 'rollback';


                    /* Server Performance - Too Much Free Memory - CheckID 34 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 34                                                                                                       as checkid,
                           50                                                                                                       as priority,
                           'Server Performance'                                                                                     as findinggroup,
                           'Too Much Free Memory'                                                                                   as finding,
                           'https://BrentOzar.com/go/freememory'                                                                    as url,
                           CAST((CAST(cfree.cntr_value as bigint) / 1024 / 1024) as nvarchar(100)) +
                           N'GB of free memory inside SQL Server''s buffer pool,' + @linefeed + ' which is ' +
                           CAST((CAST(ctotal.cntr_value as bigint) / 1024 / 1024) as nvarchar(100)) +
                           N'GB. You would think lots of free memory would be good, but check out the URL for more information.'    as details,
                           'Run sp_BlitzCache @SortOrder = ''memory grant'' to find queries with huge memory grants and tune them.' as howtostopit
                    from sys.dm_os_performance_counters cfree
                             inner join sys.dm_os_performance_counters ctotal
                                        on ctotal.object_name like N'%Memory Manager%'
                                            and ctotal.counter_name =
                                                N'Total Server Memory (KB)                                                                                                        '
                    where cfree.object_name like N'%Memory Manager%'
                      and cfree.counter_name =
                          N'Free Memory (KB)                                                                                                                '
                      and CAST(cfree.cntr_value as bigint) > 20480000000
                      and CAST(ctotal.cntr_value as bigint) * .3 <= CAST(cfree.cntr_value as bigint)
                      and CAST(SERVERPROPERTY('edition') as varchar(100)) not like '%Standard%';

                    /* Server Performance - Target Memory Lower Than Max - CheckID 35 */
                    if SERVERPROPERTY('Edition') <> 'SQL Azure'
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                        select 35                                                                                                            as checkid,
                               10                                                                                                            as priority,
                               'Server Performance'                                                                                          as findinggroup,
                               'Target Memory Lower Than Max'                                                                                as finding,
                               'https://BrentOzar.com/go/target'                                                                             as url,
                               N'Max server memory is ' + CAST(cmax.value_in_use as nvarchar(50)) +
                               N' MB but target server memory is only ' +
                               CAST((CAST(ctarget.cntr_value as bigint) / 1024) as nvarchar(50)) + N' MB,' + @linefeed
                                   +
                               N'indicating that SQL Server may be under external memory pressure or max server memory may be set too high.' as details,
                               'Investigate what OS processes are using memory, and double-check the max server memory setting.'             as howtostopit
                        from sys.configurations cmax
                                 inner join sys.dm_os_performance_counters ctarget
                                            on ctarget.object_name like N'%Memory Manager%'
                                                and ctarget.counter_name =
                                                    N'Target Server Memory (KB)                                                                                                       '
                        where cmax.name = 'max server memory (MB)'
                          and CAST(cmax.value_in_use as bigint) >= 1.5 * (CAST(ctarget.cntr_value as bigint) / 1024)
                          and CAST(cmax.value_in_use as bigint) < 2147483647 /* Not set to default of unlimited */
                          and CAST(ctarget.cntr_value as bigint) <
                              .8 * (select available_physical_memory_kb from sys.dm_os_sys_memory);
                    /* Target memory less than 80% of physical memory (in case they set max too high) */

                    /* Server Info - Database Size, Total GB - CheckID 21 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                    select 21                                                                   as checkid,
                           251                                                                  as priority,
                           'Server Info'                                                        as findinggroup,
                           'Database Size, Total GB'                                            as finding,
                           CAST(SUM(CAST(size as bigint) * 8. / 1024. / 1024.) as varchar(100)) as details,
                           SUM(CAST(size as bigint)) * 8. / 1024. / 1024.                       as detailsint,
                           'http://www.BrentOzar.com/askbrent/'                                 as url
                    from #masterfiles
                    where database_id > 4;

                    /* Server Info - Database Count - CheckID 22 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                    select 22                                   as checkid,
                           251                                  as priority,
                           'Server Info'                        as findinggroup,
                           'Database Count'                     as finding,
                           CAST(SUM(1) as varchar(100))         as details,
                           SUM(1)                               as detailsint,
                           'http://www.BrentOzar.com/askbrent/' as url
                    from sys.databases
                    where database_id > 4;

                    /* Server Info - Memory Grants pending - CheckID 39 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                    select 39                                               as checkid,
                           50                                               as priority,
                           'Server Performance'                             as findinggroup,
                           'Memory Grants Pending'                          as finding,
                           CAST(pendinggrants.details as nvarchar(50))      as details,
                           pendinggrants.detailsint,
                           'https://www.brentozar.com/blitz/memory-grants/' as url
                    from (
                             select COUNT(1) as details,
                                    COUNT(1) as detailsint
                             from sys.dm_exec_query_memory_grants as grants
                             where queue_id is not null
                         ) as pendinggrants
                    where pendinggrants.details > 0;

                    /* Server Info - Memory Grant/Workspace info - CheckID 40 */
                    declare @maxworkspace bigint
                    set @maxworkspace = (select CAST(cntr_value as bigint) / 1024
                                         from #perfmonstats
                                         where counter_name = N'Maximum Workspace Memory (KB)')

                    if (@maxworkspace is null
                        or @maxworkspace = 0)
                        begin
                            set @maxworkspace = 1
                        end

                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                    select 40 as                                                                                        checkid,
                           251 as                                                                                       priority,
                           'Server Info' as                                                                             findinggroup,
                           'Memory Grant/Workspace info' as                                                             finding,
                           + 'Grants Outstanding: ' + CAST((select COUNT(*)
                                                            from sys.dm_exec_query_memory_grants
                                                            where queue_id is null) as nvarchar(50)) + @linefeed
                               + 'Total Granted(MB): ' +
                           CAST(ISNULL(SUM(grants.granted_memory_kb) / 1024, 0) as nvarchar(50)) + @linefeed
                               + 'Total WorkSpace(MB): ' + CAST(ISNULL(@maxworkspace, 0) as nvarchar(50)) + @linefeed
                               + 'Granted workspace: ' +
                           CAST(ISNULL((CAST(SUM(grants.granted_memory_kb) / 1024 as money)
                               / CAST(@maxworkspace as money)) * 100, 0) as nvarchar(50)) + '%' + @linefeed
                               + 'Oldest Grant in seconds: ' + CAST(
                                   ISNULL(DATEDIFF(second, MIN(grants.request_time), GETDATE()), 0) as nvarchar(50)) as details,
                           (select COUNT(*)
                            from sys.dm_exec_query_memory_grants
                            where queue_id is null) as                                                                  detailsint,
                           'http://www.BrentOzar.com/askbrent/' as                                                      url
                    from sys.dm_exec_query_memory_grants as grants;

                    /* Query Problems - Memory Leak in USERSTORE_TOKENPERM Cache */
                    if EXISTS(select *
                              from sys.all_columns
                              where object_id = OBJECT_ID('sys.dm_os_memory_clerks') and name = 'pages_kb')
                        begin
                            /* SQL 2012+ version */
                            set @stringtoexecute = N'
        INSERT  INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, URL)
        SELECT 45 AS CheckID,
                50 AS Priority,
                ''Query Problems'' AS FindingsGroup,
                ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
                N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100))
                    + N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
                AS details,
                ''https://www.BrentOzar.com/go/userstore'' AS URL
        FROM sys.dm_os_memory_clerks
        HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN pages_kb * 1.0 ELSE 0.0 END) / SUM(pages_kb) >= 0.1
            AND SUM(pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
                            exec sp_executesql @stringtoexecute;
                        end
                    else
                        begin
                            /* Antiques Roadshow SQL 2008R2 - version */
                            set @stringtoexecute = N'
        INSERT  INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, Details, URL)
        SELECT 45 AS CheckID,
                50 AS Priority,
                ''Performance'' AS FindingsGroup,
                ''Memory Leak in USERSTORE_TOKENPERM Cache'' AS Finding,
                N''UserStore_TokenPerm clerk is using '' + CAST(CAST(SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100))
                    + N''GB RAM, total buffer pool is '' + CAST(CAST(SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 AS INT) AS NVARCHAR(100)) + N''GB.''
                AS details,
                ''https://www.BrentOzar.com/go/userstore'' AS URL
        FROM sys.dm_os_memory_clerks
        HAVING SUM(CASE WHEN type = ''USERSTORE_TOKENPERM'' AND name = ''TokenAndPermUserStore'' THEN single_pages_kb + multi_pages_kb * 1.0 ELSE 0.0 END) / SUM(single_pages_kb + multi_pages_kb) >= 0.1
            AND SUM(single_pages_kb + multi_pages_kb) / 1024.0 / 1024.0 >= 1; /* At least 1GB RAM overall */';
                            exec sp_executesql @stringtoexecute;
                        end


                    if @seconds > 0
                        begin

                            if EXISTS(select 1 / 0
                                      from sys.all_objects as ao
                                      where ao.name = 'dm_exec_query_profiles')
                                begin

                                    if EXISTS(select 1 / 0
                                              from sys.dm_exec_requests as r
                                                       join sys.dm_exec_sessions as s
                                                            on r.session_id = s.session_id
                                              where s.host_name is not null
                                                and r.total_elapsed_time > 5000)
                                        begin

                                            set @stringtoexecute = N'
                   DECLARE @bad_estimate TABLE
                     (
                       session_id INT,
                       request_id INT,
                       estimate_inaccuracy BIT
                     );

                   INSERT @bad_estimate ( session_id, request_id, estimate_inaccuracy )
                   SELECT x.session_id,
                          x.request_id,
                          x.estimate_inaccuracy
                   FROM (
                         SELECT deqp.session_id,
                                deqp.request_id,
                                CASE WHEN deqp.row_count > ( deqp.estimate_row_count * 10000 )
                                     THEN 1
                                     ELSE 0
                                END AS estimate_inaccuracy
                         FROM   sys.dm_exec_query_profiles AS deqp
						 WHERE deqp.session_id <> @@SPID
                   ) AS x
                   WHERE x.estimate_inaccuracy = 1
                   GROUP BY x.session_id,
                            x.request_id,
                            x.estimate_inaccuracy;

                   DECLARE @parallelism_skew TABLE
                     (
                       session_id INT,
                       request_id INT,
                       parallelism_skew BIT
                     );

                   INSERT @parallelism_skew ( session_id, request_id, parallelism_skew )
                   SELECT y.session_id,
                          y.request_id,
                          y.parallelism_skew
                   FROM (
                         SELECT x.session_id,
                                x.request_id,
                                x.node_id,
                                x.thread_id,
                                x.row_count,
                                x.sum_node_rows,
                                x.node_dop,
                                x.sum_node_rows / x.node_dop AS even_distribution,
                                x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) AS skew_percent,
                                CASE
                                    WHEN x.row_count > 10000
                                    AND x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) > 2.
                                    THEN 1
                                    WHEN x.row_count > 10000
                                    AND x.row_count / (1. * ISNULL(NULLIF(x.sum_node_rows / x.node_dop, 0), 1)) < 0.5
                                    THEN 1
                                    ELSE 0
                         	   END AS parallelism_skew
                         FROM (
                         	       SELECT deqp.session_id,
                                              deqp.request_id,
                                              deqp.node_id,
                                              deqp.thread_id,
                         	       	   deqp.row_count,
                         	       	   SUM(deqp.row_count)
                         	       		OVER ( PARTITION BY deqp.session_id,
                                                                   deqp.request_id,
                         	       		                    deqp.node_id
                         	       			   ORDER BY deqp.row_count
                         	       			   ROWS BETWEEN UNBOUNDED PRECEDING
                         	       			   AND UNBOUNDED FOLLOWING )
                         	       			   AS sum_node_rows,
                         	       	   COUNT(*)
                         	       		OVER ( PARTITION BY deqp.session_id,
                                                                   deqp.request_id,
                         	       		                    deqp.node_id
                         	       			   ORDER BY deqp.row_count
                         	       			   ROWS BETWEEN UNBOUNDED PRECEDING
                         	       			   AND UNBOUNDED FOLLOWING )
                         	       			   AS node_dop
                         	       FROM sys.dm_exec_query_profiles AS deqp
                         	       WHERE deqp.thread_id > 0
								   AND deqp.session_id <> @@SPID
                         	       AND EXISTS
                         	       	(
                         	       		SELECT 1/0
                         	       		FROM   sys.dm_exec_query_profiles AS deqp2
                         	       		WHERE deqp.session_id = deqp2.session_id
                         	       		AND   deqp.node_id = deqp2.node_id
                         	       		AND   deqp2.thread_id > 0
                         	       		GROUP BY deqp2.session_id, deqp2.node_id
                         	       		HAVING COUNT(deqp2.node_id) > 1
                         	       	)
                         	   ) AS x
                         ) AS y
                   WHERE y.parallelism_skew = 1
                   GROUP BY y.session_id,
                            y.request_id,
                            y.parallelism_skew;

                   /*
                   CheckID 42: Queries in dm_exec_query_profiles showing signs of poor cardinality estimates
                   */
                   INSERT INTO #BlitzFirstResults
                   (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount, QueryHash, QueryPlan)
                   SELECT 42 AS CheckID,
                          100 AS Priority,
                          ''Query Performance'' AS FindingsGroup,
                          ''Queries with 10000x cardinality misestimations'' AS Findings,
                          ''https://brentozar.com/go/skewedup'' AS URL,
                          ''The query on SPID ''
                              + RTRIM(b.session_id)
                              + '' has been running for ''
                              + RTRIM(r.total_elapsed_time / 1000)
                              + '' seconds,  with a large cardinality misestimate'' AS Details,
                          ''No quick fix here: time to dig into the actual execution plan. '' AS HowToStopIt,
                          r.start_time,
                          s.login_name,
                          s.nt_user_name,
                          s.program_name,
                          s.host_name,
                          r.database_id,
                          DB_NAME(r.database_id),
                          dest.text,
                          s.open_transaction_count,
                          r.query_hash, ';

                                            if @dm_exec_query_statistics_xml = 1
                                                set @stringtoexecute = @stringtoexecute +
                                                                       N' COALESCE(qs_live.query_plan, qp.query_plan) AS query_plan ';
                                            else
                                                set @stringtoexecute = @stringtoexecute + N' qp.query_plan ';

                                            set @stringtoexecute = @stringtoexecute + N'
                  FROM @bad_estimate AS b
                  JOIN sys.dm_exec_requests AS r
                  ON r.session_id = b.session_id
                  AND r.request_id = b.request_id
                  JOIN sys.dm_exec_sessions AS s
                  ON s.session_id = b.session_id
                  CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS dest
				  CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp ';

                                            if EXISTS(
                                                    select * from sys.all_objects where name = 'dm_exec_query_statistics_xml')
                                                set @stringtoexecute = @stringtoexecute +
                                                                       N' OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live ';


                                            set @stringtoexecute = @stringtoexecute + N';

                   /*
                   CheckID 43: Queries in dm_exec_query_profiles showing signs of unbalanced parallelism
                   */
                   INSERT INTO #BlitzFirstResults
                   (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, StartTime, LoginName, NTUserName, ProgramName, HostName, DatabaseID, DatabaseName, QueryText, OpenTransactionCount, QueryHash, QueryPlan)
                   SELECT 43 AS CheckID,
                          100 AS Priority,
                          ''Query Performance'' AS FindingsGroup,
                          ''Queries with 10000x skewed parallelism'' AS Findings,
                          ''https://brentozar.com/go/skewedup'' AS URL,
                          ''The query on SPID ''
                              + RTRIM(p.session_id)
                              + '' has been running for ''
                              + RTRIM(r.total_elapsed_time / 1000)
                              + '' seconds,  with a parallel threads doing uneven work.'' AS Details,
                          ''No quick fix here: time to dig into the actual execution plan. '' AS HowToStopIt,
                          r.start_time,
                          s.login_name,
                          s.nt_user_name,
                          s.program_name,
                          s.host_name,
                          r.database_id,
                          DB_NAME(r.database_id),
                          dest.text,
                          s.open_transaction_count,
                          r.query_hash, ';

                                            if @dm_exec_query_statistics_xml = 1
                                                set @stringtoexecute = @stringtoexecute +
                                                                       N' COALESCE(qs_live.query_plan, qp.query_plan) AS query_plan ';
                                            else
                                                set @stringtoexecute = @stringtoexecute + N' qp.query_plan ';

                                            set @stringtoexecute = @stringtoexecute + N'
                  FROM @parallelism_skew AS p
                  JOIN sys.dm_exec_requests AS r
                  ON r.session_id = p.session_id
                  AND r.request_id = p.request_id
                  JOIN sys.dm_exec_sessions AS s
                  ON s.session_id = p.session_id
                  CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS dest
				  CROSS APPLY sys.dm_exec_query_plan(r.plan_handle) AS qp ';

                                            if EXISTS(
                                                    select * from sys.all_objects where name = 'dm_exec_query_statistics_xml')
                                                set @stringtoexecute = @stringtoexecute +
                                                                       N' OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live ';


                                            set @stringtoexecute = @stringtoexecute + N';';

                                            execute sp_executesql @stringtoexecute;
                                        end

                                end
                        end

                    /* Server Performance - High CPU Utilization CheckID 24 */
                    if @seconds < 30
                        begin
                            /* If we're waiting less than 30 seconds, run this check now rather than wait til the end.
           We get this data from the ring buffers, and it's only updated once per minute, so might
           as well get it now - whereas if we're checking 30+ seconds, it might get updated by the
           end of our sp_BlitzFirst session. */
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                            select 24,
                                   50,
                                   'Server Performance',
                                   'High CPU Utilization',
                                   CAST(100 - systemidle as nvarchar(20)) + N'%.',
                                   100 - systemidle,
                                   'http://www.BrentOzar.com/go/cpu'
                            from (
                                     select record,
                                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                                                         'int') as systemidle
                                     from (
                                              select top 1 CONVERT(xml, record) as record
                                              from sys.dm_os_ring_buffers
                                              where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                and record like '%<SystemHealth>%'
                                              order by timestamp desc) as rb
                                 ) as y
                            where 100 - systemidle >= 50;

                            if SERVERPROPERTY('Edition') <> 'SQL Azure'
                                with y
                                         as
                                         (
                                             select CONVERT(varchar(5), 100 - ca.c.value('.', 'INT')) as system_idle,
                                                    CONVERT(varchar(30), rb.event_date)               as event_date,
                                                    CONVERT(varchar(8000), rb.record)                 as record
                                             from (select CONVERT(xml, dorb.record)                              as record,
                                                          DATEADD(ms, (ts.ms_ticks - dorb.timestamp), GETDATE()) as event_date
                                                   from sys.dm_os_ring_buffers as dorb
                                                            cross join
                                                            (select dosi.ms_ticks from sys.dm_os_sys_info as dosi) as ts
                                                   where dorb.ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                     and record like '%<SystemHealth>%') as rb
                                                      cross apply rb.record.nodes(
                                                     '/Record/SchedulerMonitorEvent/SystemHealth/SystemIdle') as ca(c)
                                         )
                                insert
                                into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint,
                                                         url, howtostopit)
                                select top 1 23,
                                             250,
                                             'Server Info',
                                             'CPU Utilization',
                                             y.system_idle + N'%. Ring buffer details: ' +
                                             CAST(y.record as nvarchar(4000)),
                                             y.system_idle,
                                             'http://www.BrentOzar.com/go/cpu',
                                             STUFF((select top 2147483647 CHAR(10) + CHAR(13)
                                                                              + y2.system_idle
                                                                              + '% ON '
                                                                              + y2.event_date
                                                                              + ' Ring buffer details:  '
                                                                              + y2.record
                                                    from y as y2
                                                    order by y2.event_date desc
                                                    for xml path(N''), type).value(N'.[1]', N'VARCHAR(MAX)'), 1, 1,
                                                   N'') as query
                                from y
                                order by y.event_date desc;


                            /* Highlight if non SQL processes are using >25% CPU */
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                            select 28,
                                   50,
                                   'Server Performance',
                                   'High CPU Utilization - Not SQL',
                                   CONVERT(nvarchar(100), 100 - (y.sqlusage + y.systemidle)) +
                                   N'% - Other Processes (not SQL Server) are using this much CPU. This may impact on the performance of your SQL Server instance',
                                   100 - (y.sqlusage + y.systemidle),
                                   'http://www.BrentOzar.com/go/cpu'
                            from (
                                     select record,
                                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                                                         'int') as systemidle
                                             ,
                                            record.value(
                                                    '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]',
                                                    'int')      as sqlusage
                                     from (
                                              select top 1 CONVERT(xml, record) as record
                                              from sys.dm_os_ring_buffers
                                              where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                and record like '%<SystemHealth>%'
                                              order by timestamp desc) as rb
                                 ) as y
                            where 100 - (y.sqlusage + y.systemidle) >= 25;

                        end;
                    /* IF @Seconds < 30 */

                    /* Query Problems - Statistics Updated Recently - CheckID 44 */
                    if 20 >=
                       (select COUNT(*) from sys.databases where name not in ('master', 'model', 'msdb', 'tempdb'))
                        begin
                            create table #updatedstats
                            (
                                howtostopit nvarchar(4000),
                                rowsforsorting bigint
                            );
                            if EXISTS(select * from sys.all_objects where name = 'dm_db_stats_properties')
                                begin
                                    exec sp_MSforeachdb N'USE [?];
			INSERT INTO #UpdatedStats(HowToStopIt, RowsForSorting)
			SELECT HowToStopIt =
						QUOTENAME(DB_NAME()) + N''.'' +
						QUOTENAME(SCHEMA_NAME(obj.schema_id)) + N''.'' +
						QUOTENAME(obj.name) +
						N'' statistic '' + QUOTENAME(stat.name) +
						N'' was updated on '' + CONVERT(NVARCHAR(50), sp.last_updated, 121) + N'','' +
						N'' had '' + CAST(sp.rows AS NVARCHAR(50)) + N'' rows, with '' +
						CAST(sp.rows_sampled AS NVARCHAR(50)) + N'' rows sampled,'' +
						N'' producing '' + CAST(sp.steps AS NVARCHAR(50)) + N'' steps in the histogram.'',
				sp.rows
			FROM sys.objects AS obj
			INNER JOIN sys.stats AS stat ON stat.object_id = obj.object_id
			CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
			WHERE sp.last_updated > DATEADD(MI, -15, GETDATE())
			AND obj.is_ms_shipped = 0
			AND ''[?]'' <> ''[tempdb]'';';
                                end;

                            if EXISTS(select * from #updatedstats)
                                insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                                select            44                                  as checkid,
                                                  50                                  as priority,
                                                  'Query Problems'                    as findinggroup,
                                                  'Statistics Updated Recently'       as finding,
                                                  'http://www.BrentOzar.com/go/stats' as url,
                                                  'In the last 15 minutes, statistics were updated. To see which ones, click the HowToStopIt column.' +
                                                  @linefeed + @linefeed
                                                      +
                                                  'This effectively clears the plan cache for queries that involve these tables,' +
                                                  @linefeed
                                                      +
                                                  'which thereby causes parameter sniffing: those queries are now getting brand new' +
                                                  @linefeed
                                                      +
                                                  'query plans based on whatever parameters happen to call them next.' +
                                                  @linefeed + @linefeed
                                                      +
                                                  'Be on the lookout for sudden parameter sniffing issues after this time range.',
                                    howtostopit = (select (select howtostopit + NCHAR(10))
                                                   from #updatedstats
                                                   order by rowsforsorting desc
                                                   for xml path(''));

                        end

                    raiserror ('Finished running investigatory queries',10,1) with nowait;


                    /* End of checks. If we haven't waited @Seconds seconds, wait. */
                    if DATEADD(second, 1, SYSDATETIMEOFFSET()) < @finishsampletime
                        begin
                            raiserror ('Waiting to match @Seconds parameter',10,1) with nowait;
                            waitfor time @finishsampletimewaitfor;
                        end;

                    raiserror ('Capturing second pass of wait stats, perfmon counters, file stats',10,1) with nowait;
                    /* Populate #FileStats, #PerfmonStats, #WaitStats with DMV data. In a second, we'll compare these. */
                    insert #waitstats(pass, sampletime, wait_type, wait_time_ms, signal_wait_time_ms,
                                      waiting_tasks_count)
                    select x.pass,
                           x.sampletime,
                           x.wait_type,
                           SUM(x.sum_wait_time_ms)        as sum_wait_time_ms,
                           SUM(x.sum_signal_wait_time_ms) as sum_signal_wait_time_ms,
                           SUM(x.sum_waiting_tasks)       as sum_waiting_tasks
                    from (
                             select 2                                                              as pass,
                                    SYSDATETIMEOFFSET()                                            as sampletime,
                                    owt.wait_type,
                                    SUM(owt.wait_duration_ms) over (partition by owt.wait_type, owt.session_id)
                                        - case when @seconds = 0 then 0 else (@seconds * 1000) end as sum_wait_time_ms,
                                    0                                                              as sum_signal_wait_time_ms,
                                    case @seconds when 0 then 0 else 1 end                         as sum_waiting_tasks
                             from sys.dm_os_waiting_tasks owt
                             where owt.session_id > 50
                               and owt.wait_duration_ms >= case @seconds when 0 then 0 else @seconds * 1000 end
                             union all
                             select 2                                                             as pass,
                                    SYSDATETIMEOFFSET()                                           as sampletime,
                                    os.wait_type,
                                    SUM(os.wait_time_ms) over (partition by os.wait_type)         as sum_wait_time_ms,
                                    SUM(os.signal_wait_time_ms) over (partition by os.wait_type ) as sum_signal_wait_time_ms,
                                    SUM(os.waiting_tasks_count) over (partition by os.wait_type)  as sum_waiting_tasks
                             from sys.dm_os_wait_stats os
                         ) x
                    where EXISTS
                              (
                                  select 1 / 0
                                  from ##waitcategories as wc
                                  where wc.waittype = x.wait_type
                                    and wc.ignorable = 0
                              )
                    group by x.pass, x.sampletime, x.wait_type
                    order by sum_wait_time_ms desc;

                    insert into #filestats (pass, sampletime, databaseid, fileid, databasename, filelogicalname,
                                            sizeondiskmb, io_stall_read_ms,
                                            num_of_reads, [bytes_read], io_stall_write_ms, num_of_writes,
                                            [bytes_written], physicalname, typedesc, avg_stall_read_ms,
                                            avg_stall_write_ms)
                    select 2                                                                          as pass,
                           SYSDATETIMEOFFSET()                                                        as sampletime,
                           mf.[database_id],
                           mf.[file_id],
                           DB_NAME(vfs.database_id)                                                   as [db_name],
                           mf.name + N' [' + mf.type_desc collate sql_latin1_general_cp1_ci_as +
                           N']'                                                                       as file_logical_name,
                           CAST(((vfs.size_on_disk_bytes / 1024.0) / 1024.0) as int)                  as size_on_disk_mb,
                           vfs.io_stall_read_ms,
                           vfs.num_of_reads,
                           vfs.[num_of_bytes_read],
                           vfs.io_stall_write_ms,
                           vfs.num_of_writes,
                           vfs.[num_of_bytes_written],
                           mf.physical_name,
                           mf.type_desc,
                           0,
                           0
                    from sys.dm_io_virtual_file_stats(null, null) as vfs
                             inner join #masterfiles as mf on vfs.file_id = mf.file_id
                        and vfs.database_id = mf.database_id
                    where vfs.num_of_reads > 0
                       or vfs.num_of_writes > 0;

                    insert into #perfmonstats (pass, sampletime, [object_name], [counter_name], [instance_name],
                                               [cntr_value], [cntr_type])
                    select 2                   as pass,
                           SYSDATETIMEOFFSET() as sampletime,
                           RTRIM(dmv.object_name),
                           RTRIM(dmv.counter_name),
                           RTRIM(dmv.instance_name),
                           dmv.cntr_value,
                           dmv.cntr_type
                    from #perfmoncounters counters
                             inner join sys.dm_os_performance_counters dmv
                                        on counters.counter_name collate sql_latin1_general_cp1_ci_as =
                                           RTRIM(dmv.counter_name) collate sql_latin1_general_cp1_ci_as
                                            and counters.[object_name] collate sql_latin1_general_cp1_ci_as =
                                                RTRIM(dmv.[object_name]) collate sql_latin1_general_cp1_ci_as
                                            and (counters.[instance_name] is null or
                                                 counters.[instance_name] collate sql_latin1_general_cp1_ci_as =
                                                 RTRIM(dmv.[instance_name]) collate sql_latin1_general_cp1_ci_as);

                    /* Set the latencies and averages. We could do this with a CTE, but we're not ambitious today. */
                    update fnow
                    set avg_stall_read_ms = ((fnow.io_stall_read_ms - fbase.io_stall_read_ms) /
                                             (fnow.num_of_reads - fbase.num_of_reads))
                    from #filestats fnow
                             inner join #filestats fbase
                                        on fnow.databaseid = fbase.databaseid and fnow.fileid = fbase.fileid and
                                           fnow.sampletime > fbase.sampletime and
                                           fnow.num_of_reads > fbase.num_of_reads and
                                           fnow.io_stall_read_ms > fbase.io_stall_read_ms
                    where (fnow.num_of_reads - fbase.num_of_reads) > 0;

                    update fnow
                    set avg_stall_write_ms = ((fnow.io_stall_write_ms - fbase.io_stall_write_ms) /
                                              (fnow.num_of_writes - fbase.num_of_writes))
                    from #filestats fnow
                             inner join #filestats fbase
                                        on fnow.databaseid = fbase.databaseid and fnow.fileid = fbase.fileid and
                                           fnow.sampletime > fbase.sampletime and
                                           fnow.num_of_writes > fbase.num_of_writes and
                                           fnow.io_stall_write_ms > fbase.io_stall_write_ms
                    where (fnow.num_of_writes - fbase.num_of_writes) > 0;

                    update pnow
                    set [value_delta]      = pnow.cntr_value - pfirst.cntr_value,
                        [value_per_second] = ((1.0 * pnow.cntr_value - pfirst.cntr_value) /
                                              DATEDIFF(ss, pfirst.sampletime, pnow.sampletime))
                    from #perfmonstats pnow
                             inner join #perfmonstats pfirst on pfirst.[object_name] = pnow.[object_name] and
                                                                pfirst.counter_name = pnow.counter_name and
                                                                (pfirst.instance_name = pnow.instance_name or
                                                                 (pfirst.instance_name is null and pnow.instance_name is null))
                        and pnow.id > pfirst.id
                    where DATEDIFF(ss, pfirst.sampletime, pnow.sampletime) > 0;


                    /* If we're within 10 seconds of our projected finish time, do the plan cache analysis. */
                    if DATEDIFF(ss, @finishsampletime, SYSDATETIMEOFFSET()) > 10 and @checkprocedurecache = 1
                        begin

                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details)
                            values (18, 210, 'Query Stats', 'Plan Cache Analysis Skipped',
                                    'http://www.BrentOzar.com/go/topqueries',
                                    'Due to excessive load, the plan cache analysis was skipped. To override this, use @ExpertMode = 1.');

                        end;
                    else
                        if @checkprocedurecache = 1
                            begin


                                raiserror ('@CheckProcedureCache = 1, capturing second pass of plan cache',10,1) with nowait;

                                /* Populate #QueryStats. SQL 2005 doesn't have query hash or query plan hash. */
                                if @@VERSION like 'Microsoft SQL Server 2005%'
                                    begin
                                        if @filterplansbydatabase is null
                                            begin
                                                set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= @StartSampleTimeText;';
                                            end;
                                        else
                                            begin
                                                set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
												CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
												INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= @StartSampleTimeText
												AND attr.attribute = ''dbid'';';
                                            end;
                                    end;
                                else
                                    begin
                                        if @filterplansbydatabase is null
                                            begin
                                                set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											WHERE qs.last_execution_time >= @StartSampleTimeText';
                                            end;
                                        else
                                            begin
                                                set @stringtoexecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
											SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
											FROM sys.dm_exec_query_stats qs
											CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS attr
											INNER JOIN #FilterPlansByDatabase dbs ON CAST(attr.value AS INT) = dbs.DatabaseID
											WHERE qs.last_execution_time >= @StartSampleTimeText
												AND attr.attribute = ''dbid'';';
                                            end;
                                    end;
                                /* Old version pre-2016/06/13:
        IF @@VERSION LIKE 'Microsoft SQL Server 2005%'
            SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
                                        SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, NULL AS query_hash, NULL AS query_plan_hash, 0
                                        FROM sys.dm_exec_query_stats qs
                                        WHERE qs.last_execution_time >= @StartSampleTimeText;';
        ELSE
            SET @StringToExecute = N'INSERT INTO #QueryStats ([sql_handle], Pass, SampleTime, statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, Points)
                                        SELECT [sql_handle], 2 AS Pass, SYSDATETIMEOFFSET(), statement_start_offset, statement_end_offset, plan_generation_num, plan_handle, execution_count, total_worker_time, total_physical_reads, total_logical_writes, total_logical_reads, total_clr_time, total_elapsed_time, creation_time, query_hash, query_plan_hash, 0
                                        FROM sys.dm_exec_query_stats qs
                                        WHERE qs.last_execution_time >= @StartSampleTimeText;';
		*/
                                set @parmdefinitions = N'@StartSampleTimeText NVARCHAR(100)';
                                set @parm1 = CONVERT(nvarchar(100), CAST(@startsampletime as datetime), 127);

                                execute sp_executesql @stringtoexecute, @parmdefinitions, @startsampletimetext = @parm1;

                                raiserror ('@CheckProcedureCache = 1, totaling up plan cache metrics',10,1) with nowait;

                                /* Get the totals for the entire plan cache */
                                insert into #querystats (pass, sampletime, execution_count, total_worker_time,
                                                         total_physical_reads, total_logical_writes,
                                                         total_logical_reads, total_clr_time, total_elapsed_time,
                                                         creation_time)
                                select 0 as pass,
                                       SYSDATETIMEOFFSET(),
                                       SUM(execution_count),
                                       SUM(total_worker_time),
                                       SUM(total_physical_reads),
                                       SUM(total_logical_writes),
                                       SUM(total_logical_reads),
                                       SUM(total_clr_time),
                                       SUM(total_elapsed_time),
                                       MIN(creation_time)
                                from sys.dm_exec_query_stats qs;


                                raiserror ('@CheckProcedureCache = 1, so analyzing execution plans',10,1) with nowait;
                                /*
        Pick the most resource-intensive queries to review. Update the Points field
        in #QueryStats - if a query is in the top 10 for logical reads, CPU time,
        duration, or execution, add 1 to its points.
        */
                                with qstop as (
                                    select top 10 qsnow.id
                                    from #querystats qsnow
                                             inner join #querystats qsfirst
                                                        on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                           qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                           qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                           qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                           qsnow.plan_handle = qsfirst.plan_handle and qsfirst.pass = 1
                                    where qsnow.total_elapsed_time > qsfirst.total_elapsed_time
                                      and qsnow.pass = 2
                                      and qsnow.total_elapsed_time - qsfirst.total_elapsed_time > 1000000 /* Only queries with over 1 second of runtime */
                                    order by (qsnow.total_elapsed_time - COALESCE(qsfirst.total_elapsed_time, 0)) desc)
                                update #querystats
                                set points = points + 1
                                from #querystats qs
                                         inner join qstop on qs.id = qstop.id;

                                with qstop as (
                                    select top 10 qsnow.id
                                    from #querystats qsnow
                                             inner join #querystats qsfirst
                                                        on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                           qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                           qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                           qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                           qsnow.plan_handle = qsfirst.plan_handle and qsfirst.pass = 1
                                    where qsnow.total_logical_reads > qsfirst.total_logical_reads
                                      and qsnow.pass = 2
                                      and qsnow.total_logical_reads - qsfirst.total_logical_reads > 1000 /* Only queries with over 1000 reads */
                                    order by (qsnow.total_logical_reads - COALESCE(qsfirst.total_logical_reads, 0)) desc)
                                update #querystats
                                set points = points + 1
                                from #querystats qs
                                         inner join qstop on qs.id = qstop.id;

                                with qstop as (
                                    select top 10 qsnow.id
                                    from #querystats qsnow
                                             inner join #querystats qsfirst
                                                        on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                           qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                           qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                           qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                           qsnow.plan_handle = qsfirst.plan_handle and qsfirst.pass = 1
                                    where qsnow.total_worker_time > qsfirst.total_worker_time
                                      and qsnow.pass = 2
                                      and qsnow.total_worker_time - qsfirst.total_worker_time > 1000000 /* Only queries with over 1 second of worker time */
                                    order by (qsnow.total_worker_time - COALESCE(qsfirst.total_worker_time, 0)) desc)
                                update #querystats
                                set points = points + 1
                                from #querystats qs
                                         inner join qstop on qs.id = qstop.id;

                                with qstop as (
                                    select top 10 qsnow.id
                                    from #querystats qsnow
                                             inner join #querystats qsfirst
                                                        on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                           qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                           qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                           qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                           qsnow.plan_handle = qsfirst.plan_handle and qsfirst.pass = 1
                                    where qsnow.execution_count > qsfirst.execution_count
                                      and qsnow.pass = 2
                                      and (qsnow.total_elapsed_time - qsfirst.total_elapsed_time > 1000000 /* Only queries with over 1 second of runtime */
                                        or
                                           qsnow.total_logical_reads - qsfirst.total_logical_reads > 1000 /* Only queries with over 1000 reads */
                                        or
                                           qsnow.total_worker_time - qsfirst.total_worker_time > 1000000 /* Only queries with over 1 second of worker time */)
                                    order by (qsnow.execution_count - COALESCE(qsfirst.execution_count, 0)) desc)
                                update #querystats
                                set points = points + 1
                                from #querystats qs
                                         inner join qstop on qs.id = qstop.id;

                                /* Query Stats - CheckID 17 - Most Resource-Intensive Queries */
                                insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                                howtostopit, queryplan, querytext, querystatsnowid,
                                                                querystatsfirstid, planhandle, queryhash)
                                select          17,
                                                210,
                                                'Query Stats',
                                                'Most Resource-Intensive Queries',
                                                'http://www.BrentOzar.com/go/topqueries',
                                                'Query stats during the sample:' + @linefeed +
                                                'Executions: ' + CAST(
                                                        qsnow.execution_count - (COALESCE(qsfirst.execution_count, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                'Elapsed Time: ' + CAST(
                                                        qsnow.total_elapsed_time - (COALESCE(qsfirst.total_elapsed_time, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                'CPU Time: ' + CAST(
                                                        qsnow.total_worker_time - (COALESCE(qsfirst.total_worker_time, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                'Logical Reads: ' + CAST(qsnow.total_logical_reads -
                                                                         (COALESCE(qsfirst.total_logical_reads, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                'Logical Writes: ' + CAST(qsnow.total_logical_writes -
                                                                          (COALESCE(qsfirst.total_logical_writes, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                'CLR Time: ' + CAST(
                                                        qsnow.total_clr_time - (COALESCE(qsfirst.total_clr_time, 0)) as nvarchar(100)) +
                                                @linefeed +
                                                @linefeed + @linefeed + 'Query stats since ' +
                                                CONVERT(nvarchar(100), qsnow.creation_time, 121) + @linefeed +
                                                'Executions: ' + CAST(qsnow.execution_count as nvarchar(100)) +
                                                case qstotal.execution_count
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.execution_count / qstotal.execution_count as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                'Elapsed Time: ' + CAST(qsnow.total_elapsed_time as nvarchar(100)) +
                                                case qstotal.total_elapsed_time
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.total_elapsed_time / qstotal.total_elapsed_time as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                'CPU Time: ' + CAST(qsnow.total_worker_time as nvarchar(100)) +
                                                case qstotal.total_worker_time
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.total_worker_time / qstotal.total_worker_time as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                'Logical Reads: ' + CAST(qsnow.total_logical_reads as nvarchar(100)) +
                                                case qstotal.total_logical_reads
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.total_logical_reads / qstotal.total_logical_reads as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                'Logical Writes: ' + CAST(qsnow.total_logical_writes as nvarchar(100)) +
                                                case qstotal.total_logical_writes
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.total_logical_writes / qstotal.total_logical_writes as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                'CLR Time: ' + CAST(qsnow.total_clr_time as nvarchar(100)) +
                                                case qstotal.total_clr_time
                                                    when 0 then ''
                                                    else (' - Percent of Server Total: ' + CAST(CAST(
                                                            100.0 * qsnow.total_clr_time / qstotal.total_clr_time as decimal(6, 2)) as nvarchar(100)) +
                                                          '%') end + @linefeed +
                                                    --@LineFeed + @LineFeed + 'Query hash: ' + CAST(qsNow.query_hash AS NVARCHAR(100)) + @LineFeed +
                                                    --@LineFeed + @LineFeed + 'Query plan hash: ' + CAST(qsNow.query_plan_hash AS NVARCHAR(100)) +
                                                @linefeed                                                                   as details,
                                                'See the URL for tuning tips on why this query may be consuming resources.' as howtostopit,
                                                qp.query_plan,
                                    querytext = SUBSTRING(st.text,
                                                          (qsnow.statement_start_offset / 2) + 1,
                                                          ((case qsnow.statement_end_offset
                                                                when -1 then DATALENGTH(st.text)
                                                                else qsnow.statement_end_offset
                                                                end - qsnow.statement_start_offset) / 2) + 1),
                                                qsnow.id                                                                    as querystatsnowid,
                                                qsfirst.id                                                                  as querystatsfirstid,
                                                qsnow.plan_handle                                                           as planhandle,
                                                qsnow.query_hash
                                from #querystats qsnow
                                         inner join #querystats qstotal on qstotal.pass = 0
                                         left outer join #querystats qsfirst
                                                         on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                            qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                            qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                            qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                            qsnow.plan_handle = qsfirst.plan_handle and qsfirst.pass = 1
                                         cross apply sys.dm_exec_sql_text(qsnow.sql_handle) as st
                                         cross apply sys.dm_exec_query_plan(qsnow.plan_handle) as qp
                                where qsnow.points > 0
                                  and st.text is not null
                                  and qp.query_plan is not null;

                                update #blitzfirstresults
                                set databaseid   = CAST(attr.value as int),
                                    databasename = DB_NAME(CAST(attr.value as int))
                                from #blitzfirstresults
                                         cross apply sys.dm_exec_plan_attributes(#blitzfirstresults.planhandle) as attr
                                where attr.attribute = 'dbid';


                            end; /* IF DATEDIFF(ss, @FinishSampleTime, SYSDATETIMEOFFSET()) > 10 AND @CheckProcedureCache = 1 */


                    raiserror ('Analyzing changes between first and second passes of DMVs',10,1) with nowait;

                    /* Wait Stats - CheckID 6 */
                    /* Compare the current wait stats to the sample we took at the start, and insert the top 10 waits. */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                    howtostopit, detailsint)
                    select top 10 6                                                                                 as checkid,
                                  200                                                                               as priority,
                                  'Wait Stats'                                                                      as findinggroup,
                                  wnow.wait_type                                                                    as finding, /* IF YOU CHANGE THIS, STUFF WILL BREAK. Other checks look for wait type names in the Finding field. See checks 11, 12 as example. */
                                  N'https://www.sqlskills.com/help/waits/' + LOWER(wnow.wait_type) + '/'            as url,
                                  'For ' + CAST(
                                          ((wnow.wait_time_ms - COALESCE(wbase.wait_time_ms, 0)) / 1000) as nvarchar(100)) +
                                  ' seconds over the last ' + case @seconds
                                                                  when 0 then (CAST(
                                                                                       DATEDIFF(dd, @startsampletime, @finishsampletime) as nvarchar(10)) +
                                                                               ' days')
                                                                  else (CAST(@seconds as nvarchar(10)) + ' seconds') end +
                                  ', SQL Server was waiting on this particular bottleneck.' + @linefeed +
                                  @linefeed                                                                         as details,
                                  'See the URL for more details on how to mitigate this wait type.'                 as howtostopit,
                                  ((wnow.wait_time_ms - COALESCE(wbase.wait_time_ms, 0)) / 1000)                    as detailsint
                    from #waitstats wnow
                             left outer join #waitstats wbase
                                             on wnow.wait_type = wbase.wait_type and wnow.sampletime > wbase.sampletime
                    where wnow.wait_time_ms > (wbase.wait_time_ms +
                                               (.5 * (DATEDIFF(ss, @startsampletime, @finishsampletime)) * 1000)) /* Only look for things we've actually waited on for half of the time or more */
                    order by (wnow.wait_time_ms - COALESCE(wbase.wait_time_ms, 0)) desc;

                    /* Server Performance - Poison Wait Detected - CheckID 30 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                    howtostopit, detailsint)
                    select 30                                                                                as checkid,
                           10                                                                                as priority,
                           'Server Performance'                                                              as findinggroup,
                           'Poison Wait Detected: ' + wnow.wait_type                                         as finding,
                           N'http://www.brentozar.com/go/poison/#' + wnow.wait_type                          as url,
                           'For ' +
                           CAST(((wnow.wait_time_ms - COALESCE(wbase.wait_time_ms, 0)) / 1000) as nvarchar(100)) +
                           ' seconds over the last ' + case @seconds
                                                           when 0 then (CAST(
                                                                                DATEDIFF(dd, @startsampletime, @finishsampletime) as nvarchar(10)) +
                                                                        ' days')
                                                           else (CAST(@seconds as nvarchar(10)) + ' seconds') end +
                           ', SQL Server was waiting on this particular bottleneck.' + @linefeed + @linefeed as details,
                           'See the URL for more details on how to mitigate this wait type.'                 as howtostopit,
                           ((wnow.wait_time_ms - COALESCE(wbase.wait_time_ms, 0)) / 1000)                    as detailsint
                    from #waitstats wnow
                             left outer join #waitstats wbase
                                             on wnow.wait_type = wbase.wait_type and wnow.sampletime > wbase.sampletime
                    where wnow.wait_type in
                          ('IO_QUEUE_LIMIT', 'IO_RETRY', 'LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR',
                           'PREEMPTIVE_DEBUG', 'RESMGR_THROTTLED', 'RESOURCE_SEMAPHORE',
                           'RESOURCE_SEMAPHORE_QUERY_COMPILE', 'SE_REPL_CATCHUP_THROTTLE', 'SE_REPL_COMMIT_ACK',
                           'SE_REPL_COMMIT_TURN', 'SE_REPL_ROLLBACK_ACK', 'SE_REPL_SLOW_SECONDARY_THROTTLE',
                           'THREADPOOL')
                      and wnow.wait_time_ms > (wbase.wait_time_ms + 1000);


                    /* Server Performance - Slow Data File Reads - CheckID 11 */
                    if EXISTS(select * from #blitzfirstresults where finding like 'PAGEIOLATCH%')
                        begin
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                            howtostopit, databaseid, databasename)
                            select top 10 11                                                                as checkid,
                                          50                                                                as priority,
                                          'Server Performance'                                              as findinggroup,
                                          'Slow Data File Reads'                                            as finding,
                                          'http://www.BrentOzar.com/go/slow/'                               as url,
                                          'Your server is experiencing PAGEIOLATCH% waits due to slow data file reads. This file is one of the reasons why.' +
                                          @linefeed
                                              + 'File: ' + fnow.physicalname + @linefeed
                                              + 'Number of reads during the sample: ' +
                                          CAST((fnow.num_of_reads - fbase.num_of_reads) as nvarchar(20)) + @linefeed
                                              + 'Seconds spent waiting on storage for these reads: ' + CAST(
                                                  ((fnow.io_stall_read_ms - fbase.io_stall_read_ms) / 1000.0) as nvarchar(20)) +
                                          @linefeed
                                              + 'Average read latency during the sample: ' + CAST(
                                                  ((fnow.io_stall_read_ms - fbase.io_stall_read_ms) /
                                                   (fnow.num_of_reads - fbase.num_of_reads)) as nvarchar(20)) +
                                          ' milliseconds' + @linefeed
                                              + 'Microsoft guidance for data file read speed: 20ms or less.' +
                                          @linefeed + @linefeed                                             as details,
                                          'See the URL for more details on how to mitigate this wait type.' as howtostopit,
                                          fnow.databaseid,
                                          fnow.databasename
                            from #filestats fnow
                                     inner join #filestats fbase
                                                on fnow.databaseid = fbase.databaseid and fnow.fileid = fbase.fileid and
                                                   fnow.sampletime > fbase.sampletime and
                                                   fnow.num_of_reads > fbase.num_of_reads and
                                                   fnow.io_stall_read_ms > (fbase.io_stall_read_ms + 1000)
                            where (fnow.io_stall_read_ms - fbase.io_stall_read_ms) /
                                  (fnow.num_of_reads - fbase.num_of_reads) >= @filelatencythresholdms
                              and fnow.typedesc = 'ROWS'
                            order by (fnow.io_stall_read_ms - fbase.io_stall_read_ms) /
                                     (fnow.num_of_reads - fbase.num_of_reads) desc;
                        end;

                    /* Server Performance - Slow Log File Writes - CheckID 12 */
                    if EXISTS(select * from #blitzfirstresults where finding like 'WRITELOG%')
                        begin
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details,
                                                            howtostopit, databaseid, databasename)
                            select top 10 12                                                                as checkid,
                                          50                                                                as priority,
                                          'Server Performance'                                              as findinggroup,
                                          'Slow Log File Writes'                                            as finding,
                                          'http://www.BrentOzar.com/go/slow/'                               as url,
                                          'Your server is experiencing WRITELOG waits due to slow log file writes. This file is one of the reasons why.' +
                                          @linefeed
                                              + 'File: ' + fnow.physicalname + @linefeed
                                              + 'Number of writes during the sample: ' +
                                          CAST((fnow.num_of_writes - fbase.num_of_writes) as nvarchar(20)) + @linefeed
                                              + 'Seconds spent waiting on storage for these writes: ' + CAST(
                                                  ((fnow.io_stall_write_ms - fbase.io_stall_write_ms) / 1000.0) as nvarchar(20)) +
                                          @linefeed
                                              + 'Average write latency during the sample: ' + CAST(
                                                  ((fnow.io_stall_write_ms - fbase.io_stall_write_ms) /
                                                   (fnow.num_of_writes - fbase.num_of_writes)) as nvarchar(20)) +
                                          ' milliseconds' + @linefeed
                                              + 'Microsoft guidance for log file write speed: 3ms or less.' +
                                          @linefeed + @linefeed                                             as details,
                                          'See the URL for more details on how to mitigate this wait type.' as howtostopit,
                                          fnow.databaseid,
                                          fnow.databasename
                            from #filestats fnow
                                     inner join #filestats fbase
                                                on fnow.databaseid = fbase.databaseid and fnow.fileid = fbase.fileid and
                                                   fnow.sampletime > fbase.sampletime and
                                                   fnow.num_of_writes > fbase.num_of_writes and
                                                   fnow.io_stall_write_ms > (fbase.io_stall_write_ms + 1000)
                            where (fnow.io_stall_write_ms - fbase.io_stall_write_ms) /
                                  (fnow.num_of_writes - fbase.num_of_writes) >= @filelatencythresholdms
                              and fnow.typedesc = 'LOG'
                            order by (fnow.io_stall_write_ms - fbase.io_stall_write_ms) /
                                     (fnow.num_of_writes - fbase.num_of_writes) desc;
                        end;


                    /* SQL Server Internal Maintenance - Log File Growing - CheckID 13 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 13                                                                                                                                       as checkid,
                           1                                                                                                                                        as priority,
                           'SQL Server Internal Maintenance'                                                                                                        as findinggroup,
                           'Log File Growing'                                                                                                                       as finding,
                           'http://www.BrentOzar.com/askbrent/file-growing/'                                                                                        as url,
                           'Number of growths during the sample: ' + CAST(ps.value_delta as nvarchar(20)) + @linefeed
                               + 'Determined by sampling Perfmon counter ' + ps.object_name + ' - ' + ps.counter_name +
                           @linefeed                                                                                                                                as details,
                           'Pre-grow data and log files during maintenance windows so that they do not grow during production loads. See the URL for more details.' as howtostopit
                    from #perfmonstats ps
                    where ps.pass = 2
                      and object_name = @servicename + ':Databases'
                      and counter_name = 'Log Growths'
                      and value_delta > 0;


                    /* SQL Server Internal Maintenance - Log File Shrinking - CheckID 14 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 14                                                                                                                                       as checkid,
                           1                                                                                                                                        as priority,
                           'SQL Server Internal Maintenance'                                                                                                        as findinggroup,
                           'Log File Shrinking'                                                                                                                     as finding,
                           'http://www.BrentOzar.com/askbrent/file-shrinking/'                                                                                      as url,
                           'Number of shrinks during the sample: ' + CAST(ps.value_delta as nvarchar(20)) + @linefeed
                               + 'Determined by sampling Perfmon counter ' + ps.object_name + ' - ' + ps.counter_name +
                           @linefeed                                                                                                                                as details,
                           'Pre-grow data and log files during maintenance windows so that they do not grow during production loads. See the URL for more details.' as howtostopit
                    from #perfmonstats ps
                    where ps.pass = 2
                      and object_name = @servicename + ':Databases'
                      and counter_name = 'Log Shrinks'
                      and value_delta > 0;

                    /* Query Problems - Compilations/Sec High - CheckID 15 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 15                                                                                                                                   as checkid,
                           50                                                                                                                                   as priority,
                           'Query Problems'                                                                                                                     as findinggroup,
                           'Compilations/Sec High'                                                                                                              as finding,
                           'http://www.BrentOzar.com/askbrent/compilations/'                                                                                    as url,
                           'Number of batch requests during the sample: ' + CAST(ps.value_delta as nvarchar(20)) +
                           @linefeed
                               + 'Number of compilations during the sample: ' +
                           CAST(pscomp.value_delta as nvarchar(20)) + @linefeed
                               +
                           'For OLTP environments, Microsoft recommends that 90% of batch requests should hit the plan cache, and not be compiled from scratch. We are exceeding that threshold.' +
                           @linefeed                                                                                                                            as details,
                           'To find the queries that are compiling, start with:' + @linefeed
                               + 'sp_BlitzCache @SortOrder = ''recent compilations''' + @linefeed
                               +
                           'If dynamic SQL or non-parameterized strings are involved, consider enabling Forced Parameterization. See the URL for more details.' as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp
                                        on pscomp.pass = 2 and pscomp.object_name = @servicename + ':SQL Statistics' and
                                           pscomp.counter_name = 'SQL Compilations/sec' and pscomp.value_delta > 0
                    where ps.pass = 2
                      and ps.object_name = @servicename + ':SQL Statistics'
                      and ps.counter_name = 'Batch Requests/sec'
                      and ps.value_delta > (1000 * @seconds) /* Ignore servers sitting idle */
                      and (pscomp.value_delta * 10) > ps.value_delta;
                    /* Compilations are more than 10% of batch requests per second */

                    /* Query Problems - Re-Compilations/Sec High - CheckID 16 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 16                                                                                                                                              as checkid,
                           50                                                                                                                                              as priority,
                           'Query Problems'                                                                                                                                as findinggroup,
                           'Re-Compilations/Sec High'                                                                                                                      as finding,
                           'http://www.BrentOzar.com/askbrent/recompilations/'                                                                                             as url,
                           'Number of batch requests during the sample: ' + CAST(ps.value_delta as nvarchar(20)) +
                           @linefeed
                               + 'Number of recompilations during the sample: ' +
                           CAST(pscomp.value_delta as nvarchar(20)) + @linefeed
                               +
                           'More than 10% of our queries are being recompiled. This is typically due to statistics changing on objects.' +
                           @linefeed                                                                                                                                       as details,
                           'To find the queries that are being forced to recompile, start with:' + @linefeed
                               + 'sp_BlitzCache @SortOrder = ''recent compilations''' + @linefeed
                               +
                           'Examine those plans to find out which objects are changing so quickly that they hit the stats update threshold. See the URL for more details.' as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp
                                        on pscomp.pass = 2 and pscomp.object_name = @servicename + ':SQL Statistics' and
                                           pscomp.counter_name = 'SQL Re-Compilations/sec' and pscomp.value_delta > 0
                    where ps.pass = 2
                      and ps.object_name = @servicename + ':SQL Statistics'
                      and ps.counter_name = 'Batch Requests/sec'
                      and ps.value_delta > (1000 * @seconds) /* Ignore servers sitting idle */
                      and (pscomp.value_delta * 10) > ps.value_delta;
                    /* Recompilations are more than 10% of batch requests per second */

                    /* Table Problems - Forwarded Fetches/Sec High - CheckID 29 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 29                                                                                                                                                                           as checkid,
                           40                                                                                                                                                                           as priority,
                           'Table Problems'                                                                                                                                                             as findinggroup,
                           'Forwarded Fetches/Sec High'                                                                                                                                                 as finding,
                           'https://BrentOzar.com/go/fetch/'                                                                                                                                            as url,
                           CAST(ps.value_delta as nvarchar(20)) +
                           ' Forwarded Records (from SQLServer:Access Methods counter)' + @linefeed
                               + 'Check your heaps: they need to be rebuilt, or they need a clustered index applied.' +
                           @linefeed                                                                                                                                                                    as details,
                           'Rebuild your heaps. If you use Ola Hallengren maintenance scripts, those do not rebuild heaps by default: https://www.brentozar.com/archive/2016/07/fix-forwarded-records/' as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp
                                        on pscomp.pass = 2 and pscomp.object_name = @servicename + ':Access Methods' and
                                           pscomp.counter_name = 'Forwarded Records/sec' and pscomp.value_delta > 100
                    where ps.pass = 2
                      and ps.object_name = @servicename + ':Access Methods'
                      and ps.counter_name = 'Forwarded Records/sec'
                      and ps.value_delta > (100 * @seconds);
                    /* Ignore servers sitting idle */

                    /* Check for temp objects with high forwarded fetches.
		This has to be done as dynamic SQL because we have to execute OBJECT_NAME inside TempDB. */
                    if @@ROWCOUNT > 0
                        begin
                            set @stringtoexecute = N'USE tempdb;
		INSERT INTO #BlitzFirstResults (CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt)
		SELECT TOP 10 29 AS CheckID,
			40 AS Priority,
			''Table Problems'' AS FindingGroup,
			''Forwarded Fetches/Sec High: Temp Table'' AS Finding,
			''https://BrentOzar.com/go/fetch/'' AS URL,
			CAST(COALESCE(os.forwarded_fetch_count,0) AS NVARCHAR(20)) + '' forwarded fetches on temp table '' + COALESCE(OBJECT_NAME(os.object_id), ''Unknown'') AS Details,
			''Look through your source code to find the object creating these temp tables, and tune the creation and population to reduce fetches. See the URL for details.'' AS HowToStopIt
		FROM tempdb.sys.dm_db_index_operational_stats(DB_ID(''tempdb''), NULL, NULL, NULL) os
		WHERE os.database_id = DB_ID(''tempdb'')
			AND os.forwarded_fetch_count > 100
		ORDER BY os.forwarded_fetch_count DESC;'

                            execute sp_executesql @stringtoexecute;
                        end

                    /* In-Memory OLTP - Garbage Collection in Progress - CheckID 31 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 31                                                                                                                                                                                               as checkid,
                           50                                                                                                                                                                                               as priority,
                           'In-Memory OLTP'                                                                                                                                                                                 as findinggroup,
                           'Garbage Collection in Progress'                                                                                                                                                                 as finding,
                           'https://BrentOzar.com/go/garbage/'                                                                                                                                                              as url,
                           CAST(ps.value_delta as nvarchar(50)) +
                           ' rows processed (from SQL Server YYYY XTP Garbage Collection:Rows processed/sec counter)' +
                           @linefeed
                               +
                           'This can happen due to memory pressure (causing In-Memory OLTP to shrink its footprint) or' +
                           @linefeed
                               +
                           'due to transactional workloads that constantly insert/delete data.'                                                                                                                             as details,
                           'Sadly, you cannot choose when garbage collection occurs. This is one of the many gotchas of Hekaton. Learn more: http://nedotter.com/archive/2016/04/row-version-lifecycle-for-in-memory-oltp/' as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp
                                        on pscomp.pass = 2 and pscomp.object_name like '%XTP Garbage Collection' and
                                           pscomp.counter_name = 'Rows processed/sec' and pscomp.value_delta > 100
                    where ps.pass = 2
                      and ps.object_name like '%XTP Garbage Collection'
                      and ps.counter_name = 'Rows processed/sec'
                      and ps.value_delta > (100 * @seconds);
                    /* Ignore servers sitting idle */

                    /* In-Memory OLTP - Transactions Aborted - CheckID 32 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 32                                                                                                                as checkid,
                           100                                                                                                               as priority,
                           'In-Memory OLTP'                                                                                                  as findinggroup,
                           'Transactions Aborted'                                                                                            as finding,
                           'https://BrentOzar.com/go/aborted/'                                                                               as url,
                           CAST(ps.value_delta as nvarchar(50)) +
                           ' transactions aborted (from SQL Server YYYY XTP Transactions:Transactions aborted/sec counter)' +
                           @linefeed
                               +
                           'This may indicate that data is changing, or causing folks to retry their transactions, thereby increasing load.' as details,
                           'Dig into your In-Memory OLTP transactions to figure out which ones are failing and being retried.'               as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp
                                        on pscomp.pass = 2 and pscomp.object_name like '%XTP Transactions' and
                                           pscomp.counter_name = 'Transactions aborted/sec' and pscomp.value_delta > 100
                    where ps.pass = 2
                      and ps.object_name like '%XTP Transactions'
                      and ps.counter_name = 'Transactions aborted/sec'
                      and ps.value_delta > (10 * @seconds);
                    /* Ignore servers sitting idle */

                    /* Query Problems - Suboptimal Plans/Sec High - CheckID 33 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                    select 32                                                                                                                           as checkid,
                           100                                                                                                                          as priority,
                           'Query Problems'                                                                                                             as findinggroup,
                           'Suboptimal Plans/Sec High'                                                                                                  as finding,
                           'https://BrentOzar.com/go/suboptimal/'                                                                                       as url,
                           CAST(ps.value_delta as nvarchar(50)) + ' plans reported in the ' +
                           CAST(ps.instance_name as nvarchar(100)) +
                           ' workload group (from Workload GroupStats:Suboptimal plans/sec counter)' + @linefeed
                               +
                           'Even if you are not using Resource Governor, it still tracks information about user queries, memory grants, etc.'           as details,
                           'Check out sp_BlitzCache to get more information about recent queries, or try sp_BlitzWho to see currently running queries.' as howtostopit
                    from #perfmonstats ps
                             inner join #perfmonstats pscomp on pscomp.pass = 2 and
                                                                pscomp.object_name = @servicename + ':Workload GroupStats' and
                                                                pscomp.counter_name = 'Suboptimal plans/sec' and
                                                                pscomp.value_delta > 100
                    where ps.pass = 2
                      and ps.object_name = @servicename + ':Workload GroupStats'
                      and ps.counter_name = 'Suboptimal plans/sec'
                      and ps.value_delta > (10 * @seconds);
                    /* Ignore servers sitting idle */

                    /* Azure Performance - Database is Maxed Out - CheckID 41 */
                    if SERVERPROPERTY('Edition') = 'SQL Azure'
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, howtostopit)
                        select 41                                                                                                               as checkid,
                               10                                                                                                               as priority,
                               'Azure Performance'                                                                                              as findinggroup,
                               'Database is Maxed Out'                                                                                          as finding,
                               'https://BrentOzar.com/go/maxedout'                                                                              as url,
                               N'At ' + CONVERT(nvarchar(100), s.end_time, 121) +
                               N', your database approached (or hit) your DTU limits:' + @linefeed
                                   + N'Average CPU percent: ' + CAST(avg_cpu_percent as nvarchar(50)) + @linefeed
                                   + N'Average data IO percent: ' + CAST(avg_data_io_percent as nvarchar(50)) +
                               @linefeed
                                   + N'Average log write percent: ' + CAST(avg_log_write_percent as nvarchar(50)) +
                               @linefeed
                                   + N'Max worker percent: ' + CAST(max_worker_percent as nvarchar(50)) + @linefeed
                                   + N'Max session percent: ' +
                               CAST(max_session_percent as nvarchar(50))                                                                        as details,
                               'Tune your queries or indexes with sp_BlitzCache or sp_BlitzIndex, or consider upgrading to a higher DTU level.' as howtostopit
                        from sys.dm_db_resource_stats s
                        where s.end_time >= DATEADD(mi, -5, GETDATE())
                          and (avg_cpu_percent > 90
                            or avg_data_io_percent >= 90
                            or avg_log_write_percent >= 90
                            or max_worker_percent >= 90
                            or max_session_percent >= 90);

                    /* Server Info - Batch Requests per Sec - CheckID 19 */
                    insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, detailsint)
                    select 19                                                                  as checkid,
                           250                                                                 as priority,
                           'Server Info'                                                       as findinggroup,
                           'Batch Requests per Sec'                                            as finding,
                           'http://www.BrentOzar.com/go/measure'                               as url,
                           CAST(CAST(ps.value_delta as money) /
                                (DATEDIFF(ss, ps1.sampletime, ps.sampletime)) as nvarchar(20)) as details,
                           ps.value_delta / (DATEDIFF(ss, ps1.sampletime, ps.sampletime))      as detailsint
                    from #perfmonstats ps
                             inner join #perfmonstats ps1
                                        on ps.object_name = ps1.object_name and ps.counter_name = ps1.counter_name and
                                           ps1.pass = 1
                    where ps.pass = 2
                      and ps.object_name = @servicename + ':SQL Statistics'
                      and ps.counter_name = 'Batch Requests/sec';


                    insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                    values (@servicename + ':SQL Statistics', 'SQL Compilations/sec', null);
                    insert into #perfmoncounters ([object_name], [counter_name], [instance_name])
                    values (@servicename + ':SQL Statistics', 'SQL Re-Compilations/sec', null);

                    /* Server Info - SQL Compilations/sec - CheckID 25 */
                    if @expertmode = 1
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, detailsint)
                        select 25 as                                                                                      checkid,
                               250 as                                                                                     priority,
                               'Server Info' as                                                                           findinggroup,
                               'SQL Compilations per Sec' as                                                              finding,
                               'http://www.BrentOzar.com/go/measure' as                                                   url,
                               CAST(
                                       ps.value_delta / (DATEDIFF(ss, ps1.sampletime, ps.sampletime)) as nvarchar(20)) as details,
                               ps.value_delta / (DATEDIFF(ss, ps1.sampletime, ps.sampletime)) as                          detailsint
                        from #perfmonstats ps
                                 inner join #perfmonstats ps1 on ps.object_name = ps1.object_name and
                                                                 ps.counter_name = ps1.counter_name and ps1.pass = 1
                        where ps.pass = 2
                          and ps.object_name = @servicename + ':SQL Statistics'
                          and ps.counter_name = 'SQL Compilations/sec';

                    /* Server Info - SQL Re-Compilations/sec - CheckID 26 */
                    if @expertmode = 1
                        insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, detailsint)
                        select 26 as                                                                                      checkid,
                               250 as                                                                                     priority,
                               'Server Info' as                                                                           findinggroup,
                               'SQL Re-Compilations per Sec' as                                                           finding,
                               'http://www.BrentOzar.com/go/measure' as                                                   url,
                               CAST(
                                       ps.value_delta / (DATEDIFF(ss, ps1.sampletime, ps.sampletime)) as nvarchar(20)) as details,
                               ps.value_delta / (DATEDIFF(ss, ps1.sampletime, ps.sampletime)) as                          detailsint
                        from #perfmonstats ps
                                 inner join #perfmonstats ps1 on ps.object_name = ps1.object_name and
                                                                 ps.counter_name = ps1.counter_name and ps1.pass = 1
                        where ps.pass = 2
                          and ps.object_name = @servicename + ':SQL Statistics'
                          and ps.counter_name = 'SQL Re-Compilations/sec';

                    /* Server Info - Wait Time per Core per Sec - CheckID 20 */
                    if @seconds > 0
                        begin
                            ;
                            with waits1(sampletime, waits_ms) as (select sampletime, SUM(ws1.wait_time_ms)
                                                                  from #waitstats ws1
                                                                  where ws1.pass = 1
                                                                  group by sampletime),
                                 waits2(sampletime, waits_ms) as (select sampletime, SUM(ws2.wait_time_ms)
                                                                  from #waitstats ws2
                                                                  where ws2.pass = 2
                                                                  group by sampletime),
                                 cores(cpu_count) as (select SUM(1)
                                                      from sys.dm_os_schedulers
                                                      where status = 'VISIBLE ONLINE' and is_online = 1)
                            insert
                            into #blitzfirstresults (checkid, priority, findingsgroup, finding, url, details, detailsint)
                            select 20                                                                       as checkid,
                                   250                                                                      as priority,
                                   'Server Info'                                                            as findinggroup,
                                   'Wait Time per Core per Sec'                                             as finding,
                                   'http://www.BrentOzar.com/go/measure'                                    as url,
                                   CAST((CAST(waits2.waits_ms - waits1.waits_ms as money)) / 1000 / i.cpu_count /
                                        DATEDIFF(ss, waits1.sampletime, waits2.sampletime) as nvarchar(20)) as details,
                                   (waits2.waits_ms - waits1.waits_ms) / 1000 / i.cpu_count /
                                   DATEDIFF(ss, waits1.sampletime, waits2.sampletime)                       as detailsint
                            from cores i
                                     cross join waits1
                                     cross join waits2;
                        end;

                    /* Server Performance - High CPU Utilization CheckID 24 */
                    if @seconds >= 30
                        begin
                            /* If we're waiting 30+ seconds, run this check at the end.
           We get this data from the ring buffers, and it's only updated once per minute, so might
           as well get it now - whereas if we're checking 30+ seconds, it might get updated by the
           end of our sp_BlitzFirst session. */
                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                            select 24,
                                   50,
                                   'Server Performance',
                                   'High CPU Utilization',
                                   CAST(100 - systemidle as nvarchar(20)) + N'%. Ring buffer details: ' +
                                   CAST(record as nvarchar(4000)),
                                   100 - systemidle,
                                   'http://www.BrentOzar.com/go/cpu'
                            from (
                                     select record,
                                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                                                         'int') as systemidle
                                     from (
                                              select top 1 CONVERT(xml, record) as record
                                              from sys.dm_os_ring_buffers
                                              where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                and record like '%<SystemHealth>%'
                                              order by timestamp desc) as rb
                                 ) as y
                            where 100 - systemidle >= 50;

                            insert into #blitzfirstresults (checkid, priority, findingsgroup, finding, details, detailsint, url)
                            select 23,
                                   250,
                                   'Server Info',
                                   'CPU Utilization',
                                   CAST(100 - systemidle as nvarchar(20)) + N'%. Ring buffer details: ' +
                                   CAST(record as nvarchar(4000)),
                                   100 - systemidle,
                                   'http://www.BrentOzar.com/go/cpu'
                            from (
                                     select record,
                                            record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                                                         'int') as systemidle
                                     from (
                                              select top 1 CONVERT(xml, record) as record
                                              from sys.dm_os_ring_buffers
                                              where ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
                                                and record like '%<SystemHealth>%'
                                              order by timestamp desc) as rb
                                 ) as y;

                        end;
                    /* IF @Seconds >= 30 */


                    /* If we didn't find anything, apologize. */
                    if not EXISTS(select * from #blitzfirstresults where priority < 250)
                        begin

                            insert into #blitzfirstresults
                            (checkid,
                             priority,
                             findingsgroup,
                             finding,
                             url,
                             details)
                            values (-1,
                                    1,
                                    'No Problems Found',
                                    'From Your Community Volunteers',
                                    'http://FirstResponderKit.org/',
                                    'Try running our more in-depth checks with sp_Blitz, or there may not be an unusual SQL Server performance problem. ');

                        end;
                    /*IF NOT EXISTS (SELECT * FROM #BlitzFirstResults) */

                    /* Add credits for the nice folks who put so much time into building and maintaining this for free: */
                    insert into #blitzfirstresults
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
                            'http://FirstResponderKit.org/',
                            'To get help or add your own contributions, join us at http://FirstResponderKit.org.');

                    insert into #blitzfirstresults
                    (checkid,
                     priority,
                     findingsgroup,
                     finding,
                     url,
                     details)
                    values (-1,
                            0,
                            'sp_BlitzFirst ' + CAST(CONVERT(datetimeoffset, @versiondate, 102) as varchar(100)),
                            'From Your Community Volunteers',
                            'http://FirstResponderKit.org/',
                            'We hope you found this tool useful.');

                    /* Outdated sp_BlitzFirst - sp_BlitzFirst is Over 6 Months Old */
                    if DATEDIFF(mm, @versiondate, SYSDATETIMEOFFSET()) > 6
                        begin
                            insert into #blitzfirstresults
                            (checkid,
                             priority,
                             findingsgroup,
                             finding,
                             url,
                             details)
                            select 27                                                                                                                                                         as checkid,
                                   0                                                                                                                                                          as priority,
                                   'Outdated sp_BlitzFirst'                                                                                                                                   as findingsgroup,
                                   'sp_BlitzFirst is Over 6 Months Old'                                                                                                                       as finding,
                                   'http://FirstResponderKit.org/'                                                                                                                            as url,
                                   'Some things get better with age, like fine wine and your T-SQL. However, sp_BlitzFirst is not one of those things - time to go download the current one.' as details;
                        end;

                    if @checkserverinfo = 0 /* Github #1680 */
                        begin
                            delete #blitzfirstresults
                            where findingsgroup = 'Server Info';
                        end

                    raiserror ('Analysis finished, outputting results',10,1) with nowait;


                    /* If they want to run sp_BlitzCache and export to table, go for it. */
                    if @outputtablenameblitzcache is not null
                        and @outputdatabasename is not null
                        and @outputschemaname is not null
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
                        begin


                            raiserror ('Calling sp_BlitzCache',10,1) with nowait;


                            /* If they have an newer version of sp_BlitzCache that supports @MinutesBack and @CheckDateOverride */
                            if EXISTS(select *
                                      from sys.objects o
                                               inner join sys.parameters pmb
                                                          on o.object_id = pmb.object_id and pmb.name = '@MinutesBack'
                                               inner join sys.parameters pcdo
                                                          on o.object_id = pcdo.object_id and pcdo.name = '@CheckDateOverride'
                                      where o.name = 'sp_BlitzCache')
                                begin
                                    /* Get the most recent sp_BlitzCache execution before this one - don't use sp_BlitzFirst because user logs are added in there at any time */
                                    set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                        + @outputdatabasename
                                        + '.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
                                        + @outputschemaname + ''' AND QUOTENAME(TABLE_NAME) = '''
                                        + QUOTENAME(@outputtablenameblitzcache) +
                                                           ''') SELECT TOP 1 @BlitzCacheMinutesBack = DATEDIFF(MI,CheckDate,SYSDATETIMEOFFSET()) FROM '
                                        + @outputdatabasename + '.'
                                        + @outputschemaname + '.'
                                        + QUOTENAME(@outputtablenameblitzcache)
                                        + ' WHERE ServerName = ''' +
                                                           CAST(SERVERPROPERTY('ServerName') as nvarchar(128)) +
                                                           ''' ORDER BY CheckDate DESC;';
                                    exec sp_executesql @stringtoexecute, N'@BlitzCacheMinutesBack INT OUTPUT',
                                         @blitzcacheminutesback output;

                                    /* If there's no data, let's just analyze the last 15 minutes of the plan cache */
                                    if @blitzcacheminutesback is null or @blitzcacheminutesback < 1 or
                                       @blitzcacheminutesback > 60
                                        set @blitzcacheminutesback = 15;

                                    exec sp_BlitzCache
                                         @outputdatabasename = @unquotedoutputdatabasename,
                                         @outputschemaname = @unquotedoutputschemaname,
                                         @outputtablename = @outputtablenameblitzcache,
                                         @checkdateoverride = @startsampletime,
                                         @sortorder = 'all',
                                         @skipanalysis = @blitzcacheskipanalysis,
                                         @minutesback = @blitzcacheminutesback,
                                         @debug = @debug;

                                    /* Delete history older than @OutputTableRetentionDays */
                                    set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                        + @outputdatabasename
                                        + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                        + @outputschemaname + ''') DELETE '
                                        + @outputdatabasename + '.'
                                        + @outputschemaname + '.'
                                        + QUOTENAME(@outputtablenameblitzcache)
                                        + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate;';
                                    exec sp_executesql @stringtoexecute,
                                         N'@SrvName NVARCHAR(128), @CheckDate date',
                                         @localservername, @outputtablecleanupdate;


                                end;

                            else /* No sp_BlitzCache found, or it's outdated */
                                begin
                                    insert into #blitzfirstresults
                                    (checkid,
                                     priority,
                                     findingsgroup,
                                     finding,
                                     url,
                                     details)
                                    select 36                                                                                                                          as checkid,
                                           0                                                                                                                           as priority,
                                           'Outdated or Missing sp_BlitzCache'                                                                                         as findingsgroup,
                                           'Update Your sp_BlitzCache'                                                                                                 as finding,
                                           'http://FirstResponderKit.org/'                                                                                             as url,
                                           'You passed in @OutputTableNameBlitzCache, but we need a newer version of sp_BlitzCache in master or the current database.' as details;
                                end;

                            raiserror ('sp_BlitzCache Finished',10,1) with nowait;

                        end;
                    /* End running sp_BlitzCache */

                    /* @OutputTableName lets us export the results to a permanent table */
                    if @outputdatabasename is not null
                        and @outputschemaname is not null
                        and @outputtablename is not null
                        and @outputtablename not like '#%'
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
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
                CheckID INT NOT NULL,
                Priority TINYINT NOT NULL,
                FindingsGroup VARCHAR(50) NOT NULL,
                Finding VARCHAR(200) NOT NULL,
                URL VARCHAR(200) NOT NULL,
                Details NVARCHAR(4000) NULL,
                HowToStopIt [XML] NULL,
                QueryPlan [XML] NULL,
                QueryText NVARCHAR(MAX) NULL,
                StartTime DATETIMEOFFSET NULL,
                LoginName NVARCHAR(128) NULL,
                NTUserName NVARCHAR(128) NULL,
                OriginalLoginName NVARCHAR(128) NULL,
                ProgramName NVARCHAR(128) NULL,
                HostName NVARCHAR(128) NULL,
                DatabaseID INT NULL,
                DatabaseName NVARCHAR(128) NULL,
                OpenTransactionCount INT NULL,
                DetailsInt INT NULL,
                QueryHash BINARY(8) NULL,
                JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
                PRIMARY KEY CLUSTERED (ID ASC));';

                            exec (@stringtoexecute);

                            /* If the table doesn't have the new QueryHash column, add it. See Github #2162. */
                            set @objectfullname =
                                    @outputdatabasename + N'.' + @outputschemaname + N'.' + @outputtablename;
                            set @stringtoexecute = N'IF NOT EXISTS (SELECT * FROM ' + @outputdatabasename + N'.sys.all_columns
            WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''QueryHash'')
            ALTER TABLE ' + @objectfullname + N' ADD QueryHash BINARY(8) NULL;';
                            exec (@stringtoexecute);

                            /* If the table doesn't have the new JoinKey computed column, add it. See Github #2164. */
                            set @objectfullname =
                                    @outputdatabasename + N'.' + @outputschemaname + N'.' + @outputtablename;
                            set @stringtoexecute = N'IF NOT EXISTS (SELECT * FROM ' + @outputdatabasename + N'.sys.all_columns
            WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
            ALTER TABLE ' + @objectfullname + N' ADD JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50));';
                            exec (@stringtoexecute);

                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') INSERT '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablename
                                +
                                                   ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt, QueryHash) SELECT '
                                +
                                                   ' @SrvName, @CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt, QueryHash FROM #BlitzFirstResults ORDER BY Priority , FindingsGroup , Finding , Details';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                 @localservername, @startsampletime;

                            /* Delete history older than @OutputTableRetentionDays */
                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') DELETE '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablename
                                + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate date',
                                 @localservername, @outputtablecleanupdate;

                        end;
                    else
                        if (SUBSTRING(@outputtablename, 2, 2) = '##')
                            begin
                                set @stringtoexecute = N' IF (OBJECT_ID(''tempdb..'
                                    + @outputtablename
                                    + ''') IS NULL) CREATE TABLE '
                                    + @outputtablename
                                    + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                CheckID INT NOT NULL,
                Priority TINYINT NOT NULL,
                FindingsGroup VARCHAR(50) NOT NULL,
                Finding VARCHAR(200) NOT NULL,
                URL VARCHAR(200) NOT NULL,
                Details NVARCHAR(4000) NULL,
                HowToStopIt [XML] NULL,
                QueryPlan [XML] NULL,
                QueryText NVARCHAR(MAX) NULL,
                StartTime DATETIMEOFFSET NULL,
                LoginName NVARCHAR(128) NULL,
                NTUserName NVARCHAR(128) NULL,
                OriginalLoginName NVARCHAR(128) NULL,
                ProgramName NVARCHAR(128) NULL,
                HostName NVARCHAR(128) NULL,
                DatabaseID INT NULL,
                DatabaseName NVARCHAR(128) NULL,
                OpenTransactionCount INT NULL,
                DetailsInt INT NULL,
                QueryHash BINARY(8) NULL,
                JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
                PRIMARY KEY CLUSTERED (ID ASC));'
                                    + ' INSERT '
                                    + @outputtablename
                                    +
                                                       ' (ServerName, CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt) SELECT '
                                    +
                                                       ' @SrvName, @CheckDate, CheckID, Priority, FindingsGroup, Finding, URL, Details, HowToStopIt, QueryPlan, QueryText, StartTime, LoginName, NTUserName, OriginalLoginName, ProgramName, HostName, DatabaseID, DatabaseName, OpenTransactionCount, DetailsInt FROM #BlitzFirstResults ORDER BY Priority , FindingsGroup , Finding , Details';

                                exec sp_executesql @stringtoexecute,
                                     N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                     @localservername, @startsampletime;
                            end;
                        else
                            if (SUBSTRING(@outputtablename, 2, 1) = '#')
                                begin
                                    raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                                end;

                    /* @OutputTableNameFileStats lets us export the results to a permanent table */
                    if @outputdatabasename is not null
                        and @outputschemaname is not null
                        and @outputtablenamefilestats is not null
                        and @outputtablenamefilestats not like '#%'
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
                        begin
                            /* Create the table */
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
                                + @outputtablenamefilestats + ''') CREATE TABLE '
                                + @outputschemaname + '.'
                                + @outputtablenamefilestats
                                + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                DatabaseID INT NOT NULL,
                FileID INT NOT NULL,
                DatabaseName NVARCHAR(256) ,
                FileLogicalName NVARCHAR(256) ,
                TypeDesc NVARCHAR(60) ,
                SizeOnDiskMB BIGINT ,
                io_stall_read_ms BIGINT ,
                num_of_reads BIGINT ,
                bytes_read BIGINT ,
                io_stall_write_ms BIGINT ,
                num_of_writes BIGINT ,
                bytes_written BIGINT,
                PhysicalName NVARCHAR(520) ,
                PRIMARY KEY CLUSTERED (ID ASC));';

                            exec (@stringtoexecute);

                            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' +
                                                  @outputtablenamefilestats_view;

                            /* If the view exists without the most recently added columns, drop it. See Github #2162. */
                            if OBJECT_ID(@objectfullname) is not null
                                begin
                                    set @stringtoexecute =
                                                N'USE ' + @outputdatabasename + N'; IF NOT EXISTS (SELECT * FROM ' +
                                                @outputdatabasename + N'.sys.all_columns
                WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @outputschemaname + N'.' + @outputtablenamefilestats_view + N';';

                                    exec (@stringtoexecute);
                                end

                            /* Create the view */
                            if OBJECT_ID(@objectfullname) is null
                                begin
                                    set @stringtoexecute = 'USE '
                                        + @outputdatabasename
                                        + '; EXEC (''CREATE VIEW '
                                        + @outputschemaname + '.'
                                        + @outputtablenamefilestats_view + ' AS ' + @linefeed
                                        + 'WITH RowDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ' + @linefeed
                                        + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' +
                                                           @linefeed
                                        + '                [CheckDate]' + @linefeed
                                        + '        FROM ' + @outputschemaname + '.' + @outputtablenamefilestats + '' +
                                                           @linefeed
                                        + '        GROUP BY [ServerName], [CheckDate]' + @linefeed
                                        + '),' + @linefeed
                                        + 'CheckDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ThisDate.CheckDate,' + @linefeed
                                        + '               LastDate.CheckDate as PreviousCheckDate' + @linefeed
                                        + '        FROM RowDates ThisDate' + @linefeed
                                        + '        JOIN RowDates LastDate' + @linefeed
                                        + '        ON ThisDate.ID = LastDate.ID + 1' + @linefeed
                                        + ')' + @linefeed
                                        + '     SELECT f.ServerName,' + @linefeed
                                        + '            f.CheckDate,' + @linefeed
                                        + '            f.DatabaseID,' + @linefeed
                                        + '            f.DatabaseName,' + @linefeed
                                        + '            f.FileID,' + @linefeed
                                        + '            f.FileLogicalName,' + @linefeed
                                        + '            f.TypeDesc,' + @linefeed
                                        + '            f.PhysicalName,' + @linefeed
                                        + '            f.SizeOnDiskMB,' + @linefeed
                                        + '            DATEDIFF(ss, fPrior.CheckDate, f.CheckDate) AS ElapsedSeconds,' +
                                                           @linefeed
                                        + '            (f.SizeOnDiskMB - fPrior.SizeOnDiskMB) AS SizeOnDiskMBgrowth,' +
                                                           @linefeed
                                        +
                                                           '            (f.io_stall_read_ms - fPrior.io_stall_read_ms) AS io_stall_read_ms,' +
                                                           @linefeed
                                        + '            io_stall_read_ms_average = CASE' + @linefeed
                                        +
                                                           '                                           WHEN(f.num_of_reads - fPrior.num_of_reads) = 0' +
                                                           @linefeed
                                        + '                                           THEN 0' + @linefeed
                                        +
                                                           '                                           ELSE(f.io_stall_read_ms - fPrior.io_stall_read_ms) /     (f.num_of_reads   -           fPrior.num_of_reads)' +
                                                           @linefeed
                                        + '                                       END,' + @linefeed
                                        + '            (f.num_of_reads - fPrior.num_of_reads) AS num_of_reads,' +
                                                           @linefeed
                                        +
                                                           '            (f.bytes_read - fPrior.bytes_read) / 1024.0 / 1024.0 AS megabytes_read,' +
                                                           @linefeed
                                        +
                                                           '            (f.io_stall_write_ms - fPrior.io_stall_write_ms) AS io_stall_write_ms,' +
                                                           @linefeed
                                        + '            io_stall_write_ms_average = CASE' + @linefeed
                                        +
                                                           '                                            WHEN(f.num_of_writes - fPrior.num_of_writes) = 0' +
                                                           @linefeed
                                        + '                                            THEN 0' + @linefeed
                                        +
                                                           '                                            ELSE(f.io_stall_write_ms - fPrior.io_stall_write_ms) /         (f.num_of_writes   -       fPrior.num_of_writes)' +
                                                           @linefeed
                                        + '                                        END,' + @linefeed
                                        + '            (f.num_of_writes - fPrior.num_of_writes) AS num_of_writes,' +
                                                           @linefeed
                                        +
                                                           '            (f.bytes_written - fPrior.bytes_written) / 1024.0 / 1024.0 AS megabytes_written, ' +
                                                           @linefeed
                                        + '            f.ServerName + CAST(f.CheckDate AS NVARCHAR(50)) AS JoinKey' +
                                                           @linefeed
                                        + '     FROM   ' + @outputschemaname + '.' + @outputtablenamefilestats + ' f' +
                                                           @linefeed
                                        +
                                                           '            INNER HASH JOIN CheckDates DATES ON f.CheckDate = DATES.CheckDate' +
                                                           @linefeed
                                        + '            INNER JOIN ' + @outputschemaname + '.' +
                                                           @outputtablenamefilestats +
                                                           ' fPrior ON f.ServerName =                 fPrior.ServerName' +
                                                           @linefeed
                                        +
                                                           '                                                              AND f.DatabaseID = fPrior.DatabaseID' +
                                                           @linefeed
                                        +
                                                           '                                                              AND f.FileID = fPrior.FileID' +
                                                           @linefeed
                                        +
                                                           '                                                              AND fPrior.CheckDate =   DATES.PreviousCheckDate' +
                                                           @linefeed
                                        + '' + @linefeed
                                        + '     WHERE  f.num_of_reads >= fPrior.num_of_reads' + @linefeed
                                        + '            AND f.num_of_writes >= fPrior.num_of_writes' + @linefeed
                                        +
                                                           '            AND DATEDIFF(MI, fPrior.CheckDate, f.CheckDate) BETWEEN 1 AND 60;'')'

                                    exec (@stringtoexecute);
                                end;


                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') INSERT '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenamefilestats
                                +
                                                   ' (ServerName, CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName) SELECT '
                                +
                                                   ' @SrvName, @CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName FROM #FileStats WHERE Pass = 2';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                 @localservername, @startsampletime;

                            /* Delete history older than @OutputTableRetentionDays */
                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') DELETE '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenamefilestats
                                + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate date',
                                 @localservername, @outputtablecleanupdate;

                        end;
                    else
                        if (SUBSTRING(@outputtablenamefilestats, 2, 2) = '##')
                            begin
                                set @stringtoexecute = N' IF (OBJECT_ID(''tempdb..'
                                    + @outputtablenamefilestats
                                    + ''') IS NULL) CREATE TABLE '
                                    + @outputtablenamefilestats
                                    + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                DatabaseID INT NOT NULL,
                FileID INT NOT NULL,
                DatabaseName NVARCHAR(256) ,
                FileLogicalName NVARCHAR(256) ,
                TypeDesc NVARCHAR(60) ,
                SizeOnDiskMB BIGINT ,
                io_stall_read_ms BIGINT ,
                num_of_reads BIGINT ,
                bytes_read BIGINT ,
                io_stall_write_ms BIGINT ,
                num_of_writes BIGINT ,
                bytes_written BIGINT,
                PhysicalName NVARCHAR(520) ,
                DetailsInt INT NULL,
                PRIMARY KEY CLUSTERED (ID ASC));'
                                    + ' INSERT '
                                    + @outputtablenamefilestats
                                    +
                                                       ' (ServerName, CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName) SELECT '
                                    +
                                                       ' @SrvName, @CheckDate, DatabaseID, FileID, DatabaseName, FileLogicalName, TypeDesc, SizeOnDiskMB, io_stall_read_ms, num_of_reads, bytes_read, io_stall_write_ms, num_of_writes, bytes_written, PhysicalName FROM #FileStats WHERE Pass = 2';

                                exec sp_executesql @stringtoexecute,
                                     N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                     @localservername, @startsampletime;
                            end;
                        else
                            if (SUBSTRING(@outputtablenamefilestats, 2, 1) = '#')
                                begin
                                    raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                                end;


                    /* @OutputTableNamePerfmonStats lets us export the results to a permanent table */
                    if @outputdatabasename is not null
                        and @outputschemaname is not null
                        and @outputtablenameperfmonstats is not null
                        and @outputtablenameperfmonstats not like '#%'
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
                        begin
                            /* Create the table */
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
                                + @outputtablenameperfmonstats + ''') CREATE TABLE '
                                + @outputschemaname + '.'
                                + @outputtablenameperfmonstats
                                + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                [object_name] NVARCHAR(128) NOT NULL,
                [counter_name] NVARCHAR(128) NOT NULL,
                [instance_name] NVARCHAR(128) NULL,
                [cntr_value] BIGINT NULL,
                [cntr_type] INT NOT NULL,
                [value_delta] BIGINT NULL,
                [value_per_second] DECIMAL(18,2) NULL,
                PRIMARY KEY CLUSTERED (ID ASC));';

                            exec (@stringtoexecute);

                            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' +
                                                  @outputtablenameperfmonstats_view;

                            /* If the view exists without the most recently added columns, drop it. See Github #2162. */
                            if OBJECT_ID(@objectfullname) is not null
                                begin
                                    set @stringtoexecute =
                                                N'USE ' + @outputdatabasename + N'; IF NOT EXISTS (SELECT * FROM ' +
                                                @outputdatabasename + N'.sys.all_columns
                WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @outputschemaname + N'.' + @outputtablenameperfmonstats_view + N';';

                                    exec (@stringtoexecute);
                                end

                            /* Create the view */
                            if OBJECT_ID(@objectfullname) is null
                                begin
                                    set @stringtoexecute = 'USE '
                                        + @outputdatabasename
                                        + '; EXEC (''CREATE VIEW '
                                        + @outputschemaname + '.'
                                        + @outputtablenameperfmonstats_view + ' AS ' + @linefeed
                                        + 'WITH RowDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ' + @linefeed
                                        + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' +
                                                           @linefeed
                                        + '                [CheckDate]' + @linefeed
                                        + '        FROM ' + @outputschemaname + '.' + @outputtablenameperfmonstats +
                                                           '' + @linefeed
                                        + '        GROUP BY [ServerName], [CheckDate]' + @linefeed
                                        + '),' + @linefeed
                                        + 'CheckDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ThisDate.CheckDate,' + @linefeed
                                        + '               LastDate.CheckDate as PreviousCheckDate' + @linefeed
                                        + '        FROM RowDates ThisDate' + @linefeed
                                        + '        JOIN RowDates LastDate' + @linefeed
                                        + '        ON ThisDate.ID = LastDate.ID + 1' + @linefeed
                                        + ')' + @linefeed
                                        + 'SELECT' + @linefeed
                                        + '       pMon.[ServerName]' + @linefeed
                                        + '      ,pMon.[CheckDate]' + @linefeed
                                        + '      ,pMon.[object_name]' + @linefeed
                                        + '      ,pMon.[counter_name]' + @linefeed
                                        + '      ,pMon.[instance_name]' + @linefeed
                                        +
                                                           '      ,DATEDIFF(SECOND,pMonPrior.[CheckDate],pMon.[CheckDate]) AS ElapsedSeconds' +
                                                           @linefeed
                                        + '      ,pMon.[cntr_value]' + @linefeed
                                        + '      ,pMon.[cntr_type]' + @linefeed
                                        + '      ,(pMon.[cntr_value] - pMonPrior.[cntr_value]) AS cntr_delta' +
                                                           @linefeed
                                        +
                                                           '      ,(pMon.cntr_value - pMonPrior.cntr_value) * 1.0 / DATEDIFF(ss, pMonPrior.CheckDate, pMon.CheckDate) AS cntr_delta_per_second' +
                                                           @linefeed
                                        + '      ,pMon.ServerName + CAST(pMon.CheckDate AS NVARCHAR(50)) AS JoinKey' +
                                                           @linefeed
                                        + '  FROM ' + @outputschemaname + '.' + @outputtablenameperfmonstats + ' pMon' +
                                                           @linefeed
                                        + '  INNER HASH JOIN CheckDates Dates' + @linefeed
                                        + '  ON Dates.CheckDate = pMon.CheckDate' + @linefeed
                                        + '  JOIN ' + @outputschemaname + '.' + @outputtablenameperfmonstats +
                                                           ' pMonPrior' + @linefeed
                                        + '  ON  Dates.PreviousCheckDate = pMonPrior.CheckDate' + @linefeed
                                        + '      AND pMon.[ServerName]    = pMonPrior.[ServerName]   ' + @linefeed
                                        + '      AND pMon.[object_name]   = pMonPrior.[object_name]  ' + @linefeed
                                        + '      AND pMon.[counter_name]  = pMonPrior.[counter_name] ' + @linefeed
                                        + '      AND pMon.[instance_name] = pMonPrior.[instance_name]' + @linefeed
                                        +
                                                           '    WHERE DATEDIFF(MI, pMonPrior.CheckDate, pMon.CheckDate) BETWEEN 1 AND 60;'')'

                                    exec (@stringtoexecute);
                                end

                            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' +
                                                  @outputtablenameperfmonstatsactuals_view;

                            /* If the view exists without the most recently added columns, drop it. See Github #2162. */
                            if OBJECT_ID(@objectfullname) is not null
                                begin
                                    set @stringtoexecute =
                                                N'USE ' + @outputdatabasename + N'; IF NOT EXISTS (SELECT * FROM ' +
                                                @outputdatabasename + N'.sys.all_columns
                WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @outputschemaname + N'.' + @outputtablenameperfmonstatsactuals_view + N';';

                                    exec (@stringtoexecute);
                                end

                            /* Create the second view */
                            if OBJECT_ID(@objectfullname) is null
                                begin
                                    set @stringtoexecute = 'USE '
                                        + @outputdatabasename
                                        + '; EXEC (''CREATE VIEW '
                                        + @outputschemaname + '.'
                                        + @outputtablenameperfmonstatsactuals_view + ' AS ' + @linefeed
                                        + 'WITH PERF_AVERAGE_BULK AS' + @linefeed
                                        + '(' + @linefeed
                                        + '    SELECT ServerName,' + @linefeed
                                        + '           object_name,' + @linefeed
                                        + '           instance_name,' + @linefeed
                                        + '           counter_name,' + @linefeed
                                        +
                                                           '           CASE WHEN CHARINDEX(''''('''', counter_name) = 0 THEN counter_name ELSE LEFT (counter_name, CHARINDEX(''''('''',counter_name)-1) END    AS   counter_join,' +
                                                           @linefeed
                                        + '           CheckDate,' + @linefeed
                                        + '           cntr_delta' + @linefeed
                                        + '    FROM   ' + @outputschemaname + '.' + @outputtablenameperfmonstats_view +
                                                           @linefeed
                                        + '    WHERE  cntr_type IN(1073874176)' + @linefeed
                                        + '    AND cntr_delta <> 0' + @linefeed
                                        + '),' + @linefeed
                                        + 'PERF_LARGE_RAW_BASE AS' + @linefeed
                                        + '(' + @linefeed
                                        + '    SELECT ServerName,' + @linefeed
                                        + '           object_name,' + @linefeed
                                        + '           instance_name,' + @linefeed
                                        +
                                                           '           LEFT(counter_name, CHARINDEX(''''BASE'''', UPPER(counter_name))-1) AS counter_join,' +
                                                           @linefeed
                                        + '           CheckDate,' + @linefeed
                                        + '           cntr_delta' + @linefeed
                                        + '    FROM   ' + @outputschemaname + '.' + @outputtablenameperfmonstats_view +
                                                           '' + @linefeed
                                        + '    WHERE  cntr_type IN(1073939712)' + @linefeed
                                        + '    AND cntr_delta <> 0' + @linefeed
                                        + '),' + @linefeed
                                        + 'PERF_AVERAGE_FRACTION AS' + @linefeed
                                        + '(' + @linefeed
                                        + '    SELECT ServerName,' + @linefeed
                                        + '           object_name,' + @linefeed
                                        + '           instance_name,' + @linefeed
                                        + '           counter_name,' + @linefeed
                                        + '           counter_name AS counter_join,' + @linefeed
                                        + '           CheckDate,' + @linefeed
                                        + '           cntr_delta' + @linefeed
                                        + '    FROM   ' + @outputschemaname + '.' + @outputtablenameperfmonstats_view +
                                                           '' + @linefeed
                                        + '    WHERE  cntr_type IN(537003264)' + @linefeed
                                        + '    AND cntr_delta <> 0' + @linefeed
                                        + '),' + @linefeed
                                        + 'PERF_COUNTER_BULK_COUNT AS' + @linefeed
                                        + '(' + @linefeed
                                        + '    SELECT ServerName,' + @linefeed
                                        + '           object_name,' + @linefeed
                                        + '           instance_name,' + @linefeed
                                        + '           counter_name,' + @linefeed
                                        + '           CheckDate,' + @linefeed
                                        + '           cntr_delta / ElapsedSeconds AS cntr_value' + @linefeed
                                        + '    FROM   ' + @outputschemaname + '.' + @outputtablenameperfmonstats_view +
                                                           '' + @linefeed
                                        + '    WHERE  cntr_type IN(272696576, 272696320)' + @linefeed
                                        + '    AND cntr_delta <> 0' + @linefeed
                                        + '),' + @linefeed
                                        + 'PERF_COUNTER_RAWCOUNT AS' + @linefeed
                                        + '(' + @linefeed
                                        + '    SELECT ServerName,' + @linefeed
                                        + '           object_name,' + @linefeed
                                        + '           instance_name,' + @linefeed
                                        + '           counter_name,' + @linefeed
                                        + '           CheckDate,' + @linefeed
                                        + '           cntr_value' + @linefeed
                                        + '    FROM   ' + @outputschemaname + '.' + @outputtablenameperfmonstats_view +
                                                           '' + @linefeed
                                        + '    WHERE  cntr_type IN(65792, 65536)' + @linefeed
                                        + ')' + @linefeed
                                        + '' + @linefeed
                                        + 'SELECT NUM.ServerName,' + @linefeed
                                        + '       NUM.object_name,' + @linefeed
                                        + '       NUM.counter_name,' + @linefeed
                                        + '       NUM.instance_name,' + @linefeed
                                        + '       NUM.CheckDate,' + @linefeed
                                        + '       NUM.cntr_delta / DEN.cntr_delta AS cntr_value,' + @linefeed
                                        + '       NUM.ServerName + CAST(NUM.CheckDate AS NVARCHAR(50)) AS JoinKey' +
                                                           @linefeed
                                        + '       ' + @linefeed
                                        + 'FROM   PERF_AVERAGE_BULK AS NUM' + @linefeed
                                        +
                                                           '       JOIN PERF_LARGE_RAW_BASE AS DEN ON NUM.counter_join = DEN.counter_join' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.CheckDate = DEN.CheckDate' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.ServerName = DEN.ServerName' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.object_name = DEN.object_name' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.instance_name = DEN.instance_name' +
                                                           @linefeed
                                        + '                                          AND DEN.cntr_delta <> 0' +
                                                           @linefeed
                                        + '' + @linefeed
                                        + 'UNION ALL' + @linefeed
                                        + '' + @linefeed
                                        + 'SELECT NUM.ServerName,' + @linefeed
                                        + '       NUM.object_name,' + @linefeed
                                        + '       NUM.counter_name,' + @linefeed
                                        + '       NUM.instance_name,' + @linefeed
                                        + '       NUM.CheckDate,' + @linefeed
                                        +
                                                           '       CAST((CAST(NUM.cntr_delta as DECIMAL(19)) / DEN.cntr_delta) as decimal(23,3))  AS cntr_value,' +
                                                           @linefeed
                                        + '       NUM.ServerName + CAST(NUM.CheckDate AS NVARCHAR(50)) AS JoinKey' +
                                                           @linefeed
                                        + 'FROM   PERF_AVERAGE_FRACTION AS NUM' + @linefeed
                                        +
                                                           '       JOIN PERF_LARGE_RAW_BASE AS DEN ON NUM.counter_join = DEN.counter_join' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.CheckDate = DEN.CheckDate' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.ServerName = DEN.ServerName' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.object_name = DEN.object_name' +
                                                           @linefeed
                                        +
                                                           '                                          AND NUM.instance_name = DEN.instance_name' +
                                                           @linefeed
                                        + '                                          AND DEN.cntr_delta <> 0' +
                                                           @linefeed
                                        + 'UNION ALL' + @linefeed
                                        + '' + @linefeed
                                        + 'SELECT ServerName,' + @linefeed
                                        + '       object_name,' + @linefeed
                                        + '       counter_name,' + @linefeed
                                        + '       instance_name,' + @linefeed
                                        + '       CheckDate,' + @linefeed
                                        + '       cntr_value,' + @linefeed
                                        + '       ServerName + CAST(CheckDate AS NVARCHAR(50)) AS JoinKey' + @linefeed
                                        + 'FROM   PERF_COUNTER_BULK_COUNT' + @linefeed
                                        + '' + @linefeed
                                        + 'UNION ALL' + @linefeed
                                        + '' + @linefeed
                                        + 'SELECT ServerName,' + @linefeed
                                        + '       object_name,' + @linefeed
                                        + '       counter_name,' + @linefeed
                                        + '       instance_name,' + @linefeed
                                        + '       CheckDate,' + @linefeed
                                        + '       cntr_value,' + @linefeed
                                        + '       ServerName + CAST(CheckDate AS NVARCHAR(50)) AS JoinKey' + @linefeed
                                        + 'FROM   PERF_COUNTER_RAWCOUNT;'')';

                                    exec (@stringtoexecute);
                                end;


                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') INSERT '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenameperfmonstats
                                +
                                                   ' (ServerName, CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second) SELECT '
                                +
                                                   ' @SrvName, @CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second FROM #PerfmonStats WHERE Pass = 2';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                 @localservername, @startsampletime;

                            /* Delete history older than @OutputTableRetentionDays */
                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') DELETE '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenameperfmonstats
                                + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate date',
                                 @localservername, @outputtablecleanupdate;


                        end;
                    else
                        if (SUBSTRING(@outputtablenameperfmonstats, 2, 2) = '##')
                            begin
                                set @stringtoexecute = N' IF (OBJECT_ID(''tempdb..'
                                    + @outputtablenameperfmonstats
                                    + ''') IS NULL) CREATE TABLE '
                                    + @outputtablenameperfmonstats
                                    + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                [object_name] NVARCHAR(128) NOT NULL,
                [counter_name] NVARCHAR(128) NOT NULL,
                [instance_name] NVARCHAR(128) NULL,
                [cntr_value] BIGINT NULL,
                [cntr_type] INT NOT NULL,
                [value_delta] BIGINT NULL,
                [value_per_second] DECIMAL(18,2) NULL,
                PRIMARY KEY CLUSTERED (ID ASC));'
                                    + ' INSERT '
                                    + @outputtablenameperfmonstats
                                    +
                                                       ' (ServerName, CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second) SELECT '
                                    + CAST(SERVERPROPERTY('ServerName') as nvarchar(128))
                                    +
                                                       ' @SrvName, @CheckDate, object_name, counter_name, instance_name, cntr_value, cntr_type, value_delta, value_per_second FROM #PerfmonStats WHERE Pass = 2';

                                exec sp_executesql @stringtoexecute,
                                     N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                     @localservername, @startsampletime;
                            end;
                        else
                            if (SUBSTRING(@outputtablenameperfmonstats, 2, 1) = '#')
                                begin
                                    raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                                end;


                    /* @OutputTableNameWaitStats lets us export the results to a permanent table */
                    if @outputdatabasename is not null
                        and @outputschemaname is not null
                        and @outputtablenamewaitstats is not null
                        and @outputtablenamewaitstats not like '#%'
                        and EXISTS(select *
                                   from sys.databases
                                   where QUOTENAME([name]) = @outputdatabasename)
                        begin
                            /* Create the table */
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
                                + @outputtablenamewaitstats + ''') ' + @linefeed
                                + 'BEGIN' + @linefeed
                                + 'CREATE TABLE '
                                + @outputschemaname + '.'
                                + @outputtablenamewaitstats
                                + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                wait_type NVARCHAR(60),
                wait_time_ms BIGINT,
                signal_wait_time_ms BIGINT,
                waiting_tasks_count BIGINT ,
                PRIMARY KEY CLUSTERED (ID));' + @linefeed
                                + 'CREATE NONCLUSTERED INDEX IX_ServerName_wait_type_CheckDate_Includes ON ' +
                                                   @outputschemaname + '.' + @outputtablenamewaitstats + @linefeed
                                +
                                                   '(ServerName, wait_type, CheckDate) INCLUDE (wait_time_ms, signal_wait_time_ms, waiting_tasks_count);' +
                                                   @linefeed
                                + 'END';

                            exec (@stringtoexecute);

                            /* Create the wait stats category table */
                            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' +
                                                  @outputtablenamewaitstats_categories;
                            if OBJECT_ID(@objectfullname) is null
                                begin
                                    set @stringtoexecute = 'USE '
                                        + @outputdatabasename
                                        + '; EXEC (''CREATE TABLE '
                                        + @outputschemaname + '.'
                                        + @outputtablenamewaitstats_categories +
                                                           ' (WaitType NVARCHAR(60) PRIMARY KEY CLUSTERED, WaitCategory NVARCHAR(128) NOT NULL, Ignorable BIT DEFAULT 0);'')';

                                    exec (@stringtoexecute);
                                end;

                            /* Make sure the wait stats category table has the current number of rows */
                            set @stringtoexecute = 'USE '
                                + @outputdatabasename
                                + '; EXEC (''IF (SELECT COALESCE(SUM(1),0) FROM ' + @outputschemaname + '.' +
                                                   @outputtablenamewaitstats_categories +
                                                   ') <> (SELECT COALESCE(SUM(1),0) FROM ##WaitCategories)' + @linefeed
                                + 'BEGIN ' + @linefeed
                                + 'TRUNCATE TABLE ' + @outputschemaname + '.' + @outputtablenamewaitstats_categories +
                                                   @linefeed
                                + 'INSERT INTO ' + @outputschemaname + '.' + @outputtablenamewaitstats_categories +
                                                   ' (WaitType, WaitCategory, Ignorable) SELECT WaitType, WaitCategory, Ignorable FROM ##WaitCategories;' +
                                                   @linefeed
                                + 'END'')';

                            exec (@stringtoexecute);


                            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' +
                                                  @outputtablenamewaitstats_view;

                            /* If the view exists without the most recently added columns, drop it. See Github #2162. */
                            if OBJECT_ID(@objectfullname) is not null
                                begin
                                    set @stringtoexecute =
                                                N'USE ' + @outputdatabasename + N'; IF NOT EXISTS (SELECT * FROM ' +
                                                @outputdatabasename + N'.sys.all_columns
                WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
                DROP VIEW ' + @outputschemaname + N'.' + @outputtablenamewaitstats_view + N';';

                                    exec (@stringtoexecute);
                                end


                            /* Create the wait stats view */
                            if OBJECT_ID(@objectfullname) is null
                                begin
                                    set @stringtoexecute = 'USE '
                                        + @outputdatabasename
                                        + '; EXEC (''CREATE VIEW '
                                        + @outputschemaname + '.'
                                        + @outputtablenamewaitstats_view + ' AS ' + @linefeed
                                        + 'WITH RowDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ' + @linefeed
                                        + '                ROW_NUMBER() OVER (ORDER BY [ServerName], [CheckDate]) ID,' +
                                                           @linefeed
                                        + '                [CheckDate]' + @linefeed
                                        + '        FROM ' + @outputschemaname + '.' + @outputtablenamewaitstats +
                                                           @linefeed
                                        + '        GROUP BY [ServerName], [CheckDate]' + @linefeed
                                        + '),' + @linefeed
                                        + 'CheckDates as' + @linefeed
                                        + '(' + @linefeed
                                        + '        SELECT ThisDate.CheckDate,' + @linefeed
                                        + '               LastDate.CheckDate as PreviousCheckDate' + @linefeed
                                        + '        FROM RowDates ThisDate' + @linefeed
                                        + '        JOIN RowDates LastDate' + @linefeed
                                        + '        ON ThisDate.ID = LastDate.ID + 1' + @linefeed
                                        + ')' + @linefeed
                                        +
                                                           'SELECT w.ServerName, w.CheckDate, w.wait_type, COALESCE(wc.WaitCategory, ''''Other'''') AS WaitCategory, COALESCE(wc.Ignorable,0) AS Ignorable' +
                                                           @linefeed
                                        + ', DATEDIFF(ss, wPrior.CheckDate, w.CheckDate) AS ElapsedSeconds' + @linefeed
                                        + ', (w.wait_time_ms - wPrior.wait_time_ms) AS wait_time_ms_delta' + @linefeed
                                        +
                                                           ', (w.wait_time_ms - wPrior.wait_time_ms) / 60000.0 AS wait_time_minutes_delta' +
                                                           @linefeed
                                        +
                                                           ', (w.wait_time_ms - wPrior.wait_time_ms) / 1000.0 / DATEDIFF(ss, wPrior.CheckDate, w.CheckDate) AS wait_time_minutes_per_minute' +
                                                           @linefeed
                                        +
                                                           ', (w.signal_wait_time_ms - wPrior.signal_wait_time_ms) AS signal_wait_time_ms_delta' +
                                                           @linefeed
                                        +
                                                           ', (w.waiting_tasks_count - wPrior.waiting_tasks_count) AS waiting_tasks_count_delta' +
                                                           @linefeed
                                        + ', w.ServerName + CAST(w.CheckDate AS NVARCHAR(50)) AS JoinKey' + @linefeed
                                        + 'FROM ' + @outputschemaname + '.' + @outputtablenamewaitstats + ' w' +
                                                           @linefeed
                                        + 'INNER HASH JOIN CheckDates Dates' + @linefeed
                                        + 'ON Dates.CheckDate = w.CheckDate' + @linefeed
                                        + 'INNER JOIN ' + @outputschemaname + '.' + @outputtablenamewaitstats +
                                                           ' wPrior ON w.ServerName = wPrior.ServerName AND w.wait_type = wPrior.wait_type AND Dates.PreviousCheckDate = wPrior.CheckDate' +
                                                           @linefeed
                                        + 'LEFT OUTER JOIN ' + @outputschemaname + '.' +
                                                           @outputtablenamewaitstats_categories +
                                                           ' wc ON w.wait_type = wc.WaitType' + @linefeed
                                        + 'WHERE DATEDIFF(MI, wPrior.CheckDate, w.CheckDate) BETWEEN 1 AND 60' +
                                                           @linefeed
                                        + 'AND [w].[wait_time_ms] >= [wPrior].[wait_time_ms];'')'

                                    exec (@stringtoexecute);
                                end;


                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') INSERT '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenamewaitstats
                                +
                                                   ' (ServerName, CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count) SELECT '
                                +
                                                   ' @SrvName, @CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count FROM #WaitStats WHERE Pass = 2 AND wait_time_ms > 0 AND waiting_tasks_count > 0';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                 @localservername, @startsampletime;

                            /* Delete history older than @OutputTableRetentionDays */
                            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                                + @outputdatabasename
                                + '.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                                + @outputschemaname + ''') DELETE '
                                + @outputdatabasename + '.'
                                + @outputschemaname + '.'
                                + @outputtablenamewaitstats
                                + ' WHERE ServerName = @SrvName AND CheckDate < @CheckDate ;';

                            exec sp_executesql @stringtoexecute,
                                 N'@SrvName NVARCHAR(128), @CheckDate date',
                                 @localservername, @outputtablecleanupdate;

                        end;
                    else
                        if (SUBSTRING(@outputtablenamewaitstats, 2, 2) = '##')
                            begin
                                set @stringtoexecute = N' IF (OBJECT_ID(''tempdb..'
                                    + @outputtablenamewaitstats
                                    + ''') IS NULL) CREATE TABLE '
                                    + @outputtablenamewaitstats
                                    + ' (ID INT IDENTITY(1,1) NOT NULL,
                ServerName NVARCHAR(128),
                CheckDate DATETIMEOFFSET,
                wait_type NVARCHAR(60),
                wait_time_ms BIGINT,
                signal_wait_time_ms BIGINT,
                waiting_tasks_count BIGINT ,
                PRIMARY KEY CLUSTERED (ID ASC));'
                                    + ' INSERT '
                                    + @outputtablenamewaitstats
                                    +
                                                       ' (ServerName, CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count) SELECT '
                                    +
                                                       ' @SrvName, @CheckDate, wait_type, wait_time_ms, signal_wait_time_ms, waiting_tasks_count FROM #WaitStats WHERE Pass = 2 AND wait_time_ms > 0 AND waiting_tasks_count > 0';

                                exec sp_executesql @stringtoexecute,
                                     N'@SrvName NVARCHAR(128), @CheckDate datetimeoffset',
                                     @localservername, @startsampletime;
                            end;
                        else
                            if (SUBSTRING(@outputtablenamewaitstats, 2, 1) = '#')
                                begin
                                    raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                                end;


                    declare @separator as varchar(1);
                    if @outputtype = 'RSV'
                        set @separator = CHAR(31);
                    else
                        set @separator = ',';

                    if @outputtype = 'COUNT' and @sincestartup = 0
                        begin
                            select COUNT(*) as warnings
                            from #blitzfirstresults;
                        end;
                    else
                        if @outputtype = 'Opserver1' and @sincestartup = 0
                            begin

                                select                         r.[Priority],
                                                               r.[FindingsGroup],
                                                               r.[Finding],
                                                               r.[URL],
                                                               r.[Details],
                                                               r.[HowToStopIt],
                                                               r.[CheckID],
                                                               r.[StartTime],
                                                               r.[LoginName],
                                                               r.[NTUserName],
                                                               r.[OriginalLoginName],
                                                               r.[ProgramName],
                                                               r.[HostName],
                                                               r.[DatabaseID],
                                                               r.[DatabaseName],
                                                               r.[OpenTransactionCount],
                                                               r.[QueryPlan],
                                                               r.[QueryText],
                                                               qsnow.plan_handle            as planhandle,
                                                               qsnow.sql_handle             as sqlhandle,
                                                               qsnow.statement_start_offset as statementstartoffset,
                                                               qsnow.statement_end_offset   as statementendoffset,
                                    [Executions]             = qsnow.execution_count - (COALESCE(qsfirst.execution_count, 0)),
                                    [ExecutionsPercent]      = CAST(100.0 *
                                                                    (qsnow.execution_count - (COALESCE(qsfirst.execution_count, 0))) /
                                                                    (qstotal.execution_count - qstotalfirst.execution_count) as decimal(6, 2)),
                                    [Duration]               = qsnow.total_elapsed_time - (COALESCE(qsfirst.total_elapsed_time, 0)),
                                    [DurationPercent]        = CAST(100.0 *
                                                                    (qsnow.total_elapsed_time - (COALESCE(qsfirst.total_elapsed_time, 0))) /
                                                                    (qstotal.total_elapsed_time - qstotalfirst.total_elapsed_time) as decimal(6, 2)),
                                    [CPU]                    = qsnow.total_worker_time - (COALESCE(qsfirst.total_worker_time, 0)),
                                    [CPUPercent]             = CAST(100.0 *
                                                                    (qsnow.total_worker_time - (COALESCE(qsfirst.total_worker_time, 0))) /
                                                                    (qstotal.total_worker_time - qstotalfirst.total_worker_time) as decimal(6, 2)),
                                    [Reads]                  = qsnow.total_logical_reads -
                                                               (COALESCE(qsfirst.total_logical_reads, 0)),
                                    [ReadsPercent]           = CAST(100.0 * (qsnow.total_logical_reads -
                                                                             (COALESCE(qsfirst.total_logical_reads, 0))) /
                                                                    (qstotal.total_logical_reads - qstotalfirst.total_logical_reads) as decimal(6, 2)),
                                    [PlanCreationTime]       = CONVERT(nvarchar(100), qsnow.creation_time, 121),
                                    [TotalExecutions]        = qsnow.execution_count,
                                    [TotalExecutionsPercent] = CAST(
                                                                       100.0 * qsnow.execution_count / qstotal.execution_count as decimal(6, 2)),
                                    [TotalDuration]          = qsnow.total_elapsed_time,
                                    [TotalDurationPercent]   = CAST(
                                                                       100.0 * qsnow.total_elapsed_time / qstotal.total_elapsed_time as decimal(6, 2)),
                                    [TotalCPU]               = qsnow.total_worker_time,
                                    [TotalCPUPercent]        = CAST(
                                                                       100.0 * qsnow.total_worker_time / qstotal.total_worker_time as decimal(6, 2)),
                                    [TotalReads]             = qsnow.total_logical_reads,
                                    [TotalReadsPercent]      = CAST(
                                                                       100.0 * qsnow.total_logical_reads / qstotal.total_logical_reads as decimal(6, 2)),
                                                               r.[DetailsInt]
                                from #blitzfirstresults r
                                         left outer join #querystats qstotal on qstotal.pass = 0
                                         left outer join #querystats qstotalfirst on qstotalfirst.pass = -1
                                         left outer join #querystats qsnow on r.querystatsnowid = qsnow.id
                                         left outer join #querystats qsfirst on r.querystatsfirstid = qsfirst.id
                                order by r.priority,
                                         r.findingsgroup,
                                         case
                                             when r.checkid = 6 then detailsint
                                             else 0
                                             end desc,
                                         r.finding,
                                         r.id;
                            end;
                        else
                            if @outputtype in ('CSV', 'RSV') and @sincestartup = 0
                                begin

                                    select result = CAST([Priority] as nvarchar(100))
                                        + @separator + CAST(checkid as nvarchar(100))
                                        + @separator + COALESCE([FindingsGroup],
                                                                '(N/A)') + @separator
                                        + COALESCE([Finding], '(N/A)') + @separator
                                        + COALESCE(databasename, '(N/A)') + @separator
                                        + COALESCE([URL], '(N/A)') + @separator
                                        + COALESCE([Details], '(N/A)')
                                    from #blitzfirstresults
                                    order by priority,
                                             findingsgroup,
                                             case
                                                 when checkid = 6 then detailsint
                                                 else 0
                                                 end desc,
                                             finding,
                                             details;
                                end;
                            else
                                if @outputtype = 'Top10'
                                    begin
                                        /* Measure waits in hours */
                                        ;
                                        with max_batch as (
                                            select MAX(sampletime) as sampletime
                                            from #waitstats
                                        )
                                        select top 10 CAST(
                                                              DATEDIFF(mi, wd1.sampletime, wd2.sampletime) / 60.0 as decimal(18, 1)) as [Hours Sample],
                                                      wd1.wait_type,
                                                      COALESCE(wcat.waitcategory, 'Other')                                           as wait_category,
                                                      CAST(c.[Wait Time (Seconds)] / 60.0 / 60 as decimal(18, 1))                    as [Wait Time (Hours)],
                                                      CAST((wd2.wait_time_ms - wd1.wait_time_ms) / 1000.0 /
                                                           cores.cpu_count /
                                                           DATEDIFF(ss, wd1.sampletime, wd2.sampletime) as decimal(18, 1))           as [Per Core Per Hour],
                                                      (wd2.waiting_tasks_count - wd1.waiting_tasks_count)                            as [Number of Waits],
                                                      case
                                                          when (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                                              then
                                                              CAST((wd2.wait_time_ms - wd1.wait_time_ms) /
                                                                   (1.0 * (wd2.waiting_tasks_count - wd1.waiting_tasks_count)) as numeric(12, 1))
                                                          else 0 end                                                                 as [Avg ms Per Wait]
                                        from max_batch b
                                                 join #waitstats wd2 on
                                            wd2.sampletime = b.sampletime
                                                 join #waitstats wd1 on
                                                wd1.wait_type = wd2.wait_type and
                                                wd2.sampletime > wd1.sampletime
                                                 cross apply (select SUM(1) as cpu_count
                                                              from sys.dm_os_schedulers
                                                              where status = 'VISIBLE ONLINE'
                                                                and is_online = 1) as cores
                                                 cross apply (select CAST(
                                                                             (wd2.wait_time_ms - wd1.wait_time_ms) / 1000. as numeric(12, 1))               as [Wait Time (Seconds)],
                                                                     CAST(
                                                                             (wd2.signal_wait_time_ms - wd1.signal_wait_time_ms) / 1000. as numeric(12, 1)) as [Signal Wait Time (Seconds)]) as c
                                                 left outer join ##waitcategories wcat on wd1.wait_type = wcat.waittype
                                        where (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                          and wd2.wait_time_ms - wd1.wait_time_ms > 0
                                        order by [Wait Time (Seconds)] desc;
                                    end;
                                else
                                    if @expertmode = 0 and @outputtype <> 'NONE' and @outputxmlasnvarchar = 0 and
                                       @sincestartup = 0
                                        begin
                                            select [Priority],
                                                   [FindingsGroup],
                                                   [Finding],
                                                   [URL],
                                                   CAST(@stockdetailsheader + [Details] + @stockdetailsfooter as xml)   as details,
                                                   CAST(@stockwarningheader + howtostopit + @stockwarningfooter as xml) as howtostopit,
                                                   [QueryText],
                                                   [QueryPlan]
                                            from #blitzfirstresults
                                            where (@seconds > 0 or (priority in (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
                                            order by priority,
                                                     findingsgroup,
                                                     case
                                                         when checkid = 6 then detailsint
                                                         else 0
                                                         end desc,
                                                     finding,
                                                     id,
                                                     CAST(details as nvarchar(4000));
                                        end;
                                    else
                                        if @outputtype <> 'NONE' and @outputxmlasnvarchar = 1 and @sincestartup = 0
                                            begin
                                                select [Priority],
                                                       [FindingsGroup],
                                                       [Finding],
                                                       [URL],
                                                       CAST(
                                                               @stockdetailsheader + [Details] + @stockdetailsfooter as nvarchar(max)) as details,
                                                       CAST([HowToStopIt] as nvarchar(max))                                            as howtostopit,
                                                       CAST([QueryText] as nvarchar(max))                                              as querytext,
                                                       CAST([QueryPlan] as nvarchar(max))                                              as queryplan
                                                from #blitzfirstresults
                                                where (@seconds > 0 or (priority in (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
                                                order by priority,
                                                         findingsgroup,
                                                         case
                                                             when checkid = 6 then detailsint
                                                             else 0
                                                             end desc,
                                                         finding,
                                                         id,
                                                         CAST(details as nvarchar(4000));
                                            end;
                                        else
                                            if @expertmode = 1 and @outputtype <> 'NONE'
                                                begin
                                                    if @sincestartup = 0
                                                        select                         r.[Priority],
                                                                                       r.[FindingsGroup],
                                                                                       r.[Finding],
                                                                                       r.[URL],
                                                                                       CAST(@stockdetailsheader + r.[Details] + @stockdetailsfooter as xml)   as details,
                                                                                       CAST(@stockwarningheader + r.howtostopit + @stockwarningfooter as xml) as howtostopit,
                                                                                       r.[CheckID],
                                                                                       r.[StartTime],
                                                                                       r.[LoginName],
                                                                                       r.[NTUserName],
                                                                                       r.[OriginalLoginName],
                                                                                       r.[ProgramName],
                                                                                       r.[HostName],
                                                                                       r.[DatabaseID],
                                                                                       r.[DatabaseName],
                                                                                       r.[OpenTransactionCount],
                                                                                       r.[QueryPlan],
                                                                                       r.[QueryText],
                                                                                       qsnow.plan_handle                                                      as planhandle,
                                                                                       qsnow.sql_handle                                                       as sqlhandle,
                                                                                       qsnow.statement_start_offset                                           as statementstartoffset,
                                                                                       qsnow.statement_end_offset                                             as statementendoffset,
                                                            [Executions]             = qsnow.execution_count - (COALESCE(qsfirst.execution_count, 0)),
                                                            [ExecutionsPercent]      = CAST(100.0 *
                                                                                            (qsnow.execution_count - (COALESCE(qsfirst.execution_count, 0))) /
                                                                                            (qstotal.execution_count - qstotalfirst.execution_count) as decimal(6, 2)),
                                                            [Duration]               = qsnow.total_elapsed_time - (COALESCE(qsfirst.total_elapsed_time, 0)),
                                                            [DurationPercent]        = CAST(100.0 *
                                                                                            (qsnow.total_elapsed_time - (COALESCE(qsfirst.total_elapsed_time, 0))) /
                                                                                            (qstotal.total_elapsed_time - qstotalfirst.total_elapsed_time) as decimal(6, 2)),
                                                            [CPU]                    = qsnow.total_worker_time - (COALESCE(qsfirst.total_worker_time, 0)),
                                                            [CPUPercent]             = CAST(100.0 *
                                                                                            (qsnow.total_worker_time - (COALESCE(qsfirst.total_worker_time, 0))) /
                                                                                            (qstotal.total_worker_time - qstotalfirst.total_worker_time) as decimal(6, 2)),
                                                            [Reads]                  = qsnow.total_logical_reads -
                                                                                       (COALESCE(qsfirst.total_logical_reads, 0)),
                                                            [ReadsPercent]           = CAST(100.0 *
                                                                                            (qsnow.total_logical_reads -
                                                                                             (COALESCE(qsfirst.total_logical_reads, 0))) /
                                                                                            (qstotal.total_logical_reads - qstotalfirst.total_logical_reads) as decimal(6, 2)),
                                                            [PlanCreationTime]       = CONVERT(nvarchar(100), qsnow.creation_time, 121),
                                                            [TotalExecutions]        = qsnow.execution_count,
                                                            [TotalExecutionsPercent] = CAST(
                                                                                               100.0 * qsnow.execution_count / qstotal.execution_count as decimal(6, 2)),
                                                            [TotalDuration]          = qsnow.total_elapsed_time,
                                                            [TotalDurationPercent]   = CAST(
                                                                                               100.0 * qsnow.total_elapsed_time / qstotal.total_elapsed_time as decimal(6, 2)),
                                                            [TotalCPU]               = qsnow.total_worker_time,
                                                            [TotalCPUPercent]        = CAST(
                                                                                               100.0 * qsnow.total_worker_time / qstotal.total_worker_time as decimal(6, 2)),
                                                            [TotalReads]             = qsnow.total_logical_reads,
                                                            [TotalReadsPercent]      = CAST(
                                                                                               100.0 * qsnow.total_logical_reads / qstotal.total_logical_reads as decimal(6, 2)),
                                                                                       r.[DetailsInt]
                                                        from #blitzfirstresults r
                                                                 left outer join #querystats qstotal on qstotal.pass = 0
                                                                 left outer join #querystats qstotalfirst on qstotalfirst.pass = -1
                                                                 left outer join #querystats qsnow on r.querystatsnowid = qsnow.id
                                                                 left outer join #querystats qsfirst on r.querystatsfirstid = qsfirst.id
                                                        where (@seconds > 0 or (priority in (0, 250, 251, 255))) /* For @Seconds = 0, filter out broken checks for now */
                                                        order by r.priority,
                                                                 r.findingsgroup,
                                                                 case
                                                                     when r.checkid = 6 then detailsint
                                                                     else 0
                                                                     end desc,
                                                                 r.finding,
                                                                 r.id,
                                                                 CAST(r.details as nvarchar(4000));

                                                    -------------------------
                                                    --What happened: #WaitStats
                                                    -------------------------
                                                    if @seconds = 0
                                                        begin
                                                            /* Measure waits in hours */
                                                            ;
                                                            with max_batch as (
                                                                select MAX(sampletime) as sampletime
                                                                from #waitstats
                                                            )
                                                            select 'WAIT STATS'                                                                   as pattern,
                                                                   b.sampletime                                                                   as [Sample Ended],
                                                                   CAST(
                                                                           DATEDIFF(mi, wd1.sampletime, wd2.sampletime) / 60.0 as decimal(18, 1)) as [Hours Sample],
                                                                   wd1.wait_type,
                                                                   COALESCE(wcat.waitcategory, 'Other')                                           as wait_category,
                                                                   CAST(c.[Wait Time (Seconds)] / 60.0 / 60 as decimal(18, 1))                    as [Wait Time (Hours)],
                                                                   CAST((wd2.wait_time_ms - wd1.wait_time_ms) / 1000.0 /
                                                                        cores.cpu_count /
                                                                        DATEDIFF(ss, wd1.sampletime, wd2.sampletime) as decimal(18, 1))           as [Per Core Per Hour],
                                                                   CAST(c.[Signal Wait Time (Seconds)] / 60.0 / 60 as decimal(18, 1))             as [Signal Wait Time (Hours)],
                                                                   case
                                                                       when c.[Wait Time (Seconds)] > 0
                                                                           then CAST(
                                                                               100. * (c.[Signal Wait Time (Seconds)] / c.[Wait Time (Seconds)]) as numeric(4, 1))
                                                                       else 0 end                                                                 as [Percent Signal Waits],
                                                                   (wd2.waiting_tasks_count - wd1.waiting_tasks_count)                            as [Number of Waits],
                                                                   case
                                                                       when (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                                                           then
                                                                           CAST((wd2.wait_time_ms - wd1.wait_time_ms) /
                                                                                (1.0 * (wd2.waiting_tasks_count - wd1.waiting_tasks_count)) as numeric(12, 1))
                                                                       else 0 end                                                                 as [Avg ms Per Wait],
                                                                   N'https://www.sqlskills.com/help/waits/' + LOWER(wd1.wait_type) + '/'          as url
                                                            from max_batch b
                                                                     join #waitstats wd2 on
                                                                wd2.sampletime = b.sampletime
                                                                     join #waitstats wd1 on
                                                                    wd1.wait_type = wd2.wait_type and
                                                                    wd2.sampletime > wd1.sampletime
                                                                     cross apply (select SUM(1) as cpu_count
                                                                                  from sys.dm_os_schedulers
                                                                                  where status = 'VISIBLE ONLINE'
                                                                                    and is_online = 1) as cores
                                                                     cross apply (select CAST(
                                                                                                 (wd2.wait_time_ms - wd1.wait_time_ms) / 1000. as numeric(12, 1))               as [Wait Time (Seconds)],
                                                                                         CAST(
                                                                                                 (wd2.signal_wait_time_ms - wd1.signal_wait_time_ms) / 1000. as numeric(12, 1)) as [Signal Wait Time (Seconds)]) as c
                                                                     left outer join ##waitcategories wcat on wd1.wait_type = wcat.waittype
                                                            where (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                                              and wd2.wait_time_ms - wd1.wait_time_ms > 0
                                                            order by [Wait Time (Seconds)] desc;
                                                        end;
                                                    else
                                                        begin
                                                            /* Measure waits in seconds */
                                                            ;
                                                            with max_batch as (
                                                                select MAX(sampletime) as sampletime
                                                                from #waitstats
                                                            )
                                                            select 'WAIT STATS'                                                                as pattern,
                                                                   b.sampletime                                                                as [Sample Ended],
                                                                   DATEDIFF(ss, wd1.sampletime, wd2.sampletime)                                as [Seconds Sample],
                                                                   wd1.wait_type,
                                                                   COALESCE(wcat.waitcategory, 'Other')                                        as wait_category,
                                                                   c.[Wait Time (Seconds)],
                                                                   CAST(
                                                                               (CAST(wd2.wait_time_ms - wd1.wait_time_ms as money)) /
                                                                               1000.0 / cores.cpu_count /
                                                                               DATEDIFF(ss, wd1.sampletime, wd2.sampletime) as decimal(18, 1)) as [Per Core Per Second],
                                                                   c.[Signal Wait Time (Seconds)],
                                                                   case
                                                                       when c.[Wait Time (Seconds)] > 0
                                                                           then CAST(
                                                                               100. * (c.[Signal Wait Time (Seconds)] / c.[Wait Time (Seconds)]) as numeric(4, 1))
                                                                       else 0 end                                                              as [Percent Signal Waits],
                                                                   (wd2.waiting_tasks_count - wd1.waiting_tasks_count)                         as [Number of Waits],
                                                                   case
                                                                       when (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                                                           then
                                                                           CAST((wd2.wait_time_ms - wd1.wait_time_ms) /
                                                                                (1.0 * (wd2.waiting_tasks_count - wd1.waiting_tasks_count)) as numeric(12, 1))
                                                                       else 0 end                                                              as [Avg ms Per Wait],
                                                                   N'https://www.sqlskills.com/help/waits/' + LOWER(wd1.wait_type) + '/'       as url
                                                            from max_batch b
                                                                     join #waitstats wd2 on
                                                                wd2.sampletime = b.sampletime
                                                                     join #waitstats wd1 on
                                                                    wd1.wait_type = wd2.wait_type and
                                                                    wd2.sampletime > wd1.sampletime
                                                                     cross apply (select SUM(1) as cpu_count
                                                                                  from sys.dm_os_schedulers
                                                                                  where status = 'VISIBLE ONLINE'
                                                                                    and is_online = 1) as cores
                                                                     cross apply (select CAST(
                                                                                                 (wd2.wait_time_ms - wd1.wait_time_ms) / 1000. as numeric(12, 1))               as [Wait Time (Seconds)],
                                                                                         CAST(
                                                                                                 (wd2.signal_wait_time_ms - wd1.signal_wait_time_ms) / 1000. as numeric(12, 1)) as [Signal Wait Time (Seconds)]) as c
                                                                     left outer join ##waitcategories wcat on wd1.wait_type = wcat.waittype
                                                            where (wd2.waiting_tasks_count - wd1.waiting_tasks_count) > 0
                                                              and wd2.wait_time_ms - wd1.wait_time_ms > 0
                                                            order by [Wait Time (Seconds)] desc;
                                                        end;

                                                    -------------------------
                                                    --What happened: #FileStats
                                                    -------------------------
                                                    with readstats as (
                                                        select 'PHYSICAL READS'                                        as pattern,
                                                               ROW_NUMBER() over (order by wd2.avg_stall_read_ms desc) as stallrank,
                                                               wd2.sampletime                                          as [Sample Time],
                                                               DATEDIFF(ss, wd1.sampletime, wd2.sampletime)            as [Sample (seconds)],
                                                               wd1.databasename,
                                                               wd1.filelogicalname                                     as [File Name],
                                                               UPPER(SUBSTRING(wd1.physicalname, 1, 2))                as [Drive],
                                                               wd1.sizeondiskmb,
                                                               (wd2.num_of_reads - wd1.num_of_reads)                   as [# Reads/Writes],
                                                               case
                                                                   when wd2.num_of_reads - wd1.num_of_reads > 0
                                                                       then CAST(
                                                                           (wd2.bytes_read - wd1.bytes_read) / 1024. / 1024. as numeric(21, 1))
                                                                   else 0
                                                                   end                                                 as [MB Read/Written],
                                                               wd2.avg_stall_read_ms                                   as [Avg Stall (ms)],
                                                               wd1.physicalname                                        as [file physical name]
                                                        from #filestats wd2
                                                                 join #filestats wd1 on wd2.sampletime > wd1.sampletime
                                                            and wd1.databaseid = wd2.databaseid
                                                            and wd1.fileid = wd2.fileid
                                                    ),
                                                         writestats as (
                                                             select 'PHYSICAL WRITES'                                        as pattern,
                                                                    ROW_NUMBER() over (order by wd2.avg_stall_write_ms desc) as stallrank,
                                                                    wd2.sampletime                                           as [Sample Time],
                                                                    DATEDIFF(ss, wd1.sampletime, wd2.sampletime)             as [Sample (seconds)],
                                                                    wd1.databasename,
                                                                    wd1.filelogicalname                                      as [File Name],
                                                                    UPPER(SUBSTRING(wd1.physicalname, 1, 2))                 as [Drive],
                                                                    wd1.sizeondiskmb,
                                                                    (wd2.num_of_writes - wd1.num_of_writes)                  as [# Reads/Writes],
                                                                    case
                                                                        when wd2.num_of_writes - wd1.num_of_writes > 0
                                                                            then CAST(
                                                                                (wd2.bytes_written - wd1.bytes_written) / 1024. / 1024. as numeric(21, 1))
                                                                        else 0
                                                                        end                                                  as [MB Read/Written],
                                                                    wd2.avg_stall_write_ms                                   as [Avg Stall (ms)],
                                                                    wd1.physicalname                                         as [file physical name]
                                                             from #filestats wd2
                                                                      join #filestats wd1
                                                                           on wd2.sampletime > wd1.sampletime
                                                                               and wd1.databaseid = wd2.databaseid
                                                                               and wd1.fileid = wd2.fileid
                                                         )
                                                    select pattern,
                                                           [Sample Time],
                                                           [Sample (seconds)],
                                                           [File Name],
                                                           [Drive],
                                                           [# Reads/Writes],
                                                           [MB Read/Written],
                                                           [Avg Stall (ms)],
                                                           [file physical name]
                                                    from readstats
                                                    where stallrank <= 5
                                                      and [MB Read/Written] > 0
                                                    union all
                                                    select pattern,
                                                           [Sample Time],
                                                           [Sample (seconds)],
                                                           [File Name],
                                                           [Drive],
                                                           [# Reads/Writes],
                                                           [MB Read/Written],
                                                           [Avg Stall (ms)],
                                                           [file physical name]
                                                    from writestats
                                                    where stallrank <= 5
                                                      and [MB Read/Written] > 0;


                                                    -------------------------
                                                    --What happened: #PerfmonStats
                                                    -------------------------

                                                    select 'PERFMON'                                           as pattern,
                                                           plast.[object_name],
                                                           plast.counter_name,
                                                           plast.instance_name,
                                                           pfirst.sampletime                                   as firstsampletime,
                                                           pfirst.cntr_value                                   as firstsamplevalue,
                                                           plast.sampletime                                    as lastsampletime,
                                                           plast.cntr_value                                    as lastsamplevalue,
                                                           plast.cntr_value - pfirst.cntr_value                as valuedelta,
                                                           ((1.0 * plast.cntr_value - pfirst.cntr_value) /
                                                            DATEDIFF(ss, pfirst.sampletime, plast.sampletime)) as valuepersecond
                                                    from #perfmonstats plast
                                                             inner join #perfmonstats pfirst
                                                                        on pfirst.[object_name] = plast.[object_name] and
                                                                           pfirst.counter_name = plast.counter_name and
                                                                           (pfirst.instance_name = plast.instance_name or
                                                                            (pfirst.instance_name is null and plast.instance_name is null))
                                                                            and plast.id > pfirst.id
                                                    where plast.cntr_value <> pfirst.cntr_value
                                                    order by pattern, plast.[object_name], plast.counter_name,
                                                             plast.instance_name;


                                                    -------------------------
                                                    --What happened: #QueryStats
                                                    -------------------------
                                                    if @checkprocedurecache = 1
                                                        begin

                                                            select qsnow.*, qsfirst.*
                                                            from #querystats qsnow
                                                                     inner join #querystats qsfirst
                                                                                on qsnow.[sql_handle] = qsfirst.[sql_handle] and
                                                                                   qsnow.statement_start_offset = qsfirst.statement_start_offset and
                                                                                   qsnow.statement_end_offset = qsfirst.statement_end_offset and
                                                                                   qsnow.plan_generation_num = qsfirst.plan_generation_num and
                                                                                   qsnow.plan_handle = qsfirst.plan_handle and
                                                                                   qsfirst.pass = 1
                                                            where qsnow.pass = 2;
                                                        end;
                                                    else
                                                        begin
                                                            select 'Plan Cache'                                                                 as [Pattern],
                                                                   'Plan cache not analyzed'                                                    as [Finding],
                                                                   'Use @CheckProcedureCache = 1 or run sp_BlitzCache for more analysis'        as [More Info],
                                                                   CONVERT(xml,
                                                                           @stockdetailsheader + 'firstresponderkit.org' + @stockdetailsfooter) as [Details];
                                                        end;
                                                end;

                    drop table #blitzfirstresults;

                    /* What's running right now? This is the first and last result set. */
                    if @sincestartup = 0 and @seconds > 0 and @expertmode = 1 and @outputtype <> 'NONE'
                        begin
                            if OBJECT_ID('master.dbo.sp_BlitzWho') is null and OBJECT_ID('dbo.sp_BlitzWho') is null
                                begin
                                    print N'sp_BlitzWho is not installed in the current database_files.  You can get a copy from http://FirstResponderKit.org';
                                end;
                            else
                                begin
                                    exec (@blitzwho);
                                end;
                        end; /* IF @SinceStartup = 0 AND @Seconds > 0 AND @ExpertMode = 1 AND @OutputType <> 'NONE'   -   What's running right now? This is the first and last result set. */

                end; /* IF @LogMessage IS NULL */
end; /* ELSE IF @OutputType = 'SCHEMA' */

    set nocount off;
go


/* How to run it:
EXEC dbo.sp_BlitzFirst

With extra diagnostic info:
EXEC dbo.sp_BlitzFirst @ExpertMode = 1;

Saving output to tables:
EXEC sp_BlitzFirst
  @OutputDatabaseName = 'DBAtools'
, @OutputSchemaName = 'dbo'
, @OutputTableName = 'BlitzFirst'
, @OutputTableNameFileStats = 'BlitzFirst_FileStats'
, @OutputTableNamePerfmonStats = 'BlitzFirst_PerfmonStats'
, @OutputTableNameWaitStats = 'BlitzFirst_WaitStats'
, @OutputTableNameBlitzCache = 'BlitzCache'
, @OutputTableNameBlitzWho = 'BlitzWho'
*/
