set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

if (
       select case
                  when CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')) like '8%' then 0
                  when CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')) like '9%' then 0
                  else 1
                  end
   ) = 0
    begin
        declare @msg varchar(8000);
        select @msg =
               'Sorry, sp_BlitzCache doesn''t work on versions of SQL prior to 2008.' + REPLICATE(CHAR(13), 7933);
        print @msg;
        return;
    end;

if OBJECT_ID('dbo.sp_BlitzCache') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzCache AS RETURN 0;');
go

if OBJECT_ID('dbo.sp_BlitzCache') is not null and OBJECT_ID('tempdb.dbo.##BlitzCacheProcs', 'U') is not null
    exec ('DROP TABLE ##BlitzCacheProcs;');
go

if OBJECT_ID('dbo.sp_BlitzCache') is not null and OBJECT_ID('tempdb.dbo.##BlitzCacheResults', 'U') is not null
    exec ('DROP TABLE ##BlitzCacheResults;');
go

create table ##blitzcacheresults
(
    spid int,
    id int identity (1,1),
    checkid int,
    priority tinyint,
    findingsgroup varchar(50),
    finding varchar(500),
    url varchar(200),
    details varchar(4000)
);

create table ##blitzcacheprocs
(
    spid int,
    querytype nvarchar(258),
    databasename sysname,
    averagecpu decimal(38, 4),
    averagecpuperminute decimal(38, 4),
    totalcpu decimal(38, 4),
    percentcpubytype money,
    percentcpu money,
    averageduration decimal(38, 4),
    totalduration decimal(38, 4),
    percentduration money,
    percentdurationbytype money,
    averagereads bigint,
    totalreads bigint,
    percentreads money,
    percentreadsbytype money,
    executioncount bigint,
    percentexecutions money,
    percentexecutionsbytype money,
    executionsperminute money,
    totalwrites bigint,
    averagewrites money,
    percentwrites money,
    percentwritesbytype money,
    writesperminute money,
    plancreationtime datetime,
    plancreationtimehours as DATEDIFF(hour, plancreationtime, SYSDATETIME()),
    lastexecutiontime datetime,
    lastcompletiontime datetime,
    planhandle varbinary(64),
    [Remove Plan Handle From Cache] as
        case
            when [PlanHandle] is not null
                then 'DBCC FREEPROCCACHE (' + CONVERT(varchar(128), [PlanHandle], 1) + ');'
            else 'N/A' end,
    sqlhandle varbinary(64),
    [Remove SQL Handle From Cache] as
        case
            when [SqlHandle] is not null
                then 'DBCC FREEPROCCACHE (' + CONVERT(varchar(128), [SqlHandle], 1) + ');'
            else 'N/A' end,
    [SQL Handle More Info] as
        case
            when [SqlHandle] is not null
                then 'EXEC sp_BlitzCache @OnlySqlHandles = ''' + CONVERT(varchar(128), [SqlHandle], 1) + '''; '
            else 'N/A' end,
    queryhash binary(8),
    [Query Hash More Info] as
        case
            when [QueryHash] is not null
                then 'EXEC sp_BlitzCache @OnlyQueryHashes = ''' + CONVERT(varchar(32), [QueryHash], 1) + '''; '
            else 'N/A' end,
    queryplanhash binary(8),
    statementstartoffset int,
    statementendoffset int,
    minreturnedrows bigint,
    maxreturnedrows bigint,
    averagereturnedrows money,
    totalreturnedrows bigint,
    lastreturnedrows bigint,
    /*The Memory Grant columns are only supported
		  in certain versions, giggle giggle.
		*/
    mingrantkb bigint,
    maxgrantkb bigint,
    minusedgrantkb bigint,
    maxusedgrantkb bigint,
    percentmemorygrantused money,
    avgmaxmemorygrant money,
    minspills bigint,
    maxspills bigint,
    totalspills bigint,
    avgspills money,
    querytext nvarchar(max),
    queryplan xml,
    /* these next four columns are the total for the type of query.
            don't actually use them for anything apart from math by type.
            */
    totalworkertimefortype bigint,
    totalelapsedtimefortype bigint,
    totalreadsfortype bigint,
    totalexecutioncountfortype bigint,
    totalwritesfortype bigint,
    numberofplans int,
    numberofdistinctplans int,
    serialdesiredmemory float,
    serialrequiredmemory float,
    cachedplansize float,
    compiletime float,
    compilecpu float,
    compilememory float,
    maxcompilememory float,
    min_worker_time bigint,
    max_worker_time bigint,
    is_forced_plan bit,
    is_forced_parameterized bit,
    is_cursor bit,
    is_optimistic_cursor bit,
    is_forward_only_cursor bit,
    is_fast_forward_cursor bit,
    is_cursor_dynamic bit,
    is_parallel bit,
    is_forced_serial bit,
    is_key_lookup_expensive bit,
    key_lookup_cost float,
    is_remote_query_expensive bit,
    remote_query_cost float,
    frequent_execution bit,
    parameter_sniffing bit,
    unparameterized_query bit,
    near_parallel bit,
    plan_warnings bit,
    plan_multiple_plans int,
    long_running bit,
    downlevel_estimator bit,
    implicit_conversions bit,
    busy_loops bit,
    tvf_join bit,
    tvf_estimate bit,
    compile_timeout bit,
    compile_memory_limit_exceeded bit,
    warning_no_join_predicate bit,
    queryplancost float,
    missing_index_count int,
    unmatched_index_count int,
    min_elapsed_time bigint,
    max_elapsed_time bigint,
    age_minutes money,
    age_minutes_lifetime money,
    is_trivial bit,
    trace_flags_session varchar(1000),
    is_unused_grant bit,
    function_count int,
    clr_function_count int,
    is_table_variable bit,
    no_stats_warning bit,
    relop_warnings bit,
    is_table_scan bit,
    backwards_scan bit,
    forced_index bit,
    forced_seek bit,
    forced_scan bit,
    columnstore_row_mode bit,
    is_computed_scalar bit,
    is_sort_expensive bit,
    sort_cost float,
    is_computed_filter bit,
    op_name varchar(100) null,
    index_insert_count int null,
    index_update_count int null,
    index_delete_count int null,
    cx_insert_count int null,
    cx_update_count int null,
    cx_delete_count int null,
    table_insert_count int null,
    table_update_count int null,
    table_delete_count int null,
    index_ops as (index_insert_count + index_update_count + index_delete_count +
                  cx_insert_count + cx_update_count + cx_delete_count +
                  table_insert_count + table_update_count + table_delete_count),
    is_row_level bit,
    is_spatial bit,
    index_dml bit,
    table_dml bit,
    long_running_low_cpu bit,
    low_cost_high_cpu bit,
    stale_stats bit,
    is_adaptive bit,
    index_spool_cost float,
    index_spool_rows float,
    table_spool_cost float,
    table_spool_rows float,
    is_spool_expensive bit,
    is_spool_more_rows bit,
    is_table_spool_expensive bit,
    is_table_spool_more_rows bit,
    estimated_rows float,
    is_bad_estimate bit,
    is_paul_white_electric bit,
    is_row_goal bit,
    is_big_spills bit,
    is_mstvf bit,
    is_mm_join bit,
    is_nonsargable bit,
    select_with_writes bit,
    implicit_conversion_info xml,
    cached_execution_parameters xml,
    missing_indexes xml,
    setoptions varchar(max),
    warnings varchar(max)
);
go

alter procedure dbo.sp_blitzcache @help bit = 0,
                                  @top int = null,
                                  @sortorder varchar(50) = 'CPU',
                                  @usetriggersanyway bit = null,
                                  @exporttoexcel bit = 0,
                                  @expertmode tinyint = 0,
                                  @outputservername nvarchar(258) = null,
                                  @outputdatabasename nvarchar(258) = null,
                                  @outputschemaname nvarchar(258) = null,
                                  @outputtablename nvarchar(258) = null,
                                  @configurationdatabasename nvarchar(128) = null,
                                  @configurationschemaname nvarchar(258) = null,
                                  @configurationtablename nvarchar(258) = null,
                                  @durationfilter decimal(38, 4) = null,
                                  @hidesummary bit = 0,
                                  @ignoresystemdbs bit = 1,
                                  @onlyqueryhashes varchar(max) = null,
                                  @ignorequeryhashes varchar(max) = null,
                                  @onlysqlhandles varchar(max) = null,
                                  @ignoresqlhandles varchar(max) = null,
                                  @queryfilter varchar(10) = 'ALL',
                                  @databasename nvarchar(128) = null,
                                  @storedprocname nvarchar(128) = null,
                                  @slowlysearchplansfor nvarchar(4000) = null,
                                  @reanalyze bit = 0,
                                  @skipanalysis bit = 0,
                                  @bringthepain bit = 0,
                                  @minimumexecutioncount int = 0,
                                  @debug bit = 0,
                                  @checkdateoverride datetimeoffset = null,
                                  @minutesback int = null,
                                  @version varchar(30) = null output,
                                  @versiondate datetime = null output,
                                  @versioncheckmode bit = 0
    with recompile
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
sp_BlitzCache from http://FirstResponderKit.org

This script displays your most resource-intensive queries from the plan cache,
and points to ways you can tune these queries to make them faster.


To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - This query will not run on SQL Server 2005.
 - SQL Server 2008 and 2008R2 have a bug in trigger stats, so that output is
   excluded by default.
 - @IgnoreQueryHashes and @OnlyQueryHashes require a CSV list of hashes
   with no spaces between the hash values.
 - @OutputServerName is not functional yet.

Unknown limitations of this version:
 - May or may not be vulnerable to the wick effect.

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

    declare @nl nvarchar(2) = NCHAR(13) + NCHAR(10);

    if @help = 1
        begin
            select N'@Help'                       as [Parameter Name],
                   N'BIT'                         as [Data Type],
                   N'Displays this help message.' as [Parameter Description]

            union all
            select N'@Top',
                   N'INT',
                   N'The number of records to retrieve and analyze from the plan cache. The following DMVs are used as the plan cache: dm_exec_query_stats, dm_exec_procedure_stats, dm_exec_trigger_stats.'

            union all
            select N'@SortOrder',
                   N'VARCHAR(10)',
                   N'Data processing and display order. @SortOrder will still be used, even when preparing output for a table or for excel. Possible values are: "CPU", "Reads", "Writes", "Duration", "Executions", "Recent Compilations", "Memory Grant", "Spills", "Query Hash". Additionally, the word "Average" or "Avg" can be used to sort on averages rather than total. "Executions per minute" and "Executions / minute" can be used to sort by execution per minute. For the truly lazy, "xpm" can also be used. Note that when you use all or all avg, the only parameters you can use are @Top and @DatabaseName. All others will be ignored.'

            union all
            select N'@UseTriggersAnyway',
                   N'BIT',
                   N'On SQL Server 2008R2 and earlier, trigger execution count is incorrect - trigger execution count is incremented once per execution of a SQL agent job. If you still want to see relative execution count of triggers, then you can force sp_BlitzCache to include this information.'

            union all
            select N'@ExportToExcel',
                   N'BIT',
                   N'Prepare output for exporting to Excel. Newlines and additional whitespace are removed from query text and the execution plan is not displayed.'

            union all
            select N'@ExpertMode',
                   N'TINYINT',
                   N'Default 0. When set to 1, results include more columns. When 2, mode is optimized for Opserver, the open source dashboard.'

            union all
            select N'@OutputDatabaseName',
                   N'NVARCHAR(128)',
                   N'The output database. If this does not exist SQL Server will divide by zero and everything will fall apart.'

            union all
            select N'@OutputSchemaName',
                   N'NVARCHAR(258)',
                   N'The output schema. If this does not exist SQL Server will divide by zero and everything will fall apart.'

            union all
            select N'@OutputTableName',
                   N'NVARCHAR(258)',
                   N'The output table. If this does not exist, it will be created for you.'

            union all
            select N'@DurationFilter',
                   N'DECIMAL(38,4)',
                   N'Excludes queries with an average duration (in seconds) less than @DurationFilter.'

            union all
            select N'@HideSummary',
                   N'BIT',
                   N'Hides the findings summary result set.'

            union all
            select N'@IgnoreSystemDBs',
                   N'BIT',
                   N'Ignores plans found in the system databases (master, model, msdb, tempdb, and resourcedb)'

            union all
            select N'@OnlyQueryHashes',
                   N'VARCHAR(MAX)',
                   N'A list of query hashes to query. All other query hashes will be ignored. Stored procedures and triggers will be ignored.'

            union all
            select N'@IgnoreQueryHashes',
                   N'VARCHAR(MAX)',
                   N'A list of query hashes to ignore.'

            union all
            select N'@OnlySqlHandles',
                   N'VARCHAR(MAX)',
                   N'One or more sql_handles to use for filtering results.'

            union all
            select N'@IgnoreSqlHandles',
                   N'VARCHAR(MAX)',
                   N'One or more sql_handles to ignore.'

            union all
            select N'@DatabaseName',
                   N'NVARCHAR(128)',
                   N'A database name which is used for filtering results.'

            union all
            select N'@StoredProcName',
                   N'NVARCHAR(128)',
                   N'Name of stored procedure you want to find plans for.'

            union all
            select N'@SlowlySearchPlansFor',
                   N'NVARCHAR(4000)',
                   N'String to search for in plan text. % wildcards allowed.'

            union all
            select N'@BringThePain',
                   N'BIT',
                   N'When using @SortOrder = ''all'' and @Top > 10, we require you to set @BringThePain = 1 so you understand that sp_BlitzCache will take a while to run.'

            union all
            select N'@QueryFilter',
                   N'VARCHAR(10)',
                   N'Filter out stored procedures or statements. The default value is ''ALL''. Allowed values are ''procedures'', ''statements'', ''functions'', or ''all'' (any variation in capitalization is acceptable).'

            union all
            select N'@Reanalyze',
                   N'BIT',
                   N'The default is 0. When set to 0, sp_BlitzCache will re-evalute the plan cache. Set this to 1 to reanalyze existing results'

            union all
            select N'@MinimumExecutionCount',
                   N'INT',
                   N'Queries with fewer than this number of executions will be omitted from results.'

            union all
            select N'@Debug',
                   N'BIT',
                   N'Setting this to 1 will print dynamic SQL and select data from all tables used.'

            union all
            select N'@MinutesBack',
                   N'INT',
                   N'How many minutes back to begin plan cache analysis. If you put in a positive number, we''ll flip it to negtive.';


            /* Column definitions */
            select N'# Executions'                                                                                                                                      as [Column Name],
                   N'BIGINT'                                                                                                                                            as [Data Type],
                   N'The number of executions of this particular query. This is computed across statements, procedures, and triggers and aggregated by the SQL handle.' as [Column Description]

            union all
            select N'Executions / Minute',
                   N'MONEY',
                   N'Number of executions per minute - calculated for the life of the current plan. Plan life is the last execution time minus the plan creation time.'

            union all
            select N'Execution Weight',
                   N'MONEY',
                   N'An arbitrary metric of total "execution-ness". A weight of 2 is "one more" than a weight of 1.'

            union all
            select N'Database',
                   N'sysname',
                   N'The name of the database where the plan was encountered. If the database name cannot be determined for some reason, a value of NA will be substituted. A value of 32767 indicates the plan comes from ResourceDB.'

            union all
            select N'Total CPU',
                   N'BIGINT',
                   N'Total CPU time, reported in milliseconds, that was consumed by all executions of this query since the last compilation.'

            union all
            select N'Avg CPU',
                   N'BIGINT',
                   N'Average CPU time, reported in milliseconds, consumed by each execution of this query since the last compilation.'

            union all
            select N'CPU Weight',
                   N'MONEY',
                   N'An arbitrary metric of total "CPU-ness". A weight of 2 is "one more" than a weight of 1.'

            union all
            select N'Total Duration',
                   N'BIGINT',
                   N'Total elapsed time, reported in milliseconds, consumed by all executions of this query since last compilation.'

            union all
            select N'Avg Duration',
                   N'BIGINT',
                   N'Average elapsed time, reported in milliseconds, consumed by each execution of this query since the last compilation.'

            union all
            select N'Duration Weight',
                   N'MONEY',
                   N'An arbitrary metric of total "Duration-ness". A weight of 2 is "one more" than a weight of 1.'

            union all
            select N'Total Reads',
                   N'BIGINT',
                   N'Total logical reads performed by this query since last compilation.'

            union all
            select N'Average Reads',
                   N'BIGINT',
                   N'Average logical reads performed by each execution of this query since the last compilation.'

            union all
            select N'Read Weight',
                   N'MONEY',
                   N'An arbitrary metric of "Read-ness". A weight of 2 is "one more" than a weight of 1.'

            union all
            select N'Total Writes',
                   N'BIGINT',
                   N'Total logical writes performed by this query since last compilation.'

            union all
            select N'Average Writes',
                   N'BIGINT',
                   N'Average logical writes performed by each execution this query since last compilation.'

            union all
            select N'Write Weight',
                   N'MONEY',
                   N'An arbitrary metric of "Write-ness". A weight of 2 is "one more" than a weight of 1.'

            union all
            select N'Query Type',
                   N'NVARCHAR(258)',
                   N'The type of query being examined. This can be "Procedure", "Statement", or "Trigger".'

            union all
            select N'Query Text',
                   N'NVARCHAR(4000)',
                   N'The text of the query. This may be truncated by either SQL Server or by sp_BlitzCache(tm) for display purposes.'

            union all
            select N'% Executions (Type)',
                   N'MONEY',
                   N'Percent of executions relative to the type of query - e.g. 17.2% of all stored procedure executions.'

            union all
            select N'% CPU (Type)',
                   N'MONEY',
                   N'Percent of CPU time consumed by this query for a given type of query - e.g. 22% of CPU of all stored procedures executed.'

            union all
            select N'% Duration (Type)',
                   N'MONEY',
                   N'Percent of elapsed time consumed by this query for a given type of query - e.g. 12% of all statements executed.'

            union all
            select N'% Reads (Type)',
                   N'MONEY',
                   N'Percent of reads consumed by this query for a given type of query - e.g. 34.2% of all stored procedures executed.'

            union all
            select N'% Writes (Type)',
                   N'MONEY',
                   N'Percent of writes performed by this query for a given type of query - e.g. 43.2% of all statements executed.'

            union all
            select N'Total Rows',
                   N'BIGINT',
                   N'Total number of rows returned for all executions of this query. This only applies to query level stats, not stored procedures or triggers.'

            union all
            select N'Average Rows',
                   N'MONEY',
                   N'Average number of rows returned by each execution of the query.'

            union all
            select N'Min Rows',
                   N'BIGINT',
                   N'The minimum number of rows returned by any execution of this query.'

            union all
            select N'Max Rows',
                   N'BIGINT',
                   N'The maximum number of rows returned by any execution of this query.'

            union all
            select N'MinGrantKB',
                   N'BIGINT',
                   N'The minimum memory grant the query received in kb.'

            union all
            select N'MaxGrantKB',
                   N'BIGINT',
                   N'The maximum memory grant the query received in kb.'

            union all
            select N'MinUsedGrantKB',
                   N'BIGINT',
                   N'The minimum used memory grant the query received in kb.'

            union all
            select N'MaxUsedGrantKB',
                   N'BIGINT',
                   N'The maximum used memory grant the query received in kb.'

            union all
            select N'MinSpills',
                   N'BIGINT',
                   N'The minimum amount this query has spilled to tempdb in 8k pages.'

            union all
            select N'MaxSpills',
                   N'BIGINT',
                   N'The maximum amount this query has spilled to tempdb in 8k pages.'

            union all
            select N'TotalSpills',
                   N'BIGINT',
                   N'The total amount this query has spilled to tempdb in 8k pages.'

            union all
            select N'AvgSpills',
                   N'BIGINT',
                   N'The average amount this query has spilled to tempdb in 8k pages.'

            union all
            select N'PercentMemoryGrantUsed',
                   N'MONEY',
                   N'Result of dividing the maximum grant used by the minimum granted.'

            union all
            select N'AvgMaxMemoryGrant',
                   N'MONEY',
                   N'The average maximum memory grant for a query.'

            union all
            select N'# Plans',
                   N'INT',
                   N'The total number of execution plans found that match a given query.'

            union all
            select N'# Distinct Plans',
                   N'INT',
                   N'The number of distinct execution plans that match a given query. '
                       + NCHAR(13) + NCHAR(10)
                       +
                   N'This may be caused by running the same query across multiple databases or because of a lack of proper parameterization in the database.'

            union all
            select N'Created At',
                   N'DATETIME',
                   N'Time that the execution plan was last compiled.'

            union all
            select N'Last Execution',
                   N'DATETIME',
                   N'The last time that this query was executed.'

            union all
            select N'Query Plan',
                   N'XML',
                   N'The query plan. Click to display a graphical plan or, if you need to patch SSMS, a pile of XML.'

            union all
            select N'Plan Handle',
                   N'VARBINARY(64)',
                   N'An arbitrary identifier referring to the compiled plan this query is a part of.'

            union all
            select N'SQL Handle',
                   N'VARBINARY(64)',
                   N'An arbitrary identifier referring to a batch or stored procedure that this query is a part of.'

            union all
            select N'Query Hash',
                   N'BINARY(8)',
                   N'A hash of the query. Queries with the same query hash have similar logic but only differ by literal values or database.'

            union all
            select N'Warnings',
                   N'VARCHAR(MAX)',
                   N'A list of individual warnings generated by this query.';


            /* Configuration table description */
            select N'Frequent Execution Threshold'                                                      as [Configuration Parameter],
                   N'100'                                                                               as [Default Value],
                   N'Executions / Minute'                                                               as [Unit of Measure],
                   N'Executions / Minute before a "Frequent Execution Threshold" warning is triggered.' as [Description]

            union all
            select N'Parameter Sniffing Variance Percent',
                   N'30',
                   N'Percent',
                   N'Variance required between min/max values and average values before a "Parameter Sniffing" warning is triggered. Applies to worker time and returned rows.'

            union all
            select N'Parameter Sniffing IO Threshold',
                   N'100,000',
                   N'Logical reads',
                   N'Minimum number of average logical reads before parameter sniffing checks are evaluated.'

            union all
            select N'Cost Threshold for Parallelism Warning' as [Configuration Parameter],
                   N'10',
                   N'Percent',
                   N'Trigger a "Nearly Parallel" warning when a query''s cost is within X percent of the cost threshold for parallelism.'

            union all
            select N'Long Running Query Warning' as [Configuration Parameter],
                   N'300',
                   N'Seconds',
                   N'Triggers a "Long Running Query Warning" when average duration, max CPU time, or max clock time is higher than this number.'

            union all
            select N'Unused Memory Grant Warning' as [Configuration Parameter],
                   N'10',
                   N'Percent',
                   N'Triggers an "Unused Memory Grant Warning" when a query uses >= X percent of its memory grant.';
            return;
        end;

/*Validate version*/
    if (
           select case
                      when CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')) like '8%' then 0
                      when CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')) like '9%' then 0
                      else 1
                      end
       ) = 0
        begin
            declare @version_msg varchar(8000);
            select @version_msg =
                   'Sorry, sp_BlitzCache doesn''t work on versions of SQL prior to 2008.' + REPLICATE(CHAR(13), 7933);
            print @version_msg;
            return;
        end;

/* If they want to sort by query hash, populate the @OnlyQueryHashes list for them */
    if @sortorder like 'query hash%'
        begin
            raiserror ('Beginning query hash sort', 0, 1) with nowait;

            select qs.query_hash,
                   MAX(qs.max_worker_time) as max_worker_time,
                   COUNT_BIG(*)            as records
            into #query_hash_grouped
            from sys.dm_exec_query_stats as qs
                     cross apply (select pa.value
                                  from sys.dm_exec_plan_attributes(qs.plan_handle) as pa
                                  where pa.attribute = 'dbid') as ca
            group by qs.query_hash, ca.value
            having COUNT_BIG(*) > 1
            order by max_worker_time desc,
                     records desc;

            select top (1) @onlyqueryhashes = STUFF((select distinct N',' + CONVERT(nvarchar(max), qhg.query_hash, 1)
                                                     from #query_hash_grouped as qhg
                                                     where qhg.query_hash <> 0x00
                                                     for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1,
                                                    N'')
            option (recompile);

            /* When they ran it, @SortOrder probably looked like 'query hash, cpu', so strip the first sort order out: */
            select @sortorder = LTRIM(REPLACE(REPLACE(@sortorder, 'query hash', ''), ',', ''));

            /* If they just called it with @SortOrder = 'query hash', set it to 'cpu' for backwards compatibility: */
            if @sortorder = '' set @sortorder = 'cpu';

        end


/* Set @Top based on sort */
    if (
            @top is null
            and LOWER(@sortorder) in ('all', 'all sort')
        )
        begin
            set @top = 5;
        end;

    if (
            @top is null
            and LOWER(@sortorder) not in ('all', 'all sort')
        )
        begin
            set @top = 10;
        end;

/* validate user inputs */
    if @top is null
        or @sortorder is null
        or @queryfilter is null
        or @reanalyze is null
        begin
            raiserror (N'Several parameters (@Top, @SortOrder, @QueryFilter, @renalyze) are required. Do not set them to NULL. Please try again.', 16, 1) with nowait;
            return;
        end;

    raiserror (N'Checking @MinutesBack validity.', 0, 1) with nowait;
    if @minutesback is not null
        begin
            if @minutesback > 0
                begin
                    raiserror (N'Setting @MinutesBack to a negative number', 0, 1) with nowait;
                    set @minutesback *= -1;
                end;
            if @minutesback = 0
                begin
                    raiserror (N'@MinutesBack can''t be 0, setting to -1', 0, 1) with nowait;
                    set @minutesback = -1;
                end;
        end;


    raiserror (N'Creating temp tables for results and warnings.', 0, 1) with nowait;


    if OBJECT_ID('tempdb.dbo.##BlitzCacheResults') is null
        begin
            create table ##blitzcacheresults
            (
                spid int,
                id int identity (1,1),
                checkid int,
                priority tinyint,
                findingsgroup varchar(50),
                finding varchar(500),
                url varchar(200),
                details varchar(4000)
            );
        end;

    if OBJECT_ID('tempdb.dbo.##BlitzCacheProcs') is null
        begin
            create table ##blitzcacheprocs
            (
                spid int,
                querytype nvarchar(258),
                databasename sysname,
                averagecpu decimal(38, 4),
                averagecpuperminute decimal(38, 4),
                totalcpu decimal(38, 4),
                percentcpubytype money,
                percentcpu money,
                averageduration decimal(38, 4),
                totalduration decimal(38, 4),
                percentduration money,
                percentdurationbytype money,
                averagereads bigint,
                totalreads bigint,
                percentreads money,
                percentreadsbytype money,
                executioncount bigint,
                percentexecutions money,
                percentexecutionsbytype money,
                executionsperminute money,
                totalwrites bigint,
                averagewrites money,
                percentwrites money,
                percentwritesbytype money,
                writesperminute money,
                plancreationtime datetime,
                plancreationtimehours as DATEDIFF(hour, plancreationtime, SYSDATETIME()),
                lastexecutiontime datetime,
                lastcompletiontime datetime,
                planhandle varbinary(64),
                [Remove Plan Handle From Cache] as
                    case
                        when [PlanHandle] is not null
                            then 'DBCC FREEPROCCACHE (' + CONVERT(varchar(128), [PlanHandle], 1) + ');'
                        else 'N/A' end,
                sqlhandle varbinary(64),
                [Remove SQL Handle From Cache] as
                    case
                        when [SqlHandle] is not null
                            then 'DBCC FREEPROCCACHE (' + CONVERT(varchar(128), [SqlHandle], 1) + ');'
                        else 'N/A' end,
                [SQL Handle More Info] as
                    case
                        when [SqlHandle] is not null
                            then 'EXEC sp_BlitzCache @OnlySqlHandles = ''' + CONVERT(varchar(128), [SqlHandle], 1) +
                                 '''; '
                        else 'N/A' end,
                queryhash binary(8),
                [Query Hash More Info] as
                    case
                        when [QueryHash] is not null
                            then 'EXEC sp_BlitzCache @OnlyQueryHashes = ''' + CONVERT(varchar(32), [QueryHash], 1) +
                                 '''; '
                        else 'N/A' end,
                queryplanhash binary(8),
                statementstartoffset int,
                statementendoffset int,
                minreturnedrows bigint,
                maxreturnedrows bigint,
                averagereturnedrows money,
                totalreturnedrows bigint,
                lastreturnedrows bigint,
                mingrantkb bigint,
                maxgrantkb bigint,
                minusedgrantkb bigint,
                maxusedgrantkb bigint,
                percentmemorygrantused money,
                avgmaxmemorygrant money,
                minspills bigint,
                maxspills bigint,
                totalspills bigint,
                avgspills money,
                querytext nvarchar(max),
                queryplan xml,
                /* these next four columns are the total for the type of query.
            don't actually use them for anything apart from math by type.
            */
                totalworkertimefortype bigint,
                totalelapsedtimefortype bigint,
                totalreadsfortype bigint,
                totalexecutioncountfortype bigint,
                totalwritesfortype bigint,
                numberofplans int,
                numberofdistinctplans int,
                serialdesiredmemory float,
                serialrequiredmemory float,
                cachedplansize float,
                compiletime float,
                compilecpu float,
                compilememory float,
                maxcompilememory float,
                min_worker_time bigint,
                max_worker_time bigint,
                is_forced_plan bit,
                is_forced_parameterized bit,
                is_cursor bit,
                is_optimistic_cursor bit,
                is_forward_only_cursor bit,
                is_fast_forward_cursor bit,
                is_cursor_dynamic bit,
                is_parallel bit,
                is_forced_serial bit,
                is_key_lookup_expensive bit,
                key_lookup_cost float,
                is_remote_query_expensive bit,
                remote_query_cost float,
                frequent_execution bit,
                parameter_sniffing bit,
                unparameterized_query bit,
                near_parallel bit,
                plan_warnings bit,
                plan_multiple_plans int,
                long_running bit,
                downlevel_estimator bit,
                implicit_conversions bit,
                busy_loops bit,
                tvf_join bit,
                tvf_estimate bit,
                compile_timeout bit,
                compile_memory_limit_exceeded bit,
                warning_no_join_predicate bit,
                queryplancost float,
                missing_index_count int,
                unmatched_index_count int,
                min_elapsed_time bigint,
                max_elapsed_time bigint,
                age_minutes money,
                age_minutes_lifetime money,
                is_trivial bit,
                trace_flags_session varchar(1000),
                is_unused_grant bit,
                function_count int,
                clr_function_count int,
                is_table_variable bit,
                no_stats_warning bit,
                relop_warnings bit,
                is_table_scan bit,
                backwards_scan bit,
                forced_index bit,
                forced_seek bit,
                forced_scan bit,
                columnstore_row_mode bit,
                is_computed_scalar bit,
                is_sort_expensive bit,
                sort_cost float,
                is_computed_filter bit,
                op_name varchar(100) null,
                index_insert_count int null,
                index_update_count int null,
                index_delete_count int null,
                cx_insert_count int null,
                cx_update_count int null,
                cx_delete_count int null,
                table_insert_count int null,
                table_update_count int null,
                table_delete_count int null,
                index_ops as (index_insert_count + index_update_count + index_delete_count +
                              cx_insert_count + cx_update_count + cx_delete_count +
                              table_insert_count + table_update_count + table_delete_count),
                is_row_level bit,
                is_spatial bit,
                index_dml bit,
                table_dml bit,
                long_running_low_cpu bit,
                low_cost_high_cpu bit,
                stale_stats bit,
                is_adaptive bit,
                index_spool_cost float,
                index_spool_rows float,
                table_spool_cost float,
                table_spool_rows float,
                is_spool_expensive bit,
                is_spool_more_rows bit,
                is_table_spool_expensive bit,
                is_table_spool_more_rows bit,
                estimated_rows float,
                is_bad_estimate bit,
                is_paul_white_electric bit,
                is_row_goal bit,
                is_big_spills bit,
                is_mstvf bit,
                is_mm_join bit,
                is_nonsargable bit,
                select_with_writes bit,
                implicit_conversion_info xml,
                cached_execution_parameters xml,
                missing_indexes xml,
                setoptions varchar(max),
                warnings varchar(max)
            );
        end;

    declare @durationfilter_i int,
        @minmemoryperquery int,
        @msg nvarchar(4000),
        @noobsaibot bit = 0,
        @versionshowsairquoteactualplans bit,
        @objectfullname nvarchar(2000),
        @user_perm_sql nvarchar(max) = N'',
        @user_perm_gb_out decimal(10, 2),
        @common_version decimal(10, 2),
        @buffer_pool_memory_gb decimal(10, 2),
        @user_perm_percent decimal(10, 2),
        @is_tokenstore_big bit = 0;


    if @sortorder = 'sp_BlitzIndex'
        begin
            raiserror (N'OUTSTANDING!', 0, 1) with nowait;
            set @sortorder = 'reads';
            set @noobsaibot = 1;

        end


/* Change duration from seconds to milliseconds */
    if @durationfilter is not null
        begin
            raiserror (N'Converting Duration Filter to milliseconds', 0, 1) with nowait;
            set @durationfilter_i = CAST((@durationfilter * 1000.0) as int);
        end;

    raiserror (N'Checking database validity', 0, 1) with nowait;
    set @databasename = LTRIM(RTRIM(@databasename));

    if SERVERPROPERTY('EngineEdition') in (5, 6) and DB_NAME() <> @databasename
        begin
            raiserror ('You specified a database name other than the current database, but Azure SQL DB does not allow you to change databases. Execute sp_BlitzCache from the database you want to analyze.', 16, 1);
            return;
        end;
    if (DB_ID(@databasename)) is null and @databasename <> N''
        begin
            raiserror ('The database you specified does not exist. Please check the name and try again.', 16, 1);
            return;
        end;
    if (select DATABASEPROPERTYEX(ISNULL(@databasename, 'master'), 'Collation')) is null and
       SERVERPROPERTY('EngineEdition') not in (5, 6, 8)
        begin
            raiserror ('The database you specified is not readable. Please check the name and try again. Better yet, check your server.', 16, 1);
            return;
        end;

    select @minmemoryperquery = CONVERT(int, c.value)
    from sys.configurations as c
    where c.name = 'min memory per query (KB)';

    set @sortorder = LOWER(@sortorder);
    set @sortorder = REPLACE(REPLACE(@sortorder, 'average', 'avg'), '.', '');

    set @sortorder = case
                         when @sortorder in ('executions per minute', 'execution per minute', 'executions / minute',
                                             'execution / minute', 'xpm') then 'avg executions'
                         when @sortorder in ('recent compilations', 'recent compilation', 'compile') then 'compiles'
                         when @sortorder in ('read') then 'reads'
                         when @sortorder in ('avg read') then 'avg reads'
                         when @sortorder in ('write') then 'writes'
                         when @sortorder in ('avg write') then 'avg writes'
                         when @sortorder in ('memory grants') then 'memory grant'
                         when @sortorder in ('avg memory grants') then 'avg memory grant'
                         when @sortorder in ('spill') then 'spills'
                         when @sortorder in ('avg spill') then 'avg spills'
                         when @sortorder in ('execution') then 'executions'
                         else @sortorder end

    raiserror (N'Checking sort order', 0, 1) with nowait;
    if @sortorder not in ('cpu', 'avg cpu', 'reads', 'avg reads', 'writes', 'avg writes',
                          'duration', 'avg duration', 'executions', 'avg executions',
                          'compiles', 'memory grant', 'avg memory grant',
                          'spills', 'avg spills', 'all', 'all avg', 'sp_BlitzIndex',
                          'query hash')
        begin
            raiserror (N'Invalid sort order chosen, reverting to cpu', 16, 1) with nowait;
            set @sortorder = 'cpu';
        end;

    set @queryfilter = LOWER(@queryfilter);

    if LEFT(@queryfilter, 3) not in ('all', 'sta', 'pro', 'fun')
        begin
            raiserror (N'Invalid query filter chosen. Reverting to all.', 0, 1) with nowait;
            set @queryfilter = 'all';
        end;

    if @skipanalysis = 1
        begin
            raiserror (N'Skip Analysis set to 1, hiding Summary', 0, 1) with nowait;
            set @hidesummary = 1;
        end;

    declare @allsortsql nvarchar(max) = N'';
    declare @versionshowsmemorygrants bit;
    if EXISTS(select *
              from sys.all_columns
              where object_id = OBJECT_ID('sys.dm_exec_query_stats') and name = 'max_grant_kb')
        set @versionshowsmemorygrants = 1;
    else
        set @versionshowsmemorygrants = 0;

    declare @versionshowsspills bit;
    if EXISTS(select *
              from sys.all_columns
              where object_id = OBJECT_ID('sys.dm_exec_query_stats') and name = 'max_spills')
        set @versionshowsspills = 1;
    else
        set @versionshowsspills = 0;

    if EXISTS(select *
              from sys.all_columns
              where object_id = OBJECT_ID('sys.dm_exec_query_plan_stats') and name = 'query_plan')
        set @versionshowsairquoteactualplans = 1;
    else
        set @versionshowsairquoteactualplans = 0;

    if @reanalyze = 1 and OBJECT_ID('tempdb..##BlitzCacheResults') is null
        begin
            raiserror (N'##BlitzCacheResults does not exist, can''t reanalyze', 0, 1) with nowait;
            set @reanalyze = 0;
        end;

    if @reanalyze = 0
        begin
            raiserror (N'Cleaning up old warnings for your SPID', 0, 1) with nowait;
            delete ##blitzcacheresults
            where spid = @@SPID
            option (recompile);
            raiserror (N'Cleaning up old plans for your SPID', 0, 1) with nowait;
            delete ##blitzcacheprocs
            where spid = @@SPID
            option (recompile);
        end;

    if @reanalyze = 1
        begin
            raiserror (N'Reanalyzing current data, skipping to results', 0, 1) with nowait;
            goto results;
        end;


    if @sortorder in ('all', 'all avg')
        begin
            raiserror (N'Checking all sort orders, please be patient', 0, 1) with nowait;
            goto allsorts;
        end;

    raiserror (N'Creating temp tables for internal processing', 0, 1) with nowait;
    if OBJECT_ID('tempdb..#only_query_hashes') is not null
        drop table #only_query_hashes ;

    if OBJECT_ID('tempdb..#ignore_query_hashes') is not null
        drop table #ignore_query_hashes ;

    if OBJECT_ID('tempdb..#only_sql_handles') is not null
        drop table #only_sql_handles ;

    if OBJECT_ID('tempdb..#ignore_sql_handles') is not null
        drop table #ignore_sql_handles ;

    if OBJECT_ID('tempdb..#p') is not null
        drop table #p;

    if OBJECT_ID('tempdb..#checkversion') is not null
        drop table #checkversion;

    if OBJECT_ID('tempdb..#configuration') is not null
        drop table #configuration;

    if OBJECT_ID('tempdb..#stored_proc_info') is not null
        drop table #stored_proc_info;

    if OBJECT_ID('tempdb..#plan_creation') is not null
        drop table #plan_creation;

    if OBJECT_ID('tempdb..#est_rows') is not null
        drop table #est_rows;

    if OBJECT_ID('tempdb..#plan_cost') is not null
        drop table #plan_cost;

    if OBJECT_ID('tempdb..#proc_costs') is not null
        drop table #proc_costs;

    if OBJECT_ID('tempdb..#stats_agg') is not null
        drop table #stats_agg;

    if OBJECT_ID('tempdb..#trace_flags') is not null
        drop table #trace_flags;

    if OBJECT_ID('tempdb..#variable_info') is not null
        drop table #variable_info;

    if OBJECT_ID('tempdb..#conversion_info') is not null
        drop table #conversion_info;

    if OBJECT_ID('tempdb..#missing_index_xml') is not null
        drop table #missing_index_xml;

    if OBJECT_ID('tempdb..#missing_index_schema') is not null
        drop table #missing_index_schema;

    if OBJECT_ID('tempdb..#missing_index_usage') is not null
        drop table #missing_index_usage;

    if OBJECT_ID('tempdb..#missing_index_detail') is not null
        drop table #missing_index_detail;

    if OBJECT_ID('tempdb..#missing_index_pretty') is not null
        drop table #missing_index_pretty;

    if OBJECT_ID('tempdb..#index_spool_ugly') is not null
        drop table #index_spool_ugly;

    if OBJECT_ID('tempdb..#ReadableDBs') is not null
        drop table #readabledbs;

    if OBJECT_ID('tempdb..#plan_usage') is not null
        drop table #plan_usage;

    create table #only_query_hashes
    (
        query_hash binary(8)
    );

    create table #ignore_query_hashes
    (
        query_hash binary(8)
    );

    create table #only_sql_handles
    (
        sql_handle varbinary(64)
    );

    create table #ignore_sql_handles
    (
        sql_handle varbinary(64)
    );

    create table #p
    (
        sqlhandle varbinary(64),
        totalcpu bigint,
        totalduration bigint,
        totalreads bigint,
        totalwrites bigint,
        executioncount bigint
    );

    create table #checkversion
    (
        version nvarchar(128),
        common_version as SUBSTRING(version, 1, CHARINDEX('.', version) + 1),
        major as PARSENAME(CONVERT(varchar(32), version), 4),
        minor as PARSENAME(CONVERT(varchar(32), version), 3),
        build as PARSENAME(CONVERT(varchar(32), version), 2),
        revision as PARSENAME(CONVERT(varchar(32), version), 1)
    );

    create table #configuration
    (
        parameter_name varchar(100),
        value decimal(38, 0)
    );

    create table #plan_creation
    (
        percent_24 decimal(5, 2),
        percent_4 decimal(5, 2),
        percent_1 decimal(5, 2),
        total_plans int,
        spid int
    );

    create table #est_rows
    (
        queryhash binary(8),
        estimated_rows float
    );

    create table #plan_cost
    (
        queryplancost float,
        sqlhandle varbinary(64),
        planhandle varbinary(64),
        queryhash binary(8),
        queryplanhash binary(8)
    );

    create table #proc_costs
    (
        plantotalquery float,
        planhandle varbinary(64),
        sqlhandle varbinary(64)
    );

    create table #stats_agg
    (
        sqlhandle varbinary(64),
        lastupdate datetime2(7),
        modificationcount bigint,
        samplingpercent float,
        [Statistics] nvarchar(258),
        [Table] nvarchar(258),
        [Schema] nvarchar(258),
        [Database] nvarchar(258),
    );

    create table #trace_flags
    (
        sqlhandle varbinary(64),
        queryhash binary(8),
        global_trace_flags varchar(1000),
        session_trace_flags varchar(1000)
    );

    create table #stored_proc_info
    (
        spid int,
        sqlhandle varbinary(64),
        queryhash binary(8),
        variable_name nvarchar(258),
        variable_datatype nvarchar(258),
        converted_column_name nvarchar(258),
        compile_time_value nvarchar(258),
        proc_name nvarchar(1000),
        column_name nvarchar(4000),
        converted_to nvarchar(258),
        set_options nvarchar(1000)
    );

    create table #variable_info
    (
        spid int,
        queryhash binary(8),
        sqlhandle varbinary(64),
        proc_name nvarchar(1000),
        variable_name nvarchar(258),
        variable_datatype nvarchar(258),
        compile_time_value nvarchar(258)
    );

    create table #conversion_info
    (
        spid int,
        queryhash binary(8),
        sqlhandle varbinary(64),
        proc_name nvarchar(258),
        expression nvarchar(4000),
        at_charindex as CHARINDEX('@', expression),
        bracket_charindex as CHARINDEX(']', expression, CHARINDEX('@', expression)) - CHARINDEX('@', expression),
        comma_charindex as CHARINDEX(',', expression) + 1,
        second_comma_charindex as
                CHARINDEX(',', expression, CHARINDEX(',', expression) + 1) - CHARINDEX(',', expression) - 1,
        equal_charindex as CHARINDEX('=', expression) + 1,
        paren_charindex as CHARINDEX('(', expression) + 1,
        comma_paren_charindex as
                CHARINDEX(',', expression, CHARINDEX('(', expression) + 1) - CHARINDEX('(', expression) - 1,
        convert_implicit_charindex as CHARINDEX('=CONVERT_IMPLICIT', expression)
    );


    create table #missing_index_xml
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        index_xml xml
    );


    create table #missing_index_schema
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        index_xml xml
    );


    create table #missing_index_usage
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        usage nvarchar(128),
        index_xml xml
    );


    create table #missing_index_detail
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        usage nvarchar(128),
        column_name nvarchar(128)
    );


    create table #missing_index_pretty
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        equality nvarchar(max),
        inequality nvarchar(max),
        [include] nvarchar(max),
        executions nvarchar(128),
        query_cost nvarchar(128),
        creation_hours nvarchar(128),
        is_spool bit,
        details as N'/* '
            + CHAR(10)
            + case is_spool
                  when 0
                      then N'The Query Processor estimates that implementing the '
                  else N'We estimate that implementing the '
                       end
            + N'following index could improve query cost (' + query_cost + N')'
            + CHAR(10)
            + N'by '
            + CONVERT(nvarchar(30), impact)
            + N'% for ' + executions + N' executions of the query'
            + N' over the last ' +
                   case
                       when creation_hours < 24
                           then creation_hours + N' hours.'
                       when creation_hours = 24
                           then ' 1 day.'
                       when creation_hours > 24
                           then (CONVERT(nvarchar(128), creation_hours / 24)) + N' days.'
                       else N''
                       end
            + CHAR(10)
            + N'*/'
            + CHAR(10) + CHAR(13)
            + N'/* '
            + CHAR(10)
            + N'USE '
            + database_name
            + CHAR(10)
            + N'GO'
            + CHAR(10) + CHAR(13)
            + N'CREATE NONCLUSTERED INDEX ix_'
            + ISNULL(REPLACE(REPLACE(REPLACE(equality, '[', ''), ']', ''), ', ', '_'), '')
            + ISNULL(REPLACE(REPLACE(REPLACE(inequality, '[', ''), ']', ''), ', ', '_'), '')
            + case when [include] is not null then + N'_Includes' else N'' end
            + CHAR(10)
            + N' ON '
            + schema_name
            + N'.'
            + table_name
            + N' (' +
                   + case
                         when equality is not null
                             then equality
                             + case
                                   when inequality is not null
                                       then N', ' + inequality
                                   else N''
                                      end
                         else inequality
                       end
            + N')'
            + CHAR(10)
            + case
                  when include is not null
                      then N'INCLUDE (' + include +
                           N') WITH (FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?);'
                  else N' WITH (FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?);'
                       end
            + CHAR(10)
            + N'GO'
            + CHAR(10)
            + N'*/'
    );


    create table #index_spool_ugly
    (
        queryhash binary(8),
        sqlhandle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        equality nvarchar(max),
        inequality nvarchar(max),
        [include] nvarchar(max),
        executions nvarchar(128),
        query_cost nvarchar(128),
        creation_hours nvarchar(128)
    );


    create table #readabledbs
    (
        database_id int
    );


    create table #plan_usage
    (
        duplicate_plan_handles bigint null,
        percent_duplicate numeric(7, 2) null,
        single_use_plan_count bigint null,
        percent_single numeric(7, 2) null,
        total_plans bigint null,
        spid int
    );


    if EXISTS(select * from sys.all_objects o where o.name = 'dm_hadr_database_replica_states')
        begin
            raiserror ('Checking for Read intent databases to exclude',0,0) with nowait;

            exec ('INSERT INTO #ReadableDBs (database_id) SELECT DBs.database_id FROM sys.databases DBs INNER JOIN sys.availability_replicas Replicas ON DBs.replica_id = Replicas.replica_id WHERE replica_server_name NOT IN (SELECT DISTINCT primary_replica FROM sys.dm_hadr_availability_group_states States) AND Replicas.secondary_role_allow_connections_desc = ''READ_ONLY'' AND replica_server_name = @@SERVERNAME OPTION (RECOMPILE);');
        end

    raiserror (N'Checking plan cache age', 0, 1) with nowait;
    with x as (
        select SUM(case when DATEDIFF(hour, deqs.creation_time, SYSDATETIME()) <= 24 then 1 else 0 end) as [plans_24],
               SUM(case when DATEDIFF(hour, deqs.creation_time, SYSDATETIME()) <= 4 then 1 else 0 end)  as [plans_4],
               SUM(case when DATEDIFF(hour, deqs.creation_time, SYSDATETIME()) <= 1 then 1 else 0 end)  as [plans_1],
               COUNT(deqs.creation_time)                                                                as [total_plans]
        from sys.dm_exec_query_stats as deqs
    )
    insert
    into #plan_creation (percent_24, percent_4, percent_1, total_plans, spid)
    select CONVERT(decimal(5, 2), NULLIF(x.plans_24, 0) / (1. * NULLIF(x.total_plans, 0))) * 100 as [percent_24],
           CONVERT(decimal(5, 2), NULLIF(x.plans_4, 0) / (1. * NULLIF(x.total_plans, 0))) * 100  as [percent_4],
           CONVERT(decimal(5, 2), NULLIF(x.plans_1, 0) / (1. * NULLIF(x.total_plans, 0))) * 100  as [percent_1],
           x.total_plans,
           @@SPID                                                                                as spid
    from x
    option (recompile);


    raiserror (N'Checking for single use plans and plans with many queries', 0, 1) with nowait;
    with total_plans as
             (
                 select COUNT_BIG(*) as total_plans
                 from sys.dm_exec_query_stats as deqs
             ),
         many_plans as
             (
                 select SUM(x.duplicate_plan_handles) as duplicate_plan_handles
                 from (
                          select COUNT_BIG(distinct plan_handle) as duplicate_plan_handles
                          from sys.dm_exec_query_stats qs
                                   cross apply sys.dm_exec_plan_attributes(qs.plan_handle) pa
                          where pa.attribute = N'dbid'
                          group by qs.query_hash, pa.value
                          having COUNT_BIG(distinct plan_handle) > 5
                      ) as x
             ),
         single_use_plans as
             (
                 select COUNT_BIG(*) as single_use_plan_count
                 from sys.dm_exec_cached_plans as cp
                 where cp.usecounts = 1
                   and cp.objtype = N'Adhoc'
                   and EXISTS(select 1 / 0
                              from sys.configurations as c
                              where c.name = N'optimize for ad hoc workloads'
                                and c.value_in_use = 0)
                 having COUNT_BIG(*) > 1
             )
    insert
    #plan_usage
    (
    duplicate_plan_handles
    ,
    percent_duplicate
    ,
    single_use_plan_count
    ,
    percent_single
    ,
    total_plans
    ,
    spid
    )
    select m.duplicate_plan_handles,
           CONVERT(decimal(5, 2), m.duplicate_plan_handles / (1. * NULLIF(t.total_plans, 0))) *
           100.                                                                                     as percent_duplicate,
           s.single_use_plan_count,
           CONVERT(decimal(5, 2), s.single_use_plan_count / (1. * NULLIF(t.total_plans, 0))) * 100. as percent_single,
           t.total_plans,
           @@SPID
    from many_plans as m,
         single_use_plans as s,
         total_plans as t;


    set @onlysqlhandles = LTRIM(RTRIM(@onlysqlhandles));
    set @onlyqueryhashes = LTRIM(RTRIM(@onlyqueryhashes));
    set @ignorequeryhashes = LTRIM(RTRIM(@ignorequeryhashes));

    declare @individual varchar(100);

    if (@onlysqlhandles is not null and @ignoresqlhandles is not null)
        begin
            raiserror ('You shouldn''t need to ignore and filter on SqlHandle at the same time.', 0, 1) with nowait;
            return;
        end;

    if (@storedprocname is not null and (@onlysqlhandles is not null or @ignoresqlhandles is not null))
        begin
            raiserror ('You can''t filter on stored procedure name and SQL Handle.', 0, 1) with nowait;
            return;
        end;

    if @onlysqlhandles is not null
        and LEN(@onlysqlhandles) > 0
        begin
            raiserror (N'Processing SQL Handles', 0, 1) with nowait;
            set @individual = '';

            while LEN(@onlysqlhandles) > 0
                begin
                    if PATINDEX('%,%', @onlysqlhandles) > 0
                        begin
                            set @individual = SUBSTRING(@onlysqlhandles, 0, PATINDEX('%,%', @onlysqlhandles));

                            insert into #only_sql_handles
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

                            set @onlysqlhandles =
                                    SUBSTRING(@onlysqlhandles, LEN(@individual + ',') + 1, LEN(@onlysqlhandles));
                        end;
                    else
                        begin
                            set @individual = @onlysqlhandles;
                            set @onlysqlhandles = null;

                            insert into #only_sql_handles
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
                        end;
                end;
        end;

    if @ignoresqlhandles is not null
        and LEN(@ignoresqlhandles) > 0
        begin
            raiserror (N'Processing SQL Handles To Ignore', 0, 1) with nowait;
            set @individual = '';

            while LEN(@ignoresqlhandles) > 0
                begin
                    if PATINDEX('%,%', @ignoresqlhandles) > 0
                        begin
                            set @individual = SUBSTRING(@ignoresqlhandles, 0, PATINDEX('%,%', @ignoresqlhandles));

                            insert into #ignore_sql_handles
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

                            set @ignoresqlhandles =
                                    SUBSTRING(@ignoresqlhandles, LEN(@individual + ',') + 1, LEN(@ignoresqlhandles));
                        end;
                    else
                        begin
                            set @individual = @ignoresqlhandles;
                            set @ignoresqlhandles = null;

                            insert into #ignore_sql_handles
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
                        end;
                end;
        end;

    if @storedprocname is not null and @storedprocname <> N''
        begin
            raiserror (N'Setting up filter for stored procedure name', 0, 1) with nowait;

            declare @function_search_sql nvarchar(max) = N''

            insert #only_sql_handles
                (sql_handle)
            select ISNULL(deps.sql_handle, CONVERT(varbinary(64),
                    '0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'))
            from sys.dm_exec_procedure_stats as deps
            where OBJECT_NAME(deps.object_id, deps.database_id) = @storedprocname

            union all

            select ISNULL(dets.sql_handle, CONVERT(varbinary(64),
                    '0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000'))
            from sys.dm_exec_trigger_stats as dets
            where OBJECT_NAME(dets.object_id, dets.database_id) = @storedprocname
            option (recompile);

            if EXISTS(select 1 / 0 from sys.all_objects as o where o.name = 'dm_exec_function_stats')
                begin
                    set @function_search_sql = @function_search_sql + N'
         SELECT  ISNULL(defs.sql_handle, CONVERT(VARBINARY(64),''0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000''))
	     FROM sys.dm_exec_function_stats AS defs
	     WHERE OBJECT_NAME(defs.object_id, defs.database_id) = @i_StoredProcName
         OPTION (RECOMPILE);
         '
                    insert #only_sql_handles (sql_handle)
                        exec sys.sp_executesql @function_search_sql, N'@i_StoredProcName NVARCHAR(128)', @storedprocname
                end

            if (select COUNT(*) from #only_sql_handles) = 0
                begin
                    raiserror (N'No information for that stored procedure was found.', 0, 1) with nowait;
                    return;
                end;

        end;


    if ((@onlyqueryhashes is not null and LEN(@onlyqueryhashes) > 0)
        or (@ignorequeryhashes is not null and LEN(@ignorequeryhashes) > 0))
        and LEFT(@queryfilter, 3) in ('pro', 'fun')
        begin
            raiserror ('You cannot limit by query hash and filter by stored procedure', 16, 1);
            return;
        end;

/* If the user is attempting to limit by query hash, set up the
   #only_query_hashes temp table. This will be used to narrow down
   results.

   Just a reminder: Using @OnlyQueryHashes will ignore stored
   procedures and triggers.
 */
    if @onlyqueryhashes is not null
        and LEN(@onlyqueryhashes) > 0
        begin
            raiserror (N'Setting up filter for Query Hashes', 0, 1) with nowait;
            set @individual = '';

            while LEN(@onlyqueryhashes) > 0
                begin
                    if PATINDEX('%,%', @onlyqueryhashes) > 0
                        begin
                            set @individual = SUBSTRING(@onlyqueryhashes, 0, PATINDEX('%,%', @onlyqueryhashes));

                            insert into #only_query_hashes
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS BINARY(8));

                            set @onlyqueryhashes =
                                    SUBSTRING(@onlyqueryhashes, LEN(@individual + ',') + 1, LEN(@onlyqueryhashes));
                        end;
                    else
                        begin
                            set @individual = @onlyqueryhashes;
                            set @onlyqueryhashes = null;

                            insert into #only_query_hashes
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            --SELECT CAST(SUBSTRING(@individual, 1, 2) AS VARBINARY(MAX)) ;
                        end;
                end;
        end;

/* If the user is setting up a list of query hashes to ignore, those
   values will be inserted into #ignore_query_hashes. This is used to
   exclude values from query results.

   Just a reminder: Using @IgnoreQueryHashes will ignore stored
   procedures and triggers.
 */
    if @ignorequeryhashes is not null
        and LEN(@ignorequeryhashes) > 0
        begin
            raiserror (N'Setting up filter to ignore query hashes', 0, 1) with nowait;
            set @individual = '';

            while LEN(@ignorequeryhashes) > 0
                begin
                    if PATINDEX('%,%', @ignorequeryhashes) > 0
                        begin
                            set @individual = SUBSTRING(@ignorequeryhashes, 0, PATINDEX('%,%', @ignorequeryhashes));

                            insert into #ignore_query_hashes
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);

                            set @ignorequeryhashes =
                                    SUBSTRING(@ignorequeryhashes, LEN(@individual + ',') + 1, LEN(@ignorequeryhashes));
                        end;
                    else
                        begin
                            set @individual = @ignorequeryhashes;
                            set @ignorequeryhashes = null;

                            insert into #ignore_query_hashes
                            select CAST('' as xml).value(
                                           'xs:hexBinary( substring(sql:variable("@individual"), sql:column("t.pos")) )',
                                           'varbinary(max)')
                            from (select case SUBSTRING(@individual, 1, 2) when '0x' then 3 else 0 end) as t(pos)
                            option (recompile);
                        end;
                end;
        end;

    if @configurationdatabasename is not null
        begin
            raiserror (N'Reading values from Configuration Database', 0, 1) with nowait;
            declare @config_sql nvarchar(max) = N'INSERT INTO #configuration SELECT parameter_name, value FROM '
                + QUOTENAME(@configurationdatabasename)
                + '.' + QUOTENAME(@configurationschemaname)
                + '.' + QUOTENAME(@configurationtablename)
                + ' ; ';
            exec (@config_sql);
        end;

    raiserror (N'Setting up variables', 0, 1) with nowait;
    declare @sql nvarchar(max) = N'',
        @insert_list nvarchar(max) = N'',
        @plans_triggers_select_list nvarchar(max) = N'',
        @body nvarchar(max) = N'',
        @body_where nvarchar(max) = N'WHERE 1 = 1 ' + @nl,
        @body_order nvarchar(max) = N'ORDER BY #sortable# DESC OPTION (RECOMPILE) ',

        @q nvarchar(1) = N'''',
        @pv varchar(20),
        @pos tinyint,
        @v decimal(6, 2),
        @build int;


    raiserror (N'Determining SQL Server version.',0,1) with nowait;

    insert into #checkversion (version)
    select CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))
    option (recompile);


    select @v = common_version,
           @build = build
    from #checkversion
    option (recompile);

    if (@sortorder in ('memory grant', 'avg memory grant')) and @versionshowsmemorygrants = 0
        begin
            raiserror ('Your version of SQL does not support sorting by memory grant or average memory grant. Please use another sort order.', 16, 1);
            return;
        end;

    if (@sortorder in ('spills', 'avg spills') and @versionshowsspills = 0)
        begin
            raiserror ('Your version of SQL does not support sorting by spills. Please use another sort order.', 16, 1);
            return;
        end;

    if ((LEFT(@queryfilter, 3) = 'fun') and (@v < 13))
        begin
            raiserror ('Your version of SQL does not support filtering by functions. Please use another filter.', 16, 1);
            return;
        end;

    raiserror (N'Creating dynamic SQL based on SQL Server version.',0,1) with nowait;

    set @insert_list += N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT INTO ##BlitzCacheProcs (SPID, QueryType, DatabaseName, AverageCPU, TotalCPU, AverageCPUPerMinute, PercentCPUByType, PercentDurationByType,
                    PercentReadsByType, PercentExecutionsByType, AverageDuration, TotalDuration, AverageReads, TotalReads, ExecutionCount,
                    ExecutionsPerMinute, TotalWrites, AverageWrites, PercentWritesByType, WritesPerMinute, PlanCreationTime,
                    LastExecutionTime, LastCompletionTime, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows,
                    LastReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills,
					QueryText, QueryPlan, TotalWorkerTimeForType, TotalElapsedTimeForType, TotalReadsForType,
                    TotalExecutionCountForType, TotalWritesForType, SqlHandle, PlanHandle, QueryHash, QueryPlanHash,
                    min_worker_time, max_worker_time, is_parallel, min_elapsed_time, max_elapsed_time, age_minutes, age_minutes_lifetime) ';

    set @body += N'
FROM   (SELECT TOP (@Top) x.*, xpa.*,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE())
                          ELSE NULL END) as MONEY) as age_minutes,
               CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time)
                          ELSE Null END) as MONEY) as age_minutes_lifetime
        FROM   sys.#view# x
               CROSS APPLY (SELECT * FROM sys.dm_exec_plan_attributes(x.plan_handle) AS ixpa
                            WHERE ixpa.attribute = ''dbid'') AS xpa ' + @nl;


    if @versionshowsairquoteactualplans = 1
        begin
            set @body += N'     CROSS APPLY sys.dm_exec_query_plan_stats(x.plan_handle) AS deqps ' + @nl;
        end

    set @body += N'        WHERE  1 = 1 ' + @nl;

    if EXISTS(select * from sys.all_objects o where o.name = 'dm_hadr_database_replica_states')
        begin
            raiserror (N'Ignoring readable secondaries databases by default', 0, 1) with nowait;
            set @body += N'               AND CAST(xpa.value AS INT) NOT IN (SELECT database_id FROM #ReadableDBs)' +
                         @nl;
        end

    if @ignoresystemdbs = 1
        begin
            raiserror (N'Ignoring system databases by default', 0, 1) with nowait;
            set @body +=
                    N'               AND COALESCE(DB_NAME(CAST(xpa.value AS INT)), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(CAST(xpa.value AS INT)), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' +
                    @nl;
        end;

    if @databasename is not null or @databasename <> N''
        begin
            raiserror (N'Filtering database name chosen', 0, 1) with nowait;
            set @body += N'               AND CAST(xpa.value AS BIGINT) = DB_ID(N'
                + QUOTENAME(@databasename, N'''')
                + N') ' + @nl;
        end;

    if (select COUNT(*) from #only_sql_handles) > 0
        begin
            raiserror (N'Including only chosen SQL Handles', 0, 1) with nowait;
            set @body +=
                    N'               AND EXISTS(SELECT 1/0 FROM #only_sql_handles q WHERE q.sql_handle = x.sql_handle) ' +
                    @nl;
        end;

    if (select COUNT(*) from #ignore_sql_handles) > 0
        begin
            raiserror (N'Including only chosen SQL Handles', 0, 1) with nowait;
            set @body +=
                    N'               AND NOT EXISTS(SELECT 1/0 FROM #ignore_sql_handles q WHERE q.sql_handle = x.sql_handle) ' +
                    @nl;
        end;

    if (select COUNT(*) from #only_query_hashes) > 0
        and (select COUNT(*) from #ignore_query_hashes) = 0
        and (select COUNT(*) from #only_sql_handles) = 0
        and (select COUNT(*) from #ignore_sql_handles) = 0
        begin
            raiserror (N'Including only chosen Query Hashes', 0, 1) with nowait;
            set @body +=
                    N'               AND EXISTS(SELECT 1/0 FROM #only_query_hashes q WHERE q.query_hash = x.query_hash) ' +
                    @nl;
        end;

/* filtering for query hashes */
    if (select COUNT(*) from #ignore_query_hashes) > 0
        and (select COUNT(*) from #only_query_hashes) = 0
        begin
            raiserror (N'Excluding chosen Query Hashes', 0, 1) with nowait;
            set @body +=
                    N'               AND NOT EXISTS(SELECT 1/0 FROM #ignore_query_hashes iq WHERE iq.query_hash = x.query_hash) ' +
                    @nl;
        end;
/* end filtering for query hashes */


    if @durationfilter is not null
        begin
            raiserror (N'Setting duration filter', 0, 1) with nowait;
            set @body += N'       AND (total_elapsed_time / 1000.0) / execution_count > @min_duration ' + @nl;
        end;

    if @minutesback is not null
        begin
            raiserror (N'Setting minutes back filter', 0, 1) with nowait;
            set @body +=
                    N'       AND DATEADD(MILLISECOND, (x.last_elapsed_time / 1000.), x.last_execution_time) >= DATEADD(MINUTE, @min_back, GETDATE()) ' +
                    @nl;
        end;

    if @slowlysearchplansfor is not null
        begin
            raiserror (N'Setting string search for @SlowlySearchPlansFor, so remember, this is gonna be slow', 0, 1) with nowait;
            set @slowlysearchplansfor =
                    REPLACE((REPLACE((REPLACE((REPLACE(@slowlysearchplansfor, N'[', N'_')), N']', N'_')), N'^', N'_')),
                            N'''', N'''''');
            set @body_where += N'       AND CAST(qp.query_plan AS NVARCHAR(MAX)) LIKE N''%' + @slowlysearchplansfor +
                               N'%'' ' + @nl;
        end


/* Apply the sort order here to only grab relevant plans.
   This should make it faster to process since we'll be pulling back fewer
   plans for processing.
 */
    raiserror (N'Applying chosen sort order', 0, 1) with nowait;
    select @body += N'        ORDER BY ' +
                    case @sortorder
                        when N'cpu' then N'total_worker_time'
                        when N'reads' then N'total_logical_reads'
                        when N'writes' then N'total_logical_writes'
                        when N'duration' then N'total_elapsed_time'
                        when N'executions' then N'execution_count'
                        when N'compiles' then N'cached_time'
                        when N'memory grant' then N'max_grant_kb'
                        when N'spills' then N'max_spills'
                        /* And now the averages */
                        when N'avg cpu' then N'total_worker_time / execution_count'
                        when N'avg reads' then N'total_logical_reads / execution_count'
                        when N'avg writes' then N'total_logical_writes / execution_count'
                        when N'avg duration' then N'total_elapsed_time / execution_count'
                        when N'avg memory grant'
                            then N'CASE WHEN max_grant_kb = 0 THEN 0 ELSE max_grant_kb / execution_count END'
                        when N'avg spills'
                            then N'CASE WHEN total_spills = 0 THEN 0 ELSE total_spills / execution_count END'
                        when N'avg executions' then 'CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE())
                          ELSE NULL END) as MONEY), CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time)
                          ELSE Null END) as MONEY), 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(CAST((CASE WHEN DATEDIFF(mi, cached_time, GETDATE()) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, GETDATE())
                          ELSE NULL END) as MONEY), CAST((CASE WHEN DATEDIFF(mi, cached_time, last_execution_time) > 0 AND execution_count > 1
                          THEN DATEDIFF(mi, cached_time, last_execution_time)
                          ELSE Null END) as MONEY))) AS money)
            END '
                        end + N' DESC ' + @nl;


    set @body += N') AS qs
	   CROSS JOIN(SELECT SUM(execution_count) AS t_TotalExecs,
                         SUM(CAST(total_elapsed_time AS BIGINT) / 1000.0) AS t_TotalElapsed,
                         SUM(CAST(total_worker_time AS BIGINT) / 1000.0) AS t_TotalWorker,
                         SUM(CAST(total_logical_reads AS BIGINT)) AS t_TotalReads,
                         SUM(CAST(total_logical_writes AS BIGINT)) AS t_TotalWrites
                  FROM   sys.#view#) AS t
       CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) AS pa
       CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
       CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp ' + @nl;

    if @versionshowsairquoteactualplans = 1
        begin
            set @body += N'     CROSS APPLY sys.dm_exec_query_plan_stats(qs.plan_handle) AS deqps ' + @nl;
        end

    set @body_where += N'       AND pa.attribute = ' + QUOTENAME('dbid', @q) + @nl;


    if @noobsaibot = 1
        begin
            set @body_where +=
                    N'       AND qp.query_plan.exist(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";//p:StmtSimple//p:MissingIndex'') = 1' +
                    @nl;
        end

    set @plans_triggers_select_list += N'
SELECT TOP (@Top)
       @@SPID ,
       ''Procedure or Function: ''
	   + QUOTENAME(COALESCE(OBJECT_SCHEMA_NAME(qs.object_id, qs.database_id),''''))
	   + ''.''
	   + QUOTENAME(COALESCE(OBJECT_NAME(qs.object_id, qs.database_id),'''')) AS QueryType,
       COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), N''-- N/A --'') AS DatabaseName,
       (total_worker_time / 1000.0) / execution_count AS AvgCPU ,
       (total_worker_time / 1000.0) AS TotalCPU ,
       CASE WHEN total_worker_time = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((total_worker_time / 1000.0) / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time)) AS MONEY)
            END AS AverageCPUPerMinute ,
       CASE WHEN t.t_TotalWorker = 0 THEN 0
            ELSE CAST(ROUND(100.00 * (total_worker_time / 1000.0) / t.t_TotalWorker, 2) AS MONEY)
            END AS PercentCPUByType,
       CASE WHEN t.t_TotalElapsed = 0 THEN 0
            ELSE CAST(ROUND(100.00 * (total_elapsed_time / 1000.0) / t.t_TotalElapsed, 2) AS MONEY)
            END AS PercentDurationByType,
       CASE WHEN t.t_TotalReads = 0 THEN 0
            ELSE CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY)
            END AS PercentReadsByType,
       CASE WHEN t.t_TotalExecs = 0 THEN 0
            ELSE CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY)
            END AS PercentExecutionsByType,
       (total_elapsed_time / 1000.0) / execution_count AS AvgDuration ,
       (total_elapsed_time / 1000.0) AS TotalDuration ,
       total_logical_reads / execution_count AS AvgReads ,
       total_logical_reads AS TotalReads ,
       execution_count AS ExecutionCount ,
       CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time))) AS money)
            END AS ExecutionsPerMinute ,
       total_logical_writes AS TotalWrites ,
       total_logical_writes / execution_count AS AverageWrites ,
       CASE WHEN t.t_TotalWrites = 0 THEN 0
            ELSE CAST(ROUND(100.00 * total_logical_writes / t.t_TotalWrites, 2) AS MONEY)
            END AS PercentWritesByType,
       CASE WHEN total_logical_writes = 0 THEN 0
            WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0) = 0 THEN 0
            ELSE CAST((1.00 * total_logical_writes / COALESCE(age_minutes, DATEDIFF(mi, qs.cached_time, qs.last_execution_time), 0)) AS money)
            END AS WritesPerMinute,
       qs.cached_time AS PlanCreationTime,
       qs.last_execution_time AS LastExecutionTime,
	   DATEADD(MILLISECOND, (qs.last_elapsed_time / 1000.), qs.last_execution_time) AS LastCompletionTime,
       NULL AS StatementStartOffset,
       NULL AS StatementEndOffset,
       NULL AS MinReturnedRows,
       NULL AS MaxReturnedRows,
       NULL AS AvgReturnedRows,
       NULL AS TotalReturnedRows,
       NULL AS LastReturnedRows,
       NULL AS MinGrantKB,
       NULL AS MaxGrantKB,
       NULL AS MinUsedGrantKB,
	   NULL AS MaxUsedGrantKB,
	   NULL AS PercentMemoryGrantUsed,
	   NULL AS AvgMaxMemoryGrant,';

    if @versionshowsspills = 1
        begin
            raiserror (N'Getting spill information for newer versions of SQL', 0, 1) with nowait;
            set @plans_triggers_select_list += N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills, ';
        end;
    else
        begin
            raiserror (N'Substituting NULLs for spill columns in older versions of SQL', 0, 1) with nowait;
            set @plans_triggers_select_list += N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills,
		   NULL AS AvgSpills, ';
        end;

    set @plans_triggers_select_list +=
        N'st.text AS QueryText ,';

    if @versionshowsairquoteactualplans = 1
        begin
            set @plans_triggers_select_list +=
                    N' CASE WHEN DATALENGTH(COALESCE(deqps.query_plan,'''')) > DATALENGTH(COALESCE(qp.query_plan,'''')) THEN deqps.query_plan ELSE qp.query_plan END AS QueryPlan, ' +
                    @nl;
        end;
    else
        begin
            set @plans_triggers_select_list += N' qp.query_plan AS QueryPlan, ' + @nl;
        end;

    set @plans_triggers_select_list +=
        N't.t_TotalWorker,
       t.t_TotalElapsed,
       t.t_TotalReads,
       t.t_TotalExecs,
       t.t_TotalWrites,
       qs.sql_handle AS SqlHandle,
       qs.plan_handle AS PlanHandle,
       NULL AS QueryHash,
       NULL AS QueryPlanHash,
       qs.min_worker_time / 1000.0,
       qs.max_worker_time / 1000.0,
       CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN 1 ELSE 0 END,
       qs.min_elapsed_time / 1000.0,
       qs.max_elapsed_time / 1000.0,
       age_minutes,
       age_minutes_lifetime ';


    if LEFT(@queryfilter, 3) in ('all', 'sta')
        begin
            set @sql += @insert_list;

            set @sql += N'
    SELECT TOP (@Top)
           @@SPID ,
           ''Statement'' AS QueryType,
           COALESCE(DB_NAME(CAST(pa.value AS INT)), N''-- N/A --'') AS DatabaseName,
           (total_worker_time / 1000.0) / execution_count AS AvgCPU ,
           (total_worker_time / 1000.0) AS TotalCPU ,
           CASE WHEN total_worker_time = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((total_worker_time / 1000.0) / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time)) AS MONEY)
                END AS AverageCPUPerMinute ,
           CASE WHEN t.t_TotalWorker = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_worker_time / t.t_TotalWorker, 2) AS MONEY)
                END AS PercentCPUByType,
           CASE WHEN t.t_TotalElapsed = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_elapsed_time / t.t_TotalElapsed, 2) AS MONEY)
                END AS PercentDurationByType,
           CASE WHEN t.t_TotalReads = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_logical_reads / t.t_TotalReads, 2) AS MONEY)
                END AS PercentReadsByType,
           CAST(ROUND(100.00 * execution_count / t.t_TotalExecs, 2) AS MONEY) AS PercentExecutionsByType,
           (total_elapsed_time / 1000.0) / execution_count AS AvgDuration ,
           (total_elapsed_time / 1000.0) AS TotalDuration ,
           total_logical_reads / execution_count AS AvgReads ,
           total_logical_reads AS TotalReads ,
           execution_count AS ExecutionCount ,
           CASE WHEN execution_count = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time))) AS money)
                END AS ExecutionsPerMinute ,
           total_logical_writes AS TotalWrites ,
           total_logical_writes / execution_count AS AverageWrites ,
           CASE WHEN t.t_TotalWrites = 0 THEN 0
                ELSE CAST(ROUND(100.00 * total_logical_writes / t.t_TotalWrites, 2) AS MONEY)
                END AS PercentWritesByType,
           CASE WHEN total_logical_writes = 0 THEN 0
                WHEN COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0) = 0 THEN 0
                ELSE CAST((1.00 * total_logical_writes / COALESCE(age_minutes, DATEDIFF(mi, qs.creation_time, qs.last_execution_time), 0)) AS money)
                END AS WritesPerMinute,
           qs.creation_time AS PlanCreationTime,
           qs.last_execution_time AS LastExecutionTime,
		   DATEADD(MILLISECOND, (qs.last_elapsed_time / 1000.), qs.last_execution_time) AS LastCompletionTime,
           qs.statement_start_offset AS StatementStartOffset,
           qs.statement_end_offset AS StatementEndOffset, ';

            if (@v >= 11) or (@v >= 10.5 and @build >= 2500)
                begin
                    raiserror (N'Adding additional info columns for newer versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           qs.min_rows AS MinReturnedRows,
           qs.max_rows AS MaxReturnedRows,
           CAST(qs.total_rows as MONEY) / execution_count AS AvgReturnedRows,
           qs.total_rows AS TotalReturnedRows,
           qs.last_rows AS LastReturnedRows, ';
                end;
            else
                begin
                    raiserror (N'Substituting NULLs for more info columns in older versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           NULL AS MinReturnedRows,
           NULL AS MaxReturnedRows,
           NULL AS AvgReturnedRows,
           NULL AS TotalReturnedRows,
           NULL AS LastReturnedRows, ';
                end;

            if @versionshowsmemorygrants = 1
                begin
                    raiserror (N'Getting memory grant information for newer versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           min_grant_kb AS MinGrantKB,
           max_grant_kb AS MaxGrantKB,
           min_used_grant_kb AS MinUsedGrantKB,
           max_used_grant_kb AS MaxUsedGrantKB,
           CAST(ISNULL(NULLIF(( max_used_grant_kb * 1.00 ), 0) / NULLIF(min_grant_kb, 0), 0) * 100. AS MONEY) AS PercentMemoryGrantUsed,
		   CAST(ISNULL(NULLIF(( max_grant_kb * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgMaxMemoryGrant, ';
                end;
            else
                begin
                    raiserror (N'Substituting NULLs for memory grant columns in older versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           NULL AS MinGrantKB,
           NULL AS MaxGrantKB,
           NULL AS MinUsedGrantKB,
		   NULL AS MaxUsedGrantKB,
		   NULL AS PercentMemoryGrantUsed,
		   NULL AS AvgMaxMemoryGrant, ';
                end;

            if @versionshowsspills = 1
                begin
                    raiserror (N'Getting spill information for newer versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills,';
                end;
            else
                begin
                    raiserror (N'Substituting NULLs for spill columns in older versions of SQL', 0, 1) with nowait;
                    set @sql += N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills,
		   NULL AS AvgSpills, ';
                end;

            set @sql += N'
           SUBSTRING(st.text, ( qs.statement_start_offset / 2 ) + 1, ( ( CASE qs.statement_end_offset
                                                                            WHEN -1 THEN DATALENGTH(st.text)
                                                                            ELSE qs.statement_end_offset
                                                                          END - qs.statement_start_offset ) / 2 ) + 1) AS QueryText , ' +
                        @nl;


            if @versionshowsairquoteactualplans = 1
                begin
                    set @sql +=
                            N'           CASE WHEN DATALENGTH(COALESCE(deqps.query_plan,'''')) > DATALENGTH(COALESCE(qp.query_plan,'''')) THEN deqps.query_plan ELSE qp.query_plan END AS QueryPlan, ' +
                            @nl;
                end
            else
                begin
                    set @sql += N'           query_plan AS QueryPlan, ' + @nl ;
                end

            set @sql += N'
           t.t_TotalWorker,
           t.t_TotalElapsed,
           t.t_TotalReads,
           t.t_TotalExecs,
           t.t_TotalWrites,
           qs.sql_handle AS SqlHandle,
           qs.plan_handle AS PlanHandle,
           qs.query_hash AS QueryHash,
           qs.query_plan_hash AS QueryPlanHash,
           qs.min_worker_time / 1000.0,
           qs.max_worker_time / 1000.0,
           CASE WHEN qp.query_plan.value(''declare namespace p="http://schemas.microsoft.com/sqlserver/2004/07/showplan";max(//p:RelOp/@Parallel)'', ''float'')  > 0 THEN 1 ELSE 0 END,
           qs.min_elapsed_time / 1000.0,
           qs.max_worker_time  / 1000.0,
           age_minutes,
           age_minutes_lifetime ';

            set @sql += REPLACE(REPLACE(@body, '#view#', 'dm_exec_query_stats'), 'cached_time', 'creation_time');

            set @sql += REPLACE(@body_where, 'cached_time', 'creation_time');

            set @sql += @body_order + @nl + @nl + @nl;

            if @sortorder = 'compiles'
                begin
                    raiserror (N'Sorting by compiles', 0, 1) with nowait;
                    set @sql = REPLACE(@sql, '#sortable#', 'creation_time');
                end;
        end;


    if (@queryfilter = 'all'
        and (select COUNT(*) from #only_query_hashes) = 0
        and (select COUNT(*) from #ignore_query_hashes) = 0)
           and (@sortorder not in ('memory grant', 'avg memory grant'))
        or (LEFT(@queryfilter, 3) = 'pro')
        begin
            set @sql += @insert_list;
            set @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Stored Procedure');

            set @sql += REPLACE(@body, '#view#', 'dm_exec_procedure_stats');
            set @sql += @body_where;

            if @ignoresystemdbs = 1
                set @sql +=
                        N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' +
                        @nl;

            set @sql += @body_order + @nl + @nl + @nl;
        end;

    if (@v >= 13
        and @queryfilter = 'all'
        and (select COUNT(*) from #only_query_hashes) = 0
        and (select COUNT(*) from #ignore_query_hashes) = 0)
           and (@sortorder not in ('memory grant', 'avg memory grant'))
           and (@sortorder not in ('spills', 'avg spills'))
        or (LEFT(@queryfilter, 3) = 'fun')
        begin
            set @sql += @insert_list;
            set @sql += REPLACE(REPLACE(@plans_triggers_select_list, '#query_type#', 'Function')
                , N'
           min_spills AS MinSpills,
           max_spills AS MaxSpills,
           total_spills AS TotalSpills,
		   CAST(ISNULL(NULLIF(( total_spills * 1. ), 0) / NULLIF(execution_count, 0), 0) AS MONEY) AS AvgSpills, ',
                                N'
           NULL AS MinSpills,
           NULL AS MaxSpills,
           NULL AS TotalSpills,
		   NULL AS AvgSpills, ');

            set @sql += REPLACE(@body, '#view#', 'dm_exec_function_stats');
            set @sql += @body_where;

            if @ignoresystemdbs = 1
                set @sql +=
                        N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' +
                        @nl;

            set @sql += @body_order + @nl + @nl + @nl;
        end;

/*******************************************************************************
 *
 * Because the trigger execution count in SQL Server 2008R2 and earlier is not
 * correct, we ignore triggers for these versions of SQL Server. If you'd like
 * to include trigger numbers, just know that the ExecutionCount,
 * PercentExecutions, and ExecutionsPerMinute are wildly inaccurate for
 * triggers on these versions of SQL Server.
 *
 * This is why we can't have nice things.
 *
 ******************************************************************************/
    if (@usetriggersanyway = 1 or @v >= 11)
        and (select COUNT(*) from #only_query_hashes) = 0
        and (select COUNT(*) from #ignore_query_hashes) = 0
        and (@queryfilter = 'all')
        and (@sortorder not in ('memory grant', 'avg memory grant'))
        begin
            raiserror (N'Adding SQL to collect trigger stats.',0,1) with nowait;

            /* Trigger level information from the plan cache */
            set @sql += @insert_list;

            set @sql += REPLACE(@plans_triggers_select_list, '#query_type#', 'Trigger');

            set @sql += REPLACE(@body, '#view#', 'dm_exec_trigger_stats');

            set @sql += @body_where;

            if @ignoresystemdbs = 1
                set @sql +=
                        N' AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (''master'', ''model'', ''msdb'', ''tempdb'', ''32767'') AND COALESCE(DB_NAME(database_id), CAST(pa.value AS sysname), '''') NOT IN (SELECT name FROM sys.databases WHERE is_distributor = 1)' +
                        @nl;

            set @sql += @body_order + @nl + @nl + @nl;
        end;

    declare @sort nvarchar(max);

    select @sort = case @sortorder
                       when N'cpu' then N'total_worker_time'
                       when N'reads' then N'total_logical_reads'
                       when N'writes' then N'total_logical_writes'
                       when N'duration' then N'total_elapsed_time'
                       when N'executions' then N'execution_count'
                       when N'compiles' then N'cached_time'
                       when N'memory grant' then N'max_grant_kb'
                       when N'spills' then N'max_spills'
        /* And now the averages */
                       when N'avg cpu' then N'total_worker_time / execution_count'
                       when N'avg reads' then N'total_logical_reads / execution_count'
                       when N'avg writes' then N'total_logical_writes / execution_count'
                       when N'avg duration' then N'total_elapsed_time / execution_count'
                       when N'avg memory grant'
                           then N'CASE WHEN max_grant_kb = 0 THEN 0 ELSE max_grant_kb / execution_count END'
                       when N'avg spills'
                           then N'CASE WHEN total_spills = 0 THEN 0 ELSE total_spills / execution_count END'
                       when N'avg executions' then N'CASE WHEN execution_count = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * execution_count / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
        end;

    select @sql = REPLACE(@sql, '#sortable#', @sort);

    set @sql += N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
INSERT INTO #p (SqlHandle, TotalCPU, TotalReads, TotalDuration, TotalWrites, ExecutionCount)
SELECT  SqlHandle,
        TotalCPU,
        TotalReads,
        TotalDuration,
        TotalWrites,
        ExecutionCount
FROM    (SELECT  SqlHandle,
                 TotalCPU,
                 TotalReads,
                 TotalDuration,
                 TotalWrites,
                 ExecutionCount,
                 ROW_NUMBER() OVER (PARTITION BY SqlHandle ORDER BY #sortable# DESC) AS rn
         FROM    ##BlitzCacheProcs
		 WHERE SPID = @@SPID) AS x
WHERE x.rn = 1
OPTION (RECOMPILE);

/*
    This block was used to delete duplicate queries, but has been removed.
    For more info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2026
WITH d AS (
SELECT  SPID,
        ROW_NUMBER() OVER (PARTITION BY SqlHandle, QueryHash ORDER BY #sortable# DESC) AS rn
FROM    ##BlitzCacheProcs
WHERE SPID = @@SPID
)
DELETE d
WHERE d.rn > 1
AND SPID = @@SPID
OPTION (RECOMPILE);
*/
';

    select @sort = case @sortorder
                       when N'cpu' then N'TotalCPU'
                       when N'reads' then N'TotalReads'
                       when N'writes' then N'TotalWrites'
                       when N'duration' then N'TotalDuration'
                       when N'executions' then N'ExecutionCount'
                       when N'compiles' then N'PlanCreationTime'
                       when N'memory grant' then N'MaxGrantKB'
                       when N'spills' then N'MaxSpills'
        /* And now the averages */
                       when N'avg cpu' then N'TotalCPU / ExecutionCount'
                       when N'avg reads' then N'TotalReads / ExecutionCount'
                       when N'avg writes' then N'TotalWrites / ExecutionCount'
                       when N'avg duration' then N'TotalDuration / ExecutionCount'
                       when N'avg memory grant' then N'AvgMaxMemoryGrant'
                       when N'avg spills' then N'AvgSpills'
                       when N'avg executions' then N'CASE WHEN ExecutionCount = 0 THEN 0
            WHEN COALESCE(age_minutes, age_minutes_lifetime, 0) = 0 THEN 0
            ELSE CAST((1.00 * ExecutionCount / COALESCE(age_minutes, age_minutes_lifetime)) AS money)
            END'
        end;

    select @sql = REPLACE(@sql, '#sortable#', @sort);


    if @debug = 1
        begin
            print SUBSTRING(@sql, 0, 4000);
            print SUBSTRING(@sql, 4000, 8000);
            print SUBSTRING(@sql, 8000, 12000);
            print SUBSTRING(@sql, 12000, 16000);
            print SUBSTRING(@sql, 16000, 20000);
            print SUBSTRING(@sql, 20000, 24000);
            print SUBSTRING(@sql, 24000, 28000);
            print SUBSTRING(@sql, 28000, 32000);
            print SUBSTRING(@sql, 32000, 36000);
            print SUBSTRING(@sql, 36000, 40000);
        end;

    if @reanalyze = 0
        begin
            raiserror ('Collecting execution plan information.', 0, 1) with nowait;

            exec sp_executesql @sql, N'@Top INT, @min_duration INT, @min_back INT', @top, @durationfilter_i,
                 @minutesback;
        end;

    if @skipanalysis = 1
        begin
            raiserror (N'Skipping analysis, going to results', 0, 1) with nowait;
            goto results;
        end;


/* Update ##BlitzCacheProcs to get Stored Proc info
 * This should get totals for all statements in a Stored Proc
 */
    raiserror (N'Attempting to aggregate stored proc info from separate statements', 0, 1) with nowait;
    ;
    with agg as (
        select b.sqlhandle,
               SUM(b.minreturnedrows)     as minreturnedrows,
               SUM(b.maxreturnedrows)     as maxreturnedrows,
               SUM(b.averagereturnedrows) as averagereturnedrows,
               SUM(b.totalreturnedrows)   as totalreturnedrows,
               SUM(b.lastreturnedrows)    as lastreturnedrows,
               SUM(b.mingrantkb)          as mingrantkb,
               SUM(b.maxgrantkb)          as maxgrantkb,
               SUM(b.minusedgrantkb)      as minusedgrantkb,
               SUM(b.maxusedgrantkb)      as maxusedgrantkb,
               SUM(b.minspills)           as minspills,
               SUM(b.maxspills)           as maxspills,
               SUM(b.totalspills)         as totalspills
        from ##blitzcacheprocs b
        where b.spid = @@SPID
          and b.queryhash is not null
        group by b.sqlhandle
    )
    update b
    set b.minreturnedrows     = b2.minreturnedrows,
        b.maxreturnedrows     = b2.maxreturnedrows,
        b.averagereturnedrows = b2.averagereturnedrows,
        b.totalreturnedrows   = b2.totalreturnedrows,
        b.lastreturnedrows    = b2.lastreturnedrows,
        b.mingrantkb          = b2.mingrantkb,
        b.maxgrantkb          = b2.maxgrantkb,
        b.minusedgrantkb      = b2.minusedgrantkb,
        b.maxusedgrantkb      = b2.maxusedgrantkb,
        b.minspills           = b2.minspills,
        b.maxspills           = b2.maxspills,
        b.totalspills         = b2.totalspills
    from ##blitzcacheprocs b
             join agg b2
                  on b2.sqlhandle = b.sqlhandle
    where b.queryhash is null
      and b.spid = @@SPID
    option (recompile);

/* Compute the total CPU, etc across our active set of the plan cache.
 * Yes, there's a flaw - this doesn't include anything outside of our @Top
 * metric.
 */
    raiserror ('Computing CPU, duration, read, and write metrics', 0, 1) with nowait;
    declare @total_duration bigint,
        @total_cpu bigint,
        @total_reads bigint,
        @total_writes bigint,
        @total_execution_count bigint;

    select @total_cpu = SUM(totalcpu),
           @total_duration = SUM(totalduration),
           @total_reads = SUM(totalreads),
           @total_writes = SUM(totalwrites),
           @total_execution_count = SUM(executioncount)
    from #p
    option (recompile);

    declare @cr nvarchar(1) = NCHAR(13);
    declare @lf nvarchar(1) = NCHAR(10);
    declare @tab nvarchar(1) = NCHAR(9);

/* Update CPU percentage for stored procedures */
    raiserror (N'Update CPU percentage for stored procedures', 0, 1) with nowait;
    update ##blitzcacheprocs
    set percentcpu          = y.percentcpu,
        percentduration     = y.percentduration,
        percentreads        = y.percentreads,
        percentwrites       = y.percentwrites,
        percentexecutions   = y.percentexecutions,
        executionsperminute = y.executionsperminute,
        /* Strip newlines and tabs. Tabs are replaced with multiple spaces
           so that the later whitespace trim will completely eliminate them
         */
        querytext           = REPLACE(REPLACE(REPLACE(querytext, @cr, ' '), @lf, ' '), @tab, '  ')
    from (
             select planhandle,
                    case @total_cpu
                        when 0 then 0
                        else CAST((100. * totalcpu) / @total_cpu as money) end                   as percentcpu,
                    case @total_duration
                        when 0 then 0
                        else CAST((100. * totalduration) / @total_duration as money) end         as percentduration,
                    case @total_reads
                        when 0 then 0
                        else CAST((100. * totalreads) / @total_reads as money) end               as percentreads,
                    case @total_writes
                        when 0 then 0
                        else CAST((100. * totalwrites) / @total_writes as money) end             as percentwrites,
                    case @total_execution_count
                        when 0 then 0
                        else CAST((100. * executioncount) / @total_execution_count as money) end as percentexecutions,
                    case DATEDIFF(mi, plancreationtime, lastexecutiontime)
                        when 0 then 0
                        else CAST((1.00 * executioncount / DATEDIFF(mi, plancreationtime, lastexecutiontime)) as money)
                        end                                                                      as executionsperminute
             from (
                      select planhandle,
                             totalcpu,
                             totalduration,
                             totalreads,
                             totalwrites,
                             executioncount,
                             plancreationtime,
                             lastexecutiontime
                      from ##blitzcacheprocs
                      where planhandle is not null
                        and spid = @@SPID
                      group by planhandle,
                               totalcpu,
                               totalduration,
                               totalreads,
                               totalwrites,
                               executioncount,
                               plancreationtime,
                               lastexecutiontime
                  ) as x
         ) as y
    where ##blitzcacheprocs.planhandle = y.planhandle
      and ##blitzcacheprocs.planhandle is not null
      and ##blitzcacheprocs.spid = @@SPID
    option (recompile);


    raiserror (N'Gather percentage information from grouped results', 0, 1) with nowait;
    update ##blitzcacheprocs
    set percentcpu          = y.percentcpu,
        percentduration     = y.percentduration,
        percentreads        = y.percentreads,
        percentwrites       = y.percentwrites,
        percentexecutions   = y.percentexecutions,
        executionsperminute = y.executionsperminute,
        /* Strip newlines and tabs. Tabs are replaced with multiple spaces
           so that the later whitespace trim will completely eliminate them
         */
        querytext           = REPLACE(REPLACE(REPLACE(querytext, @cr, ' '), @lf, ' '), @tab, '  ')
    from (
             select databasename,
                    sqlhandle,
                    queryhash,
                    case @total_cpu
                        when 0 then 0
                        else CAST((100. * totalcpu) / @total_cpu as money) end                   as percentcpu,
                    case @total_duration
                        when 0 then 0
                        else CAST((100. * totalduration) / @total_duration as money) end         as percentduration,
                    case @total_reads
                        when 0 then 0
                        else CAST((100. * totalreads) / @total_reads as money) end               as percentreads,
                    case @total_writes
                        when 0 then 0
                        else CAST((100. * totalwrites) / @total_writes as money) end             as percentwrites,
                    case @total_execution_count
                        when 0 then 0
                        else CAST((100. * executioncount) / @total_execution_count as money) end as percentexecutions,
                    case DATEDIFF(mi, plancreationtime, lastexecutiontime)
                        when 0 then 0
                        else CAST((1.00 * executioncount / DATEDIFF(mi, plancreationtime, lastexecutiontime)) as money)
                        end                                                                      as executionsperminute
             from (
                      select databasename,
                             sqlhandle,
                             queryhash,
                             totalcpu,
                             totalduration,
                             totalreads,
                             totalwrites,
                             executioncount,
                             plancreationtime,
                             lastexecutiontime
                      from ##blitzcacheprocs
                      where spid = @@SPID
                      group by databasename,
                               sqlhandle,
                               queryhash,
                               totalcpu,
                               totalduration,
                               totalreads,
                               totalwrites,
                               executioncount,
                               plancreationtime,
                               lastexecutiontime
                  ) as x
         ) as y
    where ##blitzcacheprocs.sqlhandle = y.sqlhandle
      and ##blitzcacheprocs.queryhash = y.queryhash
      and ##blitzcacheprocs.databasename = y.databasename
      and ##blitzcacheprocs.planhandle is null
    option (recompile);


/* Testing using XML nodes to speed up processing */
    raiserror (N'Begin XML nodes processing', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    select queryhash,
           sqlhandle,
           planhandle,
           q.n.query('.') as statement,
           0              as is_cursor
    into #statements
    from ##blitzcacheprocs p
             cross apply p.queryplan.nodes('//p:StmtSimple') as q(n)
    where p.spid = @@SPID
    option (recompile);

    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    insert
    #statements
    select queryhash,
           sqlhandle,
           planhandle,
           q.n.query('.') as statement,
           1              as is_cursor
    from ##blitzcacheprocs p
             cross apply p.queryplan.nodes('//p:StmtCursor') as q(n)
    where p.spid = @@SPID
    option (recompile);

    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    select queryhash,
           sqlhandle,
           q.n.query('.') as query_plan
    into #query_plan
    from #statements p
             cross apply p.statement.nodes('//p:QueryPlan') as q(n)
    option (recompile);

    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    select queryhash,
           sqlhandle,
           q.n.query('.') as relop
    into #relop
    from #query_plan p
             cross apply p.query_plan.nodes('//p:RelOp') as q(n)
    option (recompile);

-- high level plan stuff
    raiserror (N'Gathering high level plan information', 0, 1) with nowait;
    update ##blitzcacheprocs
    set numberofdistinctplans = distinct_plan_count,
        numberofplans         = number_of_plans,
        plan_multiple_plans   = case when distinct_plan_count < number_of_plans then number_of_plans end
    from (
             select COUNT(distinct queryhash) as distinct_plan_count,
                    COUNT(queryhash)          as number_of_plans,
                    queryhash
             from ##blitzcacheprocs
             where spid = @@SPID
             group by queryhash
         ) as x
    where ##blitzcacheprocs.queryhash = x.queryhash
    option (recompile);

-- query level checks
    raiserror (N'Performing query level checks', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set missing_index_count   = query_plan.value('count(//p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int'),
        unmatched_index_count = case
                                    when is_trivial <> 1 then query_plan.value(
                                            'count(//p:QueryPlan/p:UnmatchedIndexes/p:Parameterization/p:Object)',
                                            'int') end,
        serialdesiredmemory   = query_plan.value('sum(//p:QueryPlan/p:MemoryGrantInfo/@SerialDesiredMemory)', 'float'),
        serialrequiredmemory  = query_plan.value('sum(//p:QueryPlan/p:MemoryGrantInfo/@SerialRequiredMemory)', 'float'),
        cachedplansize        = query_plan.value('sum(//p:QueryPlan/@CachedPlanSize)', 'float'),
        compiletime           = query_plan.value('sum(//p:QueryPlan/@CompileTime)', 'float'),
        compilecpu            = query_plan.value('sum(//p:QueryPlan/@CompileCPU)', 'float'),
        compilememory         = query_plan.value('sum(//p:QueryPlan/@CompileMemory)', 'float'),
        maxcompilememory      = query_plan.value(
                'sum(//p:QueryPlan/p:OptimizerHardwareDependentProperties/@MaxCompileMemory)', 'float')
    from #query_plan qp
    where qp.queryhash = ##blitzcacheprocs.queryhash
      and qp.sqlhandle = ##blitzcacheprocs.sqlhandle
      and spid = @@SPID
    option (recompile);

-- statement level checks
    raiserror (N'Performing compile timeout checks', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update b
    set compile_timeout = 1
    from #statements s
             join ##blitzcacheprocs b
                  on s.queryhash = b.queryhash
                      and spid = @@SPID
    where statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
    option (recompile);

    raiserror (N'Performing compile memory limit exceeded checks', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update b
    set compile_memory_limit_exceeded = 1
    from #statements s
             join ##blitzcacheprocs b
                  on s.queryhash = b.queryhash
                      and spid = @@SPID
    where statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1
    option (recompile);

    if @expertmode > 0
        begin
            raiserror (N'Performing unparameterized query checks', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                unparameterized_query as (
                select s.QueryHash,
                unparameterized_query = case when statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 1 and
                statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList/p:ColumnReference') = 0 then 1
                when statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/p:QueryPlan/p:ParameterList') = 0 and
                statement.exist('//p:StmtSimple[@StatementOptmLevel[.="FULL"]]/*/p:RelOp/descendant::p:ScalarOperator/p:Identifier/p:ColumnReference[contains(@Column, "@")]') = 1 then 1
                end
                from #statements as s
                )
            update b
            set b.unparameterized_query = u.unparameterized_query
            from ##blitzcacheprocs b
                     join unparameterized_query u
                          on u.queryhash = b.queryhash
                              and spid = @@SPID
            where u.unparameterized_query = 1
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Performing index DML checks', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                index_dml as (
                select s.QueryHash,
                index_dml = case when statement.exist('//p:StmtSimple/@StatementType[.="CREATE INDEX"]') = 1 then 1
                when statement.exist('//p:StmtSimple/@StatementType[.="DROP INDEX"]') = 1 then 1
                end
                from #statements s
                )
            update b
            set b.index_dml = i.index_dml
            from ##blitzcacheprocs as b
                     join index_dml i
                          on i.queryhash = b.queryhash
            where i.index_dml = 1
              and b.spid = @@SPID
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Performing table DML checks', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                table_dml as (
                select s.QueryHash,
                table_dml = case when statement.exist('//p:StmtSimple/@StatementType[.="CREATE TABLE"]') = 1 then 1
                when statement.exist('//p:StmtSimple/@StatementType[.="DROP OBJECT"]') = 1 then 1
                end
                from #statements as s
                )
            update b
            set b.table_dml = t.table_dml
            from ##blitzcacheprocs as b
                     join table_dml t
                          on t.queryhash = b.queryhash
            where t.table_dml = 1
              and b.spid = @@SPID
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Gathering row estimates', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
            insert
            into #est_rows
            select distinct CONVERT(binary(8),
                                    RIGHT('0000000000000000' + SUBSTRING(c.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18),
                                          16), 2)                                      as queryhash,
                            c.n.value('(/p:StmtSimple/@StatementEstRows)[1]', 'FLOAT') as estimated_rows
            from #statements as s
                     cross apply s.statement.nodes('/p:StmtSimple') as c(n)
            where c.n.exist('/p:StmtSimple[@StatementEstRows > 0]') = 1;

            update b
            set b.estimated_rows = er.estimated_rows
            from ##blitzcacheprocs as b
                     join #est_rows er
                          on er.queryhash = b.queryhash
            where b.spid = @@SPID
              and b.querytype = 'Statement'
            option (recompile);
        end;

    raiserror (N'Gathering trivial plans', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
    update b
    set b.is_trivial = 1
    from ##blitzcacheprocs as b
             join (
        select s.sqlhandle
        from #statements as s
                 join (select r.sqlhandle
                       from #relop as r
                       where r.relop.exist('//p:RelOp[contains(@LogicalOp, "Scan")]') = 1) as r
                      on r.sqlhandle = s.sqlhandle
        where s.statement.exist('//p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1
    ) as s
                  on b.sqlhandle = s.sqlhandle
    option (recompile);


--Gather costs
    raiserror (N'Gathering statement costs', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    insert
    into #plan_cost (queryplancost, sqlhandle, planhandle, queryhash, queryplanhash)
    select distinct statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') queryplancost,
                    s.sqlhandle,
                    s.planhandle,
                    CONVERT(binary(8),
                            RIGHT('0000000000000000' + SUBSTRING(q.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18), 16),
                            2)            as                                             queryhash,
                    CONVERT(binary(8),
                            RIGHT('0000000000000000' + SUBSTRING(q.n.value('@QueryPlanHash', 'VARCHAR(18)'), 3, 18),
                                  16), 2) as                                             queryplanhash
    from #statements s
             cross apply s.statement.nodes('/p:StmtSimple') as q(n)
    where statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') > 0
    option (recompile);

    raiserror (N'Updating statement costs', 0, 1) with nowait;
    with pc as (
        select SUM(distinct pc.queryplancost) as queryplancostsum,
               pc.queryhash,
               pc.queryplanhash,
               pc.sqlhandle,
               pc.planhandle
        from #plan_cost as pc
        group by pc.queryhash, pc.queryplanhash, pc.sqlhandle, pc.planhandle
    )
    update b
    set b.queryplancost = ISNULL(pc.queryplancostsum, 0)
    from pc
             join ##blitzcacheprocs b
                  on b.sqlhandle = pc.sqlhandle
                      and b.queryhash = pc.queryhash
    where b.querytype not like '%Procedure%'
    option (recompile);

    if EXISTS(
            select 1
            from ##blitzcacheprocs as b
            where b.querytype like 'Procedure%'
        )
        begin

            raiserror (N'Gathering stored procedure costs', 0, 1) with nowait;
            ;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
               , QueryCost as (
                select
                distinct
                statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)'
               , 'float') as SubTreeCost
               , s.PlanHandle
               , s.SqlHandle
                from #statements as s
                where PlanHandle is not null
                )
               , QueryCostUpdate as (
                select
                sum (qc.SubTreeCost) over (partition by SqlHandle
               , PlanHandle) PlanTotalQuery
               , qc.PlanHandle
               , qc.SqlHandle
                from QueryCost qc
                )
            insert into #proc_costs
            select qcu.plantotalquery, planhandle, sqlhandle
            from querycostupdate as qcu
            option (recompile);


            update b
            set b.queryplancost = ca.plantotalquery
            from ##blitzcacheprocs as b
                     cross apply (
                select top 1 plantotalquery
                from #proc_costs qcu
                where qcu.planhandle = b.planhandle
                order by plantotalquery desc
            ) ca
            where b.querytype like 'Procedure%'
              and b.spid = @@SPID
            option (recompile);

        end;

    update b
    set b.queryplancost = 0.0
    from ##blitzcacheprocs b
    where b.queryplancost is null
      and b.spid = @@SPID
    option (recompile);

    raiserror (N'Checking for plan warnings', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set plan_warnings = 1
    from #query_plan qp
    where qp.sqlhandle = ##blitzcacheprocs.sqlhandle
      and spid = @@SPID
      and query_plan.exist('/p:QueryPlan/p:Warnings') = 1
    option (recompile);

    raiserror (N'Checking for implicit conversion', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set implicit_conversions = 1
    from #query_plan qp
    where qp.sqlhandle = ##blitzcacheprocs.sqlhandle
      and spid = @@SPID
      and query_plan.exist(
                  '/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') = 1
    option (recompile);

-- operator level checks
    if @expertmode > 0
        begin
            raiserror (N'Performing busy loops checks', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update p
            set busy_loops = case when (x.estimated_executions / 100.0) > x.estimated_rows then 1 end
            from ##blitzcacheprocs p
                     join (
                select qs.sqlhandle,
                       relop.value('sum(/p:RelOp/@EstimateRows)', 'float')          as estimated_rows,
                       relop.value('sum(/p:RelOp/@EstimateRewinds)', 'float') +
                       relop.value('sum(/p:RelOp/@EstimateRebinds)', 'float') + 1.0 as estimated_executions
                from #relop qs
            ) as x on p.sqlhandle = x.sqlhandle
            where spid = @@SPID
            option (recompile);
        end;


    raiserror (N'Performing TVF join check', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update p
    set p.tvf_join = case when x.tvf_join = 1 then 1 end
    from ##blitzcacheprocs p
             join (
        select r.sqlhandle,
               1 as tvf_join
        from #relop as r
        where r.relop.exist('//p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
          and r.relop.exist('//p:RelOp[contains(@LogicalOp, "Join")]') = 1
    ) as x on p.sqlhandle = x.sqlhandle
    where spid = @@SPID
    option (recompile);

    if @expertmode > 0
        begin
            raiserror (N'Checking for operator warnings', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
               , x as (
                select r.SqlHandle
               , c.n.exist('//p:Warnings[(@NoJoinPredicate[.="1"])]') as warning_no_join_predicate
               , c.n.exist('//p:ColumnsWithNoStatistics') as no_stats_warning
               , c.n.exist('//p:Warnings') as relop_warnings
                from #relop as r
                cross apply r.relop.nodes('/p:RelOp/p:Warnings') as c(n)
                )
            update p
            set p.warning_no_join_predicate = x.warning_no_join_predicate,
                p.no_stats_warning          = x.no_stats_warning,
                p.relop_warnings            = x.relop_warnings
            from ##blitzcacheprocs as p
                     join x on x.sqlhandle = p.sqlhandle
                and spid = @@SPID
            option (recompile);
        end;


    raiserror (N'Checking for table variables', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
       , x as (
        select r.SqlHandle
       , c.n.value('substring(@Table, 2, 1)'
       , 'VARCHAR(100)') as first_char
        from #relop r
        cross apply r.relop.nodes('//p:Object') as c(n)
        )
    update p
    set is_table_variable = 1
    from ##blitzcacheprocs as p
             join x on x.sqlhandle = p.sqlhandle
        and spid = @@SPID
    where x.first_char = '@'
    option (recompile);

    if @expertmode > 0
        begin
            raiserror (N'Checking for functions', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
               , x as (
                select qs.SqlHandle
               , n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))'
               , 'INT') as function_count
               , n.fn.value('count(distinct-values(//p:UserDefinedFunction[@IsClrFunction = "1"]))'
               , 'INT') as clr_function_count
                from #relop qs
                cross apply relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n (fn)
                )
            update p
            set p.function_count     = x.function_count,
                p.clr_function_count = x.clr_function_count
            from ##blitzcacheprocs as p
                     join x on x.sqlhandle = p.sqlhandle
                and spid = @@SPID
            option (recompile);
        end;


    raiserror (N'Checking for expensive key lookups', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set key_lookup_cost = x.key_lookup_cost
    from (
             select qs.sqlhandle,
                    MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) as key_lookup_cost
             from #relop qs
             where [relop].exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
             group by qs.sqlhandle
         ) as x
    where ##blitzcacheprocs.sqlhandle = x.sqlhandle
      and spid = @@SPID
    option (recompile);


    raiserror (N'Checking for expensive remote queries', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set remote_query_cost = x.remote_query_cost
    from (
             select qs.sqlhandle,
                    MAX(relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) as remote_query_cost
             from #relop qs
             where [relop].exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
             group by qs.sqlhandle
         ) as x
    where ##blitzcacheprocs.sqlhandle = x.sqlhandle
      and spid = @@SPID
    option (recompile);

    raiserror (N'Checking for expensive sorts', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set sort_cost = y.max_sort_cost
    from (
             select x.sqlhandle, MAX((x.sort_io + x.sort_cpu)) as max_sort_cost
             from (
                      select qs.sqlhandle,
                             relop.value('sum(/p:RelOp/@EstimateIO)', 'float')  as sort_io,
                             relop.value('sum(/p:RelOp/@EstimateCPU)', 'float') as sort_cpu
                      from #relop qs
                      where [relop].exist('/p:RelOp[(@PhysicalOp[.="Sort"])]') = 1
                  ) as x
             group by x.sqlhandle
         ) as y
    where ##blitzcacheprocs.sqlhandle = y.sqlhandle
      and spid = @@SPID
    option (recompile);

    if not EXISTS(select 1 / 0 from #statements as s where s.is_cursor = 1)
        begin

            raiserror (N'No cursor plans found, skipping', 0, 1) with nowait;

        end

    if EXISTS(select 1 / 0 from #statements as s where s.is_cursor = 1)
        begin

            raiserror (N'Cursor plans found, investigating', 0, 1) with nowait;

            raiserror (N'Checking for Optimistic cursors', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_optimistic_cursor = 1
            from ##blitzcacheprocs b
                     join #statements as qs
                          on b.sqlhandle = qs.sqlhandle
                     cross apply qs.statement.nodes('/p:StmtCursor') as n1(fn)
            where spid = @@SPID
              and n1.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1
              and qs.is_cursor = 1
            option (recompile);


            raiserror (N'Checking if cursor is Forward Only', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_forward_only_cursor = 1
            from ##blitzcacheprocs b
                     join #statements as qs
                          on b.sqlhandle = qs.sqlhandle
                     cross apply qs.statement.nodes('/p:StmtCursor') as n1(fn)
            where spid = @@SPID
              and n1.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1
              and qs.is_cursor = 1
            option (recompile);

            raiserror (N'Checking if cursor is Fast Forward', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_fast_forward_cursor = 1
            from ##blitzcacheprocs b
                     join #statements as qs
                          on b.sqlhandle = qs.sqlhandle
                     cross apply qs.statement.nodes('/p:StmtCursor') as n1(fn)
            where spid = @@SPID
              and n1.fn.exist('//p:CursorPlan/@CursorActualType[.="FastForward"]') = 1
              and qs.is_cursor = 1
            option (recompile);


            raiserror (N'Checking for Dynamic cursors', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_cursor_dynamic = 1
            from ##blitzcacheprocs b
                     join #statements as qs
                          on b.sqlhandle = qs.sqlhandle
                     cross apply qs.statement.nodes('/p:StmtCursor') as n1(fn)
            where spid = @@SPID
              and n1.fn.exist('//p:CursorPlan/@CursorActualType[.="Dynamic"]') = 1
              and qs.is_cursor = 1
            option (recompile);

        end

    if @expertmode > 0
        begin
            raiserror (N'Checking for bad scans and plan forcing', 0, 1) with nowait;
            ;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_table_scan  = x.is_table_scan,
                b.backwards_scan = x.backwards_scan,
                b.forced_index   = x.forced_index,
                b.forced_seek    = x.forced_seek,
                b.forced_scan    = x.forced_scan
            from ##blitzcacheprocs b
                     join (
                select qs.sqlhandle,
                       0                                         as is_table_scan,
                       q.n.exist('@ScanDirection[.="BACKWARD"]') as backwards_scan,
                       q.n.value('@ForcedIndex', 'bit')          as forced_index,
                       q.n.value('@ForceSeek', 'bit')            as forced_seek,
                       q.n.value('@ForceScan', 'bit')            as forced_scan
                from #relop qs
                         cross apply qs.relop.nodes('//p:IndexScan') as q(n)
                union all
                select qs.sqlhandle,
                       1                                         as is_table_scan,
                       q.n.exist('@ScanDirection[.="BACKWARD"]') as backwards_scan,
                       q.n.value('@ForcedIndex', 'bit')          as forced_index,
                       q.n.value('@ForceSeek', 'bit')            as forced_seek,
                       q.n.value('@ForceScan', 'bit')            as forced_scan
                from #relop qs
                         cross apply qs.relop.nodes('//p:TableScan') as q(n)
            ) as x on b.sqlhandle = x.sqlhandle
            where spid = @@SPID
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Checking for computed columns that reference scalar UDFs', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update ##blitzcacheprocs
            set is_computed_scalar = x.computed_column_function
            from (
                     select qs.sqlhandle,
                            n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))',
                                       'INT') as computed_column_function
                     from #relop qs
                              cross apply relop.nodes(
                             '/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
                     where n.fn.exist(
                                   '/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ColumnReference[(@ComputedColumn[.="1"])]') =
                           1
                 ) as x
            where ##blitzcacheprocs.sqlhandle = x.sqlhandle
              and spid = @@SPID
            option (recompile);
        end;


    raiserror (N'Checking for filters that reference scalar UDFs', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set is_computed_filter = x.filter_function
    from (
             select r.sqlhandle,
                    c.n.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))',
                              'INT') as filter_function
             from #relop as r
                      cross apply r.relop.nodes(
                     '/p:RelOp/p:Filter/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator/p:UserDefinedFunction') c(n)
         ) x
    where ##blitzcacheprocs.sqlhandle = x.sqlhandle
      and spid = @@SPID
    option (recompile);

    if @expertmode > 0
        begin
            raiserror (N'Checking modification queries that hit lots of indexes', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                IndexOps as
                (
                select
                r.QueryHash,
                c.n.value('@PhysicalOp', 'VARCHAR(100)') as op_name,
                c.n.exist('@PhysicalOp[.="Index Insert"]') as ii,
                c.n.exist('@PhysicalOp[.="Index Update"]') as iu,
                c.n.exist('@PhysicalOp[.="Index Delete"]') as id,
                c.n.exist('@PhysicalOp[.="Clustered Index Insert"]') as cii,
                c.n.exist('@PhysicalOp[.="Clustered Index Update"]') as ciu,
                c.n.exist('@PhysicalOp[.="Clustered Index Delete"]') as cid,
                c.n.exist('@PhysicalOp[.="Table Insert"]') as ti,
                c.n.exist('@PhysicalOp[.="Table Update"]') as tu,
                c.n.exist('@PhysicalOp[.="Table Delete"]') as td
                from #relop as r
                cross apply r.relop.nodes('/p:RelOp') c(n)
                outer apply r.relop.nodes('/p:RelOp/p:ScalarInsert/p:Object') q (n)
                outer apply r.relop.nodes('/p:RelOp/p:Update/p:Object') o2(n)
                outer apply r.relop.nodes('/p:RelOp/p:SimpleUpdate/p:Object') o3(n)
                ), iops as
                (
                select ios.QueryHash,
                sum (convert (tinyint, ios.ii)) as index_insert_count,
                sum (convert (tinyint, ios.iu)) as index_update_count,
                sum (convert (tinyint, ios.id)) as index_delete_count,
                sum (convert (tinyint, ios.cii)) as cx_insert_count,
                sum (convert (tinyint, ios.ciu)) as cx_update_count,
                sum (convert (tinyint, ios.cid)) as cx_delete_count,
                sum (convert (tinyint, ios.ti)) as table_insert_count,
                sum (convert (tinyint, ios.tu)) as table_update_count,
                sum (convert (tinyint, ios.td)) as table_delete_count
                from IndexOps as ios
                where ios.op_name in ('Index Insert', 'Index Delete', 'Index Update',
                'Clustered Index Insert', 'Clustered Index Delete', 'Clustered Index Update',
                'Table Insert', 'Table Delete', 'Table Update')
                group by ios.QueryHash)
            update b
            set b.index_insert_count = iops.index_insert_count,
                b.index_update_count = iops.index_update_count,
                b.index_delete_count = iops.index_delete_count,
                b.cx_insert_count    = iops.cx_insert_count,
                b.cx_update_count    = iops.cx_update_count,
                b.cx_delete_count    = iops.cx_delete_count,
                b.table_insert_count = iops.table_insert_count,
                b.table_update_count = iops.table_update_count,
                b.table_delete_count = iops.table_delete_count
            from ##blitzcacheprocs as b
                     join iops on iops.queryhash = b.queryhash
            where spid = @@SPID
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Checking for Spatial index use', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update ##blitzcacheprocs
            set is_spatial = x.is_spatial
            from (
                     select qs.sqlhandle,
                            1 as is_spatial
                     from #relop qs
                              cross apply relop.nodes('/p:RelOp//p:Object') n(fn)
                     where n.fn.exist('(@IndexKind[.="Spatial"])') = 1
                 ) as x
            where ##blitzcacheprocs.sqlhandle = x.sqlhandle
              and spid = @@SPID
            option (recompile);
        end;


    raiserror ('Checking for wonky Index Spools', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
       , selects
        as ( select s.QueryHash
        from #statements as s
        where s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 )
       , spools
        as ( select distinct r.QueryHash
       , c.n.value('@EstimateRows'
       , 'FLOAT') as estimated_rows
       , c.n.value('@EstimateIO'
       , 'FLOAT') as estimated_io
       , c.n.value('@EstimateCPU'
       , 'FLOAT') as estimated_cpu
       , c.n.value('@EstimateRebinds'
       , 'FLOAT') as estimated_rebinds
        from #relop as r
        join selects as s
        on s.QueryHash = r.QueryHash
        cross apply r.relop.nodes('/p:RelOp') as c(n)
        where r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1
        )
    update b
    set b.index_spool_rows = sp.estimated_rows,
        b.index_spool_cost = ((sp.estimated_io * sp.estimated_cpu) *
                              case when sp.estimated_rebinds < 1 then 1 else sp.estimated_rebinds end)
    from ##blitzcacheprocs b
             join spools sp
                  on sp.queryhash = b.queryhash
    option (recompile);

    raiserror ('Checking for wonky Table Spools', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
       , selects
        as ( select s.QueryHash
        from #statements as s
        where s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 )
       , spools
        as ( select distinct r.QueryHash
       , c.n.value('@EstimateRows'
       , 'FLOAT') as estimated_rows
       , c.n.value('@EstimateIO'
       , 'FLOAT') as estimated_io
       , c.n.value('@EstimateCPU'
       , 'FLOAT') as estimated_cpu
       , c.n.value('@EstimateRebinds'
       , 'FLOAT') as estimated_rebinds
        from #relop as r
        join selects as s
        on s.QueryHash = r.QueryHash
        cross apply r.relop.nodes('/p:RelOp') as c(n)
        where r.relop.exist('/p:RelOp[@PhysicalOp="Table Spool" and @LogicalOp="Lazy Spool"]') = 1
        )
    update b
    set b.table_spool_rows = (sp.estimated_rows * sp.estimated_rebinds),
        b.table_spool_cost = ((sp.estimated_io * sp.estimated_cpu * sp.estimated_rows) *
                              case when sp.estimated_rebinds < 1 then 1 else sp.estimated_rebinds end)
    from ##blitzcacheprocs b
             join spools sp
                  on sp.queryhash = b.queryhash
    option (recompile);


    raiserror ('Checking for selects that cause non-spill and index spool writes', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
       , selects
        as ( select convert (binary (8)
       , right ('0000000000000000'
        + SUBSTRING(s.statement.value('(/p:StmtSimple/@QueryHash)[1]'
       , 'VARCHAR(18)')
       , 3
       , 18)
       , 16)
       , 2) as QueryHash
        from #statements as s
        join ##BlitzCacheProcs b
        on s.QueryHash = b.QueryHash
        where b.index_spool_rows is null
        and b.index_spool_cost is null
        and b.table_spool_cost is null
        and b.table_spool_rows is null
        and b.is_big_spills is null
        and b.AverageWrites
       > 1024.
        and s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1
        )
    update b
    set b.select_with_writes = 1
    from ##blitzcacheprocs b
             join selects as s
                  on s.queryhash = b.queryhash
                      and b.averagewrites > 1024.;

/* 2012+ only */
    if @v >= 11
        begin

            raiserror (N'Checking for forced serialization', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update ##blitzcacheprocs
            set is_forced_serial = 1
            from #query_plan qp
            where qp.sqlhandle = ##blitzcacheprocs.sqlhandle
              and spid = @@SPID
              and query_plan.exist('/p:QueryPlan/@NonParallelPlanReason') = 1
              and (##blitzcacheprocs.is_parallel = 0 or ##blitzcacheprocs.is_parallel is null)
            option (recompile);

            if @expertmode > 0
                begin
                    raiserror (N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) with nowait;
                    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                    update ##blitzcacheprocs
                    set columnstore_row_mode = x.is_row_mode
                    from (
                             select qs.sqlhandle,
                                    relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') as is_row_mode
                             from #relop qs
                             where [relop].exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
                         ) as x
                    where ##blitzcacheprocs.sqlhandle = x.sqlhandle
                      and spid = @@SPID
                    option (recompile);
                end;

        end;

/* 2014+ only */
    if @v >= 12
        begin
            raiserror ('Checking for downlevel cardinality estimators being used on SQL Server 2014.', 0, 1) with nowait;

            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update p
            set downlevel_estimator = case
                                          when statement.value('min(//p:StmtSimple/@CardinalityEstimationModelVersion)',
                                                               'int') < (@v * 10) then 1 end
            from ##blitzcacheprocs p
                     join #statements s on p.queryhash = s.queryhash
            where spid = @@SPID
            option (recompile);
        end;

/* 2016+ only */
    if @v >= 13 and @expertmode > 0
        begin
            raiserror ('Checking for row level security in 2016 only', 0, 1) with nowait;

            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update p
            set p.is_row_level = 1
            from ##blitzcacheprocs p
                     join #statements s on p.queryhash = s.queryhash
            where spid = @@SPID
              and statement.exist('/p:StmtSimple/@SecurityPolicyApplied[.="true"]') = 1
            option (recompile);
        end;

/* 2017+ only */
    if @v >= 14 or (@v = 13 and @build >= 5026)
        begin

            if @expertmode > 0
                begin
                    raiserror ('Gathering stats information', 0, 1) with nowait;
                    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                    insert
                    into #stats_agg
                    select qp.sqlhandle,
                           x.c.value('@LastUpdate', 'DATETIME2(7)')  as lastupdate,
                           x.c.value('@ModificationCount', 'BIGINT') as modificationcount,
                           x.c.value('@SamplingPercent', 'FLOAT')    as samplingpercent,
                           x.c.value('@Statistics', 'NVARCHAR(258)') as [Statistics],
                           x.c.value('@Table', 'NVARCHAR(258)')      as [Table],
                           x.c.value('@Schema', 'NVARCHAR(258)')     as [Schema],
                           x.c.value('@Database', 'NVARCHAR(258)')   as [Database]
                    from #query_plan as qp
                             cross apply qp.query_plan.nodes('//p:OptimizerStatsUsage/p:StatisticsInfo') x (c)
                    option (recompile);


                    raiserror ('Checking for stale stats', 0, 1) with nowait;
                    with stale_stats as (
                        select sa.sqlhandle
                        from #stats_agg as sa
                        group by sa.sqlhandle
                        having MAX(sa.lastupdate) <= DATEADD(day, -7, SYSDATETIME())
                           and AVG(sa.modificationcount) >= 100000
                    )
                    update b
                    set stale_stats = 1
                    from ##blitzcacheprocs b
                             join stale_stats os
                                  on b.sqlhandle = os.sqlhandle
                                      and b.spid = @@SPID
                    option (recompile);
                end;

            if @v >= 14 and @expertmode > 0
                begin
                    raiserror ('Checking for adaptive joins', 0, 1) with nowait;
                    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                        aj as (
                        select
                        SqlHandle
                        from #relop as r
                        cross apply r.relop.nodes('//p:RelOp') x(c)
                        where x.c.exist('@IsAdaptive[.=1]') = 1
                        )
                    update b
                    set b.is_adaptive = 1
                    from ##blitzcacheprocs b
                             join aj
                                  on b.sqlhandle = aj.sqlhandle
                                      and b.spid = @@SPID
                    option (recompile);
                end;

            if ((@v >= 14
                or (@v = 13 and @build >= 5026)
                or (@v = 12 and @build >= 6024))
                and @expertmode > 0)
                begin
                    ;
                    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                        row_goals as (
                        select qs.QueryHash
                        from #relop qs
                        where relop.value('sum(/p:RelOp/@EstimateRowsWithoutRowGoal)', 'float') > 0
                        )
                    update b
                    set b.is_row_goal = 1
                    from ##blitzcacheprocs b
                             join row_goals
                                  on b.queryhash = row_goals.queryhash
                                      and b.spid = @@SPID
                    option (recompile);
                end;

        end;


/* END Testing using XML nodes to speed up processing */
    raiserror (N'Gathering additional plan level information', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set numberofdistinctplans = distinct_plan_count,
        numberofplans         = number_of_plans,
        plan_multiple_plans   = case when distinct_plan_count < number_of_plans then number_of_plans end
    from (
             select COUNT(distinct queryhash) as distinct_plan_count,
                    COUNT(queryhash)          as number_of_plans,
                    queryhash
             from ##blitzcacheprocs
             where spid = @@SPID
             group by queryhash
         ) as x
    where ##blitzcacheprocs.queryhash = x.queryhash
    option (recompile);

/* Update to grab stored procedure name for individual statements */
    raiserror (N'Attempting to get stored procedure name for individual statements', 0, 1) with nowait;
    update p
    set querytype = querytype + ' (parent ' +
                    + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
        + '.'
        + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + ')'
    from ##blitzcacheprocs p
             join sys.dm_exec_procedure_stats s on p.sqlhandle = s.sql_handle
    where querytype = 'Statement'
      and spid = @@SPID
    option (recompile);

    raiserror (N'Attempting to get function name for individual statements', 0, 1) with nowait;
    declare @function_update_sql nvarchar(max) = N''
    if EXISTS(select 1 / 0 from sys.all_objects as o where o.name = 'dm_exec_function_stats')
        begin
            set @function_update_sql = @function_update_sql + N'
     UPDATE  p
     SET     QueryType = QueryType + '' (parent '' +
                         + QUOTENAME(OBJECT_SCHEMA_NAME(s.object_id, s.database_id))
                         + ''.''
                         + QUOTENAME(OBJECT_NAME(s.object_id, s.database_id)) + '')''
     FROM    ##BlitzCacheProcs p
             JOIN sys.dm_exec_function_stats s ON p.SqlHandle = s.sql_handle
     WHERE   QueryType = ''Statement''
     AND SPID = @@SPID
     OPTION (RECOMPILE);
     '
            exec sys.sp_executesql @function_update_sql
        end


/* Trace Flag Checks 2012 SP3, 2014 SP2 and 2016 SP1 only)*/
    if @v >= 11
        begin

            raiserror (N'Trace flag checks', 0, 1) with nowait;
            ;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
               , tf_pretty as (
                select qp.QueryHash
               , qp.SqlHandle
               , q.n.value('@Value'
               , 'INT') as trace_flag
               , q.n.value('@Scope'
               , 'VARCHAR(10)') as scope
                from #query_plan qp
                cross apply qp.query_plan.nodes('/p:QueryPlan/p:TraceFlags/p:TraceFlag') as q (n)
                )
            insert into #trace_flags
            select distinct tf1.sqlhandle,
                            tf1.queryhash,
                            STUFF((
                                      select distinct ', ' + CONVERT(varchar(5), tf2.trace_flag)
                                      from tf_pretty as tf2
                                      where tf1.sqlhandle = tf2.sqlhandle
                                        and tf1.queryhash = tf2.queryhash
                                        and tf2.scope = 'Global'
                                      for xml path(N'')), 1, 2, N''
                                ) as global_trace_flags,
                            STUFF((
                                      select distinct ', ' + CONVERT(varchar(5), tf2.trace_flag)
                                      from tf_pretty as tf2
                                      where tf1.sqlhandle = tf2.sqlhandle
                                        and tf1.queryhash = tf2.queryhash
                                        and tf2.scope = 'Session'
                                      for xml path(N'')), 1, 2, N''
                                ) as session_trace_flags
            from tf_pretty as tf1
            option (recompile);

            update p
            set p.trace_flags_session = tf.session_trace_flags
            from ##blitzcacheprocs p
                     join #trace_flags tf on tf.queryhash = p.queryhash
            where spid = @@SPID
            option (recompile);

        end;


    raiserror (N'Checking for MSTVFs', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update b
    set b.is_mstvf = 1
    from #relop as r
             join ##blitzcacheprocs as b
                  on b.sqlhandle = r.sqlhandle
    where r.relop.exist('/p:RelOp[(@EstimateRows="100" or @EstimateRows="1") and @LogicalOp="Table-valued function"]') =
          1
    option (recompile);


    if @expertmode > 0
        begin
            raiserror (N'Checking for many to many merge joins', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            update b
            set b.is_mm_join = 1
            from #relop as r
                     join ##blitzcacheprocs as b
                          on b.sqlhandle = r.sqlhandle
            where r.relop.exist('/p:RelOp/p:Merge/@ManyToMany[.="1"]') = 1
            option (recompile);
        end;


    if @expertmode > 0
        begin
            raiserror (N'Is Paul White Electric?', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                is_paul_white_electric as (
                select 1 as [is_paul_white_electric],
                r.SqlHandle
                from #relop as r
                cross apply r.relop.nodes('//p:RelOp') c(n)
                where c.n.exist('@PhysicalOp[.="Switch"]') = 1
                )
            update b
            set b.is_paul_white_electric = ipwe.is_paul_white_electric
            from ##blitzcacheprocs as b
                     join is_paul_white_electric ipwe
                          on ipwe.sqlhandle = b.sqlhandle
            where b.spid = @@SPID
            option (recompile);
        end;


    raiserror (N'Checking for non-sargable predicates', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
       , nsarg
        as ( select r.QueryHash
       , 1 as fn
       , 0 as jo
       , 0 as lk
        from #relop as r
        cross apply r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator') as ca(x)
        where ( ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName') = 1
        or ca.x.exist('//p:ScalarOperator/p:IF') = 1 )
        union all
        select r.QueryHash
       , 0 as fn
       , 1 as jo
       , 0 as lk
        from #relop as r
        cross apply r.relop.nodes('/p:RelOp//p:ScalarOperator') as ca(x)
        where r.relop.exist('/p:RelOp[contains(@LogicalOp, "Join")]') = 1
        and ca.x.exist('//p:ScalarOperator[contains(@ScalarString, "Expr")]') = 1
        union all
        select r.QueryHash
       , 0 as fn
       , 0 as jo
       , 1 as lk
        from #relop as r
        cross apply r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator') as ca(x)
        cross apply ca.x.nodes('//p:Const') as co(x)
        where ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName[.="like"]') = 1
        and ( ( co.x.value('substring(@ConstValue, 1, 1)'
       , 'VARCHAR(100)') <> 'N'
        and co.x.value('substring(@ConstValue, 2, 1)'
       , 'VARCHAR(100)') = '%' )
        or ( co.x.value('substring(@ConstValue, 1, 1)'
       , 'VARCHAR(100)') = 'N'
        and co.x.value('substring(@ConstValue, 3, 1)'
       , 'VARCHAR(100)') = '%' )))
       , d_nsarg
        as ( select distinct
        nsarg.QueryHash
        from nsarg
        where nsarg.fn = 1
        or nsarg.jo = 1
        or nsarg.lk = 1 )
    update b
    set b.is_nonsargable = 1
    from d_nsarg as d
             join ##blitzcacheprocs as b
                  on b.queryhash = d.queryhash
    where b.spid = @@SPID
    option ( recompile );

/*Begin implicit conversion and parameter info */

    raiserror (N'Getting information about implicit conversions and stored proc parameters', 0, 1) with nowait;

    raiserror (N'Getting variable info', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
    insert
    #variable_info
    (
    spid
    ,
    queryhash
    ,
    sqlhandle
    ,
    proc_name
    ,
    variable_name
    ,
    variable_datatype
    ,
    compile_time_value
    )
    select distinct @@SPID,
                    qp.queryhash,
                    qp.sqlhandle,
                    b.querytype                                           as proc_name,
                    q.n.value('@Column', 'NVARCHAR(258)')                 as variable_name,
                    q.n.value('@ParameterDataType', 'NVARCHAR(258)')      as variable_datatype,
                    q.n.value('@ParameterCompiledValue', 'NVARCHAR(258)') as compile_time_value
    from #query_plan as qp
             join ##blitzcacheprocs as b
                  on (b.querytype = 'adhoc' and b.queryhash = qp.queryhash)
                      or (b.querytype <> 'adhoc' and b.sqlhandle = qp.sqlhandle)
             cross apply qp.query_plan.nodes('//p:QueryPlan/p:ParameterList/p:ColumnReference') as q(n)
    where b.spid = @@SPID
    option (recompile);


    raiserror (N'Getting conversion info', 0, 1) with nowait;
    with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
    insert
    #conversion_info
    (
    spid
    ,
    queryhash
    ,
    sqlhandle
    ,
    proc_name
    ,
    expression
    )
    select distinct @@SPID,
                    qp.queryhash,
                    qp.sqlhandle,
                    b.querytype                                 as proc_name,
                    qq.c.value('@Expression', 'NVARCHAR(4000)') as expression
    from #query_plan as qp
             join ##blitzcacheprocs as b
                  on (b.querytype = 'adhoc' and b.queryhash = qp.queryhash)
                      or (b.querytype <> 'adhoc' and b.sqlhandle = qp.sqlhandle)
             cross apply qp.query_plan.nodes('//p:QueryPlan/p:Warnings/p:PlanAffectingConvert') as qq(c)
    where qq.c.exist('@ConvertIssue[.="Seek Plan"]') = 1
      and qp.queryhash is not null
      and b.implicit_conversions = 1
      and b.spid = @@SPID
    option (recompile);


    raiserror (N'Parsing conversion info', 0, 1) with nowait;
    insert #stored_proc_info (spid, sqlhandle, queryhash, proc_name, variable_name, variable_datatype,
                              converted_column_name, column_name, converted_to, compile_time_value)
    select @@SPID                                                                                                    as spid,
           ci.sqlhandle,
           ci.queryhash,
           REPLACE(REPLACE(REPLACE(ci.proc_name, ')', ''), 'Statement (parent ', ''), 'Procedure or Function: ',
                   '')                                                                                               as proc_name,
           case
               when ci.at_charindex > 0
                   and ci.bracket_charindex > 0
                   then SUBSTRING(ci.expression, ci.at_charindex, ci.bracket_charindex)
               else N'**no_variable**'
               end                                                                                                   as variable_name,
           N'**no_variable**'                                                                                        as variable_datatype,
           case
               when ci.at_charindex = 0
                   and ci.comma_charindex > 0
                   and ci.second_comma_charindex > 0
                   then SUBSTRING(ci.expression, ci.comma_charindex, ci.second_comma_charindex)
               else N'**no_column**'
               end                                                                                                   as converted_column_name,
           case
               when ci.at_charindex = 0
                   and ci.equal_charindex > 0
                   and ci.convert_implicit_charindex = 0
                   then SUBSTRING(ci.expression, ci.equal_charindex, 4000)
               when ci.at_charindex = 0
                   and (ci.equal_charindex - 1) > 0
                   and ci.convert_implicit_charindex > 0
                   then SUBSTRING(ci.expression, 0, ci.equal_charindex - 1)
               when ci.at_charindex > 0
                   and ci.comma_charindex > 0
                   and ci.second_comma_charindex > 0
                   then SUBSTRING(ci.expression, ci.comma_charindex, ci.second_comma_charindex)
               else N'**no_column **'
               end                                                                                                   as column_name,
           case
               when ci.paren_charindex > 0
                   and ci.comma_paren_charindex > 0
                   then SUBSTRING(ci.expression, ci.paren_charindex, ci.comma_paren_charindex)
               end                                                                                                   as converted_to,
           case
               when ci.at_charindex = 0
                   and ci.convert_implicit_charindex = 0
                   and ci.proc_name = 'Statement'
                   then SUBSTRING(ci.expression, ci.equal_charindex, 4000)
               else '**idk_man**'
               end                                                                                                   as compile_time_value
    from #conversion_info as ci
    option (recompile);


    raiserror (N'Updating variables for inserted procs', 0, 1) with nowait;
    update sp
    set sp.variable_datatype  = vi.variable_datatype,
        sp.compile_time_value = vi.compile_time_value
    from #stored_proc_info as sp
             join #variable_info as vi
                  on (sp.proc_name = 'adhoc' and sp.queryhash = vi.queryhash)
                      or (sp.proc_name <> 'adhoc' and sp.sqlhandle = vi.sqlhandle)
                         and sp.variable_name = vi.variable_name
    option (recompile);


    raiserror (N'Inserting variables for other procs', 0, 1) with nowait;
    insert #stored_proc_info
    (spid, sqlhandle, queryhash, variable_name, variable_datatype, compile_time_value, proc_name)
    select vi.spid,
           vi.sqlhandle,
           vi.queryhash,
           vi.variable_name,
           vi.variable_datatype,
           vi.compile_time_value,
           REPLACE(REPLACE(REPLACE(vi.proc_name, ')', ''), 'Statement (parent ', ''), 'Procedure or Function: ',
                   '') as proc_name
    from #variable_info as vi
    where not EXISTS
        (
            select *
            from #stored_proc_info as sp
            where (sp.proc_name = 'adhoc' and sp.queryhash = vi.queryhash)
               or (sp.proc_name <> 'adhoc' and sp.sqlhandle = vi.sqlhandle)
        )
    option (recompile);


    raiserror (N'Updating procs', 0, 1) with nowait;
    update s
    set s.variable_datatype  = case
                                   when s.variable_datatype like '%(%)%'
                                       then LEFT(s.variable_datatype, CHARINDEX('(', s.variable_datatype) - 1)
                                   else s.variable_datatype
        end,
        s.converted_to       = case
                                   when s.converted_to like '%(%)%'
                                       then LEFT(s.converted_to, CHARINDEX('(', s.converted_to) - 1)
                                   else s.converted_to
            end,
        s.compile_time_value = case
                                   when s.compile_time_value like '%(%)%'
                                       then SUBSTRING(s.compile_time_value,
                                                      CHARINDEX('(', s.compile_time_value) + 1,
                                                      CHARINDEX(')', s.compile_time_value) - 1 -
                                                      CHARINDEX('(', s.compile_time_value)
                                       )
                                   when variable_datatype not in ('bit', 'tinyint', 'smallint', 'int', 'bigint')
                                       and s.variable_datatype not like '%binary%'
                                       and s.compile_time_value not like 'N''%'''
                                       and s.compile_time_value not like '''%'''
                                       and s.compile_time_value <> s.column_name
                                       and s.compile_time_value <> '**idk_man**'
                                       then QUOTENAME(compile_time_value, '''')
                                   else s.compile_time_value
            end
    from #stored_proc_info as s
    option (recompile);


    raiserror (N'Updating SET options', 0, 1) with nowait;
    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update s
    set set_options = set_options.ansi_set_options
    from #stored_proc_info as s
             join (
        select x.sqlhandle,
               N'SET ANSI_NULLS ' + case when [ANSI_NULLS] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
               N'SET ANSI_PADDING ' + case when [ANSI_PADDING] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
               N'SET ANSI_WARNINGS ' + case when [ANSI_WARNINGS] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
               N'SET ARITHABORT ' + case when [ARITHABORT] = 'true' then N'ON ' else N' OFF ' end + NCHAR(10) +
               N'SET CONCAT_NULL_YIELDS_NULL ' +
               case when [CONCAT_NULL_YIELDS_NULL] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
               N'SET NUMERIC_ROUNDABORT ' + case when [NUMERIC_ROUNDABORT] = 'true' then N'ON ' else N'OFF ' end +
               NCHAR(10) +
               N'SET QUOTED_IDENTIFIER ' +
               case when [QUOTED_IDENTIFIER] = 'true' then N'ON ' else N'OFF ' + NCHAR(10) end as [ansi_set_options]
        from (
                 select s.sqlhandle,
                        so.o.value('@ANSI_NULLS', 'NVARCHAR(20)')              as [ANSI_NULLS],
                        so.o.value('@ANSI_PADDING', 'NVARCHAR(20)')            as [ANSI_PADDING],
                        so.o.value('@ANSI_WARNINGS', 'NVARCHAR(20)')           as [ANSI_WARNINGS],
                        so.o.value('@ARITHABORT', 'NVARCHAR(20)')              as [ARITHABORT],
                        so.o.value('@CONCAT_NULL_YIELDS_NULL', 'NVARCHAR(20)') as [CONCAT_NULL_YIELDS_NULL],
                        so.o.value('@NUMERIC_ROUNDABORT', 'NVARCHAR(20)')      as [NUMERIC_ROUNDABORT],
                        so.o.value('@QUOTED_IDENTIFIER', 'NVARCHAR(20)')       as [QUOTED_IDENTIFIER]
                 from #statements as s
                          cross apply s.statement.nodes('//p:StatementSetOptions') as so(o)
             ) as x
    ) as set_options on set_options.sqlhandle = s.sqlhandle
    option (recompile);


    raiserror (N'Updating conversion XML', 0, 1) with nowait;
    with precheck as (
        select spi.spid,
               spi.sqlhandle,
               spi.proc_name,
               (select case
                           when spi.proc_name <> 'Statement'
                               then N'The stored procedure ' + spi.proc_name
                           else N'This ad hoc statement'
                           end
                           + N' had the following implicit conversions: '
                           + CHAR(10)
                           + STUFF((
                                       select distinct @nl
                                                           + case
                                                                 when spi2.variable_name <> N'**no_variable**'
                                                                     then N'The variable '
                                                                 when spi2.variable_name = N'**no_variable**' and
                                                                      (spi2.column_name = spi2.converted_column_name or
                                                                       spi2.column_name like '%CONVERT_IMPLICIT%')
                                                                     then N'The compiled value '
                                                                 when spi2.column_name like '%Expr%'
                                                                     then 'The expression '
                                                                 else N'The column '
                                                           end
                                                           + case
                                                                 when spi2.variable_name <> N'**no_variable**'
                                                                     then spi2.variable_name
                                                                 when spi2.variable_name = N'**no_variable**' and
                                                                      (spi2.column_name = spi2.converted_column_name or
                                                                       spi2.column_name like '%CONVERT_IMPLICIT%')
                                                                     then spi2.compile_time_value
                                                                 else spi2.column_name
                                                           end
                                                           + N' has a data type of '
                                                           + case
                                                                 when spi2.variable_datatype = N'**no_variable**'
                                                                     then spi2.converted_to
                                                                 else spi2.variable_datatype
                                                           end
                                                           + N' which caused implicit conversion on the column '
                                                           + case
                                                                 when spi2.column_name like N'%CONVERT_IMPLICIT%'
                                                                     then spi2.converted_column_name
                                                                 when spi2.column_name = N'**no_column**'
                                                                     then spi2.converted_column_name
                                                                 when spi2.converted_column_name = N'**no_column**'
                                                                     then spi2.column_name
                                                                 when spi2.column_name <> spi2.converted_column_name
                                                                     then spi2.converted_column_name
                                                                 else spi2.column_name
                                                           end
                                                           + case
                                                                 when spi2.variable_name = N'**no_variable**' and
                                                                      (spi2.column_name = spi2.converted_column_name or
                                                                       spi2.column_name like '%CONVERT_IMPLICIT%')
                                                                     then N''
                                                                 when spi2.column_name like '%Expr%'
                                                                     then N''
                                                                 when spi2.compile_time_value not in ('**declared in proc**', '**idk_man**')
                                                                     and spi2.compile_time_value <> spi2.column_name
                                                                     then ' with the value ' + RTRIM(spi2.compile_time_value)
                                                                 else N''
                                                           end
                                                           + '.'
                                       from #stored_proc_info as spi2
                                       where spi.sqlhandle = spi2.sqlhandle
                                       for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                           as [processing-instruction(ClickMe)]
                for xml path(''), type)
                   as implicit_conversion_info
        from #stored_proc_info as spi
        group by spi.spid, spi.sqlhandle, spi.proc_name
    )
    update b
    set b.implicit_conversion_info = pk.implicit_conversion_info
    from ##blitzcacheprocs as b
             join precheck pk
                  on pk.sqlhandle = b.sqlhandle
                      and pk.spid = b.spid
    option (recompile);


    raiserror (N'Updating cached parameter XML for stored procs', 0, 1) with nowait;
    with precheck as (
        select spi.spid,
               spi.sqlhandle,
               spi.proc_name,
               (select set_options
                           + @nl
                           + @nl
                           + N'EXEC '
                           + spi.proc_name
                           + N' '
                           + STUFF((
                                       select distinct N', '
                                                           + case
                                                                 when spi2.variable_name <> N'**no_variable**' and
                                                                      spi2.compile_time_value <> N'**idk_man**'
                                                                     then spi2.variable_name + N' = '
                                                                 else @nl +
                                                                      N' We could not find any cached parameter values for this stored proc. '
                                                           end
                                                           + case
                                                                 when spi2.variable_name = N'**no_variable**' or
                                                                      spi2.compile_time_value = N'**idk_man**'
                                                                     then @nl + N'More info on possible reasons: https://BrentOzar.com/go/noplans '
                                                                 when spi2.compile_time_value = N'NULL'
                                                                     then spi2.compile_time_value
                                                                 else RTRIM(spi2.compile_time_value)
                                                           end
                                       from #stored_proc_info as spi2
                                       where spi.sqlhandle = spi2.sqlhandle
                                         and spi2.proc_name <> N'Statement'
                                       for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                           as [processing-instruction(ClickMe)]
                for xml path(''), type)
                   as cached_execution_parameters
        from #stored_proc_info as spi
        group by spi.spid, spi.sqlhandle, spi.proc_name, spi.set_options
    )
    update b
    set b.cached_execution_parameters = pk.cached_execution_parameters
    from ##blitzcacheprocs as b
             join precheck pk
                  on pk.sqlhandle = b.sqlhandle
                      and pk.spid = b.spid
    where b.querytype <> N'Statement'
    option (recompile);


    raiserror (N'Updating cached parameter XML for statements', 0, 1) with nowait;
    with precheck as (
        select spi.spid,
               spi.sqlhandle,
               spi.proc_name,
               (select set_options
                           + @nl
                           + @nl
                           + N' See QueryText column for full query text'
                           + @nl
                           + @nl
                           + STUFF((
                                       select distinct N', '
                                                           + case
                                                                 when spi2.variable_name <> N'**no_variable**' and
                                                                      spi2.compile_time_value <> N'**idk_man**'
                                                                     then spi2.variable_name + N' = '
                                                                 else @nl +
                                                                      N' We could not find any cached parameter values for this stored proc. '
                                                           end
                                                           + case
                                                                 when spi2.variable_name = N'**no_variable**' or
                                                                      spi2.compile_time_value = N'**idk_man**'
                                                                     then @nl +
                                                                          N' More info on possible reasons: https://BrentOzar.com/go/noplans '
                                                                 when spi2.compile_time_value = N'NULL'
                                                                     then spi2.compile_time_value
                                                                 else RTRIM(spi2.compile_time_value)
                                                           end
                                       from #stored_proc_info as spi2
                                       where spi.sqlhandle = spi2.sqlhandle
                                         and spi2.proc_name = N'Statement'
                                         and spi2.variable_name not like N'%msparam%'
                                       for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                           as [processing-instruction(ClickMe)]
                for xml path(''), type)
                   as cached_execution_parameters
        from #stored_proc_info as spi
        group by spi.spid, spi.sqlhandle, spi.proc_name, spi.set_options
    )
    update b
    set b.cached_execution_parameters = pk.cached_execution_parameters
    from ##blitzcacheprocs as b
             join precheck pk
                  on pk.sqlhandle = b.sqlhandle
                      and pk.spid = b.spid
    where b.querytype = N'Statement'
    option (recompile);

    raiserror (N'Filling in implicit conversion and cached plan parameter info', 0, 1) with nowait;
    update b
    set b.implicit_conversion_info    = case
                                            when b.implicit_conversion_info is null
                                                or CONVERT(nvarchar(max), b.implicit_conversion_info) = N''
                                                then '<?NoNeedToClickMe -- N/A --?>'
                                            else b.implicit_conversion_info end,
        b.cached_execution_parameters = case
                                            when b.cached_execution_parameters is null
                                                or CONVERT(nvarchar(max), b.cached_execution_parameters) = N''
                                                then '<?NoNeedToClickMe -- N/A --?>'
                                            else b.cached_execution_parameters end
    from ##blitzcacheprocs as b
    where b.spid = @@SPID
    option (recompile);

    /*End implicit conversion and parameter info*/

/*Begin Missing Index*/
    if EXISTS(select 1 / 0
              from ##blitzcacheprocs as bbcp
              where bbcp.missing_index_count > 0
                 or bbcp.index_spool_cost > 0
                 or bbcp.index_spool_rows > 0
                  and bbcp.spid = @@SPID)
        begin
            raiserror (N'Inserting to #missing_index_xml', 0, 1) with nowait;
            with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
            insert
            #missing_index_xml
            select qp.queryhash,
                   qp.sqlhandle,
                   c.mg.value('@Impact', 'FLOAT') as impact,
                   c.mg.query('.')                as cmg
            from #query_plan as qp
                     cross apply qp.query_plan.nodes('//p:MissingIndexes/p:MissingIndexGroup') as c(mg)
            where qp.queryhash is not null
            option (recompile);

            raiserror (N'Inserting to #missing_index_schema', 0, 1) with nowait;
            with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
            insert
            #missing_index_schema
            select mix.queryhash,
                   mix.sqlhandle,
                   mix.impact,
                   c.mi.value('@Database', 'NVARCHAR(128)'),
                   c.mi.value('@Schema', 'NVARCHAR(128)'),
                   c.mi.value('@Table', 'NVARCHAR(128)'),
                   c.mi.query('.')
            from #missing_index_xml as mix
                     cross apply mix.index_xml.nodes('//p:MissingIndex') as c(mi)
            option (recompile);

            raiserror (N'Inserting to #missing_index_usage', 0, 1) with nowait;
            with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
            insert
            #missing_index_usage
            select ms.queryhash,
                   ms.sqlhandle,
                   ms.impact,
                   ms.database_name,
                   ms.schema_name,
                   ms.table_name,
                   c.cg.value('@Usage', 'NVARCHAR(128)'),
                   c.cg.query('.')
            from #missing_index_schema ms
                     cross apply ms.index_xml.nodes('//p:ColumnGroup') as c(cg)
            option (recompile);

            raiserror (N'Inserting to #missing_index_detail', 0, 1) with nowait;
            with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
            insert
            #missing_index_detail
            select miu.queryhash,
                   miu.sqlhandle,
                   miu.impact,
                   miu.database_name,
                   miu.schema_name,
                   miu.table_name,
                   miu.usage,
                   c.c.value('@Name', 'NVARCHAR(128)')
            from #missing_index_usage as miu
                     cross apply miu.index_xml.nodes('//p:Column') as c(c)
            option (recompile);

            raiserror (N'Inserting to missing indexes to #missing_index_pretty', 0, 1) with nowait;
            insert #missing_index_pretty
            (queryhash, sqlhandle, impact, database_name, schema_name, table_name, equality, inequality, include,
             executions, query_cost, creation_hours, is_spool)
            select distinct m.queryhash,
                            m.sqlhandle,
                            m.impact,
                            m.database_name,
                            m.schema_name,
                            m.table_name
                    ,
                            STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                   from #missing_index_detail as m2
                                   where m2.usage = 'EQUALITY'
                                     and m.queryhash = m2.queryhash
                                     and m.sqlhandle = m2.sqlhandle
                                     and m.impact = m2.impact
                                     and m.database_name = m2.database_name
                                     and m.schema_name = m2.schema_name
                                     and m.table_name = m2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as equality
                    ,
                            STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                   from #missing_index_detail as m2
                                   where m2.usage = 'INEQUALITY'
                                     and m.queryhash = m2.queryhash
                                     and m.sqlhandle = m2.sqlhandle
                                     and m.impact = m2.impact
                                     and m.database_name = m2.database_name
                                     and m.schema_name = m2.schema_name
                                     and m.table_name = m2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as inequality
                    ,
                            STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                   from #missing_index_detail as m2
                                   where m2.usage = 'INCLUDE'
                                     and m.queryhash = m2.queryhash
                                     and m.sqlhandle = m2.sqlhandle
                                     and m.impact = m2.impact
                                     and m.database_name = m2.database_name
                                     and m.schema_name = m2.schema_name
                                     and m.table_name = m2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as [include],
                            bbcp.executioncount,
                            bbcp.queryplancost,
                            bbcp.plancreationtimehours,
                            0                                                                            as is_spool
            from #missing_index_detail as m
                     join ##blitzcacheprocs as bbcp
                          on m.sqlhandle = bbcp.sqlhandle
                              and m.queryhash = bbcp.queryhash
            option (recompile);

            raiserror (N'Inserting to #index_spool_ugly', 0, 1) with nowait;
            with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
            insert
            #index_spool_ugly
            (
            queryhash
            ,
            sqlhandle
            ,
            impact
            ,
            database_name
            ,
            schema_name
            ,
            table_name
            ,
            equality
            ,
            inequality
            ,
            include
            ,
            executions
            ,
            query_cost
            ,
            creation_hours
            )
            select p.queryhash,
                   p.sqlhandle,
                   (c.n.value('@EstimateIO', 'FLOAT') + (c.n.value('@EstimateCPU', 'FLOAT')))
                       / (1 * NULLIF(p.queryplancost, 0)) * 100 as impact,
                   o.n.value('@Database', 'NVARCHAR(128)')      as output_database,
                   o.n.value('@Schema', 'NVARCHAR(128)')        as output_schema,
                   o.n.value('@Table', 'NVARCHAR(128)')         as output_table,
                   k.n.value('@Column', 'NVARCHAR(128)')        as range_column,
                   e.n.value('@Column', 'NVARCHAR(128)')        as expression_column,
                   o.n.value('@Column', 'NVARCHAR(128)')        as output_column,
                   p.executioncount,
                   p.queryplancost,
                   p.plancreationtimehours
            from #relop as r
                     join ##blitzcacheprocs p
                          on p.queryhash = r.queryhash
                     cross apply r.relop.nodes('/p:RelOp') as c(n)
                     cross apply r.relop.nodes('/p:RelOp/p:OutputList/p:ColumnReference') as o(n)
                     outer apply r.relop.nodes(
                    '/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeColumns/p:ColumnReference') as k(n)
                     outer apply r.relop.nodes(
                    '/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeExpressions/p:ColumnReference') as e(n)
            where r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1

            raiserror (N'Inserting to spools to #missing_index_pretty', 0, 1) with nowait;
            insert #missing_index_pretty
            (queryhash, sqlhandle, impact, database_name, schema_name, table_name, equality, inequality, include,
             executions, query_cost, creation_hours, is_spool)
            select distinct isu.queryhash,
                            isu.sqlhandle,
                            isu.impact,
                            isu.database_name,
                            isu.schema_name,
                            isu.table_name
                    ,
                            STUFF((select distinct N', ' + ISNULL(isu2.equality, '') as column_name
                                   from #index_spool_ugly as isu2
                                   where isu2.equality is not null
                                     and isu.queryhash = isu2.queryhash
                                     and isu.sqlhandle = isu2.sqlhandle
                                     and isu.impact = isu2.impact
                                     and isu.database_name = isu2.database_name
                                     and isu.schema_name = isu2.schema_name
                                     and isu.table_name = isu2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as equality
                    ,
                            STUFF((select distinct N', ' + ISNULL(isu2.inequality, '') as column_name
                                   from #index_spool_ugly as isu2
                                   where isu2.inequality is not null
                                     and isu.queryhash = isu2.queryhash
                                     and isu.sqlhandle = isu2.sqlhandle
                                     and isu.impact = isu2.impact
                                     and isu.database_name = isu2.database_name
                                     and isu.schema_name = isu2.schema_name
                                     and isu.table_name = isu2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as inequality
                    ,
                            STUFF((select distinct N', ' + ISNULL(isu2.include, '') as column_name
                                   from #index_spool_ugly as isu2
                                   where isu2.include is not null
                                     and isu.queryhash = isu2.queryhash
                                     and isu.sqlhandle = isu2.sqlhandle
                                     and isu.impact = isu2.impact
                                     and isu.database_name = isu2.database_name
                                     and isu.schema_name = isu2.schema_name
                                     and isu.table_name = isu2.table_name
                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'') as include,
                            isu.executions,
                            isu.query_cost,
                            isu.creation_hours,
                            1                                                                            as is_spool
            from #index_spool_ugly as isu


            raiserror (N'Updating missing index information', 0, 1) with nowait;
            with missing as (
                select distinct mip.queryhash,
                                mip.sqlhandle,
                                mip.executions,
                                N'<MissingIndexes><![CDATA['
                                    + CHAR(10) + CHAR(13)
                                    + STUFF((select CHAR(10) + CHAR(13) + ISNULL(mip2.details, '') as details
                                             from #missing_index_pretty as mip2
                                             where mip.queryhash = mip2.queryhash
                                               and mip.sqlhandle = mip2.sqlhandle
                                               and mip.executions = mip2.executions
                                             group by mip2.details
                                             order by MAX(mip2.impact) desc
                                             for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'')
                                    + CHAR(10) + CHAR(13)
                                    + N']]></MissingIndexes>'
                                    as full_details
                from #missing_index_pretty as mip
            )
            update bbcp
            set bbcp.missing_indexes = m.full_details
            from ##blitzcacheprocs as bbcp
                     join missing as m
                          on m.sqlhandle = bbcp.sqlhandle
                              and m.queryhash = bbcp.queryhash
                              and m.executions = bbcp.executioncount
                              and spid = @@SPID
            option (recompile);

        end;

    raiserror (N'Filling in missing index blanks', 0, 1) with nowait;
    update b
    set b.missing_indexes =
            case
                when b.missing_indexes is null
                    then '<?NoNeedToClickMe -- N/A --?>'
                else b.missing_indexes
                end
    from ##blitzcacheprocs as b
    where b.spid = @@SPID
    option (recompile);

    /*End Missing Index*/


/* Set configuration values */
    raiserror (N'Setting configuration values', 0, 1) with nowait;
    declare @execution_threshold int = 1000 ,
        @parameter_sniffing_warning_pct tinyint = 30,
        /* This is in average reads */
        @parameter_sniffing_io_threshold bigint = 100000 ,
        @ctp_threshold_pct tinyint = 10,
        @long_running_query_warning_seconds bigint = 300 * 1000 ,
        @memory_grant_warning_percent int = 10;

    if EXISTS(select 1 / 0 from #configuration where 'frequent execution threshold' = LOWER(parameter_name))
        begin
            select @execution_threshold = CAST(value as int)
            from #configuration
            where 'frequent execution threshold' = LOWER(parameter_name);

            set @msg = ' Setting "frequent execution threshold" to ' + CAST(@execution_threshold as varchar(10));

            raiserror (@msg, 0, 1) with nowait;
        end;

    if EXISTS(select 1 / 0 from #configuration where 'parameter sniffing variance percent' = LOWER(parameter_name))
        begin
            select @parameter_sniffing_warning_pct = CAST(value as tinyint)
            from #configuration
            where 'parameter sniffing variance percent' = LOWER(parameter_name);

            set @msg = ' Setting "parameter sniffing variance percent" to ' +
                       CAST(@parameter_sniffing_warning_pct as varchar(3));

            raiserror (@msg, 0, 1) with nowait;
        end;

    if EXISTS(select 1 / 0 from #configuration where 'parameter sniffing io threshold' = LOWER(parameter_name))
        begin
            select @parameter_sniffing_io_threshold = CAST(value as bigint)
            from #configuration
            where 'parameter sniffing io threshold' = LOWER(parameter_name);

            set @msg = ' Setting "parameter sniffing io threshold" to ' +
                       CAST(@parameter_sniffing_io_threshold as varchar(10));

            raiserror (@msg, 0, 1) with nowait;
        end;

    if EXISTS(select 1 / 0 from #configuration where 'cost threshold for parallelism warning' = LOWER(parameter_name))
        begin
            select @ctp_threshold_pct = CAST(value as tinyint)
            from #configuration
            where 'cost threshold for parallelism warning' = LOWER(parameter_name);

            set @msg = ' Setting "cost threshold for parallelism warning" to ' + CAST(@ctp_threshold_pct as varchar(3));

            raiserror (@msg, 0, 1) with nowait;
        end;

    if EXISTS(select 1 / 0 from #configuration where 'long running query warning (seconds)' = LOWER(parameter_name))
        begin
            select @long_running_query_warning_seconds = CAST(value * 1000 as bigint)
            from #configuration
            where 'long running query warning (seconds)' = LOWER(parameter_name);

            set @msg = ' Setting "long running query warning (seconds)" to ' +
                       CAST(@long_running_query_warning_seconds as varchar(10));

            raiserror (@msg, 0, 1) with nowait;
        end;

    if EXISTS(select 1 / 0 from #configuration where 'unused memory grant' = LOWER(parameter_name))
        begin
            select @memory_grant_warning_percent = CAST(value as int)
            from #configuration
            where 'unused memory grant' = LOWER(parameter_name);

            set @msg = ' Setting "unused memory grant" to ' + CAST(@memory_grant_warning_percent as varchar(10));

            raiserror (@msg, 0, 1) with nowait;
        end;

    declare @ctp int;

    select @ctp = NULLIF(CAST(value as int), 0)
    from sys.configurations
    where name = 'cost threshold for parallelism'
    option (recompile);


/* Update to populate checks columns */
    raiserror ('Checking for query level SQL Server issues.', 0, 1) with nowait;

    with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
    update ##blitzcacheprocs
    set frequent_execution        = case when executionsperminute > @execution_threshold then 1 end,
        parameter_sniffing        = case
                                        when executioncount > 3 and averagereads > @parameter_sniffing_io_threshold
                                            and min_worker_time <
                                                ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * averagecpu) then 1
                                        when executioncount > 3 and averagereads > @parameter_sniffing_io_threshold
                                            and max_worker_time >
                                                ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * averagecpu) then 1
                                        when executioncount > 3 and averagereads > @parameter_sniffing_io_threshold
                                            and minreturnedrows <
                                                ((1.0 - (@parameter_sniffing_warning_pct / 100.0)) * averagereturnedrows)
                                            then 1
                                        when executioncount > 3 and averagereads > @parameter_sniffing_io_threshold
                                            and maxreturnedrows >
                                                ((1.0 + (@parameter_sniffing_warning_pct / 100.0)) * averagereturnedrows)
                                            then 1 end,
        near_parallel             = case
                                        when is_parallel <> 1 and
                                             queryplancost between @ctp * (1 - (@ctp_threshold_pct / 100.0)) and @ctp
                                            then 1 end,
        long_running              = case
                                        when averageduration > @long_running_query_warning_seconds then 1
                                        when max_worker_time > @long_running_query_warning_seconds then 1
                                        when max_elapsed_time > @long_running_query_warning_seconds then 1 end,
        is_key_lookup_expensive   = case
                                        when queryplancost >= (@ctp / 2) and key_lookup_cost >= queryplancost * .5
                                            then 1 end,
        is_sort_expensive         = case
                                        when queryplancost >= (@ctp / 2) and sort_cost >= queryplancost * .5 then 1 end,
        is_remote_query_expensive = case when remote_query_cost >= queryplancost * .05 then 1 end,
        is_unused_grant           = case
                                        when percentmemorygrantused <= @memory_grant_warning_percent and
                                             mingrantkb > @minmemoryperquery then 1 end,
        long_running_low_cpu      = case when averageduration > averagecpu * 4 and averagecpu < 500. then 1 end,
        low_cost_high_cpu         = case when queryplancost <= 10 and averagecpu > 5000. then 1 end,
        is_spool_expensive        = case
                                        when queryplancost > (@ctp / 5) and index_spool_cost >= queryplancost * .1
                                            then 1 end,
        is_spool_more_rows        = case
                                        when index_spool_rows >=
                                             (averagereturnedrows / ISNULL(NULLIF(executioncount, 0), 1)) then 1 end,
        is_table_spool_expensive  = case
                                        when queryplancost > (@ctp / 5) and table_spool_cost >= queryplancost / 4
                                            then 1 end,
        is_table_spool_more_rows  = case
                                        when table_spool_rows >=
                                             (averagereturnedrows / ISNULL(NULLIF(executioncount, 0), 1)) then 1 end,
        is_bad_estimate           = case
                                        when averagereturnedrows > 0 and (estimated_rows * 1000 < averagereturnedrows or
                                                                          estimated_rows > averagereturnedrows * 1000)
                                            then 1 end,
        is_big_spills             = case when (avgspills / 128.) > 499. then 1 end
    where spid = @@SPID
    option (recompile);


    raiserror ('Checking for forced parameterization and cursors.', 0, 1) with nowait;

/* Set options checks */
    update p
    set is_forced_parameterized = case when (CAST(pa.value as int) & 131072 = 131072) then 1 end,
        is_forced_plan          = case when (CAST(pa.value as int) & 4 = 4) then 1 end,
        setoptions              = SUBSTRING(
                    case when (CAST(pa.value as int) & 1 = 1) then ', ANSI_PADDING' else '' end +
                    case when (CAST(pa.value as int) & 8 = 8) then ', CONCAT_NULL_YIELDS_NULL' else '' end +
                    case when (CAST(pa.value as int) & 16 = 16) then ', ANSI_WARNINGS' else '' end +
                    case when (CAST(pa.value as int) & 32 = 32) then ', ANSI_NULLS' else '' end +
                    case when (CAST(pa.value as int) & 64 = 64) then ', QUOTED_IDENTIFIER' else '' end +
                    case when (CAST(pa.value as int) & 4096 = 4096) then ', ARITH_ABORT' else '' end +
                    case when (CAST(pa.value as int) & 8192 = 8191) then ', NUMERIC_ROUNDABORT' else '' end
            , 2, 200000)
    from ##blitzcacheprocs p
             cross apply sys.dm_exec_plan_attributes(p.planhandle) pa
    where pa.attribute = 'set_options'
      and spid = @@SPID
    option (recompile);


/* Cursor checks */
    update p
    set is_cursor = case when CAST(pa.value as int) <> 0 then 1 end
    from ##blitzcacheprocs p
             cross apply sys.dm_exec_plan_attributes(p.planhandle) pa
    where pa.attribute like '%cursor%'
      and spid = @@SPID
    option (recompile);

    update p
    set is_cursor = 1
    from ##blitzcacheprocs p
    where queryhash = 0x0000000000000000
       or queryplanhash = 0x0000000000000000
        and spid = @@SPID
    option (recompile);


    raiserror ('Populating Warnings column', 0, 1) with nowait;
/* Populate warnings */
    update ##blitzcacheprocs
    set warnings = SUBSTRING(
                case when warning_no_join_predicate = 1 then ', No Join Predicate' else '' end +
                case when compile_timeout = 1 then ', Compilation Timeout' else '' end +
                case when compile_memory_limit_exceeded = 1 then ', Compile Memory Limit Exceeded' else '' end +
                case when busy_loops = 1 then ', Busy Loops' else '' end +
                case when is_forced_plan = 1 then ', Forced Plan' else '' end +
                case when is_forced_parameterized = 1 then ', Forced Parameterization' else '' end +
                case when unparameterized_query = 1 then ', Unparameterized Query' else '' end +
                case
                    when missing_index_count > 0
                        then ', Missing Indexes (' + CAST(missing_index_count as varchar(3)) + ')'
                    else '' end +
                case
                    when unmatched_index_count > 0 then ', Unmatched Indexes (' +
                                                        CAST(unmatched_index_count as varchar(3)) + ')'
                    else '' end +
                case
                    when is_cursor = 1 then ', Cursor'
                        + case when is_optimistic_cursor = 1 then '; optimistic' else '' end
                        + case when is_forward_only_cursor = 0 then '; not forward only' else '' end
                        + case when is_cursor_dynamic = 1 then '; dynamic' else '' end
                        + case when is_fast_forward_cursor = 1 then '; fast forward' else '' end
                    else '' end +
                case when is_parallel = 1 then ', Parallel' else '' end +
                case when near_parallel = 1 then ', Nearly Parallel' else '' end +
                case when frequent_execution = 1 then ', Frequent Execution' else '' end +
                case when plan_warnings = 1 then ', Plan Warnings' else '' end +
                case when parameter_sniffing = 1 then ', Parameter Sniffing' else '' end +
                case when long_running = 1 then ', Long Running Query' else '' end +
                case when downlevel_estimator = 1 then ', Downlevel CE' else '' end +
                case when implicit_conversions = 1 then ', Implicit Conversions' else '' end +
                case when tvf_join = 1 then ', Function Join' else '' end +
                case
                    when plan_multiple_plans > 0 then ', Multiple Plans' +
                                                      COALESCE(' (' + CAST(plan_multiple_plans as varchar(10)) + ')', '')
                    else '' end +
                case when is_trivial = 1 then ', Trivial Plans' else '' end +
                case when is_forced_serial = 1 then ', Forced Serialization' else '' end +
                case when is_key_lookup_expensive = 1 then ', Expensive Key Lookup' else '' end +
                case when is_remote_query_expensive = 1 then ', Expensive Remote Query' else '' end +
                case
                    when trace_flags_session is not null
                        then ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session
                    else '' end +
                case when is_unused_grant = 1 then ', Unused Memory Grant' else '' end +
                case
                    when function_count > 0 then ', Calls ' + CONVERT(varchar(10), function_count) + ' Function(s)'
                    else '' end +
                case
                    when clr_function_count > 0 then ', Calls ' + CONVERT(varchar(10), clr_function_count) +
                                                     ' CLR Function(s)'
                    else '' end +
                case when plancreationtimehours <= 4 then ', Plan created last 4hrs' else '' end +
                case when is_table_variable = 1 then ', Table Variables' else '' end +
                case when no_stats_warning = 1 then ', Columns With No Statistics' else '' end +
                case when relop_warnings = 1 then ', Operator Warnings' else '' end +
                case when is_table_scan = 1 then ', Table Scans (Heaps)' else '' end +
                case when backwards_scan = 1 then ', Backwards Scans' else '' end +
                case when forced_index = 1 then ', Forced Indexes' else '' end +
                case when forced_seek = 1 then ', Forced Seeks' else '' end +
                case when forced_scan = 1 then ', Forced Scans' else '' end +
                case when columnstore_row_mode = 1 then ', ColumnStore Row Mode ' else '' end +
                case when is_computed_scalar = 1 then ', Computed Column UDF ' else '' end +
                case when is_sort_expensive = 1 then ', Expensive Sort' else '' end +
                case when is_computed_filter = 1 then ', Filter UDF' else '' end +
                case when index_ops >= 5 then ', >= 5 Indexes Modified' else '' end +
                case when is_row_level = 1 then ', Row Level Security' else '' end +
                case when is_spatial = 1 then ', Spatial Index' else '' end +
                case when index_dml = 1 then ', Index DML' else '' end +
                case when table_dml = 1 then ', Table DML' else '' end +
                case when low_cost_high_cpu = 1 then ', Low Cost High CPU' else '' end +
                case when long_running_low_cpu = 1 then + ', Long Running With Low CPU' else '' end +
                case
                    when stale_stats = 1 then + ', Statistics used have > 100k modifications in the last 7 days'
                    else '' end +
                case when is_adaptive = 1 then + ', Adaptive Joins' else '' end +
                case when is_spool_expensive = 1 then + ', Expensive Index Spool' else '' end +
                case when is_spool_more_rows = 1 then + ', Large Index Row Spool' else '' end +
                case when is_table_spool_expensive = 1 then + ', Expensive Table Spool' else '' end +
                case when is_table_spool_more_rows = 1 then + ', Many Rows Table Spool' else '' end +
                case when is_bad_estimate = 1 then + ', Row Estimate Mismatch' else '' end +
                case when is_paul_white_electric = 1 then ', SWITCH!' else '' end +
                case when is_row_goal = 1 then ', Row Goals' else '' end +
                case when is_big_spills = 1 then ', >500mb Spills' else '' end +
                case when is_mstvf = 1 then ', MSTVFs' else '' end +
                case when is_mm_join = 1 then ', Many to Many Merge' else '' end +
                case when is_nonsargable = 1 then ', non-SARGables' else '' end +
                case when compiletime > 5000 then ', Long Compile Time' else '' end +
                case when compilecpu > 5000 then ', High Compile CPU' else '' end +
                case
                    when compilememory > 1024 and
                         ((compilememory) / (1 * case when maxcompilememory = 0 then 1 else maxcompilememory end) *
                          100.) >= 10. then ', High Compile Memory'
                    else '' end +
                case when select_with_writes > 0 then ', Select w/ Writes' else '' end
        , 3, 200000)
    where spid = @@SPID
    option (recompile);


    raiserror ('Populating Warnings column for stored procedures', 0, 1) with nowait;
    with statement_warnings as
             (
                 select distinct sqlhandle,
                                 warnings = SUBSTRING(
                                             case
                                                 when warning_no_join_predicate = 1 then ', No Join Predicate'
                                                 else '' end +
                                             case when compile_timeout = 1 then ', Compilation Timeout' else '' end +
                                             case
                                                 when compile_memory_limit_exceeded = 1
                                                     then ', Compile Memory Limit Exceeded'
                                                 else '' end +
                                             case when busy_loops = 1 then ', Busy Loops' else '' end +
                                             case when is_forced_plan = 1 then ', Forced Plan' else '' end +
                                             case
                                                 when is_forced_parameterized = 1 then ', Forced Parameterization'
                                                 else '' end +
                                             --CASE WHEN unparameterized_query = 1 THEN ', Unparameterized Query' ELSE '' END +
                                             case
                                                 when missing_index_count > 0 then ', Missing Indexes (' +
                                                                                   CONVERT(varchar(10),
                                                                                           (select SUM(b2.missing_index_count)
                                                                                            from ##blitzcacheprocs as b2
                                                                                            where b2.sqlhandle = b.sqlhandle
                                                                                              and b2.queryhash is not null
                                                                                              and spid = @@SPID)) + ')'
                                                 else '' end +
                                             case
                                                 when unmatched_index_count > 0 then ', Unmatched Indexes (' +
                                                                                     CONVERT(varchar(10),
                                                                                             (select SUM(b2.unmatched_index_count)
                                                                                              from ##blitzcacheprocs as b2
                                                                                              where b2.sqlhandle = b.sqlhandle
                                                                                                and b2.queryhash is not null
                                                                                                and spid = @@SPID)) +
                                                                                     ')'
                                                 else '' end +
                                             case
                                                 when is_cursor = 1 then ', Cursor'
                                                     +
                                                                         case when is_optimistic_cursor = 1 then '; optimistic' else '' end
                                                     +
                                                                         case when is_forward_only_cursor = 0 then '; not forward only' else '' end
                                                     + case when is_cursor_dynamic = 1 then '; dynamic' else '' end
                                                     +
                                                                         case when is_fast_forward_cursor = 1 then '; fast forward' else '' end
                                                 else '' end +
                                             case when is_parallel = 1 then ', Parallel' else '' end +
                                             case when near_parallel = 1 then ', Nearly Parallel' else '' end +
                                             case when frequent_execution = 1 then ', Frequent Execution' else '' end +
                                             case when plan_warnings = 1 then ', Plan Warnings' else '' end +
                                             case when parameter_sniffing = 1 then ', Parameter Sniffing' else '' end +
                                             case when long_running = 1 then ', Long Running Query' else '' end +
                                             case when downlevel_estimator = 1 then ', Downlevel CE' else '' end +
                                             case
                                                 when implicit_conversions = 1 then ', Implicit Conversions'
                                                 else '' end +
                                             case when tvf_join = 1 then ', Function Join' else '' end +
                                             case
                                                 when plan_multiple_plans > 0 then ', Multiple Plans' +
                                                                                   COALESCE(' (' + CAST(plan_multiple_plans as varchar(10)) + ')', '')
                                                 else '' end +
                                             case when is_trivial = 1 then ', Trivial Plans' else '' end +
                                             case when is_forced_serial = 1 then ', Forced Serialization' else '' end +
                                             case
                                                 when is_key_lookup_expensive = 1 then ', Expensive Key Lookup'
                                                 else '' end +
                                             case
                                                 when is_remote_query_expensive = 1 then ', Expensive Remote Query'
                                                 else '' end +
                                             case
                                                 when trace_flags_session is not null
                                                     then ', Session Level Trace Flag(s) Enabled: ' + trace_flags_session
                                                 else '' end +
                                             case when is_unused_grant = 1 then ', Unused Memory Grant' else '' end +
                                             case
                                                 when function_count > 0 then ', Calls ' + CONVERT(varchar(10),
                                                         (select SUM(b2.function_count)
                                                          from ##blitzcacheprocs as b2
                                                          where b2.sqlhandle = b.sqlhandle
                                                            and b2.queryhash is not null
                                                            and spid = @@SPID)) + ' function(s)'
                                                 else '' end +
                                             case
                                                 when clr_function_count > 0 then ', Calls ' + CONVERT(varchar(10),
                                                         (select SUM(b2.clr_function_count)
                                                          from ##blitzcacheprocs as b2
                                                          where b2.sqlhandle = b.sqlhandle
                                                            and b2.queryhash is not null
                                                            and spid = @@SPID)) + ' CLR function(s)'
                                                 else '' end +
                                             case
                                                 when plancreationtimehours <= 4 then ', Plan created last 4hrs'
                                                 else '' end +
                                             case when is_table_variable = 1 then ', Table Variables' else '' end +
                                             case
                                                 when no_stats_warning = 1 then ', Columns With No Statistics'
                                                 else '' end +
                                             case when relop_warnings = 1 then ', Operator Warnings' else '' end +
                                             case when is_table_scan = 1 then ', Table Scans' else '' end +
                                             case when backwards_scan = 1 then ', Backwards Scans' else '' end +
                                             case when forced_index = 1 then ', Forced Indexes' else '' end +
                                             case when forced_seek = 1 then ', Forced Seeks' else '' end +
                                             case when forced_scan = 1 then ', Forced Scans' else '' end +
                                             case
                                                 when columnstore_row_mode = 1 then ', ColumnStore Row Mode '
                                                 else '' end +
                                             case when is_computed_scalar = 1 then ', Computed Column UDF ' else '' end +
                                             case when is_sort_expensive = 1 then ', Expensive Sort' else '' end +
                                             case when is_computed_filter = 1 then ', Filter UDF' else '' end +
                                             case when index_ops >= 5 then ', >= 5 Indexes Modified' else '' end +
                                             case when is_row_level = 1 then ', Row Level Security' else '' end +
                                             case when is_spatial = 1 then ', Spatial Index' else '' end +
                                             case when index_dml = 1 then ', Index DML' else '' end +
                                             case when table_dml = 1 then ', Table DML' else '' end +
                                             case when low_cost_high_cpu = 1 then ', Low Cost High CPU' else '' end +
                                             case
                                                 when long_running_low_cpu = 1 then + ', Long Running With Low CPU'
                                                 else '' end +
                                             case
                                                 when stale_stats = 1
                                                     then + ', Statistics used have > 100k modifications in the last 7 days'
                                                 else '' end +
                                             case when is_adaptive = 1 then + ', Adaptive Joins' else '' end +
                                             case
                                                 when is_spool_expensive = 1 then + ', Expensive Index Spool'
                                                 else '' end +
                                             case
                                                 when is_spool_more_rows = 1 then + ', Large Index Row Spool'
                                                 else '' end +
                                             case
                                                 when is_table_spool_expensive = 1 then + ', Expensive Table Spool'
                                                 else '' end +
                                             case
                                                 when is_table_spool_more_rows = 1 then + ', Many Rows Table Spool'
                                                 else '' end +
                                             case when is_bad_estimate = 1 then + ', Row estimate mismatch' else '' end +
                                             case when is_paul_white_electric = 1 then ', SWITCH!' else '' end +
                                             case when is_row_goal = 1 then ', Row Goals' else '' end +
                                             case when is_big_spills = 1 then ', >500mb spills' else '' end +
                                             case when is_mstvf = 1 then ', MSTVFs' else '' end +
                                             case when is_mm_join = 1 then ', Many to Many Merge' else '' end +
                                             case when is_nonsargable = 1 then ', non-SARGables' else '' end +
                                             case when compiletime > 5000 then ', Long Compile Time' else '' end +
                                             case when compilecpu > 5000 then ', High Compile CPU' else '' end +
                                             case
                                                 when compilememory > 1024 and ((compilememory) /
                                                                                (1 * case when maxcompilememory = 0 then 1 else maxcompilememory end) *
                                                                                100.) >= 10.
                                                     then ', High Compile Memory'
                                                 else '' end +
                                             case when select_with_writes > 0 then ', Select w/ Writes' else '' end
                                     , 3, 200000)
                 from ##blitzcacheprocs b
                 where spid = @@SPID
                   and querytype like 'Statement (parent%'
             )
    update b
    set b.warnings = s.warnings
    from ##blitzcacheprocs as b
             join statement_warnings s
                  on b.sqlhandle = s.sqlhandle
    where querytype like 'Procedure or Function%'
      and spid = @@SPID
    option (recompile);

    raiserror ('Checking for plans with >128 levels of nesting', 0, 1) with nowait;
    with plan_handle as (
        select b.planhandle
        from ##blitzcacheprocs b
                 cross apply sys.dm_exec_text_query_plan(b.planhandle, 0, -1) tqp
                 cross apply sys.dm_exec_query_plan(b.planhandle) qp
        where tqp.encrypted = 0
          and b.spid = @@SPID
          and (qp.query_plan is null
            and tqp.query_plan is not null)
    )
    update b
    set warnings = ISNULL(
                'Your query plan is >128 levels of nested nodes, and can''t be converted to XML. Use SELECT * FROM sys.dm_exec_text_query_plan(' +
                CONVERT(varchar(128), ph.planhandle, 1) + ', 0, -1) to get more information'
        , 'We couldn''t find a plan for this query. More info on possible reasons: https://BrentOzar.com/go/noplans')
    from ##blitzcacheprocs b
             left join plan_handle ph on
        b.planhandle = ph.planhandle
    where b.queryplan is null
      and b.spid = @@SPID
    option (recompile);

    raiserror ('Checking for plans with no warnings', 0, 1) with nowait;
    update ##blitzcacheprocs
    set warnings = 'No warnings detected. ' + case @expertmode
                                                  when 0
                                                      then ' Try running sp_BlitzCache with @ExpertMode = 1 to find more advanced problems.'
                                                  else ''
        end
    where warnings = ''
       or warnings is null
        and spid = @@SPID
    option (recompile);


    results:
    if @exporttoexcel = 1
        begin
            raiserror ('Displaying results with Excel formatting (no plans).', 0, 1) with nowait;

            /* excel output */
            update ##blitzcacheprocs
            set querytext = SUBSTRING(
                    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(querytext)), ' ', '<>'), '><', ''), '<>', ' '), 1, 32000)
            option (recompile);

            set @sql = N'
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    SELECT  TOP (@Top)
            DatabaseName AS [Database Name],
            QueryPlanCost AS [Cost],
            QueryText,
            QueryType AS [Query Type],
            Warnings,
            ExecutionCount,
            ExecutionsPerMinute AS [Executions / Minute],
            PercentExecutions AS [Execution Weight],
            PercentExecutionsByType AS [% Executions (Type)],
            SerialDesiredMemory AS [Serial Desired Memory],
            SerialRequiredMemory AS [Serial Required Memory],
            TotalCPU AS [Total CPU (ms)],
            AverageCPU AS [Avg CPU (ms)],
            PercentCPU AS [CPU Weight],
            PercentCPUByType AS [% CPU (Type)],
            TotalDuration AS [Total Duration (ms)],
            AverageDuration AS [Avg Duration (ms)],
            PercentDuration AS [Duration Weight],
            PercentDurationByType AS [% Duration (Type)],
            TotalReads AS [Total Reads],
            AverageReads AS [Average Reads],
            PercentReads AS [Read Weight],
            PercentReadsByType AS [% Reads (Type)],
            TotalWrites AS [Total Writes],
            AverageWrites AS [Average Writes],
            PercentWrites AS [Write Weight],
            PercentWritesByType AS [% Writes (Type)],
            TotalReturnedRows,
            AverageReturnedRows,
            MinReturnedRows,
            MaxReturnedRows,
		    MinGrantKB,
		    MaxGrantKB,
		    MinUsedGrantKB,
		    MaxUsedGrantKB,
		    PercentMemoryGrantUsed,
			AvgMaxMemoryGrant,
			MinSpills,
			MaxSpills,
			TotalSpills,
			AvgSpills,
            NumberOfPlans,
            NumberOfDistinctPlans,
            PlanCreationTime AS [Created At],
            LastExecutionTime AS [Last Execution],
            StatementStartOffset,
            StatementEndOffset,
			PlanHandle AS [Plan Handle],
			SqlHandle AS [SQL Handle],
            QueryHash,
            QueryPlanHash,
            COALESCE(SetOptions, '''') AS [SET Options]
    FROM    ##BlitzCacheProcs
    WHERE   1 = 1
	AND SPID = @@SPID ' + @nl;

            if @minimumexecutioncount is not null
                begin
                    set @sql += N' AND ExecutionCount >= @minimumExecutionCount ';
                end;

            if @minutesback is not null
                begin
                    set @sql += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
                end;

            select @sql += N' ORDER BY ' + case @sortorder
                                               when N'cpu' then N' TotalCPU '
                                               when N'reads' then N' TotalReads '
                                               when N'writes' then N' TotalWrites '
                                               when N'duration' then N' TotalDuration '
                                               when N'executions' then N' ExecutionCount '
                                               when N'compiles' then N' PlanCreationTime '
                                               when N'memory grant' then N' MaxGrantKB'
                                               when N'spills' then N' MaxSpills'
                                               when N'avg cpu' then N' AverageCPU'
                                               when N'avg reads' then N' AverageReads'
                                               when N'avg writes' then N' AverageWrites'
                                               when N'avg duration' then N' AverageDuration'
                                               when N'avg executions' then N' ExecutionsPerMinute'
                                               when N'avg memory grant' then N' AvgMaxMemoryGrant'
                                               when N'avg spills' then N' AvgSpills'
                end + N' DESC ';

            set @sql += N' OPTION (RECOMPILE) ; ';

            if @debug = 1
                begin
                    print SUBSTRING(@sql, 0, 4000);
                    print SUBSTRING(@sql, 4000, 8000);
                    print SUBSTRING(@sql, 8000, 12000);
                    print SUBSTRING(@sql, 12000, 16000);
                    print SUBSTRING(@sql, 16000, 20000);
                    print SUBSTRING(@sql, 20000, 24000);
                    print SUBSTRING(@sql, 24000, 28000);
                    print SUBSTRING(@sql, 28000, 32000);
                    print SUBSTRING(@sql, 32000, 36000);
                    print SUBSTRING(@sql, 36000, 40000);
                end;

            exec sp_executesql @sql, N'@Top INT, @min_duration INT, @min_back INT, @minimumExecutionCount INT', @top,
                 @durationfilter_i, @minutesback, @minimumexecutioncount;
        end;


    raiserror ('Displaying analysis of plan cache.', 0, 1) with nowait;

    declare @columns nvarchar(max) = N'';

    if @expertmode = 0
        begin
            raiserror (N'Returning ExpertMode = 0', 0, 1) with nowait;
            set @columns = N' DatabaseName AS [Database],
    QueryPlanCost AS [Cost],
    QueryText AS [Query Text],
    QueryType AS [Query Type],
    Warnings AS [Warnings],
	QueryPlan AS [Query Plan],
	missing_indexes AS [Missing Indexes],
	implicit_conversion_info AS [Implicit Conversion Info],
	cached_execution_parameters AS [Cached Execution Parameters],
    CONVERT(NVARCHAR(30), CAST((ExecutionCount) AS BIGINT), 1) AS [# Executions],
    CONVERT(NVARCHAR(30), CAST((ExecutionsPerMinute) AS BIGINT), 1) AS [Executions / Minute],
    CONVERT(NVARCHAR(30), CAST((PercentExecutions) AS BIGINT), 1) AS [Execution Weight],
    CONVERT(NVARCHAR(30), CAST((TotalCPU) AS BIGINT), 1) AS [Total CPU (ms)],
    CONVERT(NVARCHAR(30), CAST((AverageCPU) AS BIGINT), 1) AS [Avg CPU (ms)],
    CONVERT(NVARCHAR(30), CAST((PercentCPU) AS BIGINT), 1) AS [CPU Weight],
    CONVERT(NVARCHAR(30), CAST((TotalDuration) AS BIGINT), 1) AS [Total Duration (ms)],
    CONVERT(NVARCHAR(30), CAST((AverageDuration) AS BIGINT), 1) AS [Avg Duration (ms)],
    CONVERT(NVARCHAR(30), CAST((PercentDuration) AS BIGINT), 1) AS [Duration Weight],
    CONVERT(NVARCHAR(30), CAST((TotalReads) AS BIGINT), 1) AS [Total Reads],
    CONVERT(NVARCHAR(30), CAST((AverageReads) AS BIGINT), 1) AS [Avg Reads],
    CONVERT(NVARCHAR(30), CAST((PercentReads) AS BIGINT), 1) AS [Read Weight],
    CONVERT(NVARCHAR(30), CAST((TotalWrites) AS BIGINT), 1) AS [Total Writes],
    CONVERT(NVARCHAR(30), CAST((AverageWrites) AS BIGINT), 1) AS [Avg Writes],
    CONVERT(NVARCHAR(30), CAST((PercentWrites) AS BIGINT), 1) AS [Write Weight],
    CONVERT(NVARCHAR(30), CAST((AverageReturnedRows) AS BIGINT), 1) AS [Average Rows],
	CONVERT(NVARCHAR(30), CAST((MinGrantKB) AS BIGINT), 1) AS [Minimum Memory Grant KB],
	CONVERT(NVARCHAR(30), CAST((MaxGrantKB) AS BIGINT), 1) AS [Maximum Memory Grant KB],
	CONVERT(NVARCHAR(30), CAST((MinUsedGrantKB) AS BIGINT), 1) AS [Minimum Used Grant KB],
	CONVERT(NVARCHAR(30), CAST((MaxUsedGrantKB) AS BIGINT), 1) AS [Maximum Used Grant KB],
	CONVERT(NVARCHAR(30), CAST((AvgMaxMemoryGrant) AS BIGINT), 1) AS [Average Max Memory Grant],
	CONVERT(NVARCHAR(30), CAST((MinSpills) AS BIGINT), 1) AS [Min Spills],
	CONVERT(NVARCHAR(30), CAST((MaxSpills) AS BIGINT), 1) AS [Max Spills],
	CONVERT(NVARCHAR(30), CAST((TotalSpills) AS BIGINT), 1) AS [Total Spills],
	CONVERT(NVARCHAR(30), CAST((AvgSpills) AS MONEY), 1) AS [Avg Spills],
    PlanCreationTime AS [Created At],
    LastExecutionTime AS [Last Execution],
	LastCompletionTime AS [Last Completion],
	PlanHandle AS [Plan Handle],
	SqlHandle AS [SQL Handle],
    COALESCE(SetOptions, '''') AS [SET Options],
	[Remove Plan Handle From Cache]';
        end;
    else
        begin
            set @columns = N' DatabaseName AS [Database],
		QueryPlanCost AS [Cost],
        QueryText AS [Query Text],
        QueryType AS [Query Type],
        Warnings AS [Warnings],
		QueryPlan AS [Query Plan],
		missing_indexes AS [Missing Indexes],
		implicit_conversion_info AS [Implicit Conversion Info],
		cached_execution_parameters AS [Cached Execution Parameters], ' + @nl;

            if @expertmode = 2 /* Opserver */
                begin
                    raiserror (N'Returning Expert Mode = 2', 0, 1) with nowait;
                    set @columns += N'
				  SUBSTRING(
                  CASE WHEN warning_no_join_predicate = 1 THEN '', 20'' ELSE '''' END +
                  CASE WHEN compile_timeout = 1 THEN '', 18'' ELSE '''' END +
                  CASE WHEN compile_memory_limit_exceeded = 1 THEN '', 19'' ELSE '''' END +
                  CASE WHEN busy_loops = 1 THEN '', 16'' ELSE '''' END +
                  CASE WHEN is_forced_plan = 1 THEN '', 3'' ELSE '''' END +
                  CASE WHEN is_forced_parameterized > 0 THEN '', 5'' ELSE '''' END +
                  CASE WHEN unparameterized_query = 1 THEN '', 23'' ELSE '''' END +
                  CASE WHEN missing_index_count > 0 THEN '', 10'' ELSE '''' END +
                  CASE WHEN unmatched_index_count > 0 THEN '', 22'' ELSE '''' END +
                  CASE WHEN is_cursor = 1 THEN '', 4'' ELSE '''' END +
                  CASE WHEN is_parallel = 1 THEN '', 6'' ELSE '''' END +
                  CASE WHEN near_parallel = 1 THEN '', 7'' ELSE '''' END +
                  CASE WHEN frequent_execution = 1 THEN '', 1'' ELSE '''' END +
                  CASE WHEN plan_warnings = 1 THEN '', 8'' ELSE '''' END +
                  CASE WHEN parameter_sniffing = 1 THEN '', 2'' ELSE '''' END +
                  CASE WHEN long_running = 1 THEN '', 9'' ELSE '''' END +
                  CASE WHEN downlevel_estimator = 1 THEN '', 13'' ELSE '''' END +
                  CASE WHEN implicit_conversions = 1 THEN '', 14'' ELSE '''' END +
                  CASE WHEN tvf_join = 1 THEN '', 17'' ELSE '''' END +
                  CASE WHEN plan_multiple_plans > 0 THEN '', 21'' ELSE '''' END +
                  CASE WHEN unmatched_index_count > 0 THEN '', 22'' ELSE '''' END +
                  CASE WHEN is_trivial = 1 THEN '', 24'' ELSE '''' END +
				  CASE WHEN is_forced_serial = 1 THEN '', 25'' ELSE '''' END +
                  CASE WHEN is_key_lookup_expensive = 1 THEN '', 26'' ELSE '''' END +
				  CASE WHEN is_remote_query_expensive = 1 THEN '', 28'' ELSE '''' END +
				  CASE WHEN trace_flags_session IS NOT NULL THEN '', 29'' ELSE '''' END +
				  CASE WHEN is_unused_grant = 1 THEN '', 30'' ELSE '''' END +
				  CASE WHEN function_count > 0 THEN '', 31'' ELSE '''' END +
				  CASE WHEN clr_function_count > 0 THEN '', 32'' ELSE '''' END +
				  CASE WHEN PlanCreationTimeHours <= 4 THEN '', 33'' ELSE '''' END +
				  CASE WHEN is_table_variable = 1 THEN '', 34'' ELSE '''' END  +
				  CASE WHEN no_stats_warning = 1 THEN '', 35'' ELSE '''' END  +
				  CASE WHEN relop_warnings = 1 THEN '', 36'' ELSE '''' END +
				  CASE WHEN is_table_scan = 1 THEN '', 37'' ELSE '''' END +
				  CASE WHEN backwards_scan = 1 THEN '', 38'' ELSE '''' END +
				  CASE WHEN forced_index = 1 THEN '', 39'' ELSE '''' END +
				  CASE WHEN forced_seek = 1 OR forced_scan = 1 THEN '', 40'' ELSE '''' END +
				  CASE WHEN columnstore_row_mode = 1 THEN '', 41'' ELSE '''' END +
				  CASE WHEN is_computed_scalar = 1 THEN '', 42'' ELSE '''' END +
				  CASE WHEN is_sort_expensive = 1 THEN '', 43'' ELSE '''' END +
				  CASE WHEN is_computed_filter = 1 THEN '', 44'' ELSE '''' END +
				  CASE WHEN index_ops >= 5 THEN  '', 45'' ELSE '''' END +
				  CASE WHEN is_row_level = 1 THEN  '', 46'' ELSE '''' END +
				  CASE WHEN is_spatial = 1 THEN '', 47'' ELSE '''' END +
				  CASE WHEN index_dml = 1 THEN '', 48'' ELSE '''' END +
				  CASE WHEN table_dml = 1 THEN '', 49'' ELSE '''' END +
				  CASE WHEN long_running_low_cpu = 1 THEN '', 50'' ELSE '''' END +
				  CASE WHEN low_cost_high_cpu = 1 THEN '', 51'' ELSE '''' END +
				  CASE WHEN stale_stats = 1 THEN '', 52'' ELSE '''' END +
				  CASE WHEN is_adaptive = 1 THEN '', 53'' ELSE '''' END	+
				  CASE WHEN is_spool_expensive = 1 THEN + '', 54'' ELSE '''' END +
				  CASE WHEN is_spool_more_rows = 1 THEN + '', 55'' ELSE '''' END  +
				  CASE WHEN is_table_spool_expensive = 1 THEN + '', 67'' ELSE '''' END +
				  CASE WHEN is_table_spool_more_rows = 1 THEN + '', 68'' ELSE '''' END  +
				  CASE WHEN is_bad_estimate = 1 THEN + '', 56'' ELSE '''' END  +
				  CASE WHEN is_paul_white_electric = 1 THEN '', 57'' ELSE '''' END +
				  CASE WHEN is_row_goal = 1 THEN '', 58'' ELSE '''' END +
                  CASE WHEN is_big_spills = 1 THEN '', 59'' ELSE '''' END +
				  CASE WHEN is_mstvf = 1 THEN '', 60'' ELSE '''' END +
				  CASE WHEN is_mm_join = 1 THEN '', 61'' ELSE '''' END  +
                  CASE WHEN is_nonsargable = 1 THEN '', 62'' ELSE '''' END +
				  CASE WHEN CompileTime > 5000 THEN '', 63 '' ELSE '''' END +
				  CASE WHEN CompileCPU > 5000 THEN '', 64 '' ELSE '''' END +
				  CASE WHEN CompileMemory > 1024 AND ((CompileMemory) / (1 * CASE WHEN MaxCompileMemory = 0 THEN 1 ELSE MaxCompileMemory END) * 100.) >= 10. THEN '', 65 '' ELSE '''' END +
				  CASE WHEN select_with_writes > 0 THEN '', 66'' ELSE '''' END
				  , 3, 200000) AS opserver_warning , ' + @nl;
                end;

            set @columns += N'
        CONVERT(NVARCHAR(30), CAST((ExecutionCount) AS BIGINT), 1) AS [# Executions],
        CONVERT(NVARCHAR(30), CAST((ExecutionsPerMinute) AS BIGINT), 1) AS [Executions / Minute],
        CONVERT(NVARCHAR(30), CAST((PercentExecutions) AS BIGINT), 1) AS [Execution Weight],
        CONVERT(NVARCHAR(30), CAST((SerialDesiredMemory) AS BIGINT), 1) AS [Serial Desired Memory],
        CONVERT(NVARCHAR(30), CAST((SerialRequiredMemory) AS BIGINT), 1) AS [Serial Required Memory],
        CONVERT(NVARCHAR(30), CAST((TotalCPU) AS BIGINT), 1) AS [Total CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((AverageCPU) AS BIGINT), 1) AS [Avg CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((PercentCPU) AS BIGINT), 1) AS [CPU Weight],
        CONVERT(NVARCHAR(30), CAST((TotalDuration) AS BIGINT), 1) AS [Total Duration (ms)],
        CONVERT(NVARCHAR(30), CAST((AverageDuration) AS BIGINT), 1) AS [Avg Duration (ms)],
        CONVERT(NVARCHAR(30), CAST((PercentDuration) AS BIGINT), 1) AS [Duration Weight],
        CONVERT(NVARCHAR(30), CAST((TotalReads) AS BIGINT), 1) AS [Total Reads],
        CONVERT(NVARCHAR(30), CAST((AverageReads) AS BIGINT), 1) AS [Average Reads],
        CONVERT(NVARCHAR(30), CAST((PercentReads) AS BIGINT), 1) AS [Read Weight],
        CONVERT(NVARCHAR(30), CAST((TotalWrites) AS BIGINT), 1) AS [Total Writes],
        CONVERT(NVARCHAR(30), CAST((AverageWrites) AS BIGINT), 1) AS [Average Writes],
        CONVERT(NVARCHAR(30), CAST((PercentWrites) AS BIGINT), 1) AS [Write Weight],
        CONVERT(NVARCHAR(30), CAST((PercentExecutionsByType) AS BIGINT), 1) AS [% Executions (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentCPUByType) AS BIGINT), 1) AS [% CPU (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentDurationByType) AS BIGINT), 1) AS [% Duration (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentReadsByType) AS BIGINT), 1) AS [% Reads (Type)],
        CONVERT(NVARCHAR(30), CAST((PercentWritesByType) AS BIGINT), 1) AS [% Writes (Type)],
        CONVERT(NVARCHAR(30), CAST((TotalReturnedRows) AS BIGINT), 1) AS [Total Rows],
        CONVERT(NVARCHAR(30), CAST((AverageReturnedRows) AS BIGINT), 1) AS [Avg Rows],
        CONVERT(NVARCHAR(30), CAST((MinReturnedRows) AS BIGINT), 1) AS [Min Rows],
        CONVERT(NVARCHAR(30), CAST((MaxReturnedRows) AS BIGINT), 1) AS [Max Rows],
		CONVERT(NVARCHAR(30), CAST((MinGrantKB) AS BIGINT), 1) AS [Minimum Memory Grant KB],
		CONVERT(NVARCHAR(30), CAST((MaxGrantKB) AS BIGINT), 1) AS [Maximum Memory Grant KB],
		CONVERT(NVARCHAR(30), CAST((MinUsedGrantKB) AS BIGINT), 1) AS [Minimum Used Grant KB],
		CONVERT(NVARCHAR(30), CAST((MaxUsedGrantKB) AS BIGINT), 1) AS [Maximum Used Grant KB],
		CONVERT(NVARCHAR(30), CAST((AvgMaxMemoryGrant) AS BIGINT), 1) AS [Average Max Memory Grant],
		CONVERT(NVARCHAR(30), CAST((MinSpills) AS BIGINT), 1) AS [Min Spills],
		CONVERT(NVARCHAR(30), CAST((MaxSpills) AS BIGINT), 1) AS [Max Spills],
		CONVERT(NVARCHAR(30), CAST((TotalSpills) AS BIGINT), 1) AS [Total Spills],
		CONVERT(NVARCHAR(30), CAST((AvgSpills) AS MONEY), 1) AS [Avg Spills],
        CONVERT(NVARCHAR(30), CAST((NumberOfPlans) AS BIGINT), 1) AS [# Plans],
        CONVERT(NVARCHAR(30), CAST((NumberOfDistinctPlans) AS BIGINT), 1) AS [# Distinct Plans],
        PlanCreationTime AS [Created At],
        LastExecutionTime AS [Last Execution],
		LastCompletionTime AS [Last Completion],
        CONVERT(NVARCHAR(30), CAST((CachedPlanSize) AS BIGINT), 1) AS [Cached Plan Size (KB)],
        CONVERT(NVARCHAR(30), CAST((CompileTime) AS BIGINT), 1) AS [Compile Time (ms)],
        CONVERT(NVARCHAR(30), CAST((CompileCPU) AS BIGINT), 1) AS [Compile CPU (ms)],
        CONVERT(NVARCHAR(30), CAST((CompileMemory) AS BIGINT), 1) AS [Compile memory (KB)],
        COALESCE(SetOptions, '''') AS [SET Options],
		PlanHandle AS [Plan Handle],
		SqlHandle AS [SQL Handle],
		[SQL Handle More Info],
        QueryHash AS [Query Hash],
		[Query Hash More Info],
        QueryPlanHash AS [Query Plan Hash],
        StatementStartOffset,
        StatementEndOffset,
		[Remove Plan Handle From Cache],
		[Remove SQL Handle From Cache]';
        end;


    set @sql = N'
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
SELECT  TOP (@Top) ' + @columns + @nl + N'
FROM    ##BlitzCacheProcs
WHERE   SPID = @spid ' + @nl;

    if @minimumexecutioncount is not null
        begin
            set @sql += N' AND ExecutionCount >= @minimumExecutionCount ' + @nl;
        end;

    if @minutesback is not null
        begin
            set @sql += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ' + @nl;
        end;

    select @sql += N' ORDER BY ' + case @sortorder
                                       when N'cpu' then N' TotalCPU '
                                       when N'reads' then N' TotalReads '
                                       when N'writes' then N' TotalWrites '
                                       when N'duration' then N' TotalDuration '
                                       when N'executions' then N' ExecutionCount '
                                       when N'compiles' then N' PlanCreationTime '
                                       when N'memory grant' then N' MaxGrantKB'
                                       when N'spills' then N' MaxSpills'
                                       when N'avg cpu' then N' AverageCPU'
                                       when N'avg reads' then N' AverageReads'
                                       when N'avg writes' then N' AverageWrites'
                                       when N'avg duration' then N' AverageDuration'
                                       when N'avg executions' then N' ExecutionsPerMinute'
                                       when N'avg memory grant' then N' AvgMaxMemoryGrant'
                                       when N'avg spills' then N' AvgSpills'
        end + N' DESC ';
    set @sql += N' OPTION (RECOMPILE) ; ';

    if @debug = 1
        begin
            print SUBSTRING(@sql, 0, 4000);
            print SUBSTRING(@sql, 4000, 8000);
            print SUBSTRING(@sql, 8000, 12000);
            print SUBSTRING(@sql, 12000, 16000);
            print SUBSTRING(@sql, 16000, 20000);
            print SUBSTRING(@sql, 20000, 24000);
            print SUBSTRING(@sql, 24000, 28000);
            print SUBSTRING(@sql, 28000, 32000);
            print SUBSTRING(@sql, 32000, 36000);
            print SUBSTRING(@sql, 36000, 40000);
        end;

    exec sp_executesql @sql, N'@Top INT, @spid INT, @minimumExecutionCount INT, @min_back INT', @top, @@SPID,
         @minimumexecutioncount, @minutesback;


/*

This section will check if:
 * >= 30% of plans were created in the last hour
 * Check on the memory_clerks DMV for space used by TokenAndPermUserStore
 * Compare that to the size of the buffer pool
 * If it's >10%,
*/
    if EXISTS
        (
            select 1 / 0
            from #plan_creation as pc
            where pc.percent_1 >= 30
        )
        begin

            select @common_version =
                   CONVERT(decimal(10, 2), c.common_version)
            from #checkversion as c;

            if @common_version >= 11
                set @user_perm_sql = N'
	SET @buffer_pool_memory_gb = 0;
	SELECT @buffer_pool_memory_gb = SUM(pages_kb)/ 1024. / 1024.
	FROM sys.dm_os_memory_clerks
	WHERE type = ''MEMORYCLERK_SQLBUFFERPOOL'';'
            else
                set @user_perm_sql = N'
	SET @buffer_pool_memory_gb = 0;
	SELECT @buffer_pool_memory_gb = SUM(single_pages_kb + multi_pages_kb)/ 1024. / 1024.
	FROM sys.dm_os_memory_clerks
	WHERE type = ''MEMORYCLERK_SQLBUFFERPOOL'';'

            exec sys.sp_executesql @user_perm_sql,
                 N'@buffer_pool_memory_gb DECIMAL(10,2) OUTPUT',
                 @buffer_pool_memory_gb = @buffer_pool_memory_gb output;

            if @common_version >= 11
                begin
                    set @user_perm_sql = N'
    	SELECT @user_perm_gb = CASE WHEN (pages_kb / 128.0 / 1024.) >= 2.
    			                    THEN CONVERT(DECIMAL(38, 2), (pages_kb / 128.0 / 1024.))
    			                    ELSE 0
    		                   END
    	FROM sys.dm_os_memory_clerks
    	WHERE type = ''USERSTORE_TOKENPERM''
    	AND   name = ''TokenAndPermUserStore'';';
                end;

            if @common_version < 11
                begin
                    set @user_perm_sql = N'
    	SELECT @user_perm_gb = CASE WHEN ((single_pages_kb + multi_pages_kb) / 1024.0 / 1024.) >= 2.
    			                    THEN CONVERT(DECIMAL(38, 2), ((single_pages_kb + multi_pages_kb)  / 1024.0 / 1024.))
    			                    ELSE 0
    		                   END
    	FROM sys.dm_os_memory_clerks
    	WHERE type = ''USERSTORE_TOKENPERM''
    	AND   name = ''TokenAndPermUserStore'';';
                end;

            exec sys.sp_executesql @user_perm_sql,
                 N'@user_perm_gb DECIMAL(10,2) OUTPUT',
                 @user_perm_gb = @user_perm_gb_out output;

            if @buffer_pool_memory_gb > 0
                begin
                    if (@user_perm_gb_out / (1. * @buffer_pool_memory_gb)) * 100. >= 10
                        begin
                            set @is_tokenstore_big = 1;
                            set @user_perm_percent = (@user_perm_gb_out / (1. * @buffer_pool_memory_gb)) * 100.;
                        end
                end

        end


    if @hidesummary = 0 and @exporttoexcel = 0
        begin
            if @reanalyze = 0
                begin
                    raiserror ('Building query plan summary data.', 0, 1) with nowait;

                    /* Build summary data */
                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where frequent_execution = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                1,
                                100,
                                'Execution Pattern',
                                'Frequent Execution',
                                'http://brentozar.com/blitzcache/frequently-executed-queries/',
                                'Queries are being executed more than '
                                    + CAST(@execution_threshold as varchar(5))
                                    +
                                ' times per minute. This can put additional load on the server, even when queries are lightweight.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where parameter_sniffing = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                2,
                                50,
                                'Parameterization',
                                'Parameter Sniffing',
                                'http://brentozar.com/blitzcache/parameter-sniffing/',
                                'There are signs of parameter sniffing (wide variance in rows return or time to execute). Investigate query patterns and tune code appropriately.');

                    /* Forced execution plans */
                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_forced_plan = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                3,
                                50,
                                'Parameterization',
                                'Forced Plan',
                                'http://brentozar.com/blitzcache/forced-plans/',
                                'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_cursor = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                4,
                                200,
                                'Cursors',
                                'Cursor',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are cursors in the plan cache. This is neither good nor bad, but it is a thing. Cursors are weird in SQL Server.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_cursor = 1
                                and is_optimistic_cursor = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                4,
                                200,
                                'Cursors',
                                'Optimistic Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are optimistic cursors in the plan cache, which can harm performance.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_cursor = 1
                                and is_forward_only_cursor = 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                4,
                                200,
                                'Cursors',
                                'Non-forward Only Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are non-forward only cursors in the plan cache, which can harm performance.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_cursor = 1
                                and is_cursor_dynamic = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                4,
                                200,
                                'Cursors',
                                'Dynamic Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'Dynamic Cursors inhibit parallelism!.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_cursor = 1
                                and is_fast_forward_cursor = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                4,
                                200,
                                'Cursors',
                                'Fast Forward Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'Fast forward cursors inhibit parallelism!.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_forced_parameterized = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                5,
                                50,
                                'Parameterization',
                                'Forced Parameterization',
                                'http://brentozar.com/blitzcache/forced-parameterization/',
                                'Execution plans have been compiled with forced parameterization.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_parallel = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                6,
                                200,
                                'Execution Plans',
                                'Parallel',
                                'http://brentozar.com/blitzcache/parallel-plans-detected/',
                                'Parallel plans detected. These warrant investigation, but are neither good nor bad.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where near_parallel = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                7,
                                200,
                                'Execution Plans',
                                'Nearly Parallel',
                                'http://brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                                'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where plan_warnings = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                8,
                                50,
                                'Execution Plans',
                                'Plan Warnings',
                                'http://brentozar.com/blitzcache/query-plan-warnings/',
                                'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where long_running = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                9,
                                50,
                                'Performance',
                                'Long Running Query',
                                'http://brentozar.com/blitzcache/long-running-queries/',
                                'Long running queries have been found. These are queries with an average duration longer than '
                                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 as varchar(5))
                                    +
                                ' second(s). These queries should be investigated for additional tuning options.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.missing_index_count > 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                10,
                                50,
                                'Performance',
                                'Missing Indexes',
                                'http://brentozar.com/blitzcache/missing-index-request/',
                                'Queries found with missing indexes.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.downlevel_estimator = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                13,
                                200,
                                'Cardinality',
                                'Downlevel CE',
                                'http://brentozar.com/blitzcache/legacy-cardinality-estimator/',
                                'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where implicit_conversions = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                14,
                                50,
                                'Performance',
                                'Implicit Conversions',
                                'http://brentozar.com/go/implicit',
                                'One or more queries are comparing two fields that are not of the same data type.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where busy_loops = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                16,
                                100,
                                'Performance',
                                'Busy Loops',
                                'http://brentozar.com/blitzcache/busy-loops/',
                                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where tvf_join = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                17,
                                50,
                                'Performance',
                                'Function Join',
                                'http://brentozar.com/blitzcache/tvf-join/',
                                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where compile_timeout = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                18,
                                50,
                                'Execution Plans',
                                'Compilation Timeout',
                                'http://brentozar.com/blitzcache/compilation-timeout/',
                                'Query compilation timed out for one or more queries. SQL Server did not find a plan that meets acceptable performance criteria in the time allotted so the best guess was returned. There is a very good chance that this plan isn''t even below average - it''s probably terrible.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where compile_memory_limit_exceeded = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                19,
                                50,
                                'Execution Plans',
                                'Compile Memory Limit Exceeded',
                                'http://brentozar.com/blitzcache/compile-memory-limit-exceeded/',
                                'The optimizer has a limited amount of memory available. One or more queries are complex enough that SQL Server was unable to allocate enough memory to fully optimize the query. A best fit plan was found, and it''s probably terrible.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where warning_no_join_predicate = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                20,
                                50,
                                'Execution Plans',
                                'No Join Predicate',
                                'http://brentozar.com/blitzcache/no-join-predicate/',
                                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where plan_multiple_plans > 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                21,
                                200,
                                'Execution Plans',
                                'Multiple Plans',
                                'http://brentozar.com/blitzcache/multiple-plans/',
                                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where unmatched_index_count > 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                22,
                                100,
                                'Performance',
                                'Unmatched Indexes',
                                'http://brentozar.com/blitzcache/unmatched-indexes',
                                'An index could have been used, but SQL Server chose not to use it - likely due to parameterization and filtered indexes.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where unparameterized_query = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                23,
                                100,
                                'Parameterization',
                                'Unparameterized Query',
                                'http://brentozar.com/blitzcache/unparameterized-queries',
                                'Unparameterized queries found. These could be ad hoc queries, data exploration, or queries using "OPTIMIZE FOR UNKNOWN".');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs
                              where is_trivial = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                24,
                                100,
                                'Execution Plans',
                                'Trivial Plans',
                                'http://brentozar.com/blitzcache/trivial-plans',
                                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_forced_serial = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                25,
                                10,
                                'Execution Plans',
                                'Forced Serialization',
                                'http://www.brentozar.com/blitzcache/forced-serialization/',
                                'Something in your plan is forcing a serial query. Further investigation is needed if this is not by design.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_key_lookup_expensive = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                26,
                                100,
                                'Execution Plans',
                                'Expensive Key Lookup',
                                'http://www.brentozar.com/blitzcache/expensive-key-lookups/',
                                'There''s a key lookup in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_remote_query_expensive = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                28,
                                100,
                                'Execution Plans',
                                'Expensive Remote Query',
                                'http://www.brentozar.com/blitzcache/expensive-remote-query/',
                                'There''s a remote query in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.trace_flags_session is not null
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                29,
                                200,
                                'Trace Flags',
                                'Session Level Trace Flags Enabled',
                                'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                                'Someone is enabling session level Trace Flags in a query.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_unused_grant is not null
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                30,
                                100,
                                'Memory Grant',
                                'Unused Memory Grant',
                                'https://www.brentozar.com/blitzcache/unused-memory-grants/',
                                'Queries have large unused memory grants. This can cause concurrency issues, if queries are waiting a long time to get memory to run.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.function_count > 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                31,
                                100,
                                'Compute Scalar That References A Function',
                                'Calls Functions',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'Both of these will force queries to run serially, run at least once per row, and may result in poor cardinality estimates.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.clr_function_count > 0
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                32,
                                100,
                                'Compute Scalar That References A CLR Function',
                                'Calls CLR Functions',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'May force queries to run serially, run at least once per row, and may result in poor cardinality estimates.');


                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_table_variable = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                33,
                                100,
                                'Table Variables detected',
                                'Table Variables',
                                'https://www.brentozar.com/blitzcache/table-variables/',
                                'All modifications are single threaded, and selects have really low row estimates.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.no_stats_warning = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                35,
                                100,
                                'Statistics',
                                'Columns With No Statistics',
                                'https://www.brentozar.com/blitzcache/columns-no-statistics/',
                                'Sometimes this happens with indexed views, other times because auto create stats is turned off.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.relop_warnings = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                36,
                                100,
                                'Warnings',
                                'Operator Warnings',
                                'http://brentozar.com/blitzcache/query-plan-warnings/',
                                'Check the plan for more details.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_table_scan = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                37,
                                100,
                                'Indexes',
                                'Table Scans (Heaps)',
                                'https://www.brentozar.com/archive/2012/05/video-heaps/',
                                'This may not be a problem. Run sp_BlitzIndex for more information.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.backwards_scan = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                38,
                                200,
                                'Indexes',
                                'Backwards Scans',
                                'https://www.brentozar.com/blitzcache/backwards-scans/',
                                'This isn''t always a problem. They can cause serial zones in plans, and may need an index to match sort order.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.forced_index = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                39,
                                100,
                                'Indexes',
                                'Forced Indexes',
                                'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                                'This can cause inefficient plans, and will prevent missing index requests.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.forced_seek = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                40,
                                100,
                                'Indexes',
                                'Forced Seeks',
                                'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                                'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.forced_scan = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                40,
                                100,
                                'Indexes',
                                'Forced Scans',
                                'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                                'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.columnstore_row_mode = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                41,
                                100,
                                'Indexes',
                                'ColumnStore Row Mode',
                                'https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/',
                                'ColumnStore indexes operating in Row Mode indicate really poor query choices.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_computed_scalar = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                42,
                                50,
                                'Functions',
                                'Computed Column UDF',
                                'https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/',
                                'This can cause a whole mess of bad serializartion problems.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_sort_expensive = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                43,
                                100,
                                'Execution Plans',
                                'Expensive Sort',
                                'http://www.brentozar.com/blitzcache/expensive-sorts/',
                                'There''s a sort in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_computed_filter = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                44,
                                50,
                                'Functions',
                                'Filter UDF',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'Someone put a Scalar UDF in the WHERE clause!');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.index_ops >= 5
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                45,
                                100,
                                'Indexes',
                                '>= 5 Indexes Modified',
                                'https://www.brentozar.com/blitzcache/many-indexes-modified/',
                                'This can cause lots of hidden I/O -- Run sp_BlitzIndex for more information.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_row_level = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                46,
                                200,
                                'Complexity',
                                'Row Level Security',
                                'https://www.brentozar.com/blitzcache/row-level-security/',
                                'You may see a lot of confusing junk in your query plan.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_spatial = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                47,
                                200,
                                'Complexity',
                                'Spatial Index',
                                'https://www.brentozar.com/blitzcache/spatial-indexes/',
                                'Purely informational.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.index_dml = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                48,
                                150,
                                'Complexity',
                                'Index DML',
                                'https://www.brentozar.com/blitzcache/index-dml/',
                                'This can cause recompiles and stuff.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.table_dml = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                49,
                                150,
                                'Complexity',
                                'Table DML',
                                'https://www.brentozar.com/blitzcache/table-dml/',
                                'This can cause recompiles and stuff.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.long_running_low_cpu = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                50,
                                150,
                                'Blocking',
                                'Long Running Low CPU',
                                'https://www.brentozar.com/blitzcache/long-running-low-cpu/',
                                'This can be a sign of blocking, linked servers, or poor client application code (ASYNC_NETWORK_IO).');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.low_cost_high_cpu = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                51,
                                150,
                                'Complexity',
                                'Low Cost Query With High CPU',
                                'https://www.brentozar.com/blitzcache/low-cost-high-cpu/',
                                'This can be a sign of functions or Dynamic SQL that calls black-box code.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.stale_stats = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                52,
                                150,
                                'Statistics',
                                'Statistics used have > 100k modifications in the last 7 days',
                                'https://www.brentozar.com/blitzcache/stale-statistics/',
                                'Ever heard of updating statistics?');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_adaptive = 1
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                53,
                                200,
                                'Complexity',
                                'Adaptive joins',
                                'https://www.brentozar.com/blitzcache/adaptive-joins/',
                                'This join will sometimes do seeks, and sometimes do scans.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_spool_expensive = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                54,
                                150,
                                'Indexes',
                                'Expensive Index Spool',
                                'https://www.brentozar.com/blitzcache/eager-index-spools/',
                                'Check operator predicates and output for index definition guidance');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_spool_more_rows = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                55,
                                150,
                                'Indexes',
                                'Large Index Row Spool',
                                'https://www.brentozar.com/blitzcache/eager-index-spools/',
                                'Check operator predicates and output for index definition guidance');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_bad_estimate = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                56,
                                100,
                                'Complexity',
                                'Row Estimate Mismatch',
                                'https://www.brentozar.com/blitzcache/bad-estimates/',
                                'Estimated rows are different from average rows by a factor of 10000. This may indicate a performance problem if mismatches occur regularly');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_paul_white_electric = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                57,
                                200,
                                'Is Paul White Electric?',
                                'This query has a Switch operator in it!',
                                'https://www.sql.kiwi/2013/06/hello-operator-my-switch-is-bored.html',
                                'You should email this query plan to Paul: SQLkiwi at gmail dot com');

                    if @v >= 14 or (@v = 13 and @build >= 5026)
                        begin

                            insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                            select @@SPID,
                                   997,
                                   200,
                                   'Database Level Statistics',
                                   'The database ' + sa.[Database] + ' last had a stats update on ' +
                                   CONVERT(nvarchar(10), CONVERT(date, MAX(sa.lastupdate))) + ' and has ' +
                                   CONVERT(nvarchar(10), AVG(sa.modificationcount)) +
                                   ' modifications on average.'                             as [Finding],
                                   'https://www.brentozar.com/blitzcache/stale-statistics/' as url,
                                   'Consider updating statistics more frequently,'          as [Details]
                            from #stats_agg as sa
                            group by sa.[Database]
                            having MAX(sa.lastupdate) <= DATEADD(day, -7, SYSDATETIME())
                               and AVG(sa.modificationcount) >= 100000;

                            if EXISTS(select 1 / 0
                                      from ##blitzcacheprocs p
                                      where p.is_row_goal = 1
                                )
                                insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                                values (@@SPID,
                                        58,
                                        200,
                                        'Complexity',
                                        'Row Goals',
                                        'https://www.brentozar.com/go/rowgoals/',
                                        'This query had row goals introduced, which can be good or bad, and should be investigated for high read queries.');

                            if EXISTS(select 1 / 0
                                      from ##blitzcacheprocs p
                                      where p.is_big_spills = 1
                                )
                                insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                                values (@@SPID,
                                        59,
                                        100,
                                        'TempDB',
                                        '>500mb Spills',
                                        'https://www.brentozar.com/blitzcache/tempdb-spills/',
                                        'This query spills >500mb to tempdb on average. One way or another, this query didn''t get enough memory');


                        end;

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_mstvf = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                60,
                                100,
                                'Functions',
                                'MSTVFs',
                                'http://brentozar.com/blitzcache/tvf-join/',
                                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_mm_join = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                61,
                                100,
                                'Complexity',
                                'Many to Many Merge',
                                'https://www.brentozar.com/archive/2018/04/many-mysteries-merge-joins/',
                                'These use secret worktables that could be doing lots of reads. Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_nonsargable = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                62,
                                50,
                                'Non-SARGable queries',
                                'non-SARGables',
                                'https://www.brentozar.com/blitzcache/non-sargable-predicates/',
                                'Looks for intrinsic functions and expressions as predicates, and leading wildcard LIKE searches.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where compiletime > 5000
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                63,
                                100,
                                'Complexity',
                                'Long Compile Time',
                                'https://www.brentozar.com/blitzcache/high-compilers/',
                                'Queries are taking >5 seconds to compile. This can be normal for large plans, but be careful if they compile frequently');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where compilecpu > 5000
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                64,
                                50,
                                'Complexity',
                                'High Compile CPU',
                                'https://www.brentozar.com/blitzcache/high-compilers/',
                                'Queries taking >5 seconds of CPU to compile. If CPU is high and plans like this compile frequently, they may be related');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where compilememory > 1024
                                and ((compilememory) /
                                     (1 * case when maxcompilememory = 0 then 1 else maxcompilememory end) * 100.) >=
                                    10.
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                65,
                                50,
                                'Complexity',
                                'High Compile Memory',
                                'https://www.brentozar.com/blitzcache/high-compilers/',
                                'Queries taking 10% of Max Compile Memory. If you see high RESOURCE_SEMAPHORE_QUERY_COMPILE waits, these may be related');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.select_with_writes = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                66,
                                50,
                                'Complexity',
                                'Selects w/ Writes',
                                'https://dba.stackexchange.com/questions/191825/',
                                'This is thrown when reads cause writes that are not already flagged as big spills (2016+) or index spools.');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_table_spool_expensive = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                67,
                                150,
                                'Expensive Table Spool',
                                'You have a table spool, this is usually a sign that queries are doing unnecessary work',
                                'https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools',
                                'Check for non-SARGable predicates, or a lot of work being done inside a nested loops join');

                    if EXISTS(select 1 / 0
                              from ##blitzcacheprocs p
                              where p.is_table_spool_more_rows = 1
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                68,
                                150,
                                'Table Spools Many Rows',
                                'You have a table spool that spools more rows than the query returns',
                                'https://sqlperformance.com/2019/09/sql-performance/nested-loops-joins-performance-spools',
                                'Check for non-SARGable predicates, or a lot of work being done inside a nested loops join');

                    if EXISTS(select 1 / 0
                              from #plan_creation p
                              where (p.percent_24 > 0)
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        select spid,
                               999,
                               case when ISNULL(p.percent_24, 0) > 75 then 1 else 254 end as priority,
                               'Plan Cache Information',
                               case
                                   when ISNULL(p.percent_24, 0) > 75 then 'Plan Cache Instability'
                                   else 'Plan Cache Stability' end                        as finding,
                               'https://www.brentozar.com/archive/2018/07/tsql2sday-how-much-plan-cache-history-do-you-have/',
                               'You have ' + CONVERT(nvarchar(10), ISNULL(p.total_plans, 0))
                                   + ' total plans in your cache, with '
                                   + CONVERT(nvarchar(10), ISNULL(p.percent_24, 0))
                                   + '% plans created in the past 24 hours, '
                                   + CONVERT(nvarchar(10), ISNULL(p.percent_4, 0))
                                   + '% created in the past 4 hours, and '
                                   + CONVERT(nvarchar(10), ISNULL(p.percent_1, 0))
                                   + '% created in the past 1 hour. '
                                   +
                               'When these percentages are high, it may be a sign of memory pressure or plan cache instability.'
                        from #plan_creation p;

                    if EXISTS(select 1 / 0
                              from #plan_usage p
                              where p.percent_duplicate > 5
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        select spid,
                               999,
                               case when ISNULL(p.percent_duplicate, 0) > 75 then 1 else 254 end as priority,
                               'Plan Cache Information',
                               case
                                   when ISNULL(p.percent_duplicate, 0) > 75 then 'Many Duplicate Plans'
                                   else 'Duplicate Plans' end                                    as finding,
                               'https://www.brentozar.com/archive/2018/03/why-multiple-plans-for-one-query-are-bad/',
                               'You have ' + CONVERT(nvarchar(10), p.total_plans)
                                   + ' plans in your cache, and '
                                   + CONVERT(nvarchar(10), p.percent_duplicate)
                                   + '% are duplicates with more than 5 entries'
                                   + ', meaning similar queries are generating the same plan repeatedly.'
                                   +
                               ' Forced Parameterization may fix the issue. To find troublemakers, use: EXEC sp_BlitzCache @SortOrder = ''query hash''; '
                        from #plan_usage as p;

                    if EXISTS(select 1 / 0
                              from #plan_usage p
                              where p.percent_single > 5
                                and spid = @@SPID)
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        select spid,
                               999,
                               case when ISNULL(p.percent_single, 0) > 75 then 1 else 254 end as priority,
                               'Plan Cache Information',
                               case
                                   when ISNULL(p.percent_single, 0) > 75 then 'Many Single-Use Plans'
                                   else 'Single-Use Plans' end                                as finding,
                               'https://www.brentozar.com/blitz/single-use-plans-procedure-cache/',
                               'You have ' + CONVERT(nvarchar(10), p.total_plans)
                                   + ' plans in your cache, and '
                                   + CONVERT(nvarchar(10), p.percent_single)
                                   + '% are single use plans'
                                   +
                               ', meaning SQL Server thinks it''s seeing a lot of "new" queries and creating plans for them.'
                                   + ' Forced Parameterization and/or Optimize For Ad Hoc Workloads may fix the issue.'
                                   + 'To find troublemakers, use: EXEC sp_BlitzCache @SortOrder = ''query hash''; '
                        from #plan_usage as p;

                    if @is_tokenstore_big = 1
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        select @@SPID,
                               69,
                               10,
                               N'Large USERSTORE_TOKENPERM cache: ' + CONVERT(nvarchar(11), @user_perm_gb_out) + N'GB',
                               N'The USERSTORE_TOKENPERM is taking up ' + CONVERT(nvarchar(11), @user_perm_percent)
                                   + N'% of the buffer pool, and your plan cache seems to be unstable',
                               N'https://brentozar.com/go/userstore',
                               N'A growing USERSTORE_TOKENPERM cache can cause the plan cache to clear out'

                    if @v >= 11
                        begin
                            if EXISTS(select 1 / 0
                                      from #trace_flags as tf
                                      where tf.global_trace_flags is not null
                                )
                                insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                                values (@@SPID,
                                        1000,
                                        255,
                                        'Global Trace Flags Enabled',
                                        'You have Global Trace Flags enabled on your server',
                                        'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                                        'You have the following Global Trace Flags enabled: ' +
                                        (select top 1 tf.global_trace_flags
                                         from #trace_flags as tf
                                         where tf.global_trace_flags is not null));
                        end;

                    if not EXISTS(select 1 / 0
                                  from ##blitzcacheresults as bcr
                                  where bcr.priority = 2147483646
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                2147483646,
                                255,
                                'Need more help?',
                                'Paste your plan on the internet!',
                                'http://pastetheplan.com',
                                'This makes it easy to share plans and post them to Q&A sites like https://dba.stackexchange.com/!');


                    if not EXISTS(select 1 / 0
                                  from ##blitzcacheresults as bcr
                                  where bcr.priority = 2147483647
                        )
                        insert into ##blitzcacheresults (spid, checkid, priority, findingsgroup, finding, url, details)
                        values (@@SPID,
                                2147483647,
                                255,
                                'Thanks for using sp_BlitzCache!',
                                'From Your Community Volunteers',
                                'http://FirstResponderKit.org',
                                'We hope you found this tool useful. Current version: ' + @version + ' released on ' +
                                CONVERT(nvarchar(30), @versiondate) + '.');

                end;


            select priority,
                   findingsgroup,
                   finding,
                   url,
                   details,
                   checkid
            from ##blitzcacheresults
            where spid = @@SPID
            group by priority,
                     findingsgroup,
                     finding,
                     url,
                     details,
                     checkid
            order by priority asc, findingsgroup, finding, checkid asc
            option (recompile);
        end;

    if @debug = 1
        begin

            select '##BlitzCacheResults' as table_name, *
            from ##blitzcacheresults
            option ( recompile );

            select '##BlitzCacheProcs' as table_name, *
            from ##blitzcacheprocs
            option ( recompile );

            select '#statements' as table_name, *
            from #statements as s
            option (recompile);

            select '#query_plan' as table_name, *
            from #query_plan as qp
            option (recompile);

            select '#relop' as table_name, *
            from #relop as r
            option (recompile);

            select '#only_query_hashes' as table_name, *
            from #only_query_hashes
            option ( recompile );

            select '#ignore_query_hashes' as table_name, *
            from #ignore_query_hashes
            option ( recompile );

            select '#only_sql_handles' as table_name, *
            from #only_sql_handles
            option ( recompile );

            select '#ignore_sql_handles' as table_name, *
            from #ignore_sql_handles
            option ( recompile );

            select '#p' as table_name, *
            from #p
            option ( recompile );

            select '#checkversion' as table_name, *
            from #checkversion
            option ( recompile );

            select '#configuration' as table_name, *
            from #configuration
            option ( recompile );

            select '#stored_proc_info' as table_name, *
            from #stored_proc_info
            option ( recompile );

            select '#conversion_info' as table_name, *
            from #conversion_info as ci
            option ( recompile );

            select '#variable_info' as table_name, *
            from #variable_info as vi
            option ( recompile );

            select '#missing_index_xml' as table_name, *
            from #missing_index_xml as mix
            option ( recompile );

            select '#missing_index_schema' as table_name, *
            from #missing_index_schema as mis
            option ( recompile );

            select '#missing_index_usage' as table_name, *
            from #missing_index_usage as miu
            option ( recompile );

            select '#missing_index_detail' as table_name, *
            from #missing_index_detail as mid
            option ( recompile );

            select '#missing_index_pretty' as table_name, *
            from #missing_index_pretty as mip
            option ( recompile );

            select '#plan_creation' as table_name, *
            from #plan_creation
            option ( recompile );

            select '#plan_cost' as table_name, *
            from #plan_cost
            option ( recompile );

            select '#proc_costs' as table_name, *
            from #proc_costs
            option ( recompile );

            select '#stats_agg' as table_name, *
            from #stats_agg
            option ( recompile );

            select '#trace_flags' as table_name, *
            from #trace_flags
            option ( recompile );

            select '#plan_usage' as table_name, *
            from #plan_usage
            option ( recompile );

        end;

    if @outputdatabasename is not null
        and @outputschemaname is not null
        and @outputtablename is not null
        goto outputresultstotable;
    return;
    --Avoid going into the AllSort GOTO

/*Begin code to sort by all*/
    allsorts:
    raiserror ('Beginning all sort loop', 0, 1) with nowait;


    if (
            @top > 10
            and @skipanalysis = 0
            and @bringthepain = 0
        )
        begin
            raiserror (
                '
		  You''ve chosen a value greater than 10 to sort the whole plan cache by.
		  That can take a long time and harm performance.
		  Please choose a number <= 10, or set @BringThePain = 1 to signify you understand this might be a bad idea.
		          ', 0, 1) with nowait;
            return;
        end;


    if OBJECT_ID('tempdb..#checkversion_allsort') is null
        begin
            create table #checkversion_allsort
            (
                version nvarchar(128),
                common_version as SUBSTRING(version, 1, CHARINDEX('.', version) + 1),
                major as PARSENAME(CONVERT(varchar(32), version), 4),
                minor as PARSENAME(CONVERT(varchar(32), version), 3),
                build as PARSENAME(CONVERT(varchar(32), version), 2),
                revision as PARSENAME(CONVERT(varchar(32), version), 1)
            );

            insert into #checkversion_allsort
                (version)
            select CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128))
            option ( recompile );
        end;


    select @v = common_version,
           @build = build
    from #checkversion_allsort
    option ( recompile );

    if OBJECT_ID('tempdb.. #bou_allsort') is null
        begin
            create table #bou_allsort
            (
                id int identity (1, 1),
                databasename nvarchar(128),
                cost float,
                querytext nvarchar(max),
                querytype nvarchar(258),
                warnings varchar(max),
                queryplan xml,
                missing_indexes xml,
                implicit_conversion_info xml,
                cached_execution_parameters xml,
                executioncount nvarchar(30),
                executionsperminute money,
                executionweight money,
                totalcpu nvarchar(30),
                averagecpu nvarchar(30),
                cpuweight money,
                totalduration nvarchar(30),
                averageduration nvarchar(30),
                durationweight money,
                totalreads nvarchar(30),
                averagereads nvarchar(30),
                readweight money,
                totalwrites nvarchar(30),
                averagewrites nvarchar(30),
                writeweight money,
                averagereturnedrows money,
                mingrantkb nvarchar(30),
                maxgrantkb nvarchar(30),
                minusedgrantkb nvarchar(30),
                maxusedgrantkb nvarchar(30),
                avgmaxmemorygrant money,
                minspills nvarchar(30),
                maxspills nvarchar(30),
                totalspills nvarchar(30),
                avgspills money,
                plancreationtime datetime,
                lastexecutiontime datetime,
                lastcompletiontime datetime,
                planhandle varbinary(64),
                sqlhandle varbinary(64),
                setoptions varchar(max),
                removeplanhandlefromcache nvarchar(200),
                pattern nvarchar(20)
            );
        end;


    if LOWER(@sortorder) = 'all'
        begin
            raiserror ('Beginning for ALL', 0, 1) with nowait;
            set @allsortsql += N'
					DECLARE @ISH NVARCHAR(MAX) = N''''

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''cpu'',
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''reads'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''writes'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''duration'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''executions'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 ';

            if @versionshowsmemorygrants = 0
                begin
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsmemorygrants = 1
                begin
                    set @allsortsql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''memory grant'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

										  UPDATE #bou_allsort SET Pattern = ''memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsspills = 0
                begin
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsspills = 1
                begin
                    set @allsortsql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''spills'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

										  UPDATE #bou_allsort SET Pattern = ''spills'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;
            set @allsortsql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
										  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
										  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
										  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache, Pattern
										  FROM #bou_allsort
										  ORDER BY Id
										  OPTION(RECOMPILE);  ';


        end;


    if LOWER(@sortorder) = 'all avg'
        begin
            raiserror ('Beginning for ALL AVG', 0, 1) with nowait;
            set @allsortsql += N'
					DECLARE @ISH NVARCHAR(MAX) = N''''

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg cpu'',
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''avg cpu'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg reads'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''avg reads'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg writes'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''avg writes'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg duration'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''avg duration'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

					INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

					 EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg executions'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

					 UPDATE #bou_allsort SET Pattern = ''avg executions'' WHERE Pattern IS NULL OPTION(RECOMPILE);

					 ';

            if @versionshowsmemorygrants = 0
                begin
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsmemorygrants = 1
                begin
                    set @allsortsql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg memory grant'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

										  UPDATE #bou_allsort SET Pattern = ''avg memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsspills = 0
                begin
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            if @versionshowsspills = 1
                begin
                    set @allsortsql += N' SELECT TOP 1 @ISH = STUFF((SELECT DISTINCT N'','' + CONVERT(NVARCHAR(MAX),b2.SqlHandle, 1) FROM #bou_allsort AS b2 FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 1, N'''') OPTION(RECOMPILE);

						INSERT #bou_allsort (	DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters, ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
											TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
											ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
											MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache )

										  EXEC sp_BlitzCache @ExpertMode = 0, @HideSummary = 1, @Top = @i_Top, @SortOrder = ''avg spills'', @IgnoreSqlHandles = @ISH,
                     @DatabaseName = @i_DatabaseName, @SkipAnalysis = @i_SkipAnalysis, @OutputDatabaseName = @i_OutputDatabaseName, @OutputSchemaName = @i_OutputSchemaName, @OutputTableName = @i_OutputTableName, @CheckDateOverride = @i_CheckDateOverride, @MinutesBack = @i_MinutesBack WITH RECOMPILE;

										  UPDATE #bou_allsort SET Pattern = ''avg memory grant'' WHERE Pattern IS NULL OPTION(RECOMPILE);';
                    if @exporttoexcel = 1
                        begin
                            set @allsortsql += N'  UPDATE #bou_allsort
												   SET
													QueryPlan = NULL,
													implicit_conversion_info = NULL,
													cached_execution_parameters = NULL,
													missing_indexes = NULL
												   OPTION (RECOMPILE);

												   UPDATE ##BlitzCacheProcs
												   SET QueryText = SUBSTRING(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(QueryText)),'' '',''<>''),''><'',''''),''<>'','' ''), 1, 32000)
												   OPTION(RECOMPILE);';
                        end;

                end;

            set @allsortsql += N' SELECT DatabaseName, Cost, QueryText, QueryType, Warnings, QueryPlan, missing_indexes, implicit_conversion_info, cached_execution_parameters,ExecutionCount, ExecutionsPerMinute, ExecutionWeight,
										  TotalCPU, AverageCPU, CPUWeight, TotalDuration, AverageDuration, DurationWeight, TotalReads, AverageReads,
										  ReadWeight, TotalWrites, AverageWrites, WriteWeight, AverageReturnedRows, MinGrantKB, MaxGrantKB, MinUsedGrantKB,
										  MaxUsedGrantKB, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, SetOptions, RemovePlanHandleFromCache, Pattern
										  FROM #bou_allsort
										  ORDER BY Id
										  OPTION(RECOMPILE);  ';
        end;

    if @debug = 1
        begin
            print SUBSTRING(@allsortsql, 0, 4000);
            print SUBSTRING(@allsortsql, 4000, 8000);
            print SUBSTRING(@allsortsql, 8000, 12000);
            print SUBSTRING(@allsortsql, 12000, 16000);
            print SUBSTRING(@allsortsql, 16000, 20000);
            print SUBSTRING(@allsortsql, 20000, 24000);
            print SUBSTRING(@allsortsql, 24000, 28000);
            print SUBSTRING(@allsortsql, 28000, 32000);
            print SUBSTRING(@allsortsql, 32000, 36000);
            print SUBSTRING(@allsortsql, 36000, 40000);
        end;

    exec sys.sp_executesql @stmt = @allsortsql,
         @params = N'@i_DatabaseName NVARCHAR(128), @i_Top INT, @i_SkipAnalysis BIT, @i_OutputDatabaseName NVARCHAR(258), @i_OutputSchemaName NVARCHAR(258), @i_OutputTableName NVARCHAR(258), @i_CheckDateOverride DATETIMEOFFSET, @i_MinutesBack INT ',
         @i_databasename = @databasename, @i_top = @top, @i_skipanalysis = @skipanalysis,
         @i_outputdatabasename = @outputdatabasename, @i_outputschemaname = @outputschemaname,
         @i_outputtablename = @outputtablename, @i_checkdateoverride = @checkdateoverride,
         @i_minutesback = @minutesback;


    /*End of AllSort section*/


/*Begin code to sort by all*/
    outputresultstotable:

    if @outputdatabasename is not null
        and @outputschemaname is not null
        and @outputtablename is not null
        begin
            raiserror ('Writing results to table.', 0, 1) with nowait;

            select @outputdatabasename = QUOTENAME(@outputdatabasename),
                   @outputschemaname = QUOTENAME(@outputschemaname),
                   @outputtablename = QUOTENAME(@outputtablename);

            /* send results to a table */
            declare @insert_sql nvarchar(max) = N'';

            set @insert_sql = 'USE '
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
                + N'(ID bigint NOT NULL IDENTITY(1,1),
          ServerName NVARCHAR(258),
		  CheckDate DATETIMEOFFSET,
          Version NVARCHAR(258),
          QueryType NVARCHAR(258),
          Warnings varchar(max),
          DatabaseName sysname,
          SerialDesiredMemory float,
          SerialRequiredMemory float,
          AverageCPU bigint,
          TotalCPU bigint,
          PercentCPUByType money,
          CPUWeight money,
          AverageDuration bigint,
          TotalDuration bigint,
          DurationWeight money,
          PercentDurationByType money,
          AverageReads bigint,
          TotalReads bigint,
          ReadWeight money,
          PercentReadsByType money,
          AverageWrites bigint,
          TotalWrites bigint,
          WriteWeight money,
          PercentWritesByType money,
          ExecutionCount bigint,
          ExecutionWeight money,
          PercentExecutionsByType money,' + N'
          ExecutionsPerMinute money,
          PlanCreationTime datetime,
		  PlanCreationTimeHours AS DATEDIFF(HOUR, PlanCreationTime, SYSDATETIME()),
          LastExecutionTime datetime,
		  LastCompletionTime datetime,
		  PlanHandle varbinary(64),
		  [Remove Plan Handle From Cache] AS
			CASE WHEN [PlanHandle] IS NOT NULL
			THEN ''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [PlanHandle], 1) + '');''
			ELSE ''N/A'' END,
		  SqlHandle varbinary(64),
			[Remove SQL Handle From Cache] AS
			CASE WHEN [SqlHandle] IS NOT NULL
			THEN ''DBCC FREEPROCCACHE ('' + CONVERT(VARCHAR(128), [SqlHandle], 1) + '');''
			ELSE ''N/A'' END,
		  [SQL Handle More Info] AS
			CASE WHEN [SqlHandle] IS NOT NULL
			THEN ''EXEC sp_BlitzCache @OnlySqlHandles = '''''' + CONVERT(VARCHAR(128), [SqlHandle], 1) + ''''''; ''
			ELSE ''N/A'' END,
		  QueryHash binary(8),
		  [Query Hash More Info] AS
			CASE WHEN [QueryHash] IS NOT NULL
			THEN ''EXEC sp_BlitzCache @OnlyQueryHashes = '''''' + CONVERT(VARCHAR(32), [QueryHash], 1) + ''''''; ''
			ELSE ''N/A'' END,
          QueryPlanHash binary(8),
          StatementStartOffset int,
          StatementEndOffset int,
          MinReturnedRows bigint,
          MaxReturnedRows bigint,
          AverageReturnedRows money,
          TotalReturnedRows bigint,
          QueryText nvarchar(max),
          QueryPlan xml,
          NumberOfPlans int,
          NumberOfDistinctPlans int,
		  MinGrantKB BIGINT,
		  MaxGrantKB BIGINT,
		  MinUsedGrantKB BIGINT,
		  MaxUsedGrantKB BIGINT,
		  PercentMemoryGrantUsed MONEY,
		  AvgMaxMemoryGrant MONEY,
		  MinSpills BIGINT,
		  MaxSpills BIGINT,
		  TotalSpills BIGINT,
		  AvgSpills MONEY,
		  QueryPlanCost FLOAT,
          JoinKey AS ServerName + Cast(CheckDate AS NVARCHAR(50)),
          CONSTRAINT [PK_' + REPLACE(REPLACE(@outputtablename, '[', ''), ']', '') + '] PRIMARY KEY CLUSTERED(ID ASC))';

            if @debug = 1
                begin
                    print SUBSTRING(@insert_sql, 0, 4000);
                    print SUBSTRING(@insert_sql, 4000, 8000);
                    print SUBSTRING(@insert_sql, 8000, 12000);
                    print SUBSTRING(@insert_sql, 12000, 16000);
                    print SUBSTRING(@insert_sql, 16000, 20000);
                    print SUBSTRING(@insert_sql, 20000, 24000);
                    print SUBSTRING(@insert_sql, 24000, 28000);
                    print SUBSTRING(@insert_sql, 28000, 32000);
                    print SUBSTRING(@insert_sql, 32000, 36000);
                    print SUBSTRING(@insert_sql, 36000, 40000);
                end;

            exec sp_executesql @insert_sql;

            /* If the table doesn't have the new LastCompletionTime column, add it. See Github #2377. */
            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' + @outputtablename;
            set @insert_sql = N'IF NOT EXISTS (SELECT * FROM ' + @outputdatabasename + N'.sys.all_columns
        WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''LastCompletionTime'')
        ALTER TABLE ' + @objectfullname + N' ADD LastCompletionTime DATETIME NULL;';
            exec (@insert_sql);


            if @checkdateoverride is null
                begin
                    set @checkdateoverride = SYSDATETIMEOFFSET();
                end;


            set @insert_sql = N' IF EXISTS(SELECT * FROM '
                + @outputdatabasename
                + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                + @outputschemaname + N''') '
                + N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;'
                + 'INSERT '
                + @outputdatabasename + '.'
                + @outputschemaname + '.'
                + @outputtablename
                +
                              N' (ServerName, CheckDate, Version, QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, CPUWeight, AverageDuration, TotalDuration, DurationWeight, PercentDurationByType, AverageReads, TotalReads, ReadWeight, PercentReadsByType, '
                +
                              N' AverageWrites, TotalWrites, WriteWeight, PercentWritesByType, ExecutionCount, ExecutionWeight, PercentExecutionsByType, '
                +
                              N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                +
                              N' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost ) '
                + N'SELECT TOP (@Top) '
                + QUOTENAME(CAST(SERVERPROPERTY('ServerName') as nvarchar(128)), N'''') + N', @CheckDateOverride, '
                + QUOTENAME(CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128)), N'''') + ', '
                +
                              N' QueryType, DatabaseName, AverageCPU, TotalCPU, PercentCPUByType, PercentCPU, AverageDuration, TotalDuration, PercentDuration, PercentDurationByType, AverageReads, TotalReads, PercentReads, PercentReadsByType, '
                +
                              N' AverageWrites, TotalWrites, PercentWrites, PercentWritesByType, ExecutionCount, PercentExecutions, PercentExecutionsByType, '
                +
                              N' ExecutionsPerMinute, PlanCreationTime, LastExecutionTime, LastCompletionTime, PlanHandle, SqlHandle, QueryHash, QueryPlanHash, StatementStartOffset, StatementEndOffset, MinReturnedRows, MaxReturnedRows, AverageReturnedRows, TotalReturnedRows, QueryText, QueryPlan, NumberOfPlans, NumberOfDistinctPlans, Warnings, '
                +
                              N' SerialRequiredMemory, SerialDesiredMemory, MinGrantKB, MaxGrantKB, MinUsedGrantKB, MaxUsedGrantKB, PercentMemoryGrantUsed, AvgMaxMemoryGrant, MinSpills, MaxSpills, TotalSpills, AvgSpills, QueryPlanCost '
                + N' FROM ##BlitzCacheProcs '
                + N' WHERE 1=1 ';

            if @minimumexecutioncount is not null
                begin
                    set @insert_sql += N' AND ExecutionCount >= @MinimumExecutionCount ';
                end;

            if @minutesback is not null
                begin
                    set @insert_sql += N' AND LastCompletionTime >= DATEADD(MINUTE, @min_back, GETDATE() ) ';
                end;

            set @insert_sql += N' AND SPID = @@SPID ';

            select @insert_sql += N' ORDER BY ' + case @sortorder
                                                      when 'cpu' then N' TotalCPU '
                                                      when N'reads' then N' TotalReads '
                                                      when N'writes' then N' TotalWrites '
                                                      when N'duration' then N' TotalDuration '
                                                      when N'executions' then N' ExecutionCount '
                                                      when N'compiles' then N' PlanCreationTime '
                                                      when N'memory grant' then N' MaxGrantKB'
                                                      when N'spills' then N' MaxSpills'
                                                      when N'avg cpu' then N' AverageCPU'
                                                      when N'avg reads' then N' AverageReads'
                                                      when N'avg writes' then N' AverageWrites'
                                                      when N'avg duration' then N' AverageDuration'
                                                      when N'avg executions' then N' ExecutionsPerMinute'
                                                      when N'avg memory grant' then N' AvgMaxMemoryGrant'
                                                      when 'avg spills' then N' AvgSpills'
                end + N' DESC ';

            set @insert_sql += N' OPTION (RECOMPILE) ; ';

            if @debug = 1
                begin
                    print SUBSTRING(@insert_sql, 0, 4000);
                    print SUBSTRING(@insert_sql, 4000, 8000);
                    print SUBSTRING(@insert_sql, 8000, 12000);
                    print SUBSTRING(@insert_sql, 12000, 16000);
                    print SUBSTRING(@insert_sql, 16000, 20000);
                    print SUBSTRING(@insert_sql, 20000, 24000);
                    print SUBSTRING(@insert_sql, 24000, 28000);
                    print SUBSTRING(@insert_sql, 28000, 32000);
                    print SUBSTRING(@insert_sql, 32000, 36000);
                    print SUBSTRING(@insert_sql, 36000, 40000);
                end;

            exec sp_executesql @insert_sql,
                 N'@Top INT, @min_duration INT, @min_back INT, @CheckDateOverride DATETIMEOFFSET, @MinimumExecutionCount INT',
                 @top, @durationfilter_i, @minutesback, @checkdateoverride, @minimumexecutioncount;
        end; /* End of writing results to table */

end; /*Final End*/

go
