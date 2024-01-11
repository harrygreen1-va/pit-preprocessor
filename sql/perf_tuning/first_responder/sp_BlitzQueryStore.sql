set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

declare @msg nvarchar(max) = N'';

-- Must be a compatible, on-prem version of SQL (2016+)
if ((select CONVERT(nvarchar(128), SERVERPROPERTY('EDITION'))) <> 'SQL Azure'
    and (select PARSENAME(CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) < 13
       )
    -- or Azure Database (not Azure Data Warehouse), running at database compat level 130+
    or ((select CONVERT(nvarchar(128), SERVERPROPERTY('EDITION'))) = 'SQL Azure'
        and (select SERVERPROPERTY('ENGINEEDITION')) not in (5, 8)
        and (select [compatibility_level]
             from sys.databases
             where [name] = DB_NAME()) < 130
       )
    begin
        select @msg =
               N'Sorry, sp_BlitzQueryStore doesn''t work on versions of SQL prior to 2016, or Azure Database compatibility < 130.' +
               REPLICATE(CHAR(13), 7933);
        print @msg;
        return;
    end;

if OBJECT_ID('dbo.sp_BlitzQueryStore') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzQueryStore AS RETURN 0;');
go

alter procedure dbo.sp_blitzquerystore @help bit = 0,
                                       @databasename nvarchar(128) = null,
                                       @top int = 3,
                                       @startdate datetime2 = null,
                                       @enddate datetime2 = null,
                                       @minimumexecutioncount int = null,
                                       @durationfilter decimal(38, 4) = null,
                                       @storedprocname nvarchar(128) = null,
                                       @failed bit = 0,
                                       @planidfilter int = null,
                                       @queryidfilter int = null,
                                       @exporttoexcel bit = 0,
                                       @hidesummary bit = 0,
                                       @skipxml bit = 0,
                                       @debug bit = 0,
                                       @expertmode bit = 0,
                                       @version varchar(30) = null output,
                                       @versiondate datetime = null output,
                                       @versioncheckmode bit = 0
    with recompile
as
begin
    /*First BEGIN*/

    set nocount on;
    set transaction isolation level read uncommitted;

    select @version = '3.97', @versiondate = '20200712';
    if (@versioncheckmode = 1)
        begin
            return;
        end;


    declare /*Variables for the variable Gods*/
        @msg nvarchar(max) = N'', --Used to format RAISERROR messages in some places
        @sql_select nvarchar(max) = N'', --Used to hold SELECT statements for dynamic SQL
        @sql_where nvarchar(max) = N'', -- Used to hold WHERE clause for dynamic SQL
        @duration_filter_ms decimal(38, 4) = (@durationfilter * 1000.), --We accept Duration in seconds, but we filter in milliseconds (this is grandfathered from sp_BlitzCache)
        @execution_threshold int = 1000, --Threshold at which we consider a query to be frequently executed
        @ctp_threshold_pct tinyint = 10, --Percentage of CTFP at which we consider a query to be near parallel
        @long_running_query_warning_seconds bigint = 300 * 1000 ,--Number of seconds (converted to milliseconds) at which a query is considered long running
        @memory_grant_warning_percent int = 10,--Percent of memory grant used compared to what's granted; used to trigger unused memory grant warning
        @ctp int,--Holds the CTFP value for the server
        @min_memory_per_query int,--Holds the server configuration value for min memory per query
        @cr nvarchar(1) = NCHAR(13),--Special character
        @lf nvarchar(1) = NCHAR(10),--Special character
        @tab nvarchar(1) = NCHAR(9),--Special character
        @error_severity int,--Holds error info for try/catch blocks
        @error_state int,--Holds error info for try/catch blocks
        @sp_params nvarchar(max) = N'@sp_Top INT, @sp_StartDate DATETIME2, @sp_EndDate DATETIME2, @sp_MinimumExecutionCount INT, @sp_MinDuration INT, @sp_StoredProcName NVARCHAR(128), @sp_PlanIdFilter INT, @sp_QueryIdFilter INT',--Holds parameters used in dynamic SQL
        @is_azure_db bit = 0, --Are we using Azure? I'm not. You might be. That's cool.
        @compatibility_level tinyint = 0, --Some functionality (T-SQL) isn't available in lower compat levels. We can use this to weed out those issues as we go.
        @log_size_mb decimal(38, 2) = 0,
        @avg_tempdb_data_file decimal(38, 2) = 0;

/*Grabs CTFP setting*/
    select @ctp = NULLIF(CAST(value as int), 0)
    from sys.configurations
    where name = N'cost threshold for parallelism'
    option (recompile);

/*Grabs min query memory setting*/
    select @min_memory_per_query = CONVERT(int, c.value)
    from sys.configurations as c
    where c.name = N'min memory per query (KB)'
    option (recompile);

/*Check if this is Azure first*/
    if (select CONVERT(nvarchar(128), SERVERPROPERTY('EDITION'))) <> 'SQL Azure'
        begin
            /*Grabs log size for datbase*/
            select @log_size_mb = AVG(((mf.size * 8) / 1024.))
            from sys.master_files as mf
            where mf.database_id = DB_ID(@databasename)
              and mf.type_desc = 'LOG';

            /*Grab avg tempdb file size*/
            select @avg_tempdb_data_file = AVG(((mf.size * 8) / 1024.))
            from sys.master_files as mf
            where mf.database_id = DB_ID('tempdb')
              and mf.type_desc = 'ROWS';
        end;

/*Help section*/

    if @help = 1
        begin

            select N'You have requested assistance. It will arrive as soon as humanly possible.' as [Take four red capsules, help is on the way];

            print N'
	sp_BlitzQueryStore from http://FirstResponderKit.org

	This script displays your most resource-intensive queries from the Query Store,
	and points to ways you can tune these queries to make them faster.


	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - This query will not run on SQL Server versions less than 2016.
	 - This query will not run on Azure Databases with compatibility less than 130.
	 - This query will not run on Azure Data Warehouse.

	Unknown limitations of this version:
	 - Could be tickling


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
            return;

        end;

/*Making sure your version is copasetic*/
    if ((select CONVERT(nvarchar(128), SERVERPROPERTY('EDITION'))) = 'SQL Azure')
        begin
            set @is_azure_db = 1;

            if ((select SERVERPROPERTY('ENGINEEDITION')) not in (5, 8)
                or (select [compatibility_level] from sys.databases where [name] = DB_NAME()) < 130
                )
                begin
                    select @msg =
                           N'Sorry, sp_BlitzQueryStore doesn''t work on Azure Data Warehouse, or Azure Databases with DB compatibility < 130.' +
                           REPLICATE(CHAR(13), 7933);
                    print @msg;
                    return;
                end;
        end;
    else
        if ((select PARSENAME(CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) < 13)
            begin
                select @msg = N'Sorry, sp_BlitzQueryStore doesn''t work on versions of SQL prior to 2016.' +
                              REPLICATE(CHAR(13), 7933);
                print @msg;
                return;
            end;

/*Making sure at least one database uses QS*/
    if (select COUNT(*)
        from sys.databases as d
        where d.is_query_store_on = 1
          and d.user_access_desc = 'MULTI_USER'
          and d.state_desc = 'ONLINE'
          and d.name not in ('master', 'model', 'msdb', 'tempdb', '32767')
          and d.is_distributor = 0) = 0
        begin
            select @msg =
                   N'You don''t currently have any databases with Query Store enabled.' + REPLICATE(CHAR(13), 7933);
            print @msg;
            return;
        end;

/*Making sure your databases are using QDS.*/
    raiserror ('Checking database validity', 0, 1) with nowait;

    if (@is_azure_db = 1)
        set @databasename = DB_NAME();
    else
        begin

            /*If we're on Azure we don't need to check all this @DatabaseName stuff...*/

            set @databasename = LTRIM(RTRIM(@databasename));

            /*Did you set @DatabaseName?*/
            raiserror ('Making sure [%s] isn''t NULL', 0, 1, @databasename) with nowait;
            if (@databasename is null)
                begin
                    raiserror ('@DatabaseName cannot be NULL', 0, 1) with nowait;
                    return;
                end;

            /*Does the database exist?*/
            raiserror ('Making sure [%s] exists', 0, 1, @databasename) with nowait;
            if ((DB_ID(@databasename)) is null)
                begin
                    raiserror ('The @DatabaseName you specified ([%s]) does not exist. Please check the name and try again.', 0, 1, @databasename) with nowait;
                    return;
                end;

            /*Is it online?*/
            raiserror ('Making sure [%s] is online', 0, 1, @databasename) with nowait;
            if (DATABASEPROPERTYEX(@databasename, 'Collation')) is null
                begin
                    raiserror ('The @DatabaseName you specified ([%s]) is not readable. Please check the name and try again. Better yet, check your server.', 0, 1, @databasename);
                    return;
                end;
        end;

/*Does it have Query Store enabled?*/
    raiserror ('Making sure [%s] has Query Store enabled', 0, 1, @databasename) with nowait;
    if
        (select [d].[name]
         from [sys].[databases] as d
         where [d].[is_query_store_on] = 1
           and [d].[user_access_desc] = 'MULTI_USER'
           and [d].[state_desc] = 'ONLINE'
           and [d].[database_id] = (select database_id from sys.databases where name = @databasename)
        ) is null
        begin
            raiserror ('The @DatabaseName you specified ([%s]) does not have the Query Store enabled. Please check the name or settings, and try again.', 0, 1, @databasename) with nowait;
            return;
        end;

/*Check database compat level*/

    raiserror ('Checking database compatibility level', 0, 1) with nowait;

    select @compatibility_level = d.compatibility_level
    from sys.databases as d
    where d.name = @databasename;

    raiserror ('The @DatabaseName you specified ([%s])is running in compatibility level ([%d]).', 0, 1, @databasename, @compatibility_level) with nowait;


/*Making sure top is set to something if NULL*/
    if (@top is null)
        begin
            set @top = 3;
        end;

/*
This section determines if you have the Query Store wait stats DMV
*/

    raiserror ('Checking for query_store_wait_stats', 0, 1) with nowait;

    declare @ws_out int,
        @waitstats bit,
        @ws_sql nvarchar(max) = N'SELECT @i_out = COUNT(*) FROM ' + QUOTENAME(@databasename) +
                                N'.sys.all_objects WHERE name = ''query_store_wait_stats'' OPTION (RECOMPILE);',
        @ws_params nvarchar(max) = N'@i_out INT OUTPUT';

    exec sys.sp_executesql @ws_sql, @ws_params, @i_out = @ws_out output;

    select @waitstats = case @ws_out when 0 then 0 else 1 end;

    set @msg = N'Wait stats DMV ' + case @waitstats
                                        when 0 then N' does not exist, skipping.'
                                        when 1 then N' exists, will analyze.'
        end;
    raiserror (@msg, 0, 1) with nowait;

/*
This section determines if you have some additional columns present in 2017, in case they get back ported.
*/

    raiserror ('Checking for new columns in query_store_runtime_stats', 0, 1) with nowait;

    declare @nc_out int,
        @new_columns bit,
        @nc_sql nvarchar(max) = N'SELECT @i_out = COUNT(*)
							      FROM ' + QUOTENAME(@databasename) + N'.sys.all_columns AS ac
								  WHERE OBJECT_NAME(object_id) = ''query_store_runtime_stats''
								  AND ac.name IN (
								  ''avg_num_physical_io_reads'',
								  ''last_num_physical_io_reads'',
								  ''min_num_physical_io_reads'',
								  ''max_num_physical_io_reads'',
								  ''avg_log_bytes_used'',
								  ''last_log_bytes_used'',
								  ''min_log_bytes_used'',
								  ''max_log_bytes_used'',
								  ''avg_tempdb_space_used'',
								  ''last_tempdb_space_used'',
								  ''min_tempdb_space_used'',
								  ''max_tempdb_space_used''
								  ) OPTION (RECOMPILE);',
        @nc_params nvarchar(max) = N'@i_out INT OUTPUT';

    exec sys.sp_executesql @nc_sql, @ws_params, @i_out = @nc_out output;

    select @new_columns = case @nc_out when 12 then 1 else 0 end;

    set @msg = N'New query_store_runtime_stats columns ' + case @new_columns
                                                               when 0 then N' do not exist, skipping.'
                                                               when 1 then N' exist, will analyze.'
        end;
    raiserror (@msg, 0, 1) with nowait;


    /*
These are the temp tables we use
*/


/*
This one holds the grouped data that helps use figure out which periods to examine
*/

    raiserror (N'Creating temp tables', 0, 1) with nowait;

    drop table if exists #grouped_interval;

    create table #grouped_interval
    (
        flat_date date null,
        start_range datetime null,
        end_range datetime null,
        total_avg_duration_ms decimal(38, 2) null,
        total_avg_cpu_time_ms decimal(38, 2) null,
        total_avg_logical_io_reads_mb decimal(38, 2) null,
        total_avg_physical_io_reads_mb decimal(38, 2) null,
        total_avg_logical_io_writes_mb decimal(38, 2) null,
        total_avg_query_max_used_memory_mb decimal(38, 2) null,
        total_rowcount decimal(38, 2) null,
        total_count_executions bigint null,
        total_avg_log_bytes_mb decimal(38, 2) null,
        total_avg_tempdb_space decimal(38, 2) null,
        total_max_duration_ms decimal(38, 2) null,
        total_max_cpu_time_ms decimal(38, 2) null,
        total_max_logical_io_reads_mb decimal(38, 2) null,
        total_max_physical_io_reads_mb decimal(38, 2) null,
        total_max_logical_io_writes_mb decimal(38, 2) null,
        total_max_query_max_used_memory_mb decimal(38, 2) null,
        total_max_log_bytes_mb decimal(38, 2) null,
        total_max_tempdb_space decimal(38, 2) null,
        index gi_ix_dates clustered (start_range, end_range)
    );


/*
These are the plans we focus on based on what we find in the grouped intervals
*/
    drop table if exists #working_plans;

    create table #working_plans
    (
        plan_id bigint,
        query_id bigint,
        pattern nvarchar(258),
        index wp_ix_ids clustered (plan_id, query_id)
    );


/*
These are the gathered metrics we get from query store to generate some warnings and help you find your worst offenders
*/
    drop table if exists #working_metrics;

    create table #working_metrics
    (
        database_name nvarchar(258),
        plan_id bigint,
        query_id bigint,
        query_id_all_plan_ids varchar(8000),
        /*these columns are from query_store_query*/
        proc_or_function_name nvarchar(258),
        batch_sql_handle varbinary(64),
        query_hash binary(8),
        query_parameterization_type_desc nvarchar(258),
        parameter_sniffing_symptoms nvarchar(4000),
        count_compiles bigint,
        avg_compile_duration decimal(38, 2),
        last_compile_duration decimal(38, 2),
        avg_bind_duration decimal(38, 2),
        last_bind_duration decimal(38, 2),
        avg_bind_cpu_time decimal(38, 2),
        last_bind_cpu_time decimal(38, 2),
        avg_optimize_duration decimal(38, 2),
        last_optimize_duration decimal(38, 2),
        avg_optimize_cpu_time decimal(38, 2),
        last_optimize_cpu_time decimal(38, 2),
        avg_compile_memory_kb decimal(38, 2),
        last_compile_memory_kb decimal(38, 2),
        /*These come from query_store_runtime_stats*/
        execution_type_desc nvarchar(128),
        first_execution_time datetime2,
        last_execution_time datetime2,
        count_executions bigint,
        avg_duration decimal(38, 2),
        last_duration decimal(38, 2),
        min_duration decimal(38, 2),
        max_duration decimal(38, 2),
        avg_cpu_time decimal(38, 2),
        last_cpu_time decimal(38, 2),
        min_cpu_time decimal(38, 2),
        max_cpu_time decimal(38, 2),
        avg_logical_io_reads decimal(38, 2),
        last_logical_io_reads decimal(38, 2),
        min_logical_io_reads decimal(38, 2),
        max_logical_io_reads decimal(38, 2),
        avg_logical_io_writes decimal(38, 2),
        last_logical_io_writes decimal(38, 2),
        min_logical_io_writes decimal(38, 2),
        max_logical_io_writes decimal(38, 2),
        avg_physical_io_reads decimal(38, 2),
        last_physical_io_reads decimal(38, 2),
        min_physical_io_reads decimal(38, 2),
        max_physical_io_reads decimal(38, 2),
        avg_clr_time decimal(38, 2),
        last_clr_time decimal(38, 2),
        min_clr_time decimal(38, 2),
        max_clr_time decimal(38, 2),
        avg_dop bigint,
        last_dop bigint,
        min_dop bigint,
        max_dop bigint,
        avg_query_max_used_memory decimal(38, 2),
        last_query_max_used_memory decimal(38, 2),
        min_query_max_used_memory decimal(38, 2),
        max_query_max_used_memory decimal(38, 2),
        avg_rowcount decimal(38, 2),
        last_rowcount decimal(38, 2),
        min_rowcount decimal(38, 2),
        max_rowcount decimal(38, 2),
        /*These are 2017 only, AFAIK*/
        avg_num_physical_io_reads decimal(38, 2),
        last_num_physical_io_reads decimal(38, 2),
        min_num_physical_io_reads decimal(38, 2),
        max_num_physical_io_reads decimal(38, 2),
        avg_log_bytes_used decimal(38, 2),
        last_log_bytes_used decimal(38, 2),
        min_log_bytes_used decimal(38, 2),
        max_log_bytes_used decimal(38, 2),
        avg_tempdb_space_used decimal(38, 2),
        last_tempdb_space_used decimal(38, 2),
        min_tempdb_space_used decimal(38, 2),
        max_tempdb_space_used decimal(38, 2),
        /*These are computed columns to make some stuff easier down the line*/
        total_compile_duration as avg_compile_duration * count_compiles,
        total_bind_duration as avg_bind_duration * count_compiles,
        total_bind_cpu_time as avg_bind_cpu_time * count_compiles,
        total_optimize_duration as avg_optimize_duration * count_compiles,
        total_optimize_cpu_time as avg_optimize_cpu_time * count_compiles,
        total_compile_memory_kb as avg_compile_memory_kb * count_compiles,
        total_duration as avg_duration * count_executions,
        total_cpu_time as avg_cpu_time * count_executions,
        total_logical_io_reads as avg_logical_io_reads * count_executions,
        total_logical_io_writes as avg_logical_io_writes * count_executions,
        total_physical_io_reads as avg_physical_io_reads * count_executions,
        total_clr_time as avg_clr_time * count_executions,
        total_query_max_used_memory as avg_query_max_used_memory * count_executions,
        total_rowcount as avg_rowcount * count_executions,
        total_num_physical_io_reads as avg_num_physical_io_reads * count_executions,
        total_log_bytes_used as avg_log_bytes_used * count_executions,
        total_tempdb_space_used as avg_tempdb_space_used * count_executions,
        xpm as NULLIF(count_executions, 0) / NULLIF(DATEDIFF(minute, first_execution_time, last_execution_time), 0),
        percent_memory_grant_used as CONVERT(money,
                    ISNULL(NULLIF((max_query_max_used_memory * 1.00), 0) / NULLIF(min_query_max_used_memory, 0), 0) *
                    100.),
        index wm_ix_ids clustered (plan_id, query_id, query_hash)
    );


/*
This is where we store some additional metrics, along with the query plan and text
*/
    drop table if exists #working_plan_text;

    create table #working_plan_text
    (
        database_name nvarchar(258),
        plan_id bigint,
        query_id bigint,
        /*These are from query_store_plan*/
        plan_group_id bigint,
        engine_version nvarchar(64),
        compatibility_level int,
        query_plan_hash binary(8),
        query_plan_xml xml,
        is_online_index_plan bit,
        is_trivial_plan bit,
        is_parallel_plan bit,
        is_forced_plan bit,
        is_natively_compiled bit,
        force_failure_count bigint,
        last_force_failure_reason_desc nvarchar(258),
        count_compiles bigint,
        initial_compile_start_time datetime2,
        last_compile_start_time datetime2,
        last_execution_time datetime2,
        avg_compile_duration decimal(38, 2),
        last_compile_duration bigint,
        /*These are from query_store_query*/
        query_sql_text nvarchar(max),
        statement_sql_handle varbinary(64),
        is_part_of_encrypted_module bit,
        has_restricted_text bit,
        /*This is from query_context_settings*/
        context_settings nvarchar(512),
        /*This is from #working_plans*/
        pattern nvarchar(512),
        top_three_waits nvarchar(max),
        index wpt_ix_ids clustered (plan_id, query_id, query_plan_hash)
    );


/*
This is where we store warnings that we generate from the XML and metrics
*/
    drop table if exists #working_warnings;

    create table #working_warnings
    (
        plan_id bigint,
        query_id bigint,
        query_hash binary(8),
        sql_handle varbinary(64),
        proc_or_function_name nvarchar(258),
        plan_multiple_plans bit,
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
        long_running bit,
        downlevel_estimator bit,
        implicit_conversions bit,
        tvf_estimate bit,
        compile_timeout bit,
        compile_memory_limit_exceeded bit,
        warning_no_join_predicate bit,
        query_cost float,
        missing_index_count int,
        unmatched_index_count int,
        is_trivial bit,
        trace_flags_session nvarchar(1000),
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
        op_name nvarchar(100) null,
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
        is_slow_plan bit,
        is_compile_more bit,
        index_spool_cost float,
        index_spool_rows float,
        is_spool_expensive bit,
        is_spool_more_rows bit,
        estimated_rows float,
        is_bad_estimate bit,
        is_big_log bit,
        is_big_tempdb bit,
        is_paul_white_electric bit,
        is_row_goal bit,
        is_mstvf bit,
        is_mm_join bit,
        is_nonsargable bit,
        busy_loops bit,
        tvf_join bit,
        implicit_conversion_info xml,
        cached_execution_parameters xml,
        missing_indexes xml,
        warnings nvarchar(4000)
            index ww_ix_ids clustered (plan_id, query_id, query_hash, sql_handle)
    );


    drop table if exists #working_wait_stats;

    create table #working_wait_stats
    (
        plan_id bigint,
        wait_category tinyint,
        wait_category_desc nvarchar(258),
        total_query_wait_time_ms bigint,
        avg_query_wait_time_ms decimal(38, 2),
        last_query_wait_time_ms bigint,
        min_query_wait_time_ms bigint,
        max_query_wait_time_ms bigint,
        wait_category_mapped as case wait_category
                                    when 0 then N'UNKNOWN'
                                    when 1 then N'SOS_SCHEDULER_YIELD'
                                    when 2 then N'THREADPOOL'
                                    when 3 then N'LCK_M_%'
                                    when 4 then N'LATCH_%'
                                    when 5 then N'PAGELATCH_%'
                                    when 6 then N'PAGEIOLATCH_%'
                                    when 7 then N'RESOURCE_SEMAPHORE_QUERY_COMPILE'
                                    when 8 then N'CLR%, SQLCLR%'
                                    when 9 then N'DBMIRROR%'
                                    when 10 then N'XACT%, DTC%, TRAN_MARKLATCH_%, MSQL_XACT_%, TRANSACTION_MUTEX'
                                    when 11
                                        then N'SLEEP_%, LAZYWRITER_SLEEP, SQLTRACE_BUFFER_FLUSH, SQLTRACE_INCREMENTAL_FLUSH_SLEEP, SQLTRACE_WAIT_ENTRIES, FT_IFTS_SCHEDULER_IDLE_WAIT, XE_DISPATCHER_WAIT, REQUEST_FOR_DEADLOCK_SEARCH, LOGMGR_QUEUE, ONDEMAND_TASK_QUEUE, CHECKPOINT_QUEUE, XE_TIMER_EVENT'
                                    when 12 then N'PREEMPTIVE_%'
                                    when 13 then N'BROKER_% (but not BROKER_RECEIVE_WAITFOR)'
                                    when 14
                                        then N'LOGMGR, LOGBUFFER, LOGMGR_RESERVE_APPEND, LOGMGR_FLUSH, LOGMGR_PMM_LOG, CHKPT, WRITELOG'
                                    when 15
                                        then N'ASYNC_NETWORK_IO, NET_WAITFOR_PACKET, PROXY_NETWORK_IO, EXTERNAL_SCRIPT_NETWORK_IOF'
                                    when 16 then N'CXPACKET, EXCHANGE, CXCONSUMER'
                                    when 17
                                        then N'RESOURCE_SEMAPHORE, CMEMTHREAD, CMEMPARTITIONED, EE_PMOLOCK, MEMORY_ALLOCATION_EXT, RESERVED_MEMORY_ALLOCATION_EXT, MEMORY_GRANT_UPDATE'
                                    when 18 then N'WAITFOR, WAIT_FOR_RESULTS, BROKER_RECEIVE_WAITFOR'
                                    when 19
                                        then N'TRACEWRITE, SQLTRACE_LOCK, SQLTRACE_FILE_BUFFER, SQLTRACE_FILE_WRITE_IO_COMPLETION, SQLTRACE_FILE_READ_IO_COMPLETION, SQLTRACE_PENDING_BUFFER_WRITERS, SQLTRACE_SHUTDOWN, QUERY_TRACEOUT, TRACE_EVTNOTIFF'
                                    when 20
                                        then N'FT_RESTART_CRAWL, FULLTEXT GATHERER, MSSEARCH, FT_METADATA_MUTEX, FT_IFTSHC_MUTEX, FT_IFTSISM_MUTEX, FT_IFTS_RWLOCK, FT_COMPROWSET_RWLOCK, FT_MASTER_MERGE, FT_PROPERTYLIST_CACHE, FT_MASTER_MERGE_COORDINATOR, PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC'
                                    when 21
                                        then N'ASYNC_IO_COMPLETION, IO_COMPLETION, BACKUPIO, WRITE_COMPLETION, IO_QUEUE_LIMIT, IO_RETRY'
                                    when 22
                                        then N'SE_REPL_%, REPL_%, HADR_% (but not HADR_THROTTLE_LOG_RATE_GOVERNOR), PWAIT_HADR_%, REPLICA_WRITES, FCB_REPLICA_WRITE, FCB_REPLICA_READ, PWAIT_HADRSIM'
                                    when 23
                                        then N'LOG_RATE_GOVERNOR, POOL_LOG_RATE_GOVERNOR, HADR_THROTTLE_LOG_RATE_GOVERNOR, INSTANCE_LOG_RATE_GOVERNOR'
            end,
        index wws_ix_ids clustered (plan_id)
    );


/*
The next three tables hold plan XML parsed out to different degrees
*/
    drop table if exists #statements;

    create table #statements
    (
        plan_id bigint,
        query_id bigint,
        query_hash binary(8),
        sql_handle varbinary(64),
        statement xml,
        is_cursor bit
            index s_ix_ids clustered (plan_id, query_id, query_hash, sql_handle)
    );


    drop table if exists #query_plan;

    create table #query_plan
    (
        plan_id bigint,
        query_id bigint,
        query_hash binary(8),
        sql_handle varbinary(64),
        query_plan xml,
        index qp_ix_ids clustered (plan_id, query_id, query_hash, sql_handle)
    );


    drop table if exists #relop;

    create table #relop
    (
        plan_id bigint,
        query_id bigint,
        query_hash binary(8),
        sql_handle varbinary(64),
        relop xml,
        index ix_ids clustered (plan_id, query_id, query_hash, sql_handle)
    );


    drop table if exists #plan_cost;

    create table #plan_cost
    (
        query_plan_cost decimal(38, 2),
        sql_handle varbinary(64),
        plan_id int,
        index px_ix_ids clustered (sql_handle, plan_id)
    );


    drop table if exists #est_rows;

    create table #est_rows
    (
        estimated_rows decimal(38, 2),
        query_hash binary(8),
        index px_ix_ids clustered (query_hash)
    );


    drop table if exists #stats_agg;

    create table #stats_agg
    (
        sql_handle varbinary(64),
        last_update datetime2,
        modification_count bigint,
        sampling_percent decimal(38, 2),
        [statistics] nvarchar(258),
        [table] nvarchar(258),
        [schema] nvarchar(258),
        [database] nvarchar(258),
        index sa_ix_ids clustered (sql_handle)
    );


    drop table if exists #trace_flags;

    create table #trace_flags
    (
        sql_handle varbinary(54),
        global_trace_flags nvarchar(4000),
        session_trace_flags nvarchar(4000),
        index tf_ix_ids clustered (sql_handle)
    );


    drop table if exists #warning_results;

    create table #warning_results
    (
        id int identity (1,1) primary key clustered,
        checkid int,
        priority tinyint,
        findingsgroup nvarchar(50),
        finding nvarchar(200),
        url nvarchar(200),
        details nvarchar(4000)
    );

/*These next three tables hold information about implicit conversion and cached parameters */
    drop table if exists #stored_proc_info;

    create table #stored_proc_info
    (
        sql_handle varbinary(64),
        query_hash binary(8),
        variable_name nvarchar(258),
        variable_datatype nvarchar(258),
        converted_column_name nvarchar(258),
        compile_time_value nvarchar(258),
        proc_name nvarchar(1000),
        column_name nvarchar(4000),
        converted_to nvarchar(258),
        set_options nvarchar(1000)
            index tf_ix_ids clustered (sql_handle, query_hash)
    );

    drop table if exists #variable_info;

    create table #variable_info
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        proc_name nvarchar(1000),
        variable_name nvarchar(258),
        variable_datatype nvarchar(258),
        compile_time_value nvarchar(258),
        index vif_ix_ids clustered (sql_handle, query_hash)
    );

    drop table if exists #conversion_info;

    create table #conversion_info
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        proc_name nvarchar(128),
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
        convert_implicit_charindex as CHARINDEX('=CONVERT_IMPLICIT', expression),
        index cif_ix_ids clustered (sql_handle, query_hash)
    );

/* These tables support the Missing Index details clickable*/


    drop table if exists #missing_index_xml;

    create table #missing_index_xml
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        index_xml xml,
        index mix_ix_ids clustered (sql_handle, query_hash)
    );

    drop table if exists #missing_index_schema;

    create table #missing_index_schema
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        index_xml xml,
        index mis_ix_ids clustered (sql_handle, query_hash)
    );


    drop table if exists #missing_index_usage;

    create table #missing_index_usage
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        usage nvarchar(128),
        index_xml xml,
        index miu_ix_ids clustered (sql_handle, query_hash)
    );

    drop table if exists #missing_index_detail;

    create table #missing_index_detail
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        usage nvarchar(128),
        column_name nvarchar(128),
        index mid_ix_ids clustered (sql_handle, query_hash)
    );


    drop table if exists #missing_index_pretty;

    create table #missing_index_pretty
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        equality nvarchar(max),
        inequality nvarchar(max),
        [include] nvarchar(max),
        is_spool bit,
        details as N'/* '
            + CHAR(10)
            + case is_spool
                  when 0
                      then N'The Query Processor estimates that implementing the '
                  else N'We estimate that implementing the '
                       end
            + CONVERT(nvarchar(30), impact)
            + '%.'
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
            + N'*/',
        index mip_ix_ids clustered (sql_handle, query_hash)
    );

    drop table if exists #index_spool_ugly;

    create table #index_spool_ugly
    (
        query_hash binary(8),
        sql_handle varbinary(64),
        impact float,
        database_name nvarchar(128),
        schema_name nvarchar(128),
        table_name nvarchar(128),
        equality nvarchar(max),
        inequality nvarchar(max),
        [include] nvarchar(max),
        index isu_ix_ids clustered (sql_handle, query_hash)
    );


    /*Sets up WHERE clause that gets used quite a bit*/

--Date stuff
--If they're both NULL, we'll just look at the last 7 days
    if (@startdate is null and @enddate is null)
        begin
            raiserror (N'@StartDate and @EndDate are NULL, checking last 7 days', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.last_execution_time >= DATEADD(DAY, -7, DATEDIFF(DAY, 0, SYSDATETIME() ))
					  ';
        end;

--Hey, that's nice of me
    if @startdate is not null
        begin
            raiserror (N'Setting start date filter', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.last_execution_time >= @sp_StartDate
					   ';
        end;

--Alright, sensible
    if @enddate is not null
        begin
            raiserror (N'Setting end date filter', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.last_execution_time < @sp_EndDate
					   ';
        end;

--C'mon, why would you do that?
    if (@startdate is null and @enddate is not null)
        begin
            raiserror (N'Setting reasonable start date filter', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.last_execution_time >= DATEADD(DAY, -7, @sp_EndDate)
					   ';
        end;

--Jeez, abusive
    if (@startdate is not null and @enddate is null)
        begin
            raiserror (N'Setting reasonable end date filter', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.last_execution_time < DATEADD(DAY, 7, @sp_StartDate)
					   ';
        end;

--I care about minimum execution counts
    if @minimumexecutioncount is not null
        begin
            raiserror (N'Setting execution filter', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.count_executions >= @sp_MinimumExecutionCount
					   ';
        end;

--You care about stored proc names
    if @storedprocname is not null
        begin
            raiserror (N'Setting stored proc filter', 0, 1) with nowait;
            set @sql_where += N' AND object_name(qsq.object_id, DB_ID(' + QUOTENAME(@databasename, '''') + N')) = @sp_StoredProcName
					   ';
        end;

--I will always love you, but hopefully this query will eventually end
    if @durationfilter is not null
        begin
            raiserror (N'Setting duration filter', 0, 1) with nowait;
            set @sql_where += N' AND (qsrs.avg_duration / 1000.) >= @sp_MinDuration
					    ';
        end;

--I don't know why you'd go looking for failed queries, but hey
    if (@failed = 0 or @failed is null)
        begin
            raiserror (N'Setting failed query filter to 0', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.execution_type = 0
					    ';
        end;
    if (@failed = 1)
        begin
            raiserror (N'Setting failed query filter to 3, 4', 0, 1) with nowait;
            set @sql_where += N' AND qsrs.execution_type IN (3, 4)
					    ';
        end;

/*Filtering for plan_id or query_id*/
    if (@planidfilter is not null)
        begin
            raiserror (N'Setting plan_id filter', 0, 1) with nowait;
            set @sql_where += N' AND qsp.plan_id = @sp_PlanIdFilter
					    ';
        end;

    if (@queryidfilter is not null)
        begin
            raiserror (N'Setting query_id filter', 0, 1) with nowait;
            set @sql_where += N' AND qsq.query_id = @sp_QueryIdFilter
					    ';
        end;

    if @debug = 1
        raiserror (N'Starting WHERE clause:', 0, 1) with nowait;
    print @sql_where;

    if @sql_where is null
        begin
            raiserror (N'@sql_where is NULL', 0, 1) with nowait;
            return;
        end;

    if (@exporttoexcel = 1 or @skipxml = 1)
        begin
            raiserror (N'Exporting to Excel or skipping XML, hiding summary', 0, 1) with nowait;
            set @hidesummary = 1;
        end;

    if @storedprocname is not null
        begin

            declare @sql nvarchar(max);
            declare @out int;
            declare @proc_params nvarchar(max) = N'@sp_StartDate DATETIME2, @sp_EndDate DATETIME2, @sp_MinimumExecutionCount INT, @sp_MinDuration INT, @sp_StoredProcName NVARCHAR(128), @sp_PlanIdFilter INT, @sp_QueryIdFilter INT, @i_out INT OUTPUT';


            set @sql = N'SELECT @i_out = COUNT(*)
				 FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
				 JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
				 ON qsp.plan_id = qsrs.plan_id
				 JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
				 ON qsq.query_id = qsp.query_id
				 WHERE    1 = 1
				        AND qsq.is_internal_query = 0
				 	    AND qsp.query_plan IS NOT NULL
				 ';

            set @sql += @sql_where;

            exec sys.sp_executesql @sql,
                 @proc_params,
                 @sp_startdate = @startdate, @sp_enddate = @enddate, @sp_minimumexecutioncount = @minimumexecutioncount,
                 @sp_minduration = @duration_filter_ms, @sp_storedprocname = @storedprocname,
                 @sp_planidfilter = @planidfilter, @sp_queryidfilter = @queryidfilter, @i_out = @out output;

            if @out = 0
                begin

                    set @msg = N'We couldn''t find the Stored Procedure ' + QUOTENAME(@storedprocname) +
                               N' in the Query Store views for ' + QUOTENAME(@databasename) + N' between ' +
                               CONVERT(nvarchar(30), ISNULL(@startdate,
                                                            DATEADD(day, -7, DATEDIFF(day, 0, SYSDATETIME())))) +
                               N' and ' + CONVERT(nvarchar(30), ISNULL(@enddate, SYSDATETIME())) +
                               '. Try removing schema prefixes or adjusting dates. If it was executed from a different database context, try searching there instead.';
                    raiserror (@msg, 0, 1) with nowait;

                    select @msg as [Blue Flowers, Blue Flowers, Blue Flowers];

                    return;

                end;

        end;


/*
This is our grouped interval query.

By default, it looks at queries:
	In the last 7 days
	That aren't system queries
	That have a query plan (some won't, if nested level is > 128, along with other reasons)
	And haven't failed
	This stuff, along with some other options, will be configurable in the stored proc

*/

    if @sql_where is not null
        begin try
            begin

                raiserror (N'Populating temp tables', 0, 1) with nowait;

                raiserror (N'Gathering intervals', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
SELECT   CONVERT(DATE, qsrs.last_execution_time) AS flat_date,
         MIN(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time), 0)) AS start_range,
         MAX(DATEADD(HOUR, DATEDIFF(HOUR, 0, qsrs.last_execution_time) + 1, 0)) AS end_range,
         SUM(qsrs.avg_duration / 1000.) / SUM(qsrs.count_executions) AS total_avg_duration_ms,
         SUM(qsrs.avg_cpu_time / 1000.) / SUM(qsrs.count_executions) AS total_avg_cpu_time_ms,
         SUM((qsrs.avg_logical_io_reads * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_reads_mb,
         SUM((qsrs.avg_physical_io_reads* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_physical_io_reads_mb,
         SUM((qsrs.avg_logical_io_writes* 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_logical_io_writes_mb,
         SUM((qsrs.avg_query_max_used_memory * 8 ) / 1024.) / SUM(qsrs.count_executions) AS total_avg_query_max_used_memory_mb,
         SUM(qsrs.avg_rowcount) AS total_rowcount,
         SUM(qsrs.count_executions) AS total_count_executions,
         SUM(qsrs.max_duration / 1000.) AS total_max_duration_ms,
         SUM(qsrs.max_cpu_time / 1000.) AS total_max_cpu_time_ms,
         SUM((qsrs.max_logical_io_reads * 8 ) / 1024.) AS total_max_logical_io_reads_mb,
         SUM((qsrs.max_physical_io_reads* 8 ) / 1024.) AS total_max_physical_io_reads_mb,
         SUM((qsrs.max_logical_io_writes* 8 ) / 1024.) AS total_max_logical_io_writes_mb,
         SUM((qsrs.max_query_max_used_memory * 8 ) / 1024.)  AS total_max_query_max_used_memory_mb         ';
                if @new_columns = 1
                    begin
                        set @sql_select += N',
									 SUM((qsrs.avg_log_bytes_used) / 1048576.) / SUM(qsrs.count_executions) AS total_avg_log_bytes_mb,
									 SUM(qsrs.avg_tempdb_space_used) /  SUM(qsrs.count_executions) AS total_avg_tempdb_space,
                                     SUM((qsrs.max_log_bytes_used) / 1048576.) AS total_max_log_bytes_mb,
		                             SUM(qsrs.max_tempdb_space_used) AS total_max_tempdb_space
									 ';
                    end;
                if @new_columns = 0
                    begin
                        set @sql_select += N',
									NULL AS total_avg_log_bytes_mb,
									NULL AS total_avg_tempdb_space,
                                    NULL AS total_max_log_bytes_mb,
                                    NULL AS total_max_tempdb_space
									';
                    end;


                set @sql_select += N'FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
					 JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
					 ON qsp.plan_id = qsrs.plan_id
					 JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
					 ON qsq.query_id = qsp.query_id
					 WHERE  1 = 1
					        AND qsq.is_internal_query = 0
					 	    AND qsp.query_plan IS NOT NULL
					 	  ';


                set @sql_select += @sql_where;

                set @sql_select +=
                    N'GROUP BY CONVERT(DATE, qsrs.last_execution_time)
					OPTION (RECOMPILE);
			';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                insert #grouped_interval with (tablock)
                (flat_date, start_range, end_range, total_avg_duration_ms,
                 total_avg_cpu_time_ms, total_avg_logical_io_reads_mb, total_avg_physical_io_reads_mb,
                 total_avg_logical_io_writes_mb, total_avg_query_max_used_memory_mb, total_rowcount,
                 total_count_executions, total_max_duration_ms, total_max_cpu_time_ms, total_max_logical_io_reads_mb,
                 total_max_physical_io_reads_mb, total_max_logical_io_writes_mb, total_max_query_max_used_memory_mb,
                 total_avg_log_bytes_mb, total_avg_tempdb_space, total_max_log_bytes_mb, total_max_tempdb_space)
                    exec sys.sp_executesql @stmt = @sql_select,
                         @params = @sp_params,
                         @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                         @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                         @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                         @sp_queryidfilter = @queryidfilter;


                /*
The next group of queries looks at plans in the ranges we found in the grouped interval query

We take the highest value from each metric (duration, cpu, etc) and find the top plans by that metric in the range

They insert into the #working_plans table
*/


/*Get longest duration plans*/

                raiserror (N'Gathering longest duration plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH duration_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_duration_ms DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
         qsp.plan_id, qsp.query_id, ''avg duration''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     duration_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
	AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_duration DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH duration_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_duration_ms DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
         qsp.plan_id, qsp.query_id, ''max duration''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     duration_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
	AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_duration DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get longest cpu plans*/

                raiserror (N'Gathering highest cpu plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH cpu_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_cpu_time_ms DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg cpu''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     cpu_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_cpu_time DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH cpu_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_cpu_time_ms DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max cpu''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     cpu_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_cpu_time DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get highest logical read plans*/

                raiserror (N'Gathering highest logical read plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH logical_reads_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_logical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg logical reads''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     logical_reads_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_logical_io_reads DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH logical_reads_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_logical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max logical reads''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     logical_reads_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_logical_io_reads DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get highest physical read plans*/

                raiserror (N'Gathering highest physical read plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH physical_read_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_physical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg physical reads''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     physical_read_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_physical_io_reads DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH physical_read_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_physical_io_reads_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max physical reads''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     physical_read_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_physical_io_reads DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get highest logical write plans*/

                raiserror (N'Gathering highest write plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH logical_writes_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_logical_io_writes_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg writes''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     logical_writes_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_logical_io_writes DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH logical_writes_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_logical_io_writes_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max writes''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     logical_writes_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_logical_io_writes DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get highest memory use plans*/

                raiserror (N'Gathering highest memory use plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH memory_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_query_max_used_memory_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg memory''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     memory_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_query_max_used_memory DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH memory_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_query_max_used_memory_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max memory''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     memory_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.max_query_max_used_memory DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


/*Get highest row count plans*/

                raiserror (N'Gathering highest row count plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_rowcount DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg rows''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'ORDER BY qsrs.avg_rowcount DESC
					OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;


                if @new_columns = 1
                    begin

                        raiserror (N'Gathering new 2017 new column info...', 0, 1) with nowait;

/*Get highest log byte count plans*/

                        raiserror (N'Gathering highest log byte use plans', 0, 1) with nowait;

                        set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                        set @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_log_bytes_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg log bytes''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                        set @sql_select += @sql_where;

                        set @sql_select += N'ORDER BY qsrs.avg_log_bytes_used DESC
					OPTION (RECOMPILE);
					';

                        if @debug = 1
                            print @sql_select;

                        if @sql_select is null
                            begin
                                raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                                return;
                            end;

                        exec sys.sp_executesql @stmt = @sql_select,
                             @params = @sp_params,
                             @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                             @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                             @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                             @sp_queryidfilter = @queryidfilter;

                        raiserror (N'Gathering highest log byte use plans', 0, 1) with nowait;

                        set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                        set @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_log_bytes_mb DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max log bytes''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                        set @sql_select += @sql_where;

                        set @sql_select += N'ORDER BY qsrs.max_log_bytes_used DESC
					OPTION (RECOMPILE);
					';

                        if @debug = 1
                            print @sql_select;

                        if @sql_select is null
                            begin
                                raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                                return;
                            end;

                        exec sys.sp_executesql @stmt = @sql_select,
                             @params = @sp_params,
                             @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                             @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                             @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                             @sp_queryidfilter = @queryidfilter;


/*Get highest tempdb use plans*/

                        raiserror (N'Gathering highest tempdb use plans', 0, 1) with nowait;

                        set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                        set @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_avg_tempdb_space DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''avg tempdb space''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                        set @sql_select += @sql_where;

                        set @sql_select += N'ORDER BY qsrs.avg_tempdb_space_used DESC
					OPTION (RECOMPILE);
					';

                        if @debug = 1
                            print @sql_select;

                        if @sql_select is null
                            begin
                                raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                                return;
                            end;

                        exec sys.sp_executesql @stmt = @sql_select,
                             @params = @sp_params,
                             @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                             @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                             @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                             @sp_queryidfilter = @queryidfilter;

                        set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                        set @sql_select += N'
WITH rowcount_max
AS ( SELECT   TOP 1
              gi.start_range,
              gi.end_range
     FROM     #grouped_interval AS gi
     ORDER BY gi.total_max_tempdb_space DESC )
INSERT #working_plans WITH (TABLOCK)
		( plan_id, query_id, pattern )
SELECT   TOP ( @sp_Top )
		 qsp.plan_id, qsp.query_id, ''max tempdb space''
FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
JOIN     rowcount_max AS dm
ON qsp.last_execution_time >= dm.start_range
   AND qsp.last_execution_time < dm.end_range
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = qsp.plan_id
JOIN     ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON qsq.query_id = qsp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                        set @sql_select += @sql_where;

                        set @sql_select += N'ORDER BY qsrs.max_tempdb_space_used DESC
					OPTION (RECOMPILE);
					';

                        if @debug = 1
                            print @sql_select;

                        if @sql_select is null
                            begin
                                raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                                return;
                            end;

                        exec sys.sp_executesql @stmt = @sql_select,
                             @params = @sp_params,
                             @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                             @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                             @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                             @sp_queryidfilter = @queryidfilter;


                    end;


/*
This rolls up the different patterns we find before deduplicating.

The point of this is so we know if a query was gathered by one or more of the search queries

*/

                raiserror (N'Updating patterns', 0, 1) with nowait;

                with patterns as (
                    select wp.plan_id,
                           wp.query_id,
                           pattern_path = STUFF((select distinct N', ' + wp2.pattern
                                                 from #working_plans as wp2
                                                 where wp.plan_id = wp2.plan_id
                                                   and wp.query_id = wp2.query_id
                                                 for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2, N'')
                    from #working_plans as wp
                )
                update wp
                set wp.pattern = patterns.pattern_path
                from #working_plans as wp
                         join patterns
                              on wp.plan_id = patterns.plan_id
                                  and wp.query_id = patterns.query_id
                option (recompile);


/*
This dedupes our results so we hopefully don't double-work the same plan
*/

                raiserror (N'Deduplicating gathered plans', 0, 1) with nowait;

                with dedupe as (
                    select *, ROW_NUMBER() over (partition by wp.plan_id order by wp.plan_id) as dupes
                    from #working_plans as wp
                )
                delete dedupe
                where dedupe.dupes > 1
                option (recompile);

                set @msg = N'Removed ' + CONVERT(nvarchar(10), @@ROWCOUNT) + N' duplicate plan_ids.';
                raiserror (@msg, 0, 1) with nowait;


/*
This gathers data for the #working_metrics table
*/


                raiserror (N'Collecting worker metrics', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
SELECT ' + QUOTENAME(@databasename, '''') + N' AS database_name, wp.plan_id, wp.query_id,
       QUOTENAME(object_schema_name(qsq.object_id, DB_ID(' + QUOTENAME(@databasename, '''') + N'))) + ''.'' +
	   QUOTENAME(object_name(qsq.object_id, DB_ID(' + QUOTENAME(@databasename, '''') + N'))) AS proc_or_function_name,
	   qsq.batch_sql_handle, qsq.query_hash, qsq.query_parameterization_type_desc, qsq.count_compiles,
	   (qsq.avg_compile_duration / 1000.),
	   (qsq.last_compile_duration / 1000.),
	   (qsq.avg_bind_duration / 1000.),
	   (qsq.last_bind_duration / 1000.),
	   (qsq.avg_bind_cpu_time / 1000.),
	   (qsq.last_bind_cpu_time / 1000.),
	   (qsq.avg_optimize_duration / 1000.),
	   (qsq.last_optimize_duration / 1000.),
	   (qsq.avg_optimize_cpu_time / 1000.),
	   (qsq.last_optimize_cpu_time / 1000.),
	   (qsq.avg_compile_memory_kb / 1024.),
	   (qsq.last_compile_memory_kb / 1024.),
	   qsrs.execution_type_desc, qsrs.first_execution_time, qsrs.last_execution_time, qsrs.count_executions,
	   (qsrs.avg_duration / 1000.),
	   (qsrs.last_duration / 1000.),
	   (qsrs.min_duration / 1000.),
	   (qsrs.max_duration / 1000.),
	   (qsrs.avg_cpu_time / 1000.),
	   (qsrs.last_cpu_time / 1000.),
	   (qsrs.min_cpu_time / 1000.),
	   (qsrs.max_cpu_time / 1000.),
	   ((qsrs.avg_logical_io_reads * 8 ) / 1024.),
	   ((qsrs.last_logical_io_reads * 8 ) / 1024.),
	   ((qsrs.min_logical_io_reads * 8 ) / 1024.),
	   ((qsrs.max_logical_io_reads * 8 ) / 1024.),
	   ((qsrs.avg_logical_io_writes * 8 ) / 1024.),
	   ((qsrs.last_logical_io_writes * 8 ) / 1024.),
	   ((qsrs.min_logical_io_writes * 8 ) / 1024.),
	   ((qsrs.max_logical_io_writes * 8 ) / 1024.),
	   ((qsrs.avg_physical_io_reads * 8 ) / 1024.),
	   ((qsrs.last_physical_io_reads * 8 ) / 1024.),
	   ((qsrs.min_physical_io_reads * 8 ) / 1024.),
	   ((qsrs.max_physical_io_reads * 8 ) / 1024.),
	   (qsrs.avg_clr_time / 1000.),
	   (qsrs.last_clr_time / 1000.),
	   (qsrs.min_clr_time / 1000.),
	   (qsrs.max_clr_time / 1000.),
	   qsrs.avg_dop, qsrs.last_dop, qsrs.min_dop, qsrs.max_dop,
	   ((qsrs.avg_query_max_used_memory * 8 ) / 1024.),
	   ((qsrs.last_query_max_used_memory * 8 ) / 1024.),
	   ((qsrs.min_query_max_used_memory * 8 ) / 1024.),
	   ((qsrs.max_query_max_used_memory * 8 ) / 1024.),
	   qsrs.avg_rowcount, qsrs.last_rowcount, qsrs.min_rowcount, qsrs.max_rowcount,';

                if @new_columns = 1
                    begin
                        set @sql_select += N'
			qsrs.avg_num_physical_io_reads, qsrs.last_num_physical_io_reads, qsrs.min_num_physical_io_reads, qsrs.max_num_physical_io_reads,
			(qsrs.avg_log_bytes_used / 100000000),
			(qsrs.last_log_bytes_used / 100000000),
			(qsrs.min_log_bytes_used / 100000000),
			(qsrs.max_log_bytes_used / 100000000),
			((qsrs.avg_tempdb_space_used * 8 ) / 1024.),
			((qsrs.last_tempdb_space_used * 8 ) / 1024.),
			((qsrs.min_tempdb_space_used * 8 ) / 1024.),
			((qsrs.max_tempdb_space_used * 8 ) / 1024.)
			';
                    end;
                if @new_columns = 0
                    begin
                        set @sql_select += N'
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL,
			NULL
			';
                    end;
                set @sql_select +=
                        N'FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
AND qsp.query_id = wp.query_id
JOIN ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                insert #working_metrics with (tablock)
                (database_name, plan_id, query_id,
                 proc_or_function_name,
                 batch_sql_handle, query_hash, query_parameterization_type_desc, count_compiles,
                 avg_compile_duration, last_compile_duration, avg_bind_duration, last_bind_duration, avg_bind_cpu_time,
                 last_bind_cpu_time, avg_optimize_duration,
                 last_optimize_duration, avg_optimize_cpu_time, last_optimize_cpu_time, avg_compile_memory_kb,
                 last_compile_memory_kb, execution_type_desc,
                 first_execution_time, last_execution_time, count_executions, avg_duration, last_duration, min_duration,
                 max_duration, avg_cpu_time, last_cpu_time,
                 min_cpu_time, max_cpu_time, avg_logical_io_reads, last_logical_io_reads, min_logical_io_reads,
                 max_logical_io_reads, avg_logical_io_writes,
                 last_logical_io_writes, min_logical_io_writes, max_logical_io_writes, avg_physical_io_reads,
                 last_physical_io_reads, min_physical_io_reads,
                 max_physical_io_reads, avg_clr_time, last_clr_time, min_clr_time, max_clr_time, avg_dop, last_dop,
                 min_dop, max_dop, avg_query_max_used_memory,
                 last_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory, avg_rowcount,
                 last_rowcount, min_rowcount, max_rowcount,
                    /* 2017 only columns */
                 avg_num_physical_io_reads, last_num_physical_io_reads, min_num_physical_io_reads,
                 max_num_physical_io_reads,
                 avg_log_bytes_used, last_log_bytes_used, min_log_bytes_used, max_log_bytes_used,
                 avg_tempdb_space_used, last_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used)
                    exec sys.sp_executesql @stmt = @sql_select,
                         @params = @sp_params,
                         @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                         @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                         @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                         @sp_queryidfilter = @queryidfilter;


/*This just helps us classify our queries*/
                update #working_metrics
                set proc_or_function_name = N'Statement'
                where proc_or_function_name is null
                option (recompile);

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
    WITH patterns AS (
         SELECT query_id, planid_path = STUFF((SELECT DISTINCT N'', '' + RTRIM(qsp2.plan_id)
             									FROM ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp2
             									WHERE qsp.query_id = qsp2.query_id
             									FOR XML PATH(N''''), TYPE).value(N''.[1]'', N''NVARCHAR(MAX)''), 1, 2, N'''')
         FROM ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
    )
    UPDATE wm
    SET wm.query_id_all_plan_ids = patterns.planid_path
    FROM #working_metrics AS wm
    JOIN patterns
    ON  wm.query_id = patterns.query_id
    OPTION (RECOMPILE);
'

                exec sys.sp_executesql @stmt = @sql_select;

/*
This gathers data for the #working_plan_text table
*/


                raiserror (N'Gathering working plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
SELECT ' + QUOTENAME(@databasename, '''') + N' AS database_name,  wp.plan_id, wp.query_id,
	   qsp.plan_group_id, qsp.engine_version, qsp.compatibility_level, qsp.query_plan_hash, TRY_CONVERT(XML, qsp.query_plan), qsp.is_online_index_plan, qsp.is_trivial_plan,
	   qsp.is_parallel_plan, qsp.is_forced_plan, qsp.is_natively_compiled, qsp.force_failure_count, qsp.last_force_failure_reason_desc, qsp.count_compiles,
	   qsp.initial_compile_start_time, qsp.last_compile_start_time, qsp.last_execution_time,
	   (qsp.avg_compile_duration / 1000.),
	   (qsp.last_compile_duration / 1000.),
	   qsqt.query_sql_text, qsqt.statement_sql_handle, qsqt.is_part_of_encrypted_module, qsqt.has_restricted_text
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                insert #working_plan_text with (tablock)
                (database_name, plan_id, query_id,
                 plan_group_id, engine_version, compatibility_level, query_plan_hash, query_plan_xml,
                 is_online_index_plan, is_trivial_plan,
                 is_parallel_plan, is_forced_plan, is_natively_compiled, force_failure_count,
                 last_force_failure_reason_desc, count_compiles,
                 initial_compile_start_time, last_compile_start_time, last_execution_time, avg_compile_duration,
                 last_compile_duration,
                 query_sql_text, statement_sql_handle, is_part_of_encrypted_module, has_restricted_text)
                    exec sys.sp_executesql @stmt = @sql_select,
                         @params = @sp_params,
                         @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                         @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                         @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                         @sp_queryidfilter = @queryidfilter;


/*
This gets us context settings for our queries and adds it to the #working_plan_text table
*/

                raiserror (N'Gathering context settings', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
UPDATE wp
SET wp.context_settings = SUBSTRING(
					    CASE WHEN (CAST(qcs.set_options AS INT) & 1 = 1) THEN '', ANSI_PADDING'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8 = 8) THEN '', CONCAT_NULL_YIELDS_NULL'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 16 = 16) THEN '', ANSI_WARNINGS'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 32 = 32) THEN '', ANSI_NULLS'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 64 = 64) THEN '', QUOTED_IDENTIFIER'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 4096 = 4096) THEN '', ARITH_ABORT'' ELSE '''' END +
					    CASE WHEN (CAST(qcs.set_options AS INT) & 8192 = 8192) THEN '', NUMERIC_ROUNDABORT'' ELSE '''' END
					    , 2, 200000)
FROM #working_plan_text wp
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_context_settings AS qcs
ON qcs.context_settings_id = qsq.context_settings_id
OPTION (RECOMPILE);
';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select;


/*This adds the patterns we found from each interval to the #working_plan_text table*/

                raiserror (N'Add patterns to working plans', 0, 1) with nowait;

                update wpt
                set wpt.pattern = wp.pattern
                from #working_plans as wp
                         join #working_plan_text as wpt
                              on wpt.plan_id = wp.plan_id
                                  and wpt.query_id = wp.query_id
                option (recompile);

/*This cleans up query text a bit*/

                raiserror (N'Clean awkward characters from query text', 0, 1) with nowait;

                update b
                set b.query_sql_text = REPLACE(REPLACE(REPLACE(b.query_sql_text, @cr, ' '), @lf, ' '), @tab, '  ')
                from #working_plan_text as b
                option (recompile);


/*This populates #working_wait_stats when available*/

                if @waitstats = 1
                    begin

                        raiserror (N'Collecting wait stats info', 0, 1) with nowait;


                        set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                        set @sql_select += N'
		SELECT   qws.plan_id,
		         qws.wait_category,
		         qws.wait_category_desc,
		         SUM(qws.total_query_wait_time_ms) AS total_query_wait_time_ms,
		         SUM(qws.avg_query_wait_time_ms) AS avg_query_wait_time_ms,
		         SUM(qws.last_query_wait_time_ms) AS last_query_wait_time_ms,
		         SUM(qws.min_query_wait_time_ms) AS min_query_wait_time_ms,
		         SUM(qws.max_query_wait_time_ms) AS max_query_wait_time_ms
		FROM     ' + QUOTENAME(@databasename) + N'.sys.query_store_wait_stats qws
		JOIN #working_plans AS wp
		ON qws.plan_id = wp.plan_id
		GROUP BY qws.plan_id, qws.wait_category, qws.wait_category_desc
		HAVING SUM(qws.min_query_wait_time_ms) >= 5
		OPTION (RECOMPILE);
		';

                        if @debug = 1
                            print @sql_select;

                        if @sql_select is null
                            begin
                                raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                                return;
                            end;

                        insert #working_wait_stats with (tablock)
                        (plan_id, wait_category, wait_category_desc, total_query_wait_time_ms, avg_query_wait_time_ms,
                         last_query_wait_time_ms, min_query_wait_time_ms, max_query_wait_time_ms)
                            exec sys.sp_executesql @stmt = @sql_select;


                        /*This updates #working_plan_text with the top three waits from the wait stats DMV*/

                        raiserror (N'Update working_plan_text with top three waits', 0, 1) with nowait;


                        update wpt
                        set wpt.top_three_waits = x.top_three_waits
                        from #working_plan_text as wpt
                                 join (
                            select wws.plan_id,
                                   top_three_waits = STUFF((select top 3 N', ' + wws2.wait_category_desc + N' (' +
                                                                         CONVERT(nvarchar(20),
                                                                                 SUM(CONVERT(bigint, wws2.avg_query_wait_time_ms))) +
                                                                         N' ms) '
                                                            from #working_wait_stats as wws2
                                                            where wws.plan_id = wws2.plan_id
                                                            group by wws2.wait_category_desc
                                                            order by SUM(wws2.avg_query_wait_time_ms) desc
                                                            for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'),
                                                           1, 2, N'')
                            from #working_wait_stats as wws
                            group by wws.plan_id
                        ) as x
                                      on x.plan_id = wpt.plan_id
                        option (recompile);

                    end;

/*End wait stats population*/

                update #working_plan_text
                set top_three_waits = case
                                          when @waitstats = 0
                                              then N'The query store waits stats DMV is not available'
                                          else N'No Significant waits detected!'
                    end
                where top_three_waits is null
                option (recompile);

            end;
        end try
        begin catch
            raiserror (N'Failure populating temp tables.', 0,1) with nowait;

            if @sql_select is not null
                begin
                    set @msg = N'Last @sql_select: ' + @sql_select;
                    raiserror (@msg, 0, 1) with nowait;
                end;

            select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
                   @error_severity = ERROR_SEVERITY(),
                   @error_state = ERROR_STATE();
            raiserror (@msg, @error_severity, @error_state) with nowait;


            while @@TRANCOUNT > 0
                rollback;

            return;
        end catch;

    if (@skipxml = 0)
        begin try
            begin

                /*
This sets up the #working_warnings table with the IDs we're interested in so we can tie warnings back to them
*/

                raiserror (N'Populate working warnings table with gathered plans', 0, 1) with nowait;


                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
SELECT DISTINCT wp.plan_id, wp.query_id, qsq.query_hash, qsqt.statement_sql_handle
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select += N'OPTION (RECOMPILE);
					';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                insert #working_warnings with (tablock)
                    (plan_id, query_id, query_hash, sql_handle)
                    exec sys.sp_executesql @stmt = @sql_select,
                         @params = @sp_params,
                         @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                         @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                         @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                         @sp_queryidfilter = @queryidfilter;

/*
This looks for queries in the query stores that we picked up from an internal that have multiple plans in cache

This and several of the following queries all replaced XML parsing to find plan attributes. Sweet.

Thanks, Query Store
*/

                raiserror (N'Populating object name in #working_warnings', 0, 1) with nowait;
                update w
                set w.proc_or_function_name = ISNULL(wm.proc_or_function_name, N'Statement')
                from #working_warnings as w
                         join #working_metrics as wm
                              on w.plan_id = wm.plan_id
                                  and w.query_id = wm.query_id
                option (recompile);


                raiserror (N'Checking for multiple plans', 0, 1) with nowait;

                set @sql_select = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;';
                set @sql_select += N'
UPDATE ww
SET ww.plan_multiple_plans = 1
FROM #working_warnings AS ww
JOIN
(
SELECT wp.query_id, COUNT(qsp.plan_id) AS  plans
FROM   #working_plans AS wp
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_plan AS qsp
ON qsp.plan_id = wp.plan_id
   AND qsp.query_id = wp.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query AS qsq
ON wp.query_id = qsq.query_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_query_text AS qsqt
ON qsqt.query_text_id = qsq.query_text_id
JOIN   ' + QUOTENAME(@databasename) + N'.sys.query_store_runtime_stats AS qsrs
ON qsrs.plan_id = wp.plan_id
WHERE    1 = 1
    AND qsq.is_internal_query = 0
	AND qsp.query_plan IS NOT NULL
	';

                set @sql_select += @sql_where;

                set @sql_select +=
                    N'GROUP BY wp.query_id
  HAVING COUNT(qsp.plan_id) > 1
) AS x
    ON ww.query_id = x.query_id
OPTION (RECOMPILE);
';

                if @debug = 1
                    print @sql_select;

                if @sql_select is null
                    begin
                        raiserror (N'@sql_select is NULL', 0, 1) with nowait;
                        return;
                    end;

                exec sys.sp_executesql @stmt = @sql_select,
                     @params = @sp_params,
                     @sp_top = @top, @sp_startdate = @startdate, @sp_enddate = @enddate,
                     @sp_minimumexecutioncount = @minimumexecutioncount, @sp_minduration = @duration_filter_ms,
                     @sp_storedprocname = @storedprocname, @sp_planidfilter = @planidfilter,
                     @sp_queryidfilter = @queryidfilter;

/*
This looks for forced plans
*/

                raiserror (N'Checking for forced plans', 0, 1) with nowait;

                update ww
                set ww.is_forced_plan = 1
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                                  and wp.is_forced_plan = 1
                option (recompile);


/*
This looks for forced parameterization
*/

                raiserror (N'Checking for forced parameterization', 0, 1) with nowait;

                update ww
                set ww.is_forced_parameterized = 1
                from #working_warnings as ww
                         join #working_metrics as wm
                              on ww.plan_id = wm.plan_id
                                  and ww.query_id = wm.query_id
                                  and wm.query_parameterization_type_desc = 'Forced'
                option (recompile);


/*
This looks for unparameterized queries
*/

                raiserror (N'Checking for unparameterized plans', 0, 1) with nowait;

                update ww
                set ww.unparameterized_query = 1
                from #working_warnings as ww
                         join #working_metrics as wm
                              on ww.plan_id = wm.plan_id
                                  and ww.query_id = wm.query_id
                                  and wm.query_parameterization_type_desc = 'None'
                                  and ww.proc_or_function_name = 'Statement'
                option (recompile);


/*
This looks for cursors
*/

                raiserror (N'Checking for cursors', 0, 1) with nowait;
                update ww
                set ww.is_cursor = 1
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                                  and wp.plan_group_id > 0
                option (recompile);


                update ww
                set ww.is_cursor = 1
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                where ww.query_hash = 0x0000000000000000
                   or wp.query_plan_hash = 0x0000000000000000
                option (recompile);

/*
This looks for parallel plans
*/
                update ww
                set ww.is_parallel = 1
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                                  and wp.is_parallel_plan = 1
                option (recompile);

/*This looks for old CE*/

                raiserror (N'Checking for legacy CE', 0, 1) with nowait;

                update w
                set w.downlevel_estimator = 1
                from #working_warnings as w
                         join #working_plan_text as wpt
                              on w.plan_id = wpt.plan_id
                                  and w.query_id = wpt.query_id
/*PLEASE DON'T TELL ANYONE I DID THIS*/
                where PARSENAME(wpt.engine_version, 4) <
                      PARSENAME(CONVERT(varchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)
                option (recompile);
                /*NO SERIOUSLY THIS IS A HORRIBLE IDEA*/


/*Plans that compile 2x more than they execute*/

                raiserror (N'Checking for plans that compile 2x more than they execute', 0, 1) with nowait;

                update ww
                set ww.is_compile_more = 1
                from #working_warnings as ww
                         join #working_metrics as wm
                              on ww.plan_id = wm.plan_id
                                  and ww.query_id = wm.query_id
                                  and wm.count_compiles > (wm.count_executions * 2)
                option (recompile);

/*Plans that compile 2x more than they execute*/

                raiserror (N'Checking for plans that take more than 5 seconds to bind, compile, or optimize', 0, 1) with nowait;

                update ww
                set ww.is_slow_plan = 1
                from #working_warnings as ww
                         join #working_metrics as wm
                              on ww.plan_id = wm.plan_id
                                  and ww.query_id = wm.query_id
                                  and (wm.avg_bind_duration > 5000
                                      or
                                       wm.avg_compile_duration > 5000
                                      or
                                       wm.avg_optimize_duration > 5000
                                      or
                                       wm.avg_optimize_cpu_time > 5000)
                option (recompile);


/*
This parses the XML from our top plans into smaller chunks for easier consumption
*/

                raiserror (N'Begin XML nodes parsing', 0, 1) with nowait;

                raiserror (N'Inserting #statements', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                insert
                #statements
                with (tablock)
                (
                plan_id
                ,
                query_id
                ,
                query_hash
                ,
                sql_handle
                ,
                statement
                ,
                is_cursor
                )
                select ww.plan_id,
                       ww.query_id,
                       ww.query_hash,
                       ww.sql_handle,
                       q.n.query('.') as statement,
                       0              as is_cursor
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                         cross apply wp.query_plan_xml.nodes('//p:StmtSimple') as q(n)
                option (recompile);

                raiserror (N'Inserting parsed cursor XML to #statements', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                insert
                #statements
                with (tablock)
                (
                plan_id
                ,
                query_id
                ,
                query_hash
                ,
                sql_handle
                ,
                statement
                ,
                is_cursor
                )
                select ww.plan_id,
                       ww.query_id,
                       ww.query_hash,
                       ww.sql_handle,
                       q.n.query('.') as statement,
                       1              as is_cursor
                from #working_warnings as ww
                         join #working_plan_text as wp
                              on ww.plan_id = wp.plan_id
                                  and ww.query_id = wp.query_id
                         cross apply wp.query_plan_xml.nodes('//p:StmtCursor') as q(n)
                option (recompile);

                raiserror (N'Inserting to #query_plan', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                insert
                #query_plan
                with (tablock)
                (
                plan_id
                ,
                query_id
                ,
                query_hash
                ,
                sql_handle
                ,
                query_plan
                )
                select s.plan_id, s.query_id, s.query_hash, s.sql_handle, q.n.query('.') as query_plan
                from #statements as s
                         cross apply s.statement.nodes('//p:QueryPlan') as q(n)
                option (recompile);

                raiserror (N'Inserting to #relop', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                insert
                #relop
                with (tablock)
                (
                plan_id
                ,
                query_id
                ,
                query_hash
                ,
                sql_handle
                ,
                relop
                )
                select qp.plan_id, qp.query_id, qp.query_hash, qp.sql_handle, q.n.query('.') as relop
                from #query_plan qp
                         cross apply qp.query_plan.nodes('//p:RelOp') as q(n)
                option (recompile);


-- statement level checks

                raiserror (N'Performing compile timeout checks', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.compile_timeout = 1
                from #statements s
                         join #working_warnings as b
                              on s.query_hash = b.query_hash
                where s.statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="TimeOut"]') = 1
                option (recompile);


                raiserror (N'Performing compile memory limit exceeded checks', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.compile_memory_limit_exceeded = 1
                from #statements s
                         join #working_warnings as b
                              on s.query_hash = b.query_hash
                where s.statement.exist('/p:StmtSimple/@StatementOptmEarlyAbortReason[.="MemoryLimitExceeded"]') = 1
                option (recompile);

                if @expertmode > 0
                    begin
                        raiserror (N'Performing index DML checks', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                            index_dml as (
                            select s.query_hash,
                            index_dml = case when s.statement.exist('//p:StmtSimple/@StatementType[.="CREATE INDEX"]') = 1 then 1
                            when s.statement.exist('//p:StmtSimple/@StatementType[.="DROP INDEX"]') = 1 then 1
                            end
                            from #statements s
                            )
                        update b
                        set b.index_dml = i.index_dml
                        from #working_warnings as b
                                 join index_dml i
                                      on i.query_hash = b.query_hash
                        where i.index_dml = 1
                        option (recompile);


                        raiserror (N'Performing table DML checks', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                            table_dml as (
                            select s.query_hash,
                            table_dml = case when s.statement.exist('//p:StmtSimple/@StatementType[.="CREATE TABLE"]') = 1 then 1
                            when s.statement.exist('//p:StmtSimple/@StatementType[.="DROP OBJECT"]') = 1 then 1
                            end
                            from #statements as s
                            )
                        update b
                        set b.table_dml = t.table_dml
                        from #working_warnings as b
                                 join table_dml t
                                      on t.query_hash = b.query_hash
                        where t.table_dml = 1
                        option (recompile);
                    end;


                raiserror (N'Gathering trivial plans', 0, 1) with nowait;
                with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                update b
                set b.is_trivial = 1
                from #working_warnings as b
                         join (
                    select s.sql_handle
                    from #statements as s
                             join (select r.sql_handle
                                   from #relop as r
                                   where r.relop.exist('//p:RelOp[contains(@LogicalOp, "Scan")]') = 1) as r
                                  on r.sql_handle = s.sql_handle
                    where s.statement.exist(
                                  '//p:StmtSimple[@StatementOptmLevel[.="TRIVIAL"]]/p:QueryPlan/p:ParameterList') = 1
                ) as s
                              on b.sql_handle = s.sql_handle
                option (recompile);

                if @expertmode > 0
                    begin
                        raiserror (N'Gathering row estimates', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                        insert
                        #est_rows
                        (
                        query_hash
                        ,
                        estimated_rows
                        )
                        select distinct CONVERT(binary(8), RIGHT('0000000000000000' +
                                                                 SUBSTRING(c.n.value('@QueryHash', 'VARCHAR(18)'), 3, 18),
                                                                 16), 2)                           as query_hash,
                                        c.n.value('(/p:StmtSimple/@StatementEstRows)[1]', 'FLOAT') as estimated_rows
                        from #statements as s
                                 cross apply s.statement.nodes('/p:StmtSimple') as c(n)
                        where c.n.exist('/p:StmtSimple[@StatementEstRows > 0]') = 1;

                        update b
                        set b.estimated_rows = er.estimated_rows
                        from #working_warnings as b
                                 join #est_rows er
                                      on er.query_hash = b.query_hash
                        option (recompile);
                    end;


/*Begin plan cost calculations*/
                raiserror (N'Gathering statement costs', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                insert
                #plan_cost
                with (tablock)
                (
                query_plan_cost
                ,
                sql_handle
                ,
                plan_id
                )
                select distinct s.statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') query_plan_cost,
                                s.sql_handle,
                                s.plan_id
                from #statements s
                         outer apply s.statement.nodes('/p:StmtSimple') as q(n)
                where s.statement.value('sum(/p:StmtSimple/@StatementSubTreeCost)', 'float') > 0
                option (recompile);


                raiserror (N'Updating statement costs', 0, 1) with nowait;
                with pc as (
                    select SUM(distinct pc.query_plan_cost) as queryplancostsum, pc.sql_handle, pc.plan_id
                    from #plan_cost as pc
                    group by pc.sql_handle, pc.plan_id
                )
                update b
                set b.query_cost = ISNULL(pc.queryplancostsum, 0)
                from #working_warnings as b
                         join pc
                              on pc.sql_handle = b.sql_handle
                                  and pc.plan_id = b.plan_id
                option (recompile);


/*End plan cost calculations*/


                raiserror (N'Checking for plan warnings', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.plan_warnings = 1
                from #query_plan qp
                         join #working_warnings b
                              on qp.sql_handle = b.sql_handle
                                  and qp.query_plan.exist('/p:QueryPlan/p:Warnings') = 1
                option (recompile);


                raiserror (N'Checking for implicit conversion', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.implicit_conversions = 1
                from #query_plan qp
                         join #working_warnings b
                              on qp.sql_handle = b.sql_handle
                                  and qp.query_plan.exist(
                                              '/p:QueryPlan/p:Warnings/p:PlanAffectingConvert/@Expression[contains(., "CONVERT_IMPLICIT")]') =
                                      1
                option (recompile);

                if @expertmode > 0
                    begin
                        raiserror (N'Performing busy loops checks', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update p
                        set busy_loops = case when (x.estimated_executions / 100.0) > x.estimated_rows then 1 end
                        from #working_warnings p
                                 join (
                            select qs.sql_handle,
                                   relop.value('sum(/p:RelOp/@EstimateRows)', 'float')          as estimated_rows,
                                   relop.value('sum(/p:RelOp/@EstimateRewinds)', 'float') +
                                   relop.value('sum(/p:RelOp/@EstimateRebinds)', 'float') + 1.0 as estimated_executions
                            from #relop qs
                        ) as x on p.sql_handle = x.sql_handle
                        option (recompile);
                    end;


                raiserror (N'Performing TVF join check', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update p
                set p.tvf_join = case when x.tvf_join = 1 then 1 end
                from #working_warnings p
                         join (
                    select r.sql_handle,
                           1 as tvf_join
                    from #relop as r
                    where r.relop.exist('//p:RelOp[(@LogicalOp[.="Table-valued function"])]') = 1
                      and r.relop.exist('//p:RelOp[contains(@LogicalOp, "Join")]') = 1
                ) as x on p.sql_handle = x.sql_handle
                option (recompile);

                if @expertmode > 0
                    begin
                        raiserror (N'Checking for operator warnings', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                           , x as (
                            select r.sql_handle
                           , c.n.exist('//p:Warnings[(@NoJoinPredicate[.="1"])]') as warning_no_join_predicate
                           , c.n.exist('//p:ColumnsWithNoStatistics') as no_stats_warning
                           , c.n.exist('//p:Warnings') as relop_warnings
                            from #relop as r
                            cross apply r.relop.nodes('/p:RelOp/p:Warnings') as c(n)
                            )
                        update b
                        set b.warning_no_join_predicate = x.warning_no_join_predicate,
                            b.no_stats_warning          = x.no_stats_warning,
                            b.relop_warnings            = x.relop_warnings
                        from #working_warnings b
                                 join x on x.sql_handle = b.sql_handle
                        option (recompile);
                    end;


                raiserror (N'Checking for table variables', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                   , x as (
                    select r.sql_handle
                   , c.n.value('substring(@Table, 2, 1)'
                   , 'VARCHAR(100)') as first_char
                    from #relop r
                    cross apply r.relop.nodes('//p:Object') as c(n)
                    )
                update b
                set b.is_table_variable = 1
                from #working_warnings b
                         join x on x.sql_handle = b.sql_handle
                         join #working_metrics as wm
                              on b.plan_id = wm.plan_id
                                  and b.query_id = wm.query_id
                                  and wm.batch_sql_handle is not null
                where x.first_char = '@'
                option (recompile);


                if @expertmode > 0
                    begin
                        raiserror (N'Checking for functions', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                           , x as (
                            select r.sql_handle
                           , n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))'
                           , 'INT') as function_count
                           , n.fn.value('count(distinct-values(//p:UserDefinedFunction[@IsClrFunction = "1"]))'
                           , 'INT') as clr_function_count
                            from #relop r
                            cross apply r.relop.nodes('/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n (fn)
                            )
                        update b
                        set b.function_count     = x.function_count,
                            b.clr_function_count = x.clr_function_count
                        from #working_warnings b
                                 join x on x.sql_handle = b.sql_handle
                        option (recompile);
                    end;


                raiserror (N'Checking for expensive key lookups', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.key_lookup_cost = x.key_lookup_cost
                from #working_warnings b
                         join (
                    select r.sql_handle,
                           MAX(r.relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) as key_lookup_cost
                    from #relop r
                    where r.relop.exist('/p:RelOp/p:IndexScan[(@Lookup[.="1"])]') = 1
                    group by r.sql_handle
                ) as x on x.sql_handle = b.sql_handle
                option (recompile);


                raiserror (N'Checking for expensive remote queries', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.remote_query_cost = x.remote_query_cost
                from #working_warnings b
                         join (
                    select r.sql_handle,
                           MAX(r.relop.value('sum(/p:RelOp/@EstimatedTotalSubtreeCost)', 'float')) as remote_query_cost
                    from #relop r
                    where r.relop.exist('/p:RelOp[(@PhysicalOp[contains(., "Remote")])]') = 1
                    group by r.sql_handle
                ) as x on x.sql_handle = b.sql_handle
                option (recompile);


                raiserror (N'Checking for expensive sorts', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set sort_cost = y.max_sort_cost
                from #working_warnings b
                         join (
                    select x.sql_handle, MAX((x.sort_io + x.sort_cpu)) as max_sort_cost
                    from (
                             select qs.sql_handle,
                                    relop.value('sum(/p:RelOp/@EstimateIO)', 'float')  as sort_io,
                                    relop.value('sum(/p:RelOp/@EstimateCPU)', 'float') as sort_cpu
                             from #relop qs
                             where [relop].exist('/p:RelOp[(@PhysicalOp[.="Sort"])]') = 1
                         ) as x
                    group by x.sql_handle
                ) as y
                              on b.sql_handle = y.sql_handle
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
                        from #working_warnings b
                                 join #statements as s
                                      on b.sql_handle = s.sql_handle
                                 cross apply s.statement.nodes('/p:StmtCursor') as n1(fn)
                        where n1.fn.exist('//p:CursorPlan/@CursorConcurrency[.="Optimistic"]') = 1
                          and s.is_cursor = 1
                        option (recompile);


                        raiserror (N'Checking if cursor is Forward Only', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_forward_only_cursor = 1
                        from #working_warnings b
                                 join #statements as s
                                      on b.sql_handle = s.sql_handle
                                 cross apply s.statement.nodes('/p:StmtCursor') as n1(fn)
                        where n1.fn.exist('//p:CursorPlan/@ForwardOnly[.="true"]') = 1
                          and s.is_cursor = 1
                        option (recompile);


                        raiserror (N'Checking if cursor is Fast Forward', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_fast_forward_cursor = 1
                        from #working_warnings b
                                 join #statements as s
                                      on b.sql_handle = s.sql_handle
                                 cross apply s.statement.nodes('/p:StmtCursor') as n1(fn)
                        where n1.fn.exist('//p:CursorPlan/@CursorActualType[.="FastForward"]') = 1
                          and s.is_cursor = 1
                        option (recompile);


                        raiserror (N'Checking for Dynamic cursors', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_cursor_dynamic = 1
                        from #working_warnings b
                                 join #statements as s
                                      on b.sql_handle = s.sql_handle
                                 cross apply s.statement.nodes('/p:StmtCursor') as n1(fn)
                        where n1.fn.exist('//p:CursorPlan/@CursorActualType[.="Dynamic"]') = 1
                          and s.is_cursor = 1
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
                        from #working_warnings b
                                 join (
                            select r.sql_handle,
                                   0                                         as is_table_scan,
                                   q.n.exist('@ScanDirection[.="BACKWARD"]') as backwards_scan,
                                   q.n.value('@ForcedIndex', 'bit')          as forced_index,
                                   q.n.value('@ForceSeek', 'bit')            as forced_seek,
                                   q.n.value('@ForceScan', 'bit')            as forced_scan
                            from #relop r
                                     cross apply r.relop.nodes('//p:IndexScan') as q(n)
                            union all
                            select r.sql_handle,
                                   1                                         as is_table_scan,
                                   q.n.exist('@ScanDirection[.="BACKWARD"]') as backwards_scan,
                                   q.n.value('@ForcedIndex', 'bit')          as forced_index,
                                   q.n.value('@ForceSeek', 'bit')            as forced_seek,
                                   q.n.value('@ForceScan', 'bit')            as forced_scan
                            from #relop r
                                     cross apply r.relop.nodes('//p:TableScan') as q(n)
                        ) as x on b.sql_handle = x.sql_handle
                        option (recompile);
                    end;


                if @expertmode > 0
                    begin
                        raiserror (N'Checking for computed columns that reference scalar UDFs', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_computed_scalar = x.computed_column_function
                        from #working_warnings b
                                 join (
                            select r.sql_handle,
                                   n.fn.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))',
                                              'INT') as computed_column_function
                            from #relop r
                                     cross apply r.relop.nodes(
                                    '/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ScalarOperator') n(fn)
                            where n.fn.exist(
                                          '/p:RelOp/p:ComputeScalar/p:DefinedValues/p:DefinedValue/p:ColumnReference[(@ComputedColumn[.="1"])]') =
                                  1
                        ) as x on x.sql_handle = b.sql_handle
                        option (recompile);
                    end;


                raiserror (N'Checking for filters that reference scalar UDFs', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.is_computed_filter = x.filter_function
                from #working_warnings b
                         join (
                    select r.sql_handle,
                           c.n.value('count(distinct-values(//p:UserDefinedFunction[not(@IsClrFunction)]))',
                                     'INT') as filter_function
                    from #relop as r
                             cross apply r.relop.nodes(
                            '/p:RelOp/p:Filter/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator/p:UserDefinedFunction') c(n)
                ) x on x.sql_handle = b.sql_handle
                option (recompile);


                if @expertmode > 0
                    begin
                        raiserror (N'Checking modification queries that hit lots of indexes', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                            IndexOps as
                            (
                            select
                            r.query_hash,
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
                            select ios.query_hash,
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
                            group by ios.query_hash)
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
                        from #working_warnings as b
                                 join iops on iops.query_hash = b.query_hash
                        option (recompile);
                    end;


                if @expertmode > 0
                    begin
                        raiserror (N'Checking for Spatial index use', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_spatial = x.is_spatial
                        from #working_warnings as b
                                 join (
                            select r.sql_handle,
                                   1 as is_spatial
                            from #relop r
                                     cross apply r.relop.nodes('/p:RelOp//p:Object') n(fn)
                            where n.fn.exist('(@IndexKind[.="Spatial"])') = 1
                        ) as x on x.sql_handle = b.sql_handle
                        option (recompile);
                    end;

                raiserror (N'Checking for forced serialization', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.is_forced_serial = 1
                from #query_plan qp
                         join #working_warnings as b
                              on qp.sql_handle = b.sql_handle
                                  and b.is_parallel is null
                                  and qp.query_plan.exist('/p:QueryPlan/@NonParallelPlanReason') = 1
                option (recompile);


                if @expertmode > 0
                    begin
                        raiserror (N'Checking for ColumnStore queries operating in Row Mode instead of Batch Mode', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.columnstore_row_mode = x.is_row_mode
                        from #working_warnings as b
                                 join (
                            select r.sql_handle,
                                   r.relop.exist('/p:RelOp[(@EstimatedExecutionMode[.="Row"])]') as is_row_mode
                            from #relop r
                            where r.relop.exist('/p:RelOp/p:IndexScan[(@Storage[.="ColumnStore"])]') = 1
                        ) as x on x.sql_handle = b.sql_handle
                        option (recompile);
                    end;


                if @expertmode > 0
                    begin
                        raiserror ('Checking for row level security only', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_row_level = 1
                        from #working_warnings b
                                 join #statements s
                                      on s.query_hash = b.query_hash
                        where s.statement.exist('/p:StmtSimple/@SecurityPolicyApplied[.="true"]') = 1
                        option (recompile);
                    end;


                if @expertmode > 0
                    begin
                        raiserror ('Checking for wonky Index Spools', 0, 1) with nowait;
                        with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                           , selects
                            as ( select s.plan_id
                           , s.query_id
                            from #statements as s
                            where s.statement.exist('/p:StmtSimple/@StatementType[.="SELECT"]') = 1 )
                           , spools
                            as ( select distinct r.plan_id
                           , r.query_id
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
                            on s.plan_id = r.plan_id
                            and s.query_id = r.query_id
                            cross apply r.relop.nodes('/p:RelOp') as c(n)
                            where r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1
                            )
                        update ww
                        set ww.index_spool_rows = sp.estimated_rows,
                            ww.index_spool_cost = ((sp.estimated_io * sp.estimated_cpu) *
                                                   case when sp.estimated_rebinds < 1 then 1 else sp.estimated_rebinds end)

                        from #working_warnings ww
                                 join spools sp
                                      on ww.plan_id = sp.plan_id
                                          and ww.query_id = sp.query_id
                        option (recompile);
                    end;


                if (PARSENAME(CONVERT(varchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) >= 14
                    or ((PARSENAME(CONVERT(varchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) = 13
                        and PARSENAME(CONVERT(varchar(128), SERVERPROPERTY('PRODUCTVERSION')), 2) >= 5026)
                    begin

                        raiserror (N'Beginning 2017 and 2016 SP2 specfic checks', 0, 1) with nowait;

                        if @expertmode > 0
                            begin
                                raiserror ('Gathering stats information', 0, 1) with nowait;
                                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                                insert
                                #stats_agg
                                with (tablock)
                                (
                                sql_handle
                                ,
                                last_update
                                ,
                                modification_count
                                ,
                                sampling_percent
                                ,
                                [statistics]
                                ,
                                [table]
                                ,
                                [schema]
                                ,
                                [database]
                                )
                                select qp.sql_handle,
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
                                    select sa.sql_handle
                                    from #stats_agg as sa
                                    group by sa.sql_handle
                                    having MAX(sa.last_update) <= DATEADD(day, -7, SYSDATETIME())
                                       and AVG(sa.modification_count) >= 100000
                                )
                                update b
                                set b.stale_stats = 1
                                from #working_warnings as b
                                         join stale_stats os
                                              on b.sql_handle = os.sql_handle
                                option (recompile);
                            end;


                        if (PARSENAME(CONVERT(varchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) >= 14
                            and @expertmode > 0
                            begin
                                raiserror (N'Checking for Adaptive Joins', 0, 1) with nowait;
                                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                                    aj as (
                                    select r.sql_handle
                                    from #relop as r
                                    cross apply r.relop.nodes('//p:RelOp') x(c)
                                    where x.c.exist('@IsAdaptive[.=1]') = 1
                                    )
                                update b
                                set b.is_adaptive = 1
                                from #working_warnings as b
                                         join aj
                                              on b.sql_handle = aj.sql_handle
                                option (recompile);
                            end;


                        if @expertmode > 0
                            begin
                                ;
                                raiserror (N'Checking for Row Goals', 0, 1) with nowait;
                                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                                    row_goals as (
                                    select qs.query_hash
                                    from #relop qs
                                    where relop.value('sum(/p:RelOp/@EstimateRowsWithoutRowGoal)', 'float') > 0
                                    )
                                update b
                                set b.is_row_goal = 1
                                from #working_warnings b
                                         join row_goals
                                              on b.query_hash = row_goals.query_hash
                                option (recompile);
                            end;

                    end;


                raiserror (N'Performing query level checks', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.missing_index_count   = query_plan.value(
                        'count(//p:QueryPlan/p:MissingIndexes/p:MissingIndexGroup)', 'int'),
                    b.unmatched_index_count = case
                                                  when is_trivial <> 1 then query_plan.value(
                                                          'count(//p:QueryPlan/p:UnmatchedIndexes/p:Parameterization/p:Object)',
                                                          'int') end
                from #query_plan qp
                         join #working_warnings as b
                              on b.query_hash = qp.query_hash
                option (recompile);


                raiserror (N'Trace flag checks', 0, 1) with nowait;
                ;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                   , tf_pretty as (
                    select qp.sql_handle
                   , q.n.value('@Value'
                   , 'INT') as trace_flag
                   , q.n.value('@Scope'
                   , 'VARCHAR(10)') as scope
                    from #query_plan qp
                    cross apply qp.query_plan.nodes('/p:QueryPlan/p:TraceFlags/p:TraceFlag') as q (n)
                    )
                insert #trace_flags with (tablock)
                    (sql_handle, global_trace_flags, session_trace_flags)
                select distinct tf1.sql_handle,
                                STUFF((
                                          select distinct N', ' + CONVERT(nvarchar(5), tf2.trace_flag)
                                          from tf_pretty as tf2
                                          where tf1.sql_handle = tf2.sql_handle
                                            and tf2.scope = 'Global'
                                          for xml path(N'')), 1, 2, N''
                                    ) as global_trace_flags,
                                STUFF((
                                          select distinct N', ' + CONVERT(nvarchar(5), tf2.trace_flag)
                                          from tf_pretty as tf2
                                          where tf1.sql_handle = tf2.sql_handle
                                            and tf2.scope = 'Session'
                                          for xml path(N'')), 1, 2, N''
                                    ) as session_trace_flags
                from tf_pretty as tf1
                option (recompile);

                update b
                set b.trace_flags_session = tf.session_trace_flags
                from #working_warnings as b
                         join #trace_flags tf
                              on tf.sql_handle = b.sql_handle
                option (recompile);


                raiserror (N'Checking for MSTVFs', 0, 1) with nowait;
                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.is_mstvf = 1
                from #relop as r
                         join #working_warnings as b
                              on b.sql_handle = r.sql_handle
                where r.relop.exist(
                              '/p:RelOp[(@EstimateRows="100" or @EstimateRows="1") and @LogicalOp="Table-valued function"]') =
                      1
                option (recompile);

                if @expertmode > 0
                    begin
                        raiserror (N'Checking for many to many merge joins', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        update b
                        set b.is_mm_join = 1
                        from #relop as r
                                 join #working_warnings as b
                                      on b.sql_handle = r.sql_handle
                        where r.relop.exist('/p:RelOp/p:Merge/@ManyToMany[.="1"]') = 1
                        option (recompile);
                    end;


                if @expertmode > 0
                    begin
                        raiserror (N'Is Paul White Electric?', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p),
                            is_paul_white_electric as (
                            select 1 as [is_paul_white_electric],
                            r.sql_handle
                            from #relop as r
                            cross apply r.relop.nodes('//p:RelOp') c(n)
                            where c.n.exist('@PhysicalOp[.="Switch"]') = 1
                            )
                        update b
                        set b.is_paul_white_electric = ipwe.is_paul_white_electric
                        from #working_warnings as b
                                 join is_paul_white_electric ipwe
                                      on ipwe.sql_handle = b.sql_handle
                        option (recompile);
                    end;


                raiserror (N'Checking for non-sargable predicates', 0, 1) with nowait;
                with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                   , nsarg
                    as ( select r.query_hash
                   , 1 as fn
                   , 0 as jo
                   , 0 as lk
                    from #relop as r
                    cross apply r.relop.nodes('/p:RelOp/p:IndexScan/p:Predicate/p:ScalarOperator/p:Compare/p:ScalarOperator') as ca(x)
                    where ( ca.x.exist('//p:ScalarOperator/p:Intrinsic/@FunctionName') = 1
                    or ca.x.exist('//p:ScalarOperator/p:IF') = 1 )
                    union all
                    select r.query_hash
                   , 0 as fn
                   , 1 as jo
                   , 0 as lk
                    from #relop as r
                    cross apply r.relop.nodes('/p:RelOp//p:ScalarOperator') as ca(x)
                    where r.relop.exist('/p:RelOp[contains(@LogicalOp, "Join")]') = 1
                    and ca.x.exist('//p:ScalarOperator[contains(@ScalarString, "Expr")]') = 1
                    union all
                    select r.query_hash
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
                    nsarg.query_hash
                    from nsarg
                    where nsarg.fn = 1
                    or nsarg.jo = 1
                    or nsarg.lk = 1 )
                update b
                set b.is_nonsargable = 1
                from d_nsarg as d
                         join #working_warnings as b
                              on b.query_hash = d.query_hash
                option ( recompile );


                raiserror (N'Getting information about implicit conversions and stored proc parameters', 0, 1) with nowait;

                raiserror (N'Getting variable info', 0, 1) with nowait;
                with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                insert
                #variable_info
                (
                query_hash
                ,
                sql_handle
                ,
                proc_name
                ,
                variable_name
                ,
                variable_datatype
                ,
                compile_time_value
                )
                select distinct qp.query_hash,
                                qp.sql_handle,
                                b.proc_or_function_name                               as proc_name,
                                q.n.value('@Column', 'NVARCHAR(258)')                 as variable_name,
                                q.n.value('@ParameterDataType', 'NVARCHAR(258)')      as variable_datatype,
                                q.n.value('@ParameterCompiledValue', 'NVARCHAR(258)') as compile_time_value
                from #query_plan as qp
                         join #working_warnings as b
                              on (b.query_hash = qp.query_hash and b.proc_or_function_name = 'adhoc')
                                  or (b.sql_handle = qp.sql_handle and b.proc_or_function_name <> 'adhoc')
                         cross apply qp.query_plan.nodes('//p:QueryPlan/p:ParameterList/p:ColumnReference') as q(n)
                option (recompile);

                raiserror (N'Getting conversion info', 0, 1) with nowait;
                with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                insert
                #conversion_info
                (
                query_hash
                ,
                sql_handle
                ,
                proc_name
                ,
                expression
                )
                select distinct qp.query_hash,
                                qp.sql_handle,
                                b.proc_or_function_name                     as proc_name,
                                qq.c.value('@Expression', 'NVARCHAR(4000)') as expression
                from #query_plan as qp
                         join #working_warnings as b
                              on (b.query_hash = qp.query_hash and b.proc_or_function_name = 'adhoc')
                                  or (b.sql_handle = qp.sql_handle and b.proc_or_function_name <> 'adhoc')
                         cross apply qp.query_plan.nodes('//p:QueryPlan/p:Warnings/p:PlanAffectingConvert') as qq(c)
                where qq.c.exist('@ConvertIssue[.="Seek Plan"]') = 1
                  and b.implicit_conversions = 1
                option (recompile);

                raiserror (N'Parsing conversion info', 0, 1) with nowait;
                insert #stored_proc_info (sql_handle, query_hash, proc_name, variable_name, variable_datatype,
                                          converted_column_name, column_name, converted_to, compile_time_value)
                select ci.sql_handle,
                       ci.query_hash,
                       ci.proc_name,
                       case
                           when ci.at_charindex > 0
                               and ci.bracket_charindex > 0
                               then SUBSTRING(ci.expression, ci.at_charindex, ci.bracket_charindex)
                           else N'**no_variable**'
                           end            as variable_name,
                       N'**no_variable**' as variable_datatype,
                       case
                           when ci.at_charindex = 0
                               and ci.comma_charindex > 0
                               and ci.second_comma_charindex > 0
                               then SUBSTRING(ci.expression, ci.comma_charindex, ci.second_comma_charindex)
                           else N'**no_column**'
                           end            as converted_column_name,
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
                           end            as column_name,
                       case
                           when ci.paren_charindex > 0
                               and ci.comma_paren_charindex > 0
                               then SUBSTRING(ci.expression, ci.paren_charindex, ci.comma_paren_charindex)
                           end            as converted_to,
                       case
                           when ci.at_charindex = 0
                               and ci.convert_implicit_charindex = 0
                               and ci.proc_name = 'Statement'
                               then SUBSTRING(ci.expression, ci.equal_charindex, 4000)
                           else '**idk_man**'
                           end            as compile_time_value
                from #conversion_info as ci
                option (recompile);

                raiserror (N'Updating variables inserted procs', 0, 1) with nowait;
                update sp
                set sp.variable_datatype  = vi.variable_datatype,
                    sp.compile_time_value = vi.compile_time_value
                from #stored_proc_info as sp
                         join #variable_info as vi
                              on (sp.proc_name = 'adhoc' and sp.query_hash = vi.query_hash)
                                  or (sp.proc_name <> 'adhoc' and sp.sql_handle = vi.sql_handle)
                                     and sp.variable_name = vi.variable_name
                option (recompile);


                raiserror (N'Inserting variables for other procs', 0, 1) with nowait;
                insert #stored_proc_info
                (sql_handle, query_hash, variable_name, variable_datatype, compile_time_value, proc_name)
                select vi.sql_handle,
                       vi.query_hash,
                       vi.variable_name,
                       vi.variable_datatype,
                       vi.compile_time_value,
                       vi.proc_name
                from #variable_info as vi
                where not EXISTS
                    (
                        select *
                        from #stored_proc_info as sp
                        where (sp.proc_name = 'adhoc' and sp.query_hash = vi.query_hash)
                           or (sp.proc_name <> 'adhoc' and sp.sql_handle = vi.sql_handle)
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
                    select x.sql_handle,
                           N'SET ANSI_NULLS ' + case when [ANSI_NULLS] = 'true' then N'ON ' else N'OFF ' end +
                           NCHAR(10) +
                           N'SET ANSI_PADDING ' + case when [ANSI_PADDING] = 'true' then N'ON ' else N'OFF ' end +
                           NCHAR(10) +
                           N'SET ANSI_WARNINGS ' + case when [ANSI_WARNINGS] = 'true' then N'ON ' else N'OFF ' end +
                           NCHAR(10) +
                           N'SET ARITHABORT ' + case when [ARITHABORT] = 'true' then N'ON ' else N' OFF ' end +
                           NCHAR(10) +
                           N'SET CONCAT_NULL_YIELDS_NULL ' +
                           case when [CONCAT_NULL_YIELDS_NULL] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
                           N'SET NUMERIC_ROUNDABORT ' +
                           case when [NUMERIC_ROUNDABORT] = 'true' then N'ON ' else N'OFF ' end + NCHAR(10) +
                           N'SET QUOTED_IDENTIFIER ' + case
                                                           when [QUOTED_IDENTIFIER] = 'true' then N'ON '
                                                           else N'OFF ' + NCHAR(10) end as [ansi_set_options]
                    from (
                             select s.sql_handle,
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
                ) as set_options on set_options.sql_handle = s.sql_handle
                option (recompile);


                raiserror (N'Updating conversion XML', 0, 1) with nowait;
                with precheck as (
                    select spi.sql_handle,
                           spi.proc_name,
                           (select case
                                       when spi.proc_name <> 'Statement'
                                           then N'The stored procedure ' + spi.proc_name
                                       else N'This ad hoc statement'
                                       end
                                       + N' had the following implicit conversions: '
                                       + CHAR(10)
                                       + STUFF((
                                                   select distinct @cr + @lf
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
                                                                       +
                                                                   N' which caused implicit conversion on the column '
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
                                                                                 and
                                                                                  spi2.compile_time_value <> spi2.column_name
                                                                                 then ' with the value ' + RTRIM(spi2.compile_time_value)
                                                                             else N''
                                                                       end
                                                                       + '.'
                                                   from #stored_proc_info as spi2
                                                   where spi.sql_handle = spi2.sql_handle
                                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                                       as [processing-instruction(ClickMe)]
                            for xml path(''), type)
                               as implicit_conversion_info
                    from #stored_proc_info as spi
                    group by spi.sql_handle, spi.proc_name
                )
                update b
                set b.implicit_conversion_info = pk.implicit_conversion_info
                from #working_warnings as b
                         join precheck as pk
                              on pk.sql_handle = b.sql_handle
                option (recompile);

                raiserror (N'Updating cached parameter XML for procs', 0, 1) with nowait;
                with precheck as (
                    select spi.sql_handle,
                           spi.proc_name,
                           (select set_options
                                       + @cr + @lf
                                       + @cr + @lf
                                       + N'EXEC '
                                       + spi.proc_name
                                       + N' '
                                       + STUFF((
                                                   select distinct N', '
                                                                       + case
                                                                             when spi2.variable_name <> N'**no_variable**' and
                                                                                  spi2.compile_time_value <> N'**idk_man**'
                                                                                 then spi2.variable_name + N' = '
                                                                             else @cr + @lf +
                                                                                  N' We could not find any cached parameter values for this stored proc. '
                                                                       end
                                                                       + case
                                                                             when spi2.variable_name = N'**no_variable**' or
                                                                                  spi2.compile_time_value = N'**idk_man**'
                                                                                 then @cr + @lf +
                                                                                      N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
                                                                             when spi2.compile_time_value = N'NULL'
                                                                                 then spi2.compile_time_value
                                                                             else RTRIM(spi2.compile_time_value)
                                                                       end
                                                   from #stored_proc_info as spi2
                                                   where spi.sql_handle = spi2.sql_handle
                                                     and spi2.proc_name <> N'Statement'
                                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                                       as [processing-instruction(ClickMe)]
                            for xml path(''), type)
                               as cached_execution_parameters
                    from #stored_proc_info as spi
                    group by spi.sql_handle, spi.proc_name, set_options
                )
                update b
                set b.cached_execution_parameters = pk.cached_execution_parameters
                from #working_warnings as b
                         join precheck as pk
                              on pk.sql_handle = b.sql_handle
                where b.proc_or_function_name <> N'Statement'
                option (recompile);


                raiserror (N'Updating cached parameter XML for statements', 0, 1) with nowait;
                with precheck as (
                    select spi.sql_handle,
                           spi.proc_name,
                           (select set_options
                                       + @cr + @lf
                                       + @cr + @lf
                                       + N' See QueryText column for full query text'
                                       + @cr + @lf
                                       + @cr + @lf
                                       + STUFF((
                                                   select distinct N', '
                                                                       + case
                                                                             when spi2.variable_name <> N'**no_variable**' and
                                                                                  spi2.compile_time_value <> N'**idk_man**'
                                                                                 then spi2.variable_name + N' = '
                                                                             else + @cr + @lf +
                                                                                  N' We could not find any cached parameter values for this stored proc. '
                                                                       end
                                                                       + case
                                                                             when spi2.variable_name = N'**no_variable**' or
                                                                                  spi2.compile_time_value = N'**idk_man**'
                                                                                 then + @cr + @lf +
                                                                                      N' Possible reasons include declared variables inside the procedure, recompile hints, etc. '
                                                                             when spi2.compile_time_value = N'NULL'
                                                                                 then spi2.compile_time_value
                                                                             else RTRIM(spi2.compile_time_value)
                                                                       end
                                                   from #stored_proc_info as spi2
                                                   where spi.sql_handle = spi2.sql_handle
                                                     and spi2.proc_name = N'Statement'
                                                     and spi2.variable_name not like N'%msparam%'
                                                   for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                                       as [processing-instruction(ClickMe)]
                            for xml path(''), type)
                               as cached_execution_parameters
                    from #stored_proc_info as spi
                    group by spi.sql_handle, spi.proc_name, spi.set_options
                )
                update b
                set b.cached_execution_parameters = pk.cached_execution_parameters
                from #working_warnings as b
                         join precheck as pk
                              on pk.sql_handle = b.sql_handle
                where b.proc_or_function_name = N'Statement'
                option (recompile);


                raiserror (N'Filling in implicit conversion info', 0, 1) with nowait;
                update b
                set b.implicit_conversion_info    = case
                                                        when b.implicit_conversion_info is null
                                                            or CONVERT(nvarchar(max), b.implicit_conversion_info) = N''
                                                            then N'<?NoNeedToClickMe -- N/A --?>'
                                                        else b.implicit_conversion_info
                    end,
                    b.cached_execution_parameters = case
                                                        when b.cached_execution_parameters is null
                                                            or
                                                             CONVERT(nvarchar(max), b.cached_execution_parameters) = N''
                                                            then N'<?NoNeedToClickMe -- N/A --?>'
                                                        else b.cached_execution_parameters
                        end
                from #working_warnings as b
                option (recompile);

                /*End implicit conversion and parameter info*/

/*Begin Missing Index*/
                if EXISTS(select 1 / 0
                          from #working_warnings as ww
                          where ww.missing_index_count > 0
                             or ww.index_spool_cost > 0
                             or ww.index_spool_rows > 0)
                    begin

                        raiserror (N'Inserting to #missing_index_xml', 0, 1) with nowait;
                        with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                        insert
                        #missing_index_xml
                        select qp.query_hash,
                               qp.sql_handle,
                               c.mg.value('@Impact', 'FLOAT') as impact,
                               c.mg.query('.')                as cmg
                        from #query_plan as qp
                                 cross apply qp.query_plan.nodes('//p:MissingIndexes/p:MissingIndexGroup') as c(mg)
                        where qp.query_hash is not null
                          and c.mg.value('@Impact', 'FLOAT') > 70.0
                        option (recompile);

                        raiserror (N'Inserting to #missing_index_schema', 0, 1) with nowait;
                        with xmlnamespaces ( 'http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p )
                        insert
                        #missing_index_schema
                        select mix.query_hash,
                               mix.sql_handle,
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
                        select ms.query_hash,
                               ms.sql_handle,
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
                        select miu.query_hash,
                               miu.sql_handle,
                               miu.impact,
                               miu.database_name,
                               miu.schema_name,
                               miu.table_name,
                               miu.usage,
                               c.c.value('@Name', 'NVARCHAR(128)')
                        from #missing_index_usage as miu
                                 cross apply miu.index_xml.nodes('//p:Column') as c(c)
                        option (recompile);

                        raiserror (N'Inserting to #missing_index_pretty', 0, 1) with nowait;
                        insert #missing_index_pretty
                        select distinct m.query_hash
                                      , m.sql_handle
                                      , m.impact
                                      , m.database_name
                                      , m.schema_name
                                      , m.table_name
                                      , STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                               from #missing_index_detail as m2
                                               where m2.usage = 'EQUALITY'
                                                 and m.query_hash = m2.query_hash
                                                 and m.sql_handle = m2.sql_handle
                                                 and m.impact = m2.impact
                                                 and m.database_name = m2.database_name
                                                 and m.schema_name = m2.schema_name
                                                 and m.table_name = m2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as equality
                                      , STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                               from #missing_index_detail as m2
                                               where m2.usage = 'INEQUALITY'
                                                 and m.query_hash = m2.query_hash
                                                 and m.sql_handle = m2.sql_handle
                                                 and m.impact = m2.impact
                                                 and m.database_name = m2.database_name
                                                 and m.schema_name = m2.schema_name
                                                 and m.table_name = m2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as inequality
                                      , STUFF((select distinct N', ' + ISNULL(m2.column_name, '') as column_name
                                               from #missing_index_detail as m2
                                               where m2.usage = 'INCLUDE'
                                                 and m.query_hash = m2.query_hash
                                                 and m.sql_handle = m2.sql_handle
                                                 and m.impact = m2.impact
                                                 and m.database_name = m2.database_name
                                                 and m.schema_name = m2.schema_name
                                                 and m.table_name = m2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as [include]
                                      , 0                                                                            as is_spool
                        from #missing_index_detail as m
                        group by m.query_hash, m.sql_handle, m.impact, m.database_name, m.schema_name, m.table_name
                        option (recompile);

                        raiserror (N'Inserting to #index_spool_ugly', 0, 1) with nowait;
                        with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                        insert
                        #index_spool_ugly
                        (
                        query_hash
                        ,
                        sql_handle
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
                        )
                        select r.query_hash,
                               r.sql_handle,
                               (c.n.value('@EstimateIO', 'FLOAT') + (c.n.value('@EstimateCPU', 'FLOAT')))
                                   / (1 * NULLIF(ww.query_cost, 0)) * 100 as impact,
                               o.n.value('@Database', 'NVARCHAR(128)')    as output_database,
                               o.n.value('@Schema', 'NVARCHAR(128)')      as output_schema,
                               o.n.value('@Table', 'NVARCHAR(128)')       as output_table,
                               k.n.value('@Column', 'NVARCHAR(128)')      as range_column,
                               e.n.value('@Column', 'NVARCHAR(128)')      as expression_column,
                               o.n.value('@Column', 'NVARCHAR(128)')      as output_column
                        from #relop as r
                                 join #working_warnings as ww
                                      on ww.query_hash = r.query_hash
                                 cross apply r.relop.nodes('/p:RelOp') as c(n)
                                 cross apply r.relop.nodes('/p:RelOp/p:OutputList/p:ColumnReference') as o(n)
                                 outer apply r.relop.nodes(
                                '/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeColumns/p:ColumnReference') as k(n)
                                 outer apply r.relop.nodes(
                                '/p:RelOp/p:Spool/p:SeekPredicateNew/p:SeekKeys/p:Prefix/p:RangeExpressions/p:ColumnReference') as e(n)
                        where r.relop.exist('/p:RelOp[@PhysicalOp="Index Spool" and @LogicalOp="Eager Spool"]') = 1

                        raiserror (N'Inserting to spools to #missing_index_pretty', 0, 1) with nowait;
                        insert #missing_index_pretty
                        (query_hash, sql_handle, impact, database_name, schema_name, table_name, equality, inequality,
                         include, is_spool)
                        select distinct isu.query_hash,
                                        isu.sql_handle,
                                        isu.impact,
                                        isu.database_name,
                                        isu.schema_name,
                                        isu.table_name
                                ,
                                        STUFF((select distinct N', ' + ISNULL(isu2.equality, '') as column_name
                                               from #index_spool_ugly as isu2
                                               where isu2.equality is not null
                                                 and isu.query_hash = isu2.query_hash
                                                 and isu.sql_handle = isu2.sql_handle
                                                 and isu.impact = isu2.impact
                                                 and isu.database_name = isu2.database_name
                                                 and isu.schema_name = isu2.schema_name
                                                 and isu.table_name = isu2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as equality
                                ,
                                        STUFF((select distinct N', ' + ISNULL(isu2.inequality, '') as column_name
                                               from #index_spool_ugly as isu2
                                               where isu2.inequality is not null
                                                 and isu.query_hash = isu2.query_hash
                                                 and isu.sql_handle = isu2.sql_handle
                                                 and isu.impact = isu2.impact
                                                 and isu.database_name = isu2.database_name
                                                 and isu.schema_name = isu2.schema_name
                                                 and isu.table_name = isu2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as inequality
                                ,
                                        STUFF((select distinct N', ' + ISNULL(isu2.include, '') as column_name
                                               from #index_spool_ugly as isu2
                                               where isu2.include is not null
                                                 and isu.query_hash = isu2.query_hash
                                                 and isu.sql_handle = isu2.sql_handle
                                                 and isu.impact = isu2.impact
                                                 and isu.database_name = isu2.database_name
                                                 and isu.schema_name = isu2.schema_name
                                                 and isu.table_name = isu2.table_name
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                              N'')                                                                   as include,
                                        1                                                                            as is_spool
                        from #index_spool_ugly as isu


                        raiserror (N'Updating missing index information', 0, 1) with nowait;
                        with missing as (
                            select distinct mip.query_hash,
                                            mip.sql_handle,
                                            N'<MissingIndexes><![CDATA['
                                                + CHAR(10) + CHAR(13)
                                                + STUFF(
                                                    (select CHAR(10) + CHAR(13) + ISNULL(mip2.details, '') as details
                                                     from #missing_index_pretty as mip2
                                                     where mip.query_hash = mip2.query_hash
                                                       and mip.sql_handle = mip2.sql_handle
                                                     group by mip2.details
                                                     order by MAX(mip2.impact) desc
                                                     for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 2,
                                                    N'')
                                                + CHAR(10) + CHAR(13)
                                                + N']]></MissingIndexes>'
                                                as full_details
                            from #missing_index_pretty as mip
                            group by mip.query_hash, mip.sql_handle, mip.impact
                        )
                        update ww
                        set ww.missing_indexes = m.full_details
                        from #working_warnings as ww
                                 join missing as m
                                      on m.sql_handle = ww.sql_handle
                        option (recompile);

                        raiserror (N'Filling in missing index blanks', 0, 1) with nowait;
                        update ww
                        set ww.missing_indexes =
                                case
                                    when ww.missing_indexes is null
                                        then '<?NoNeedToClickMe -- N/A --?>'
                                    else ww.missing_indexes
                                    end
                        from #working_warnings as ww
                        option (recompile);

                    end
/*End Missing Index*/

                raiserror (N'General query dispositions: frequent executions, long running, etc.', 0, 1) with nowait;

                with xmlnamespaces ('http://schemas.microsoft.com/sqlserver/2004/07/showplan' as p)
                update b
                set b.frequent_execution        = case when wm.xpm > @execution_threshold then 1 end,
                    b.near_parallel             = case
                                                      when b.query_cost between @ctp * (1 - (@ctp_threshold_pct / 100.0)) and @ctp
                                                          then 1 end,
                    b.long_running              = case
                                                      when wm.avg_duration > @long_running_query_warning_seconds then 1
                                                      when wm.max_duration > @long_running_query_warning_seconds then 1
                                                      when wm.avg_cpu_time > @long_running_query_warning_seconds then 1
                                                      when wm.max_cpu_time > @long_running_query_warning_seconds
                                                          then 1 end,
                    b.is_key_lookup_expensive   = case
                                                      when b.query_cost >= (@ctp / 2) and b.key_lookup_cost >= b.query_cost * .5
                                                          then 1 end,
                    b.is_sort_expensive         = case
                                                      when b.query_cost >= (@ctp / 2) and b.sort_cost >= b.query_cost * .5
                                                          then 1 end,
                    b.is_remote_query_expensive = case when b.remote_query_cost >= b.query_cost * .05 then 1 end,
                    b.is_unused_grant           = case
                                                      when percent_memory_grant_used <= @memory_grant_warning_percent and
                                                           min_query_max_used_memory > @min_memory_per_query then 1 end,
                    b.long_running_low_cpu      = case
                                                      when wm.avg_duration > wm.avg_cpu_time * 4 and avg_cpu_time < 500.
                                                          then 1 end,
                    b.low_cost_high_cpu         = case when b.query_cost < 10 and wm.avg_cpu_time > 5000. then 1 end,
                    b.is_spool_expensive        = case
                                                      when b.query_cost > (@ctp / 2) and b.index_spool_cost >= b.query_cost * .1
                                                          then 1 end,
                    b.is_spool_more_rows        = case when b.index_spool_rows >= wm.min_rowcount then 1 end,
                    b.is_bad_estimate           = case
                                                      when wm.avg_rowcount > 0 and
                                                           (b.estimated_rows * 1000 < wm.avg_rowcount or
                                                            b.estimated_rows > wm.avg_rowcount * 1000) then 1 end,
                    b.is_big_log                = case when wm.avg_log_bytes_used >= (@log_size_mb / 2.) then 1 end,
                    b.is_big_tempdb             = case
                                                      when wm.avg_tempdb_space_used >= (@avg_tempdb_data_file / 2.)
                                                          then 1 end
                from #working_warnings as b
                         join #working_metrics as wm
                              on b.plan_id = wm.plan_id
                                  and b.query_id = wm.query_id
                         join #working_plan_text as wpt
                              on b.plan_id = wpt.plan_id
                                  and b.query_id = wpt.query_id
                option (recompile);


                raiserror ('Populating Warnings column', 0, 1) with nowait;
/* Populate warnings */
                update b
                set b.warnings = SUBSTRING(
                            case when b.warning_no_join_predicate = 1 then ', No Join Predicate' else '' end +
                            case when b.compile_timeout = 1 then ', Compilation Timeout' else '' end +
                            case
                                when b.compile_memory_limit_exceeded = 1 then ', Compile Memory Limit Exceeded'
                                else '' end +
                            case when b.is_forced_plan = 1 then ', Forced Plan' else '' end +
                            case when b.is_forced_parameterized = 1 then ', Forced Parameterization' else '' end +
                            case when b.unparameterized_query = 1 then ', Unparameterized Query' else '' end +
                            case
                                when b.missing_index_count > 0 then ', Missing Indexes (' +
                                                                    CAST(b.missing_index_count as nvarchar(3)) + ')'
                                else '' end +
                            case
                                when b.unmatched_index_count > 0 then ', Unmatched Indexes (' +
                                                                      CAST(b.unmatched_index_count as nvarchar(3)) + ')'
                                else '' end +
                            case
                                when b.is_cursor = 1 then ', Cursor'
                                    + case when b.is_optimistic_cursor = 1 then '; optimistic' else '' end
                                    + case when b.is_forward_only_cursor = 0 then '; not forward only' else '' end
                                    + case when b.is_cursor_dynamic = 1 then '; dynamic' else '' end
                                    + case when b.is_fast_forward_cursor = 1 then '; fast forward' else '' end
                                else '' end +
                            case when b.is_parallel = 1 then ', Parallel' else '' end +
                            case when b.near_parallel = 1 then ', Nearly Parallel' else '' end +
                            case when b.frequent_execution = 1 then ', Frequent Execution' else '' end +
                            case when b.plan_warnings = 1 then ', Plan Warnings' else '' end +
                            case when b.parameter_sniffing = 1 then ', Parameter Sniffing' else '' end +
                            case when b.long_running = 1 then ', Long Running Query' else '' end +
                            case when b.downlevel_estimator = 1 then ', Downlevel CE' else '' end +
                            case when b.implicit_conversions = 1 then ', Implicit Conversions' else '' end +
                            case when b.plan_multiple_plans = 1 then ', Multiple Plans' else '' end +
                            case when b.is_trivial = 1 then ', Trivial Plans' else '' end +
                            case when b.is_forced_serial = 1 then ', Forced Serialization' else '' end +
                            case when b.is_key_lookup_expensive = 1 then ', Expensive Key Lookup' else '' end +
                            case when b.is_remote_query_expensive = 1 then ', Expensive Remote Query' else '' end +
                            case
                                when b.trace_flags_session is not null
                                    then ', Session Level Trace Flag(s) Enabled: ' + b.trace_flags_session
                                else '' end +
                            case when b.is_unused_grant = 1 then ', Unused Memory Grant' else '' end +
                            case
                                when b.function_count > 0
                                    then ', Calls ' + CONVERT(varchar(10), b.function_count) + ' function(s)'
                                else '' end +
                            case
                                when b.clr_function_count > 0 then ', Calls ' +
                                                                   CONVERT(varchar(10), b.clr_function_count) +
                                                                   ' CLR function(s)'
                                else '' end +
                            case when b.is_table_variable = 1 then ', Table Variables' else '' end +
                            case when b.no_stats_warning = 1 then ', Columns With No Statistics' else '' end +
                            case when b.relop_warnings = 1 then ', Operator Warnings' else '' end +
                            case when b.is_table_scan = 1 then ', Table Scans' else '' end +
                            case when b.backwards_scan = 1 then ', Backwards Scans' else '' end +
                            case when b.forced_index = 1 then ', Forced Indexes' else '' end +
                            case when b.forced_seek = 1 then ', Forced Seeks' else '' end +
                            case when b.forced_scan = 1 then ', Forced Scans' else '' end +
                            case when b.columnstore_row_mode = 1 then ', ColumnStore Row Mode ' else '' end +
                            case when b.is_computed_scalar = 1 then ', Computed Column UDF ' else '' end +
                            case when b.is_sort_expensive = 1 then ', Expensive Sort' else '' end +
                            case when b.is_computed_filter = 1 then ', Filter UDF' else '' end +
                            case when b.index_ops >= 5 then ', >= 5 Indexes Modified' else '' end +
                            case when b.is_row_level = 1 then ', Row Level Security' else '' end +
                            case when b.is_spatial = 1 then ', Spatial Index' else '' end +
                            case when b.index_dml = 1 then ', Index DML' else '' end +
                            case when b.table_dml = 1 then ', Table DML' else '' end +
                            case when b.low_cost_high_cpu = 1 then ', Low Cost High CPU' else '' end +
                            case when b.long_running_low_cpu = 1 then + ', Long Running With Low CPU' else '' end +
                            case
                                when b.stale_stats = 1
                                    then + ', Statistics used have > 100k modifications in the last 7 days'
                                else '' end +
                            case when b.is_adaptive = 1 then + ', Adaptive Joins' else '' end +
                            case when b.is_spool_expensive = 1 then + ', Expensive Index Spool' else '' end +
                            case when b.is_spool_more_rows = 1 then + ', Large Index Row Spool' else '' end +
                            case when b.is_bad_estimate = 1 then + ', Row estimate mismatch' else '' end +
                            case when b.is_big_log = 1 then + ', High log use' else '' end +
                            case when b.is_big_tempdb = 1 then ', High tempdb use' else '' end +
                            case when b.is_paul_white_electric = 1 then ', SWITCH!' else '' end +
                            case when b.is_row_goal = 1 then ', Row Goals' else '' end +
                            case when b.is_mstvf = 1 then ', MSTVFs' else '' end +
                            case when b.is_mm_join = 1 then ', Many to Many Merge' else '' end +
                            case when b.is_nonsargable = 1 then ', non-SARGables' else '' end
                    , 2, 200000)
                from #working_warnings b
                option (recompile);


            end;
        end try
        begin catch
            raiserror (N'Failure generating warnings.', 0,1) with nowait;

            if @sql_select is not null
                begin
                    set @msg = N'Last @sql_select: ' + @sql_select;
                    raiserror (@msg, 0, 1) with nowait;
                end;

            select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
                   @error_severity = ERROR_SEVERITY(),
                   @error_state = ERROR_STATE();
            raiserror (@msg, @error_severity, @error_state) with nowait;


            while @@TRANCOUNT > 0
                rollback;

            return;
        end catch;


    begin try
        begin

            raiserror (N'Checking for parameter sniffing symptoms', 0, 1) with nowait;

            update b
            set b.parameter_sniffing_symptoms =
                    case
                        when b.count_executions < 2 then 'Too few executions to compare (< 2).'
                        else
                            SUBSTRING(
                                /*Duration*/
                                        case
                                            when (b.min_duration * 100) < (b.avg_duration) then ', Fast sometimes'
                                            else '' end +
                                        case
                                            when (b.max_duration) > (b.avg_duration * 100) then ', Slow sometimes'
                                            else '' end +
                                        case
                                            when (b.last_duration * 100) < (b.avg_duration) then ', Fast last run'
                                            else '' end +
                                        case
                                            when (b.last_duration) > (b.avg_duration * 100) then ', Slow last run'
                                            else '' end +
                                        /*CPU*/
                                        case
                                            when (b.min_cpu_time / b.avg_dop) * 100 < (b.avg_cpu_time / b.avg_dop)
                                                then ', Low CPU sometimes'
                                            else '' end +
                                        case
                                            when (b.max_cpu_time / b.max_dop) > (b.avg_cpu_time / b.avg_dop) * 100
                                                then ', High CPU sometimes'
                                            else '' end +
                                        case
                                            when (b.last_cpu_time / b.last_dop) * 100 < (b.avg_cpu_time / b.avg_dop)
                                                then ', Low CPU last run'
                                            else '' end +
                                        case
                                            when (b.last_cpu_time / b.last_dop) > (b.avg_cpu_time / b.avg_dop) * 100
                                                then ', High CPU last run'
                                            else '' end +
                                        /*Logical Reads*/
                                        case
                                            when (b.min_logical_io_reads * 100) < (b.avg_logical_io_reads)
                                                then ', Low reads sometimes'
                                            else '' end +
                                        case
                                            when (b.max_logical_io_reads) > (b.avg_logical_io_reads * 100)
                                                then ', High reads sometimes'
                                            else '' end +
                                        case
                                            when (b.last_logical_io_reads * 100) < (b.avg_logical_io_reads)
                                                then ', Low reads last run'
                                            else '' end +
                                        case
                                            when (b.last_logical_io_reads) > (b.avg_logical_io_reads * 100)
                                                then ', High reads last run'
                                            else '' end +
                                        /*Logical Writes*/
                                        case
                                            when (b.min_logical_io_writes * 100) < (b.avg_logical_io_writes)
                                                then ', Low writes sometimes'
                                            else '' end +
                                        case
                                            when (b.max_logical_io_writes) > (b.avg_logical_io_writes * 100)
                                                then ', High writes sometimes'
                                            else '' end +
                                        case
                                            when (b.last_logical_io_writes * 100) < (b.avg_logical_io_writes)
                                                then ', Low writes last run'
                                            else '' end +
                                        case
                                            when (b.last_logical_io_writes) > (b.avg_logical_io_writes * 100)
                                                then ', High writes last run'
                                            else '' end +
                                        /*Physical Reads*/
                                        case
                                            when (b.min_physical_io_reads * 100) < (b.avg_physical_io_reads)
                                                then ', Low physical reads sometimes'
                                            else '' end +
                                        case
                                            when (b.max_physical_io_reads) > (b.avg_physical_io_reads * 100)
                                                then ', High physical reads sometimes'
                                            else '' end +
                                        case
                                            when (b.last_physical_io_reads * 100) < (b.avg_physical_io_reads)
                                                then ', Low physical reads last run'
                                            else '' end +
                                        case
                                            when (b.last_physical_io_reads) > (b.avg_physical_io_reads * 100)
                                                then ', High physical reads last run'
                                            else '' end +
                                        /*Memory*/
                                        case
                                            when (b.min_query_max_used_memory * 100) < (b.avg_query_max_used_memory)
                                                then ', Low memory sometimes'
                                            else '' end +
                                        case
                                            when (b.max_query_max_used_memory) > (b.avg_query_max_used_memory * 100)
                                                then ', High memory sometimes'
                                            else '' end +
                                        case
                                            when (b.last_query_max_used_memory * 100) < (b.avg_query_max_used_memory)
                                                then ', Low memory last run'
                                            else '' end +
                                        case
                                            when (b.last_query_max_used_memory) > (b.avg_query_max_used_memory * 100)
                                                then ', High memory last run'
                                            else '' end +
                                        /*Duration*/
                                        case
                                            when b.min_rowcount * 100 < b.avg_rowcount then ', Low row count sometimes'
                                            else '' end +
                                        case
                                            when b.max_rowcount > b.avg_rowcount * 100 then ', High row count sometimes'
                                            else '' end +
                                        case
                                            when b.last_rowcount * 100 < b.avg_rowcount then ', Low row count run'
                                            else '' end +
                                        case
                                            when b.last_rowcount > b.avg_rowcount * 100 then ', High row count last run'
                                            else '' end +
                                        /*DOP*/
                                        case when b.min_dop <> b.max_dop then ', Serial sometimes' else '' end +
                                        case
                                            when b.min_dop <> b.max_dop and b.last_dop = 1 then ', Serial last run'
                                            else '' end +
                                        case
                                            when b.min_dop <> b.max_dop and b.last_dop > 1 then ', Parallel last run'
                                            else '' end +
                                        /*tempdb*/
                                        case
                                            when b.min_tempdb_space_used * 100 < b.avg_tempdb_space_used
                                                then ', Low tempdb sometimes'
                                            else '' end +
                                        case
                                            when b.max_tempdb_space_used > b.avg_tempdb_space_used * 100
                                                then ', High tempdb sometimes'
                                            else '' end +
                                        case
                                            when b.last_tempdb_space_used * 100 < b.avg_tempdb_space_used
                                                then ', Low tempdb run'
                                            else '' end +
                                        case
                                            when b.last_tempdb_space_used > b.avg_tempdb_space_used * 100
                                                then ', High tempdb last run'
                                            else '' end +
                                        /*tlog*/
                                        case
                                            when b.min_log_bytes_used * 100 < b.avg_log_bytes_used
                                                then ', Low log use sometimes'
                                            else '' end +
                                        case
                                            when b.max_log_bytes_used > b.avg_log_bytes_used * 100
                                                then ', High log use sometimes'
                                            else '' end +
                                        case
                                            when b.last_log_bytes_used * 100 < b.avg_log_bytes_used
                                                then ', Low log use run'
                                            else '' end +
                                        case
                                            when b.last_log_bytes_used > b.avg_log_bytes_used * 100
                                                then ', High log use last run'
                                            else '' end
                                , 2, 200000)
                        end
            from #working_metrics as b
            option (recompile);

        end;
    end try
    begin catch
        raiserror (N'Failure analyzing parameter sniffing', 0,1) with nowait;

        if @sql_select is not null
            begin
                set @msg = N'Last @sql_select: ' + @sql_select;
                raiserror (@msg, 0, 1) with nowait;
            end;

        select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
               @error_severity = ERROR_SEVERITY(),
               @error_state = ERROR_STATE();
        raiserror (@msg, @error_severity, @error_state) with nowait;


        while @@TRANCOUNT > 0
            rollback;

        return;
    end catch;

    begin try

        begin

            if (@failed = 0 and @exporttoexcel = 0 and @skipxml = 0)
                begin

                    raiserror (N'Returning regular results', 0, 1) with nowait;

                    with x as (
                        select wpt.database_name,
                               ww.query_cost,
                               wm.plan_id,
                               wm.query_id,
                               wm.query_id_all_plan_ids,
                               wpt.query_sql_text,
                               wm.proc_or_function_name,
                               wpt.query_plan_xml,
                               ww.warnings,
                               wpt.pattern,
                               wm.parameter_sniffing_symptoms,
                               wpt.top_three_waits,
                               ww.missing_indexes,
                               ww.implicit_conversion_info,
                               ww.cached_execution_parameters,
                               wm.count_executions,
                               wm.count_compiles,
                               wm.total_cpu_time,
                               wm.avg_cpu_time,
                               wm.total_duration,
                               wm.avg_duration,
                               wm.total_logical_io_reads,
                               wm.avg_logical_io_reads,
                               wm.total_physical_io_reads,
                               wm.avg_physical_io_reads,
                               wm.total_logical_io_writes,
                               wm.avg_logical_io_writes,
                               wm.total_rowcount,
                               wm.avg_rowcount,
                               wm.total_query_max_used_memory,
                               wm.avg_query_max_used_memory,
                               wm.total_tempdb_space_used,
                               wm.avg_tempdb_space_used,
                               wm.total_log_bytes_used,
                               wm.avg_log_bytes_used,
                               wm.total_num_physical_io_reads,
                               wm.avg_num_physical_io_reads,
                               wm.first_execution_time,
                               wm.last_execution_time,
                               wpt.last_force_failure_reason_desc,
                               wpt.context_settings,
                               ROW_NUMBER()
                                       over (partition by wm.plan_id, wm.query_id, wm.last_execution_time order by wm.plan_id) as rn
                        from #working_plan_text as wpt
                                 join #working_warnings as ww
                                      on wpt.plan_id = ww.plan_id
                                          and wpt.query_id = ww.query_id
                                 join #working_metrics as wm
                                      on wpt.plan_id = wm.plan_id
                                          and wpt.query_id = wm.query_id
                    )
                    select *
                    from x
                    where x.rn = 1
                    order by x.last_execution_time
                    option (recompile);

                end;

            if (@failed = 1 and @exporttoexcel = 0 and @skipxml = 0)
                begin

                    raiserror (N'Returning results for failed queries', 0, 1) with nowait;

                    with x as (
                        select wpt.database_name,
                               ww.query_cost,
                               wm.plan_id,
                               wm.query_id,
                               wm.query_id_all_plan_ids,
                               wpt.query_sql_text,
                               wm.proc_or_function_name,
                               wpt.query_plan_xml,
                               ww.warnings,
                               wpt.pattern,
                               wm.parameter_sniffing_symptoms,
                               wpt.last_force_failure_reason_desc,
                               wpt.top_three_waits,
                               ww.missing_indexes,
                               ww.implicit_conversion_info,
                               ww.cached_execution_parameters,
                               wm.count_executions,
                               wm.count_compiles,
                               wm.total_cpu_time,
                               wm.avg_cpu_time,
                               wm.total_duration,
                               wm.avg_duration,
                               wm.total_logical_io_reads,
                               wm.avg_logical_io_reads,
                               wm.total_physical_io_reads,
                               wm.avg_physical_io_reads,
                               wm.total_logical_io_writes,
                               wm.avg_logical_io_writes,
                               wm.total_rowcount,
                               wm.avg_rowcount,
                               wm.total_query_max_used_memory,
                               wm.avg_query_max_used_memory,
                               wm.total_tempdb_space_used,
                               wm.avg_tempdb_space_used,
                               wm.total_log_bytes_used,
                               wm.avg_log_bytes_used,
                               wm.total_num_physical_io_reads,
                               wm.avg_num_physical_io_reads,
                               wm.first_execution_time,
                               wm.last_execution_time,
                               wpt.context_settings,
                               ROW_NUMBER()
                                       over (partition by wm.plan_id, wm.query_id, wm.last_execution_time order by wm.plan_id) as rn
                        from #working_plan_text as wpt
                                 join #working_warnings as ww
                                      on wpt.plan_id = ww.plan_id
                                          and wpt.query_id = ww.query_id
                                 join #working_metrics as wm
                                      on wpt.plan_id = wm.plan_id
                                          and wpt.query_id = wm.query_id
                    )
                    select *
                    from x
                    where x.rn = 1
                    order by x.last_execution_time
                    option (recompile);

                end;

            if (@exporttoexcel = 1 and @skipxml = 0)
                begin

                    raiserror (N'Returning results for Excel export', 0, 1) with nowait;

                    update #working_plan_text
                    set query_sql_text = SUBSTRING(
                            REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(query_sql_text)), ' ', '<>'), '><', ''), '<>', ' '), 1,
                            31000)
                    option (recompile);

                    with x as (
                        select wpt.database_name,
                               ww.query_cost,
                               wm.plan_id,
                               wm.query_id,
                               wm.query_id_all_plan_ids,
                               wpt.query_sql_text,
                               wm.proc_or_function_name,
                               ww.warnings,
                               wpt.pattern,
                               wm.parameter_sniffing_symptoms,
                               wpt.last_force_failure_reason_desc,
                               wpt.top_three_waits,
                               wm.count_executions,
                               wm.count_compiles,
                               wm.total_cpu_time,
                               wm.avg_cpu_time,
                               wm.total_duration,
                               wm.avg_duration,
                               wm.total_logical_io_reads,
                               wm.avg_logical_io_reads,
                               wm.total_physical_io_reads,
                               wm.avg_physical_io_reads,
                               wm.total_logical_io_writes,
                               wm.avg_logical_io_writes,
                               wm.total_rowcount,
                               wm.avg_rowcount,
                               wm.total_query_max_used_memory,
                               wm.avg_query_max_used_memory,
                               wm.total_tempdb_space_used,
                               wm.avg_tempdb_space_used,
                               wm.total_log_bytes_used,
                               wm.avg_log_bytes_used,
                               wm.total_num_physical_io_reads,
                               wm.avg_num_physical_io_reads,
                               wm.first_execution_time,
                               wm.last_execution_time,
                               wpt.context_settings,
                               ROW_NUMBER()
                                       over (partition by wm.plan_id, wm.query_id, wm.last_execution_time order by wm.plan_id) as rn
                        from #working_plan_text as wpt
                                 join #working_warnings as ww
                                      on wpt.plan_id = ww.plan_id
                                          and wpt.query_id = ww.query_id
                                 join #working_metrics as wm
                                      on wpt.plan_id = wm.plan_id
                                          and wpt.query_id = wm.query_id
                    )
                    select *
                    from x
                    where x.rn = 1
                    order by x.last_execution_time
                    option (recompile);

                end;

            if (@exporttoexcel = 0 and @skipxml = 1)
                begin

                    raiserror (N'Returning results for skipped XML', 0, 1) with nowait;

                    with x as (
                        select wpt.database_name,
                               wm.plan_id,
                               wm.query_id,
                               wm.query_id_all_plan_ids,
                               wpt.query_sql_text,
                               wpt.query_plan_xml,
                               wpt.pattern,
                               wm.parameter_sniffing_symptoms,
                               wpt.top_three_waits,
                               wm.count_executions,
                               wm.count_compiles,
                               wm.total_cpu_time,
                               wm.avg_cpu_time,
                               wm.total_duration,
                               wm.avg_duration,
                               wm.total_logical_io_reads,
                               wm.avg_logical_io_reads,
                               wm.total_physical_io_reads,
                               wm.avg_physical_io_reads,
                               wm.total_logical_io_writes,
                               wm.avg_logical_io_writes,
                               wm.total_rowcount,
                               wm.avg_rowcount,
                               wm.total_query_max_used_memory,
                               wm.avg_query_max_used_memory,
                               wm.total_tempdb_space_used,
                               wm.avg_tempdb_space_used,
                               wm.total_log_bytes_used,
                               wm.avg_log_bytes_used,
                               wm.total_num_physical_io_reads,
                               wm.avg_num_physical_io_reads,
                               wm.first_execution_time,
                               wm.last_execution_time,
                               wpt.last_force_failure_reason_desc,
                               wpt.context_settings,
                               ROW_NUMBER()
                                       over (partition by wm.plan_id, wm.query_id, wm.last_execution_time order by wm.plan_id) as rn
                        from #working_plan_text as wpt
                                 join #working_metrics as wm
                                      on wpt.plan_id = wm.plan_id
                                          and wpt.query_id = wm.query_id
                    )
                    select *
                    from x
                    where x.rn = 1
                    order by x.last_execution_time
                    option (recompile);

                end;

        end;
    end try
    begin catch
        raiserror (N'Failure returning results', 0,1) with nowait;

        if @sql_select is not null
            begin
                set @msg = N'Last @sql_select: ' + @sql_select;
                raiserror (@msg, 0, 1) with nowait;
            end;

        select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
               @error_severity = ERROR_SEVERITY(),
               @error_state = ERROR_STATE();
        raiserror (@msg, @error_severity, @error_state) with nowait;


        while @@TRANCOUNT > 0
            rollback;

        return;
    end catch;

    begin try
        begin

            if (@exporttoexcel = 0 and @hidesummary = 0 and @skipxml = 0)
                begin
                    raiserror ('Building query plan summary data.', 0, 1) with nowait;

                    /* Build summary data */
                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where frequent_execution = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (1,
                                100,
                                'Execution Pattern',
                                'Frequently Executed Queries',
                                'http://brentozar.com/blitzcache/frequently-executed-queries/',
                                'Queries are being executed more than '
                                    + CAST(@execution_threshold as varchar(5))
                                    +
                                ' times per minute. This can put additional load on the server, even when queries are lightweight.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where parameter_sniffing = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (2,
                                50,
                                'Parameterization',
                                'Parameter Sniffing',
                                'http://brentozar.com/blitzcache/parameter-sniffing/',
                                'There are signs of parameter sniffing (wide variance in rows return or time to execute). Investigate query patterns and tune code appropriately.');

                    /* Forced execution plans */
                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_forced_plan = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (3,
                                5,
                                'Parameterization',
                                'Forced Plans',
                                'http://brentozar.com/blitzcache/forced-plans/',
                                'Execution plans have been compiled with forced plans, either through FORCEPLAN, plan guides, or forced parameterization. This will make general tuning efforts less effective.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_cursor = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (4,
                                200,
                                'Cursors',
                                'Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are cursors in the plan cache. This is neither good nor bad, but it is a thing. Cursors are weird in SQL Server.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_cursor = 1
                                and is_optimistic_cursor = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (4,
                                200,
                                'Cursors',
                                'Optimistic Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are optimistic cursors in the plan cache, which can harm performance.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_cursor = 1
                                and is_forward_only_cursor = 0
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (4,
                                200,
                                'Cursors',
                                'Non-forward Only Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'There are non-forward only cursors in the plan cache, which can harm performance.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_cursor = 1
                                and is_cursor_dynamic = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (4,
                                200,
                                'Cursors',
                                'Dynamic Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'Dynamic Cursors inhibit parallelism!.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_cursor = 1
                                and is_fast_forward_cursor = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (4,
                                200,
                                'Cursors',
                                'Fast Forward Cursors',
                                'http://brentozar.com/blitzcache/cursors-found-slow-queries/',
                                'Fast forward cursors inhibit parallelism!.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_forced_parameterized = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (5,
                                50,
                                'Parameterization',
                                'Forced Parameterization',
                                'http://brentozar.com/blitzcache/forced-parameterization/',
                                'Execution plans have been compiled with forced parameterization.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_parallel = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (6,
                                200,
                                'Execution Plans',
                                'Parallelism',
                                'http://brentozar.com/blitzcache/parallel-plans-detected/',
                                'Parallel plans detected. These warrant investigation, but are neither good nor bad.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.near_parallel = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (7,
                                200,
                                'Execution Plans',
                                'Nearly Parallel',
                                'http://brentozar.com/blitzcache/query-cost-near-cost-threshold-parallelism/',
                                'Queries near the cost threshold for parallelism. These may go parallel when you least expect it.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.plan_warnings = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (8,
                                50,
                                'Execution Plans',
                                'Query Plan Warnings',
                                'http://brentozar.com/blitzcache/query-plan-warnings/',
                                'Warnings detected in execution plans. SQL Server is telling you that something bad is going on that requires your attention.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.long_running = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (9,
                                50,
                                'Performance',
                                'Long Running Queries',
                                'http://brentozar.com/blitzcache/long-running-queries/',
                                'Long running queries have been found. These are queries with an average duration longer than '
                                    + CAST(@long_running_query_warning_seconds / 1000 / 1000 as varchar(5))
                                    +
                                ' second(s). These queries should be investigated for additional tuning options.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.missing_index_count > 0
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (10,
                                50,
                                'Performance',
                                'Missing Index Request',
                                'http://brentozar.com/blitzcache/missing-index-request/',
                                'Queries found with missing indexes.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.downlevel_estimator = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (13,
                                200,
                                'Cardinality',
                                'Legacy Cardinality Estimator in Use',
                                'http://brentozar.com/blitzcache/legacy-cardinality-estimator/',
                                'A legacy cardinality estimator is being used by one or more queries. Investigate whether you need to be using this cardinality estimator. This may be caused by compatibility levels, global trace flags, or query level trace flags.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.implicit_conversions = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (14,
                                50,
                                'Performance',
                                'Implicit Conversions',
                                'http://brentozar.com/go/implicit',
                                'One or more queries are comparing two fields that are not of the same data type.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where busy_loops = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (16,
                                100,
                                'Performance',
                                'Busy Loops',
                                'http://brentozar.com/blitzcache/busy-loops/',
                                'Operations have been found that are executed 100 times more often than the number of rows returned by each iteration. This is an indicator that something is off in query execution.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where tvf_join = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (17,
                                50,
                                'Performance',
                                'Joining to table valued functions',
                                'http://brentozar.com/blitzcache/tvf-join/',
                                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where compile_timeout = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (18,
                                50,
                                'Execution Plans',
                                'Compilation timeout',
                                'http://brentozar.com/blitzcache/compilation-timeout/',
                                'Query compilation timed out for one or more queries. SQL Server did not find a plan that meets acceptable performance criteria in the time allotted so the best guess was returned. There is a very good chance that this plan isn''t even below average - it''s probably terrible.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where compile_memory_limit_exceeded = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (19,
                                50,
                                'Execution Plans',
                                'Compilation memory limit exceeded',
                                'http://brentozar.com/blitzcache/compile-memory-limit-exceeded/',
                                'The optimizer has a limited amount of memory available. One or more queries are complex enough that SQL Server was unable to allocate enough memory to fully optimize the query. A best fit plan was found, and it''s probably terrible.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where warning_no_join_predicate = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (20,
                                10,
                                'Execution Plans',
                                'No join predicate',
                                'http://brentozar.com/blitzcache/no-join-predicate/',
                                'Operators in a query have no join predicate. This means that all rows from one table will be matched with all rows from anther table producing a Cartesian product. That''s a whole lot of rows. This may be your goal, but it''s important to investigate why this is happening.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where plan_multiple_plans = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (21,
                                200,
                                'Execution Plans',
                                'Multiple execution plans',
                                'http://brentozar.com/blitzcache/multiple-plans/',
                                'Queries exist with multiple execution plans (as determined by query_plan_hash). Investigate possible ways to parameterize these queries or otherwise reduce the plan count.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where unmatched_index_count > 0
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (22,
                                100,
                                'Performance',
                                'Unmatched indexes',
                                'http://brentozar.com/blitzcache/unmatched-indexes',
                                'An index could have been used, but SQL Server chose not to use it - likely due to parameterization and filtered indexes.');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where unparameterized_query = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (23,
                                100,
                                'Parameterization',
                                'Unparameterized queries',
                                'http://brentozar.com/blitzcache/unparameterized-queries',
                                'Unparameterized queries found. These could be ad hoc queries, data exploration, or queries using "OPTIMIZE FOR UNKNOWN".');

                    if EXISTS(select 1 / 0
                              from #working_warnings
                              where is_trivial = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (24,
                                100,
                                'Execution Plans',
                                'Trivial Plans',
                                'http://brentozar.com/blitzcache/trivial-plans',
                                'Trivial plans get almost no optimization. If you''re finding these in the top worst queries, something may be going wrong.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_forced_serial = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (25,
                                10,
                                'Execution Plans',
                                'Forced Serialization',
                                'http://www.brentozar.com/blitzcache/forced-serialization/',
                                'Something in your plan is forcing a serial query. Further investigation is needed if this is not by design.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_key_lookup_expensive = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (26,
                                100,
                                'Execution Plans',
                                'Expensive Key Lookups',
                                'http://www.brentozar.com/blitzcache/expensive-key-lookups/',
                                'There''s a key lookup in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_remote_query_expensive = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (28,
                                100,
                                'Execution Plans',
                                'Expensive Remote Query',
                                'http://www.brentozar.com/blitzcache/expensive-remote-query/',
                                'There''s a remote query in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.trace_flags_session is not null
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (29,
                                100,
                                'Trace Flags',
                                'Session Level Trace Flags Enabled',
                                'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                                'Someone is enabling session level Trace Flags in a query.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_unused_grant is not null
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (30,
                                100,
                                'Unused memory grants',
                                'Queries are asking for more memory than they''re using',
                                'https://www.brentozar.com/blitzcache/unused-memory-grants/',
                                'Queries have large unused memory grants. This can cause concurrency issues, if queries are waiting a long time to get memory to run.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.function_count > 0
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (31,
                                100,
                                'Compute Scalar That References A Function',
                                'This could be trouble if you''re using Scalar Functions or MSTVFs',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'Both of these will force queries to run serially, run at least once per row, and may result in poor cardinality estimates.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.clr_function_count > 0
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (32,
                                100,
                                'Compute Scalar That References A CLR Function',
                                'This could be trouble if your CLR functions perform data access',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'May force queries to run serially, run at least once per row, and may result in poor cardinlity estimates.');


                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_table_variable = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (33,
                                100,
                                'Table Variables detected',
                                'Beware nasty side effects',
                                'https://www.brentozar.com/blitzcache/table-variables/',
                                'All modifications are single threaded, and selects have really low row estimates.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.no_stats_warning = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (35,
                                100,
                                'Columns with no statistics',
                                'Poor cardinality estimates may ensue',
                                'https://www.brentozar.com/blitzcache/columns-no-statistics/',
                                'Sometimes this happens with indexed views, other times because auto create stats is turned off.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.relop_warnings = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (36,
                                100,
                                'Operator Warnings',
                                'SQL is throwing operator level plan warnings',
                                'http://brentozar.com/blitzcache/query-plan-warnings/',
                                'Check the plan for more details.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_table_scan = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (37,
                                100,
                                'Table Scans',
                                'Your database has HEAPs',
                                'https://www.brentozar.com/archive/2012/05/video-heaps/',
                                'This may not be a problem. Run sp_BlitzIndex for more information.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.backwards_scan = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (38,
                                100,
                                'Backwards Scans',
                                'Indexes are being read backwards',
                                'https://www.brentozar.com/blitzcache/backwards-scans/',
                                'This isn''t always a problem. They can cause serial zones in plans, and may need an index to match sort order.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.forced_index = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (39,
                                100,
                                'Index forcing',
                                'Someone is using hints to force index usage',
                                'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                                'This can cause inefficient plans, and will prevent missing index requests.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.forced_seek = 1
                                 or p.forced_scan = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (40,
                                100,
                                'Seek/Scan forcing',
                                'Someone is using hints to force index seeks/scans',
                                'https://www.brentozar.com/blitzcache/optimizer-forcing/',
                                'This can cause inefficient plans by taking seek vs scan choice away from the optimizer.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.columnstore_row_mode = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (41,
                                100,
                                'ColumnStore indexes operating in Row Mode',
                                'Batch Mode is optimal for ColumnStore indexes',
                                'https://www.brentozar.com/blitzcache/columnstore-indexes-operating-row-mode/',
                                'ColumnStore indexes operating in Row Mode indicate really poor query choices.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_computed_scalar = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (42,
                                50,
                                'Computed Columns Referencing Scalar UDFs',
                                'This makes a whole lot of stuff run serially',
                                'https://www.brentozar.com/blitzcache/computed-columns-referencing-functions/',
                                'This can cause a whole mess of bad serializartion problems.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_sort_expensive = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (43,
                                100,
                                'Execution Plans',
                                'Expensive Sort',
                                'http://www.brentozar.com/blitzcache/expensive-sorts/',
                                'There''s a sort in your plan that costs >=50% of the total plan cost.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_computed_filter = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (44,
                                50,
                                'Filters Referencing Scalar UDFs',
                                'This forces serialization',
                                'https://www.brentozar.com/blitzcache/compute-scalar-functions/',
                                'Someone put a Scalar UDF in the WHERE clause!');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.index_ops >= 5
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (45,
                                100,
                                'Many Indexes Modified',
                                'Write Queries Are Hitting >= 5 Indexes',
                                'https://www.brentozar.com/blitzcache/many-indexes-modified/',
                                'This can cause lots of hidden I/O -- Run sp_BlitzIndex for more information.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_row_level = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (46,
                                100,
                                'Plan Confusion',
                                'Row Level Security is in use',
                                'https://www.brentozar.com/blitzcache/row-level-security/',
                                'You may see a lot of confusing junk in your query plan.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_spatial = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (47,
                                200,
                                'Spatial Abuse',
                                'You hit a Spatial Index',
                                'https://www.brentozar.com/blitzcache/spatial-indexes/',
                                'Purely informational.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.index_dml = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (48,
                                150,
                                'Index DML',
                                'Indexes were created or dropped',
                                'https://www.brentozar.com/blitzcache/index-dml/',
                                'This can cause recompiles and stuff.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.table_dml = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (49,
                                150,
                                'Table DML',
                                'Tables were created or dropped',
                                'https://www.brentozar.com/blitzcache/table-dml/',
                                'This can cause recompiles and stuff.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.long_running_low_cpu = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (50,
                                150,
                                'Long Running Low CPU',
                                'You have a query that runs for much longer than it uses CPU',
                                'https://www.brentozar.com/blitzcache/long-running-low-cpu/',
                                'This can be a sign of blocking, linked servers, or poor client application code (ASYNC_NETWORK_IO).');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.low_cost_high_cpu = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (51,
                                150,
                                'Low Cost Query With High CPU',
                                'You have a low cost query that uses a lot of CPU',
                                'https://www.brentozar.com/blitzcache/low-cost-high-cpu/',
                                'This can be a sign of functions or Dynamic SQL that calls black-box code.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.stale_stats = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (52,
                                150,
                                'Biblical Statistics',
                                'Statistics used in queries are >7 days old with >100k modifications',
                                'https://www.brentozar.com/blitzcache/stale-statistics/',
                                'Ever heard of updating statistics?');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_adaptive = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (53,
                                150,
                                'Adaptive joins',
                                'This is pretty cool -- you''re living in the future.',
                                'https://www.brentozar.com/blitzcache/adaptive-joins/',
                                'Joe Sack rules.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_spool_expensive = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (54,
                                150,
                                'Expensive Index Spool',
                                'You have an index spool, this is usually a sign that there''s an index missing somewhere.',
                                'https://www.brentozar.com/blitzcache/eager-index-spools/',
                                'Check operator predicates and output for index definition guidance');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_spool_more_rows = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (55,
                                150,
                                'Index Spools Many Rows',
                                'You have an index spool that spools more rows than the query returns',
                                'https://www.brentozar.com/blitzcache/eager-index-spools/',
                                'Check operator predicates and output for index definition guidance');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_bad_estimate = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (56,
                                100,
                                'Potentially bad cardinality estimates',
                                'Estimated rows are different from average rows by a factor of 10000',
                                'https://www.brentozar.com/blitzcache/bad-estimates/',
                                'This may indicate a performance problem if mismatches occur regularly');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_big_log = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (57,
                                100,
                                'High transaction log use',
                                'This query on average uses more than half of the transaction log',
                                'http://michaeljswart.com/2014/09/take-care-when-scripting-batches/',
                                'This is probably a sign that you need to start batching queries');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_big_tempdb = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (58,
                                100,
                                'High tempdb use',
                                'This query uses more than half of a data file on average',
                                'No URL yet',
                                'You should take a look at tempdb waits to see if you''re having problems');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_row_goal = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (59,
                                200,
                                'Row Goals',
                                'This query had row goals introduced',
                                'https://www.brentozar.com/archive/2018/01/sql-server-2017-cu3-adds-optimizer-row-goal-information-query-plans/',
                                'This can be good or bad, and should be investigated for high read queries');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_mstvf = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (60,
                                100,
                                'MSTVFs',
                                'These have many of the same problems scalar UDFs have',
                                'http://brentozar.com/blitzcache/tvf-join/',
                                'Execution plans have been found that join to table valued functions (TVFs). TVFs produce inaccurate estimates of the number of rows returned and can lead to any number of query plan problems.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_mstvf = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (61,
                                100,
                                'Many to Many Merge',
                                'These use secret worktables that could be doing lots of reads',
                                'https://www.brentozar.com/archive/2018/04/many-mysteries-merge-joins/',
                                'Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_nonsargable = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (62,
                                50,
                                'Non-SARGable queries',
                                'Queries may be using',
                                'https://www.brentozar.com/blitzcache/non-sargable-predicates/',
                                'Occurs when join inputs aren''t known to be unique. Can be really bad when parallel.');

                    if EXISTS(select 1 / 0
                              from #working_warnings p
                              where p.is_paul_white_electric = 1
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (998,
                                200,
                                'Is Paul White Electric?',
                                'This query has a Switch operator in it!',
                                'https://www.sql.kiwi/2013/06/hello-operator-my-switch-is-bored.html',
                                'You should email this query plan to Paul: SQLkiwi at gmail dot com');


                    insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                    select 999,
                           200,
                           'Database Level Statistics',
                           'The database ' + sa.[database] + ' last had a stats update on ' +
                           CONVERT(nvarchar(10), CONVERT(date, MAX(sa.last_update))) + ' and has ' +
                           CONVERT(nvarchar(10), AVG(sa.modification_count)) + ' modifications on average.' as finding,
                           'https://www.brentozar.com/blitzcache/stale-statistics/'                         as url,
                           'Consider updating statistics more frequently,'                                  as details
                    from #stats_agg as sa
                    group by sa.[database]
                    having MAX(sa.last_update) <= DATEADD(day, -7, SYSDATETIME())
                       and AVG(sa.modification_count) >= 100000;


                    if EXISTS(select 1 / 0
                              from #trace_flags as tf
                              where tf.global_trace_flags is not null
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (1000,
                                255,
                                'Global Trace Flags Enabled',
                                'You have Global Trace Flags enabled on your server',
                                'https://www.brentozar.com/blitz/trace-flags-enabled-globally/',
                                'You have the following Global Trace Flags enabled: ' +
                                (select top 1 tf.global_trace_flags
                                 from #trace_flags as tf
                                 where tf.global_trace_flags is not null));


                    /*
			Return worsts
			*/
                    with worsts as (
                        select gi.flat_date,
                               gi.start_range,
                               gi.end_range,
                               gi.total_avg_duration_ms,
                               gi.total_avg_cpu_time_ms,
                               gi.total_avg_logical_io_reads_mb,
                               gi.total_avg_physical_io_reads_mb,
                               gi.total_avg_logical_io_writes_mb,
                               gi.total_avg_query_max_used_memory_mb,
                               gi.total_rowcount,
                               gi.total_avg_log_bytes_mb,
                               gi.total_avg_tempdb_space,
                               gi.total_max_duration_ms,
                               gi.total_max_cpu_time_ms,
                               gi.total_max_logical_io_reads_mb,
                               gi.total_max_physical_io_reads_mb,
                               gi.total_max_logical_io_writes_mb,
                               gi.total_max_query_max_used_memory_mb,
                               gi.total_max_log_bytes_mb,
                               gi.total_max_tempdb_space,
                               CONVERT(nvarchar(20), gi.flat_date) as worst_date,
                               case
                                   when DATEPART(hour, gi.start_range) = 0 then ' midnight '
                                   when DATEPART(hour, gi.start_range) <= 12
                                       then CONVERT(nvarchar(3), DATEPART(hour, gi.start_range)) + 'am '
                                   when DATEPART(hour, gi.start_range) > 12
                                       then CONVERT(nvarchar(3), DATEPART(hour, gi.start_range) - 12) + 'pm '
                                   end                             as worst_start_time,
                               case
                                   when DATEPART(hour, gi.end_range) = 0 then ' midnight '
                                   when DATEPART(hour, gi.end_range) <= 12
                                       then CONVERT(nvarchar(3), DATEPART(hour, gi.end_range)) + 'am '
                                   when DATEPART(hour, gi.end_range) > 12
                                       then CONVERT(nvarchar(3), DATEPART(hour, gi.end_range) - 12) + 'pm '
                                   end                             as worst_end_time
                        from #grouped_interval as gi
                    ), /*averages*/
                         duration_worst as (
                             select top 1 'Your worst avg duration range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_duration_ms desc
                         ),
                         cpu_worst as (
                             select top 1 'Your worst avg cpu range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_cpu_time_ms desc
                         ),
                         logical_reads_worst as (
                             select top 1
                                     'Your worst avg logical read range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_logical_io_reads_mb desc
                         ),
                         physical_reads_worst as (
                             select top 1
                                     'Your worst avg physical read range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_physical_io_reads_mb desc
                         ),
                         logical_writes_worst as (
                             select top 1
                                     'Your worst avg logical write range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_logical_io_writes_mb desc
                         ),
                         memory_worst as (
                             select top 1 'Your worst avg memory range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_query_max_used_memory_mb desc
                         ),
                         rowcount_worst as (
                             select top 1 'Your worst avg row count range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_rowcount desc
                         ),
                         logbytes_worst as (
                             select top 1 'Your worst avg log bytes range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_log_bytes_mb desc
                         ),
                         tempdb_worst as (
                             select top 1 'Your worst avg tempdb range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_avg_tempdb_space desc
                         )/*maxes*/,
                         max_duration_worst as (
                             select top 1 'Your worst max duration range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_duration_ms desc
                         ),
                         max_cpu_worst as (
                             select top 1 'Your worst max cpu range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_cpu_time_ms desc
                         ),
                         max_logical_reads_worst as (
                             select top 1
                                     'Your worst max logical read range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_logical_io_reads_mb desc
                         ),
                         max_physical_reads_worst as (
                             select top 1
                                     'Your worst max physical read range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_physical_io_reads_mb desc
                         ),
                         max_logical_writes_worst as (
                             select top 1
                                     'Your worst max logical write range was on ' + worsts.worst_date + ' between ' +
                                     worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_logical_io_writes_mb desc
                         ),
                         max_memory_worst as (
                             select top 1 'Your worst max memory range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_query_max_used_memory_mb desc
                         ),
                         max_logbytes_worst as (
                             select top 1 'Your worst max log bytes range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_log_bytes_mb desc
                         ),
                         max_tempdb_worst as (
                             select top 1 'Your worst max tempdb range was on ' + worsts.worst_date + ' between ' +
                                          worsts.worst_start_time + ' and ' + worsts.worst_end_time + '.' as msg
                             from worsts
                             order by worsts.total_max_tempdb_space desc
                         )
                    insert
                    #warning_results
                    (
                    checkid
                    ,
                    priority
                    ,
                    findingsgroup
                    ,
                    finding
                    ,
                    url
                    ,
                    details
                    )
                    /*averages*/
                    select 1002, 255, 'Worsts', 'Worst Avg Duration', 'N/A', duration_worst.msg
                    from duration_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg CPU', 'N/A', cpu_worst.msg
                    from cpu_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg Logical Reads', 'N/A', logical_reads_worst.msg
                    from logical_reads_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg Physical Reads', 'N/A', physical_reads_worst.msg
                    from physical_reads_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg Logical Writes', 'N/A', logical_writes_worst.msg
                    from logical_writes_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg Memory', 'N/A', memory_worst.msg
                    from memory_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Row Counts', 'N/A', rowcount_worst.msg
                    from rowcount_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg Log Bytes', 'N/A', logbytes_worst.msg
                    from logbytes_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Avg tempdb', 'N/A', tempdb_worst.msg
                    from tempdb_worst
                    union all
                    /*maxes*/
                    select 1002, 255, 'Worsts', 'Worst Max Duration', 'N/A', max_duration_worst.msg
                    from max_duration_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max CPU', 'N/A', max_cpu_worst.msg
                    from max_cpu_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max Logical Reads', 'N/A', max_logical_reads_worst.msg
                    from max_logical_reads_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max Physical Reads', 'N/A', max_physical_reads_worst.msg
                    from max_physical_reads_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max Logical Writes', 'N/A', max_logical_writes_worst.msg
                    from max_logical_writes_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max Memory', 'N/A', max_memory_worst.msg
                    from max_memory_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max Log Bytes', 'N/A', max_logbytes_worst.msg
                    from max_logbytes_worst
                    union all
                    select 1002, 255, 'Worsts', 'Worst Max tempdb', 'N/A', max_tempdb_worst.msg
                    from max_tempdb_worst
                    option (recompile);


                    if not EXISTS(select 1 / 0
                                  from #warning_results as bcr
                                  where bcr.priority = 2147483646
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (2147483646,
                                255,
                                'Need more help?',
                                'Paste your plan on the internet!',
                                'http://pastetheplan.com',
                                'This makes it easy to share plans and post them to Q&A sites like https://dba.stackexchange.com/!');


                    if not EXISTS(select 1 / 0
                                  from #warning_results as bcr
                                  where bcr.priority = 2147483647
                        )
                        insert into #warning_results (checkid, priority, findingsgroup, finding, url, details)
                        values (2147483647,
                                255,
                                'Thanks for using sp_BlitzQueryStore!',
                                'From Your Community Volunteers',
                                'http://FirstResponderKit.org',
                                'We hope you found this tool useful. Current version: ' + @version + ' released on ' +
                                CONVERT(nvarchar(30), @versiondate) + '.');


                    select priority,
                           findingsgroup,
                           finding,
                           url,
                           details,
                           checkid
                    from #warning_results
                    group by priority,
                             findingsgroup,
                             finding,
                             url,
                             details,
                             checkid
                    order by priority asc, findingsgroup, finding, checkid asc
                    option (recompile);


                end;

        end;
    end try
    begin catch
        raiserror (N'Failure returning warnings', 0,1) with nowait;

        if @sql_select is not null
            begin
                set @msg = N'Last @sql_select: ' + @sql_select;
                raiserror (@msg, 0, 1) with nowait;
            end;

        select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
               @error_severity = ERROR_SEVERITY(),
               @error_state = ERROR_STATE();
        raiserror (@msg, @error_severity, @error_state) with nowait;


        while @@TRANCOUNT > 0
            rollback;

        return;
    end catch;

    if @debug = 1
        begin try

            begin

                raiserror (N'Returning debugging data from temp tables', 0, 1) with nowait;

--Table content debugging

                select '#working_metrics' as table_name, *
                from #working_metrics as wm
                option (recompile);

                select '#working_plan_text' as table_name, *
                from #working_plan_text as wpt
                option (recompile);

                select '#working_warnings' as table_name, *
                from #working_warnings as ww
                option (recompile);

                select '#working_wait_stats' as table_name, *
                from #working_wait_stats wws
                option (recompile);

                select '#grouped_interval' as table_name, *
                from #grouped_interval
                option (recompile);

                select '#working_plans' as table_name, *
                from #working_plans
                option (recompile);

                select '#stats_agg' as table_name, *
                from #stats_agg
                option (recompile);

                select '#trace_flags' as table_name, *
                from #trace_flags
                option (recompile);

                select '#statements' as table_name, *
                from #statements as s
                option (recompile);

                select '#query_plan' as table_name, *
                from #query_plan as qp
                option (recompile);

                select '#relop' as table_name, *
                from #relop as r
                option (recompile);

                select '#plan_cost' as table_name, *
                from #plan_cost as pc
                option (recompile);

                select '#est_rows' as table_name, *
                from #est_rows as er
                option (recompile);

                select '#stored_proc_info' as table_name, *
                from #stored_proc_info as spi
                option (recompile);

                select '#conversion_info' as table_name, *
                from #conversion_info as ci
                option ( recompile );

                select '#variable_info' as table_name, *
                from #variable_info as vi
                option ( recompile );

                select '#missing_index_xml' as table_name, *
                from #missing_index_xml
                option ( recompile );

                select '#missing_index_schema' as table_name, *
                from #missing_index_schema
                option ( recompile );

                select '#missing_index_usage' as table_name, *
                from #missing_index_usage
                option ( recompile );

                select '#missing_index_detail' as table_name, *
                from #missing_index_detail
                option ( recompile );

                select '#missing_index_pretty' as table_name, *
                from #missing_index_pretty
                option ( recompile );

            end;

        end try
        begin catch
            raiserror (N'Failure returning debug temp tables', 0,1) with nowait;

            if @sql_select is not null
                begin
                    set @msg = N'Last @sql_select: ' + @sql_select;
                    raiserror (@msg, 0, 1) with nowait;
                end;

            select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
                   @error_severity = ERROR_SEVERITY(),
                   @error_state = ERROR_STATE();
            raiserror (@msg, @error_severity, @error_state) with nowait;


            while @@TRANCOUNT > 0
                rollback;

            return;
        end catch;

/*
Ways to run this thing

--Debug
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Debug = 1

--Get the top 1
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @Debug = 1

--Use a StartDate												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170527'
				
--Use an EndDate												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @EndDate = '20170527'
				
--Use Both												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StartDate = '20170526', @EndDate = '20170527'

--Set a minimum execution count												 
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @MinimumExecutionCount = 10

--Set a duration minimum
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @DurationFilter = 5

--Look for a stored procedure name (that doesn't exist!)
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'blah'

--Look for a stored procedure name that does (at least On My Computer®)
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @StoredProcName = 'UserReportExtended'

--Look for failed queries
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @Top = 1, @Failed = 1

--Filter by plan_id
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @PlanIdFilter = 3356

--Filter by query_id
EXEC sp_BlitzQueryStore @DatabaseName = 'StackOverflow', @QueryIdFilter = 2958

*/

end;

go
