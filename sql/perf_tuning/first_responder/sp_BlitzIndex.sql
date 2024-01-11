set ansi_nulls on;
set ansi_padding on;
set ansi_warnings on;
set arithabort on;
set concat_null_yields_null on;
set quoted_identifier on;
set statistics io off;
set statistics time off;
go

if OBJECT_ID('dbo.sp_BlitzIndex') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzIndex AS RETURN 0;');
go

alter procedure dbo.sp_blitzindex @databasename nvarchar(128) = null, /*Defaults to current DB if not specified*/
                                  @schemaname nvarchar(128) = null, /*Requires table_name as well.*/
                                  @tablename nvarchar(128) = null, /*Requires schema_name as well.*/
                                  @mode tinyint=0, /*0=Diagnose, 1=Summarize, 2=Index Usage Detail, 3=Missing Index Detail, 4=Diagnose Details*/
    /*Note:@Mode doesn't matter if you're specifying schema_name and @TableName.*/
                                  @filter tinyint = 0, /* 0=no filter (default). 1=No low-usage warnings for objects with 0 reads. 2=Only warn for objects >= 500MB */
    /*Note:@Filter doesn't do anything unless @Mode=0*/
                                  @skippartitions bit = 0,
                                  @skipstatistics bit = 1,
                                  @getalldatabases bit = 0,
                                  @bringthepain bit = 0,
                                  @ignoredatabases nvarchar(max) = null, /* Comma-delimited list of databases you want to skip */
                                  @thresholdmb int = 250 /* Number of megabytes that an object must be before we include it in basic results */,
                                  @outputtype varchar(20) = 'TABLE',
                                  @outputservername nvarchar(256) = null,
                                  @outputdatabasename nvarchar(256) = null,
                                  @outputschemaname nvarchar(256) = null,
                                  @outputtablename nvarchar(256) = null,
                                  @includeinactiveindexes bit = 0 /* Will skip indexes with no reads or writes */,
                                  @help tinyint = 0,
                                  @debug bit = 0,
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
sp_BlitzIndex from http://FirstResponderKit.org

This script analyzes the design and performance of your indexes.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
 - The @OutputDatabaseName parameters are not functional yet. To check the
   status of this enhancement request, visit:
   https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/221
 - Does not analyze columnstore, spatial, XML, or full text indexes. If you
   would like to contribute code to analyze those, head over to Github and
   check out the issues list: http://FirstResponderKit.org
 - Index create statements are just to give you a rough idea of the syntax. It includes filters and fillfactor.
 --        Example 1: index creates use ONLINE=? instead of ONLINE=ON / ONLINE=OFF. This is because it is important
           for the user to understand if it is going to be offline and not just run a script.
 --        Example 2: they do not include all the options the index may have been created with (padding, compression
           filegroup/partition scheme etc.)
 --        (The compression and filegroup index create syntax is not trivial because it is set at the partition
           level and is not trivial to code.)
 - Does not advise you about data modeling for clustered indexes and primary keys (primarily looks for signs of insanity.)

Unknown limitations of this version:
 - We knew them once, but we forgot.


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


declare @scriptversionname nvarchar(50);
declare @daysuptime numeric(23, 2);
declare @databaseid int;
declare @objectid int;
declare @dsql nvarchar(max);
declare @params nvarchar(max);
declare @msg nvarchar(4000);
declare @errorseverity int;
declare @errorstate int;
declare @rowcount bigint;
declare @sqlserverproductversion nvarchar(128);
declare @sqlserveredition int;
declare @filtermb int;
declare @collation nvarchar(256);
declare @numdatabases int;
declare @linefeed nvarchar(5);
declare @daysuptimeinsertvalue nvarchar(256);
declare @databasetoignore nvarchar(max);

    set @linefeed = CHAR(13) + CHAR(10);
select @sqlserverproductversion = CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));
select @sqlserveredition = CAST(SERVERPROPERTY('EngineEdition') as int); /* We default to online index creates where EngineEdition=3*/
    set @filtermb = 250;
select @scriptversionname = 'sp_BlitzIndex(TM) v' + @version + ' - ' + DATENAME(mm, @versiondate) + ' ' +
                            RIGHT('0' + DATENAME(dd, @versiondate), 2) + ', ' + DATENAME(yy, @versiondate);
    set @ignoredatabases = REPLACE(REPLACE(LTRIM(RTRIM(@ignoredatabases)), CHAR(10), ''), CHAR(13), '');

    raiserror (N'Starting run. %s', 0,1, @scriptversionname) with nowait;

    if (@outputtype not in ('TABLE', 'NONE'))
        begin
            raiserror ('Invalid value for parameter @OutputType. Expected: (TABLE;NONE)',12,1);
            return;
        end;

    if (@outputtype = 'NONE')
        begin
            if (@outputtablename is null or @outputschemaname is null or @outputdatabasename is null)
                begin
                    raiserror ('This procedure should be called with a value for @Output* parameters, as @OutputType is set to NONE',12,1);
                    return;
                end;
            if (@bringthepain = 1)
                begin
                    raiserror ('Incompatible Parameters: @BringThePain set to 1 and @OutputType set to NONE',12,1);
                    return;
                end;
            /* Eventually limit by mode
    IF(@Mode not in (0,4))
	BEGIN
        RAISERROR('Incompatible Parameters: @Mode set to %d and @OutputType set to NONE',12,1,@Mode);
        RETURN;
	END;
	*/
        end;

    if OBJECT_ID('tempdb..#IndexSanity') is not null
        drop table #indexsanity;

    if OBJECT_ID('tempdb..#IndexPartitionSanity') is not null
        drop table #indexpartitionsanity;

    if OBJECT_ID('tempdb..#IndexSanitySize') is not null
        drop table #indexsanitysize;

    if OBJECT_ID('tempdb..#IndexColumns') is not null
        drop table #indexcolumns;

    if OBJECT_ID('tempdb..#MissingIndexes') is not null
        drop table #missingindexes;

    if OBJECT_ID('tempdb..#ForeignKeys') is not null
        drop table #foreignkeys;

    if OBJECT_ID('tempdb..#BlitzIndexResults') is not null
        drop table #blitzindexresults;

    if OBJECT_ID('tempdb..#IndexCreateTsql') is not null
        drop table #indexcreatetsql;

    if OBJECT_ID('tempdb..#DatabaseList') is not null
        drop table #databaselist;

    if OBJECT_ID('tempdb..#Statistics') is not null
        drop table #statistics;

    if OBJECT_ID('tempdb..#PartitionCompressionInfo') is not null
        drop table #partitioncompressioninfo;

    if OBJECT_ID('tempdb..#ComputedColumns') is not null
        drop table #computedcolumns;

    if OBJECT_ID('tempdb..#TraceStatus') is not null
        drop table #tracestatus;

    if OBJECT_ID('tempdb..#TemporalTables') is not null
        drop table #temporaltables;

    if OBJECT_ID('tempdb..#CheckConstraints') is not null
        drop table #checkconstraints;

    if OBJECT_ID('tempdb..#FilteredIndexes') is not null
        drop table #filteredindexes;

    if OBJECT_ID('tempdb..#Ignore_Databases') is not null
        drop table #ignore_databases

    raiserror (N'Create temp tables.',0,1) with nowait;
    create table #blitzindexresults
    (
        blitz_result_id int identity primary key,
        check_id int not null,
        index_sanity_id int null,
        priority int null,
        findings_group nvarchar(4000) not null,
        finding nvarchar(200) not null,
        [database_name] nvarchar(128) null,
        url nvarchar(200) not null,
        details nvarchar(max) not null,
        index_definition nvarchar(max) not null,
        secret_columns nvarchar(max) null,
        index_usage_summary nvarchar(max) null,
        index_size_summary nvarchar(max) null,
        create_tsql nvarchar(max) null,
        more_info nvarchar(max) null
    );

    create table #indexsanity
    (
        [index_sanity_id] int identity primary key clustered,
        [database_id] smallint not null,
        [object_id] int not null,
        [index_id] int not null,
        [index_type] tinyint not null,
        [database_name] nvarchar(128) not null,
        [schema_name] nvarchar(128) not null,
        [object_name] nvarchar(128) not null,
        index_name nvarchar(128) null,
        key_column_names nvarchar(max) null,
        key_column_names_with_sort_order nvarchar(max) null,
        key_column_names_with_sort_order_no_types nvarchar(max) null,
        count_key_columns int null,
        include_column_names nvarchar(max) null,
        include_column_names_no_types nvarchar(max) null,
        count_included_columns int null,
        partition_key_column_name nvarchar(max) null,
        filter_definition nvarchar(max) not null,
        is_indexed_view bit not null,
        is_unique bit not null,
        is_primary_key bit not null,
        is_xml bit not null,
        is_spatial bit not null,
        is_nc_columnstore bit not null,
        is_cx_columnstore bit not null,
        is_in_memory_oltp bit not null,
        is_disabled bit not null,
        is_hypothetical bit not null,
        is_padded bit not null,
        fill_factor smallint not null,
        user_seeks bigint not null,
        user_scans bigint not null,
        user_lookups bigint not null,
        user_updates bigint null,
        last_user_seek datetime null,
        last_user_scan datetime null,
        last_user_lookup datetime null,
        last_user_update datetime null,
        is_referenced_by_foreign_key bit default (0),
        secret_columns nvarchar(max) null,
        count_secret_columns int null,
        create_date datetime not null,
        modify_date datetime not null,
        filter_columns_not_in_index nvarchar(max),
        [db_schema_object_name] as [schema_name] + N'.' + [object_name],
        [db_schema_object_indexid] as [schema_name] + N'.' + [object_name]
            + case
                  when [index_name] is not null then N'.' + index_name
                  else N''
                                          end + N' (' + CAST(index_id as nvarchar(20)) + N')',
        first_key_column_name as case
                                     when count_key_columns > 1
                                         then LEFT(key_column_names, CHARINDEX(',', key_column_names, 0) - 1)
                                     else key_column_names
            end,
        index_definition as
                case
                    when partition_key_column_name is not null
                        then N'[PARTITIONED BY:' + partition_key_column_name + N']'
                    else ''
                    end +
                case index_id
                    when 0 then N'[HEAP] '
                    when 1 then N'[CX] '
                    else N'' end + case
                                       when is_indexed_view = 1 then N'[VIEW] '
                                       else N'' end + case
                                                          when is_primary_key = 1 then N'[PK] '
                                                          else N'' end + case
                                                                             when is_xml = 1 then N'[XML] '
                                                                             else N'' end + case
                                                                                                when is_spatial = 1
                                                                                                    then N'[SPATIAL] '
                                                                                                else N'' end + case
                                                                                                                   when is_nc_columnstore = 1
                                                                                                                       then N'[COLUMNSTORE] '
                                                                                                                   else N'' end +
                case
                    when is_in_memory_oltp = 1 then N'[IN-MEMORY] '
                    else N'' end + case
                                       when is_disabled = 1 then N'[DISABLED] '
                                       else N'' end + case
                                                          when is_hypothetical = 1 then N'[HYPOTHETICAL] '
                                                          else N'' end + case
                                                                             when is_unique = 1 and is_primary_key = 0
                                                                                 then N'[UNIQUE] '
                                                                             else N'' end + case
                                                                                                when count_key_columns > 0
                                                                                                    then
                                                                                                        N'[' +
                                                                                                        CAST(count_key_columns as nvarchar(10)) +
                                                                                                        N' KEY'
                                                                                                        +
                                                                                                        case when count_key_columns > 1 then N'S' else N'' end
                                                                                                        + N'] ' +
                                                                                                        LTRIM(key_column_names_with_sort_order)
                                                                                                else N'' end + case
                                                                                                                   when count_included_columns > 0
                                                                                                                       then
                                                                                                                           N' [' +
                                                                                                                           CAST(count_included_columns as nvarchar(10)) +
                                                                                                                           N' INCLUDE' +
                                                                                                                           + case when count_included_columns > 1 then N'S' else N'' end
                                                                                                                           +
                                                                                                                           N'] ' +
                                                                                                                           include_column_names
                                                                                                                   else N'' end +
                case
                    when filter_definition <> N'' then N' [FILTER] ' + filter_definition
                    else N'' end,
        [total_reads] as user_seeks + user_scans + user_lookups,
        [reads_per_write] as CAST(case
                                      when user_updates > 0
                                          then (user_seeks + user_scans + user_lookups) / (1.0 * user_updates)
                                      else 0 end as money),
        [index_usage_summary] as N'Reads: ' +
                                 REPLACE(CONVERT(nvarchar(30), CAST((user_seeks + user_scans + user_lookups) as money),
                                                               1), N'.00', N'')
            + case
                  when user_seeks + user_scans + user_lookups > 0 then
                          N' ('
                          + RTRIM(
                                      case
                                          when user_seeks > 0 then
                                                  REPLACE(CONVERT(nvarchar(30), CAST((user_seeks) as money), 1), N'.00',
                                                          N'') + N' seek '
                                          else N'' end
                                      + case
                                            when user_scans > 0 then
                                                    REPLACE(CONVERT(nvarchar(30), CAST((user_scans) as money), 1),
                                                            N'.00', N'') + N' scan '
                                            else N'' end
                                      + case
                                            when user_lookups > 0 then
                                                    REPLACE(CONVERT(nvarchar(30), CAST((user_lookups) as money), 1),
                                                            N'.00', N'') + N' lookup'
                                            else N'' end
                              )
                          + N') '
                  else N' ' end
            + N'Writes:' +
                                 REPLACE(CONVERT(nvarchar(30), CAST(user_updates as money), 1), N'.00', N''),
        [more_info] as
            case
                when is_in_memory_oltp = 1
                    then N'EXEC dbo.sp_BlitzInMemoryOLTP @dbName=' + QUOTENAME([database_name], N'''') +
                         N', @tableName=' + QUOTENAME([object_name], N'''') + N';'
                else N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name], N'''') +
                     N', @SchemaName=' + QUOTENAME([schema_name], N'''') + N', @TableName=' +
                     QUOTENAME([object_name], N'''') + N';' end
    );
    raiserror (N'Adding UQ index on #IndexSanity (database_id, object_id, index_id)',0,1) with nowait;
    if not EXISTS(select 1
                  from tempdb.sys.indexes
                  where name = 'uq_database_id_object_id_index_id')
    create unique index uq_database_id_object_id_index_id on #indexsanity (database_id, object_id, index_id);


    create table #indexpartitionsanity
    (
        [index_partition_sanity_id] int identity,
        [index_sanity_id] int null,
        [database_id] int not null,
        [object_id] int not null,
        [schema_name] nvarchar(128) not null,
        [index_id] int not null,
        [partition_number] int not null,
        row_count bigint not null,
        reserved_mb numeric(29, 2) not null,
        reserved_lob_mb numeric(29, 2) not null,
        reserved_row_overflow_mb numeric(29, 2) not null,
        leaf_insert_count bigint null,
        leaf_delete_count bigint null,
        leaf_update_count bigint null,
        range_scan_count bigint null,
        singleton_lookup_count bigint null,
        forwarded_fetch_count bigint null,
        lob_fetch_in_pages bigint null,
        lob_fetch_in_bytes bigint null,
        row_overflow_fetch_in_pages bigint null,
        row_overflow_fetch_in_bytes bigint null,
        row_lock_count bigint null,
        row_lock_wait_count bigint null,
        row_lock_wait_in_ms bigint null,
        page_lock_count bigint null,
        page_lock_wait_count bigint null,
        page_lock_wait_in_ms bigint null,
        index_lock_promotion_attempt_count bigint null,
        index_lock_promotion_count bigint null,
        data_compression_desc nvarchar(60) null,
        page_latch_wait_count bigint null,
        page_latch_wait_in_ms bigint null,
        page_io_latch_wait_count bigint null,
        page_io_latch_wait_in_ms bigint null
    );

    create table #indexsanitysize
    (
        [index_sanity_size_id] int identity not null,
        [index_sanity_id] int null,
        [database_id] int not null,
        [schema_name] nvarchar(128) not null,
        partition_count int not null,
        total_rows bigint not null,
        total_reserved_mb numeric(29, 2) not null,
        total_reserved_lob_mb numeric(29, 2) not null,
        total_reserved_row_overflow_mb numeric(29, 2) not null,
        total_leaf_delete_count bigint null,
        total_leaf_update_count bigint null,
        total_range_scan_count bigint null,
        total_singleton_lookup_count bigint null,
        total_forwarded_fetch_count bigint null,
        total_row_lock_count bigint null,
        total_row_lock_wait_count bigint null,
        total_row_lock_wait_in_ms bigint null,
        avg_row_lock_wait_in_ms bigint null,
        total_page_lock_count bigint null,
        total_page_lock_wait_count bigint null,
        total_page_lock_wait_in_ms bigint null,
        avg_page_lock_wait_in_ms bigint null,
        total_index_lock_promotion_attempt_count bigint null,
        total_index_lock_promotion_count bigint null,
        data_compression_desc nvarchar(4000) null,
        page_latch_wait_count bigint null,
        page_latch_wait_in_ms bigint null,
        page_io_latch_wait_count bigint null,
        page_io_latch_wait_in_ms bigint null,
        index_size_summary as ISNULL(
                    case
                        when partition_count > 1
                            then N'[' + CAST(partition_count as nvarchar(10)) + N' PARTITIONS] '
                        else N''
                        end + REPLACE(CONVERT(nvarchar(30), CAST([total_rows] as money), 1), N'.00', N'') + N' rows; '
                    + case
                          when total_reserved_mb > 1024 then
                                  CAST(CAST(total_reserved_mb / 1024. as numeric(29, 1)) as nvarchar(30)) + N'GB'
                          else
                                  CAST(CAST(total_reserved_mb as numeric(29, 1)) as nvarchar(30)) + N'MB'
                        end
                    + case
                          when total_reserved_lob_mb > 1024 then
                                  N'; ' + CAST(CAST(total_reserved_lob_mb / 1024. as numeric(29, 1)) as nvarchar(30)) +
                                  N'GB LOB'
                          when total_reserved_lob_mb > 0 then
                                  N'; ' + CAST(CAST(total_reserved_lob_mb as numeric(29, 1)) as nvarchar(30)) +
                                  N'MB LOB'
                          else ''
                        end
                    + case
                          when total_reserved_row_overflow_mb > 1024 then
                                  N'; ' +
                                  CAST(CAST(total_reserved_row_overflow_mb / 1024. as numeric(29, 1)) as nvarchar(30)) +
                                  N'GB Row Overflow'
                          when total_reserved_row_overflow_mb > 0 then
                                  N'; ' + CAST(CAST(total_reserved_row_overflow_mb as numeric(29, 1)) as nvarchar(30)) +
                                  N'MB Row Overflow'
                          else ''
                        end,
                    N'Error- NULL in computed column'),
        index_op_stats as ISNULL(
                (
                        REPLACE(CONVERT(nvarchar(30), CAST(total_singleton_lookup_count as money), 1), N'.00', N'') +
                        N' singleton lookups; '
                        + REPLACE(CONVERT(nvarchar(30), CAST(total_range_scan_count as money), 1), N'.00', N'') +
                        N' scans/seeks; '
                        + REPLACE(CONVERT(nvarchar(30), CAST(total_leaf_delete_count as money), 1), N'.00', N'') +
                        N' deletes; '
                        + REPLACE(CONVERT(nvarchar(30), CAST(total_leaf_update_count as money), 1), N'.00', N'') +
                        N' updates; '
                        + case
                              when ISNULL(total_forwarded_fetch_count, 0) > 0 then
                                      REPLACE(CONVERT(nvarchar(30), CAST(total_forwarded_fetch_count as money), 1),
                                              N'.00', N'') + N' forward records fetched; '
                              else N'' end

                    /* rows will only be in this dmv when data is in memory for the table */
                    ), N'Table metadata not in memory'),
        index_lock_wait_summary as ISNULL(
                case
                    when total_row_lock_wait_count = 0 and total_page_lock_wait_count = 0 and
                         total_index_lock_promotion_attempt_count = 0 then N'0 lock waits.'
                    else
                            case
                                when total_row_lock_wait_count > 0 then
                                        N'Row lock waits: ' +
                                        REPLACE(CONVERT(nvarchar(30), CAST(total_row_lock_wait_count as money), 1),
                                                N'.00', N'')
                                        + N'; total duration: ' +
                                        case
                                            when total_row_lock_wait_in_ms >= 60000 then /*More than 1 min*/
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST((total_row_lock_wait_in_ms / 60000) as money),
                                                                    1), N'.00', N'') + N' minutes; '
                                            else
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST(ISNULL(total_row_lock_wait_in_ms / 1000, 0) as money),
                                                                    1), N'.00', N'') + N' seconds; '
                                            end
                                        + N'avg duration: ' +
                                        case
                                            when avg_row_lock_wait_in_ms >= 60000 then /*More than 1 min*/
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST((avg_row_lock_wait_in_ms / 60000) as money),
                                                                    1), N'.00', N'') + N' minutes; '
                                            else
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST(ISNULL(avg_row_lock_wait_in_ms / 1000, 0) as money),
                                                                    1), N'.00', N'') + N' seconds; '
                                            end
                                else N''
                                end +
                            case
                                when total_page_lock_wait_count > 0 then
                                        N'Page lock waits: ' +
                                        REPLACE(CONVERT(nvarchar(30), CAST(total_page_lock_wait_count as money), 1),
                                                N'.00', N'')
                                        + N'; total duration: ' +
                                        case
                                            when total_page_lock_wait_in_ms >= 60000 then /*More than 1 min*/
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST((total_page_lock_wait_in_ms / 60000) as money),
                                                                    1), N'.00', N'') + N' minutes; '
                                            else
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST(ISNULL(total_page_lock_wait_in_ms / 1000, 0) as money),
                                                                    1), N'.00', N'') + N' seconds; '
                                            end
                                        + N'avg duration: ' +
                                        case
                                            when avg_page_lock_wait_in_ms >= 60000 then /*More than 1 min*/
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST((avg_page_lock_wait_in_ms / 60000) as money),
                                                                    1), N'.00', N'') + N' minutes; '
                                            else
                                                    REPLACE(CONVERT(nvarchar(30),
                                                                    CAST(ISNULL(avg_page_lock_wait_in_ms / 1000, 0) as money),
                                                                    1), N'.00', N'') + N' seconds; '
                                            end
                                else N''
                                end +
                            case
                                when total_index_lock_promotion_attempt_count > 0 then
                                        N'Lock escalation attempts: ' + REPLACE(CONVERT(nvarchar(30),
                                                                                        CAST(total_index_lock_promotion_attempt_count as money),
                                                                                        1), N'.00', N'')
                                        + N'; Actual Escalations: ' + REPLACE(CONVERT(nvarchar(30),
                                                                                      CAST(ISNULL(total_index_lock_promotion_count, 0) as money),
                                                                                      1), N'.00', N'') + N'.'
                                else N''
                                end
                    end
            , 'Error- NULL in computed column')
    );

    create table #indexcolumns
    (
        [database_id] int not null,
        [schema_name] nvarchar(128),
        [object_id] int not null,
        [index_id] int not null,
        [key_ordinal] int null,
        is_included_column bit null,
        is_descending_key bit null,
        [partition_ordinal] int null,
        column_name nvarchar(256) not null,
        system_type_name nvarchar(256) not null,
        max_length smallint not null,
        [precision] tinyint not null,
        [scale] tinyint not null,
        collation_name nvarchar(256) null,
        is_nullable bit null,
        is_identity bit null,
        is_computed bit null,
        is_replicated bit null,
        is_sparse bit null,
        is_filestream bit null,
        seed_value decimal(38, 0) null,
        increment_value decimal(38, 0) null,
        last_value decimal(38, 0) null,
        is_not_for_replication bit null
    );
    create clustered index clix_database_id_object_id_index_id on #indexcolumns
        (database_id, object_id, index_id);

    create table #missingindexes
    (
        [database_id] int not null,
        [object_id] int not null,
        [database_name] nvarchar(128) not null,
        [schema_name] nvarchar(128) not null,
        [table_name] nvarchar(128),
        [statement] nvarchar(512) not null,
        magic_benefit_number as ((user_seeks + user_scans) * avg_total_user_cost * avg_user_impact),
        avg_total_user_cost numeric(29, 4) not null,
        avg_user_impact numeric(29, 1) not null,
        user_seeks bigint not null,
        user_scans bigint not null,
        unique_compiles bigint null,
        equality_columns nvarchar(4000),
        inequality_columns nvarchar(4000),
        included_columns nvarchar(4000),
        is_low bit,
        [index_estimated_impact] as
                REPLACE(CONVERT(nvarchar(256), CAST(CAST(
                        (user_seeks + user_scans)
                    as bigint) as money), 1), '.00', '') + N' use'
                + case when (user_seeks + user_scans) > 1 then N's' else N'' end
                + N'; Impact: ' + CAST(avg_user_impact as nvarchar(30))
                + N'%; Avg query cost: '
                + CAST(avg_total_user_cost as nvarchar(30)),
        [missing_index_details] as
                case
                    when equality_columns is not null then N'EQUALITY: ' + equality_columns + N' '
                    else N''
                    end + case
                              when inequality_columns is not null then N'INEQUALITY: ' + inequality_columns + N' '
                              else N''
                    end + case
                              when included_columns is not null then N'INCLUDES: ' + included_columns + N' '
                              else N''
                    end,
        [create_tsql] as N'CREATE INDEX ['
            + REPLACE(REPLACE(REPLACE(REPLACE(
                                                  ISNULL(equality_columns, N'') +
                                                  case
                                                      when equality_columns is not null and inequality_columns is not null
                                                          then N'_'
                                                      else N'' end
                                                  + ISNULL(inequality_columns, ''), ',', '')
                                  , '[', ''), ']', ''), ' ', '_')
            + case when included_columns is not null then N'_Includes' else N'' end + N'] ON '
            + [statement] + N' (' + ISNULL(equality_columns, N'')
            + case when equality_columns is not null and inequality_columns is not null then N', ' else N'' end
            + case when inequality_columns is not null then inequality_columns else N'' end +
                         ') ' + case
                                    when included_columns is not null then N' INCLUDE (' + included_columns + N')'
                                    else N'' end
            + N' WITH ('
            + N'FILLFACTOR=100, ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?'
            + N')'
            + N';',
        [more_info] as N'EXEC dbo.sp_BlitzIndex @DatabaseName=' + QUOTENAME([database_name], '''') +
                       N', @SchemaName=' + QUOTENAME([schema_name], '''') + N', @TableName=' +
                       QUOTENAME([table_name], '''') + N';'
    );

    create table #foreignkeys
    (
        [database_id] int not null,
        [database_name] nvarchar(128) not null,
        [schema_name] nvarchar(128) not null,
        foreign_key_name nvarchar(256),
        parent_object_id int,
        parent_object_name nvarchar(256),
        referenced_object_id int,
        referenced_object_name nvarchar(256),
        is_disabled bit,
        is_not_trusted bit,
        is_not_for_replication bit,
        parent_fk_columns nvarchar(max),
        referenced_fk_columns nvarchar(max),
        update_referential_action_desc nvarchar(16),
        delete_referential_action_desc nvarchar(60)
    );

    create table #indexcreatetsql
    (
        index_sanity_id int not null,
        create_tsql nvarchar(max) not null
    );

    create table #databaselist
    (
        databasename nvarchar(256),
        secondary_role_allow_connections_desc nvarchar(50)

    );

    create table #partitioncompressioninfo
    (
        [index_sanity_id] int null,
        [partition_compression_detail] nvarchar(4000) null
    );

    create table #statistics
    (
        database_id int not null,
        database_name nvarchar(256) not null,
        table_name nvarchar(128) null,
        schema_name nvarchar(128) null,
        index_name nvarchar(128) null,
        column_names nvarchar(max) null,
        statistics_name nvarchar(128) null,
        last_statistics_update datetime null,
        days_since_last_stats_update int null,
        rows bigint null,
        rows_sampled bigint null,
        percent_sampled decimal(18, 1) null,
        histogram_steps int null,
        modification_counter bigint null,
        percent_modifications decimal(18, 1) null,
        modifications_before_auto_update int null,
        index_type_desc nvarchar(128) null,
        table_create_date datetime null,
        table_modify_date datetime null,
        no_recompute bit null,
        has_filter bit null,
        filter_definition nvarchar(max) null
    );

    create table #computedcolumns
    (
        index_sanity_id int identity (1, 1) not null,
        database_name nvarchar(128) null,
        database_id int not null,
        table_name nvarchar(128) not null,
        schema_name nvarchar(128) not null,
        column_name nvarchar(128) null,
        is_nullable bit null,
        definition nvarchar(max) null,
        uses_database_collation bit not null,
        is_persisted bit not null,
        is_computed bit not null,
        is_function int not null,
        column_definition nvarchar(max) null
    );

    create table #tracestatus
    (
        traceflag nvarchar(10),
        status bit,
        global bit,
        session bit
    );

    create table #temporaltables
    (
        index_sanity_id int identity (1, 1) not null,
        database_name nvarchar(128) not null,
        database_id int not null,
        schema_name nvarchar(128) not null,
        table_name nvarchar(128) not null,
        history_table_name nvarchar(128) not null,
        history_schema_name nvarchar(128) not null,
        start_column_name nvarchar(128) not null,
        end_column_name nvarchar(128) not null,
        period_name nvarchar(128) not null
    );

    create table #checkconstraints
    (
        index_sanity_id int identity (1, 1) not null,
        database_name nvarchar(128) null,
        database_id int not null,
        table_name nvarchar(128) not null,
        schema_name nvarchar(128) not null,
        constraint_name nvarchar(128) null,
        is_disabled bit null,
        definition nvarchar(max) null,
        uses_database_collation bit not null,
        is_not_trusted bit not null,
        is_function int not null,
        column_definition nvarchar(max) null
    );

    create table #filteredindexes
    (
        index_sanity_id int identity (1, 1) not null,
        database_name nvarchar(128) null,
        database_id int not null,
        schema_name nvarchar(128) not null,
        table_name nvarchar(128) not null,
        index_name nvarchar(128) null,
        column_name nvarchar(128) null
    );

    create table #ignore_databases
    (
        databasename nvarchar(128),
        reason nvarchar(100)
    );

/* Sanitize our inputs */
select @outputservername = QUOTENAME(@outputservername),
       @outputdatabasename = QUOTENAME(@outputdatabasename),
       @outputschemaname = QUOTENAME(@outputschemaname),
       @outputtablename = QUOTENAME(@outputtablename);


    if @getalldatabases = 1
        begin
            insert into #databaselist (databasename)
            select DB_NAME(database_id)
            from sys.databases
            where user_access_desc = 'MULTI_USER'
              and state_desc = 'ONLINE'
              and database_id > 4
              and DB_NAME(database_id) not like 'ReportServer%'
              and DB_NAME(database_id) not like 'rdsadmin%'
              and is_distributor = 0
            option ( recompile );

            /* Skip non-readable databases in an AG - see Github issue #1160 */
            if EXISTS(select *
                      from sys.all_objects o
                               inner join sys.all_columns c on o.object_id = c.object_id and
                                                               o.name = 'dm_hadr_availability_replica_states' and
                                                               c.name = 'role_desc')
                begin
                    set @dsql = N'UPDATE #DatabaseList SET secondary_role_allow_connections_desc = ''NO'' WHERE DatabaseName IN (
                        SELECT d.name
                        FROM sys.dm_hadr_availability_replica_states rs
                        INNER JOIN sys.databases d ON rs.replica_id = d.replica_id
                        INNER JOIN sys.availability_replicas r ON rs.replica_id = r.replica_id
                        WHERE rs.role_desc = ''SECONDARY''
                        AND r.secondary_role_allow_connections_desc = ''NO'')
						OPTION    ( RECOMPILE );';
                    exec sp_executesql @dsql;

                    if EXISTS(select * from #databaselist where secondary_role_allow_connections_desc = 'NO')
                        begin
                            insert #blitzindexresults (priority, check_id, findings_group, finding, database_name, url,
                                                       details, index_definition,
                                                       index_usage_summary, index_size_summary)
                            values (1,
                                    0,
                                    N'Skipped non-readable AG secondary databases.',
                                    N'You are running this on an AG secondary, and some of your databases are configured as non-readable when this is a secondary node.',
                                    N'To analyze those databases, run sp_BlitzIndex on the primary, or on a readable secondary.',
                                    'http://FirstResponderKit.org', '', '', '', '');
                        end;
                end;

            if @ignoredatabases is not null
                and LEN(@ignoredatabases) > 0
                begin
                    raiserror (N'Setting up filter to ignore databases', 0, 1) with nowait;
                    set @databasetoignore = '';

                    while LEN(@ignoredatabases) > 0
                        begin
                            if PATINDEX('%,%', @ignoredatabases) > 0
                                begin
                                    set @databasetoignore =
                                            SUBSTRING(@ignoredatabases, 0, PATINDEX('%,%', @ignoredatabases));

                                    insert into #ignore_databases (databasename, reason)
                                    select LTRIM(RTRIM(@databasetoignore)),
                                           'Specified in the @IgnoreDatabases parameter'
                                    option (recompile);

                                    set @ignoredatabases = SUBSTRING(@ignoredatabases, LEN(@databasetoignore + ',') + 1,
                                                                     LEN(@ignoredatabases));
                                end;
                            else
                                begin
                                    set @databasetoignore = @ignoredatabases;
                                    set @ignoredatabases = null;

                                    insert into #ignore_databases (databasename, reason)
                                    select LTRIM(RTRIM(@databasetoignore)),
                                           'Specified in the @IgnoreDatabases parameter'
                                    option (recompile);
                                end;
                        end;

                end

        end;
    else
        begin
            insert into #databaselist
                (databasename)
            select case
                       when @databasename is null or @databasename = N''
                           then DB_NAME()
                       else @databasename end;
        end;

    set @numdatabases = (select COUNT(*)
                         from #databaselist);
    set @msg = N'Number of databases to examine: ' + CAST(@numdatabases as nvarchar(50));
    raiserror (@msg,0,1) with nowait;


/* Running on 50+ databases can take a reaaallly long time, so we want explicit permission to do so (and only after warning about it) */


begin try
    if @numdatabases >= 50 and @bringthepain != 1 and @tablename is null
        begin

            insert #blitzindexresults (priority, check_id, findings_group, finding, url, details, index_definition,
                                       index_usage_summary, index_size_summary)
            values (-1,
                    0,
                    @scriptversionname,
                    case
                        when @getalldatabases = 1 then N'All Databases'
                        else N'Database ' + QUOTENAME(@databasename) + N' as of ' +
                             CONVERT(nvarchar(16), GETDATE(), 121) end,
                    N'From Your Community Volunteers',
                    N'http://FirstResponderKit.org',
                    N'',
                    N'',
                    N'');
            insert #blitzindexresults (priority, check_id, findings_group, finding, database_name, url, details,
                                       index_definition,
                                       index_usage_summary, index_size_summary)
            values (1,
                    0,
                    N'You''re trying to run sp_BlitzIndex on a server with ' + CAST(@numdatabases as nvarchar(8)) +
                    N' databases. ',
                    N'Running sp_BlitzIndex on a server with 50+ databases may cause temporary insanity for the server and/or user.',
                    N'If you''re sure you want to do this, run again with the parameter @BringThePain = 1.',
                    'http://FirstResponderKit.org',
                    '',
                    '',
                    '',
                    '');

            if (@outputtype <> 'NONE')
                begin
                    select bir.blitz_result_id,
                           bir.check_id,
                           bir.index_sanity_id,
                           bir.priority,
                           bir.findings_group,
                           bir.finding,
                           bir.database_name,
                           bir.url,
                           bir.details,
                           bir.index_definition,
                           bir.secret_columns,
                           bir.index_usage_summary,
                           bir.index_size_summary,
                           bir.create_tsql,
                           bir.more_info
                    from #blitzindexresults as bir;
                    raiserror ('Running sp_BlitzIndex on a server with 50+ databases may cause temporary insanity for the server', 12, 1);
                end;

            return;

        end;
end try
begin catch
    raiserror (N'Failure to execute due to number of databases.', 0,1) with nowait;

    select @msg = ERROR_MESSAGE(),
           @errorseverity = ERROR_SEVERITY(),
           @errorstate = ERROR_STATE();

    raiserror (@msg, @errorseverity, @errorstate);

    while @@trancount > 0
        rollback;

    return;
end catch;


    raiserror (N'Checking partition counts to exclude databases with over 100 partitions',0,1) with nowait;
    if @bringthepain = 0 and @skippartitions = 0 and @tablename is null
        begin
            declare partition_cursor cursor for
                select dl.databasename
                from #databaselist dl
                         left outer join #ignore_databases i on dl.databasename = i.databasename
                where COALESCE(dl.secondary_role_allow_connections_desc, 'OK') <> 'NO'
                  and i.databasename is null

            open partition_cursor
            fetch next from partition_cursor into @databasename

            while @@FETCH_STATUS = 0
                begin
                    /* Count the total number of partitions */
                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    SELECT @RowcountOUT = SUM(1) FROM ' + QUOTENAME(@databasename) +
                                '.sys.partitions WHERE partition_number > 1 OPTION    ( RECOMPILE );';
                    exec sp_executesql @dsql, N'@RowcountOUT BIGINT OUTPUT', @rowcountout = @rowcount output;
                    if @rowcount > 100
                        begin
                            raiserror (N'Skipping database %s because > 100 partitions were found. To check this database, you must set @BringThePain = 1.',0,1,@databasename) with nowait;
                            insert into #ignore_databases (databasename, reason)
                            select @databasename, 'Over 100 partitions found - use @BringThePain = 1 to analyze'
                        end;
                    fetch next from partition_cursor into @databasename
                end;
            close partition_cursor
            deallocate partition_cursor

        end;

insert #blitzindexresults (priority, check_id, findings_group, finding, url, details, index_definition,
                           index_usage_summary, index_size_summary)
select 1,
       0,
       'Database Skipped',
       i.databasename,
       'http://FirstResponderKit.org',
       i.reason,
       '',
       '',
       ''
from #ignore_databases i;


/* Last startup */
select @daysuptime = CAST(DATEDIFF(hour, create_date, GETDATE()) / 24. as numeric(23, 2))
from sys.databases
where database_id = 2;

    if @daysuptime = 0 or @daysuptime is null
        set @daysuptime = .01;

select @daysuptimeinsertvalue =
       'Server: ' + (CONVERT(varchar(256), (SERVERPROPERTY('ServerName')))) + ' Days Uptime: ' + RTRIM(@daysuptime);


/* Permission granted or unnecessary? Ok, let's go! */

    raiserror (N'Starting loop through databases',0,1) with nowait;
declare
    c1 cursor
        local fast_forward
        for
        select dl.databasename
        from #databaselist dl
                 left outer join #ignore_databases i on dl.databasename = i.databasename
        where COALESCE(dl.secondary_role_allow_connections_desc, 'OK') <> 'NO'
          and i.databasename is null
        order by dl.databasename;

    open c1;
    fetch next from c1 into @databasename;
    while @@FETCH_STATUS = 0
        begin

            raiserror (@linefeed, 0, 1) with nowait;
            raiserror (@linefeed, 0, 1) with nowait;
            raiserror (@databasename, 0, 1) with nowait;

            select @databaseid = [database_id]
            from sys.databases
            where [name] = @databasename
              and user_access_desc = 'MULTI_USER'
              and state_desc = 'ONLINE';

            ----------------------------------------
--STEP 1: OBSERVE THE PATIENT
--This step puts index information into temp tables.
----------------------------------------
            begin try
                begin

                    --Validate SQL Server Version

                    if (select LEFT(@sqlserverproductversion,
                                    CHARINDEX('.', @sqlserverproductversion, 0) - 1
                                   )) <= 9
                        begin
                            set @msg =
                                        N'sp_BlitzIndex is only supported on SQL Server 2008 and higher. The version of this instance is: ' +
                                        @sqlserverproductversion;
                            raiserror (@msg,16,1);
                        end;

                    --Short circuit here if database name does not exist.
                    if @databasename is null or @databaseid is null
                        begin
                            set @msg = 'Database does not exist or is not online/multi-user: cannot proceed.';
                            raiserror (@msg,16,1);
                        end;

                    --Validate parameters.
                    if (@mode not in (0, 1, 2, 3, 4))
                        begin
                            set @msg =
                                    N'Invalid @Mode parameter. 0=diagnose, 1=summarize, 2=index detail, 3=missing index detail, 4=diagnose detail';
                            raiserror (@msg,16,1);
                        end;

                    if (@mode <> 0 and @tablename is not null)
                        begin
                            set @msg =
                                    N'Setting the @Mode doesn''t change behavior if you supply @TableName. Use default @Mode=0 to see table detail.';
                            raiserror (@msg,16,1);
                        end;

                    if ((@mode <> 0 or @tablename is not null) and @filter <> 0)
                        begin
                            set @msg =
                                    N'@Filter only applies when @Mode=0 and @TableName is not specified. Please try again.';
                            raiserror (@msg,16,1);
                        end;

                    if (@schemaname is not null and @tablename is null)
                        begin
                            set @msg =
                                    'We can''t run against a whole schema! Specify a @TableName, or leave both NULL for diagnosis.';
                            raiserror (@msg,16,1);
                        end;


                    if (@tablename is not null and @schemaname is null)
                        begin
                            set @schemaname = N'dbo';
                            set @msg = '@SchemaName wasn''t specified-- assuming schema=dbo.';
                            raiserror (@msg,1,1) with nowait;
                        end;

                    --If a table is specified, grab the object id.
                    --Short circuit if it doesn't exist.
                    if @tablename is not null
                        begin
                            set @dsql = N'
                    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                    SELECT  @ObjectID= OBJECT_ID
                    FROM    ' + QUOTENAME(@databasename) + N'.sys.objects AS so
                    JOIN    ' + QUOTENAME(@databasename) + N'.sys.schemas AS sc on
                        so.schema_id=sc.schema_id
                    where so.type in (''U'', ''V'')
                    and so.name=' + QUOTENAME(@tablename, '''') + N'
                    and sc.name=' + QUOTENAME(@schemaname, '''') + N'
                    /*Has a row in sys.indexes. This lets us get indexed views.*/
                    and exists (
                        SELECT si.name
                        FROM ' + QUOTENAME(@databasename) + '.sys.indexes AS si
                        WHERE so.object_id=si.object_id)
                    OPTION (RECOMPILE);';

                            set @params = '@ObjectID INT OUTPUT';

                            if @dsql is null
                                raiserror ('@dsql is null',16,1);

                            exec sp_executesql @dsql, @params, @objectid=@objectid output;

                            if @objectid is null
                                begin
                                    set @msg =
                                                N'Oh, this is awkward. I can''t find the table or indexed view you''re looking for in that database.' +
                                                CHAR(10) +
                                                N'Please check your parameters.';
                                    raiserror (@msg,1,1);
                                    return;
                                end;
                        end;

                    --set @collation
                    select @collation = collation_name
                    from sys.databases
                    where database_id = @databaseid;

                    --insert columns for clustered indexes and heaps
                    --collect info on identity columns for this one
                    set @dsql = N'/* sp_BlitzIndex */
				SET LOCK_TIMEOUT 1000; /* To fix locking bug in sys.identity_columns. See Github issue #2176. */
				SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT ' + CAST(@databaseid as nvarchar(16)) + ',
					s.name,
                    si.object_id,
                    si.index_id,
                    sc.key_ordinal,
                    sc.is_included_column,
                    sc.is_descending_key,
                    sc.partition_ordinal,
                    c.name as column_name,
                    st.name as system_type_name,
                    c.max_length,
                    c.[precision],
                    c.[scale],
                    c.collation_name,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.is_replicated,
                    ' + case
                                                                                                                           when @sqlserverproductversion not like '9%'
                                                                                                                               then N'c.is_sparse'
                                                                                                                           else N'NULL as is_sparse' end + N',
                    ' + case
                                                                                                                                                                                       when @sqlserverproductversion not like '9%'
                                                                                                                                                                                           then N'c.is_filestream'
                                                                                                                                                                                       else N'NULL as is_filestream' end + N',
                    CAST(ic.seed_value AS DECIMAL(38,0)),
                    CAST(ic.increment_value AS DECIMAL(38,0)),
                    CAST(ic.last_value AS DECIMAL(38,0)),
                    ic.is_not_for_replication
                FROM    ' + QUOTENAME(@databasename) + N'.sys.indexes si
                JOIN    ' + QUOTENAME(@databasename) + N'.sys.columns c ON
                    si.object_id=c.object_id
                LEFT JOIN ' + QUOTENAME(@databasename) + N'.sys.index_columns sc ON
                    sc.object_id = si.object_id
                    and sc.index_id=si.index_id
                    AND sc.column_id=c.column_id
                LEFT JOIN ' + QUOTENAME(@databasename) + N'.sys.identity_columns ic ON
                    c.object_id=ic.object_id and
                    c.column_id=ic.column_id
                JOIN ' + QUOTENAME(@databasename) + N'.sys.types st ON
                    c.system_type_id=st.system_type_id
                    AND c.user_type_id=st.user_type_id
				JOIN ' + QUOTENAME(@databasename) + N'.sys.objects AS so  ON si.object_id = so.object_id
																		  AND so.is_ms_shipped = 0
				JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                WHERE si.index_id in (0,1) '
                        + case
                              when @objectid is not null
                                  then N' AND si.object_id=' + CAST(@objectid as nvarchar(30))
                              else N'' end
                        + N'OPTION (RECOMPILE);';

                    if @dsql is null
                        raiserror ('@dsql is null',16,1);

                    raiserror (N'Inserting data into #IndexColumns for clustered indexes and heaps',0,1) with nowait;
                    if @debug = 1
                        begin
                            print SUBSTRING(@dsql, 0, 4000);
                            print SUBSTRING(@dsql, 4000, 8000);
                            print SUBSTRING(@dsql, 8000, 12000);
                            print SUBSTRING(@dsql, 12000, 16000);
                            print SUBSTRING(@dsql, 16000, 20000);
                            print SUBSTRING(@dsql, 20000, 24000);
                            print SUBSTRING(@dsql, 24000, 28000);
                            print SUBSTRING(@dsql, 28000, 32000);
                            print SUBSTRING(@dsql, 32000, 36000);
                            print SUBSTRING(@dsql, 36000, 40000);
                        end;
                    begin try
                        insert #indexcolumns (database_id, [schema_name], [object_id], index_id, key_ordinal,
                                              is_included_column, is_descending_key, partition_ordinal,
                                              column_name, system_type_name, max_length, precision, scale,
                                              collation_name, is_nullable, is_identity, is_computed,
                                              is_replicated, is_sparse, is_filestream, seed_value, increment_value,
                                              last_value, is_not_for_replication)
                            exec sp_executesql @dsql;
                    end try
                    begin catch
                        raiserror (N'Failure inserting data into #IndexColumns for clustered indexes and heaps.', 0,1) with nowait;

                        if @dsql is not null
                            begin
                                set @msg = 'Last @dsql: ' + @dsql;
                                raiserror (@msg, 0, 1) with nowait;
                            end;

                        select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
                               @errorseverity = 0,
                               @errorstate = ERROR_STATE();
                        raiserror (@msg,@errorseverity, @errorstate ) with nowait;

                        while @@trancount > 0
                            rollback;

                        return;
                    end catch;


                    --insert columns for nonclustered indexes
                    --this uses a full join to sys.index_columns
                    --We don't collect info on identity columns here. They may be in NC indexes, but we just analyze identities in the base table.
                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT ' + CAST(@databaseid as nvarchar(16)) + ',
					s.name,
                    si.object_id,
                    si.index_id,
                    sc.key_ordinal,
                    sc.is_included_column,
                    sc.is_descending_key,
                    sc.partition_ordinal,
                    c.name as column_name,
                    st.name as system_type_name,
                    c.max_length,
                    c.[precision],
                    c.[scale],
                    c.collation_name,
                    c.is_nullable,
                    c.is_identity,
                    c.is_computed,
                    c.is_replicated,
                    ' + case
                                                                                                                           when @sqlserverproductversion not like '9%'
                                                                                                                               then N'c.is_sparse'
                                                                                                                           else N'NULL AS is_sparse' end + N',
                    ' + case
                                                                                                                                                                                       when @sqlserverproductversion not like '9%'
                                                                                                                                                                                           then N'c.is_filestream'
                                                                                                                                                                                       else N'NULL AS is_filestream' end + N'
                FROM    ' + QUOTENAME(@databasename) + N'.sys.indexes AS si
                JOIN    ' + QUOTENAME(@databasename) + N'.sys.columns AS c ON
                    si.object_id=c.object_id
                JOIN ' + QUOTENAME(@databasename) + N'.sys.index_columns AS sc ON
                    sc.object_id = si.object_id
                    and sc.index_id=si.index_id
                    AND sc.column_id=c.column_id
                JOIN ' + QUOTENAME(@databasename) + N'.sys.types AS st ON
                    c.system_type_id=st.system_type_id
                    AND c.user_type_id=st.user_type_id
				JOIN ' + QUOTENAME(@databasename) + N'.sys.objects AS so  ON si.object_id = so.object_id
																		  AND so.is_ms_shipped = 0
				JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas AS s ON s.schema_id = so.schema_id
                WHERE si.index_id not in (0,1) '
                        + case
                              when @objectid is not null
                                  then N' AND si.object_id=' + CAST(@objectid as nvarchar(30))
                              else N'' end
                        + N'OPTION (RECOMPILE);';

                    if @dsql is null
                        raiserror ('@dsql is null',16,1);

                    raiserror (N'Inserting data into #IndexColumns for nonclustered indexes',0,1) with nowait;
                    if @debug = 1
                        begin
                            print SUBSTRING(@dsql, 0, 4000);
                            print SUBSTRING(@dsql, 4000, 8000);
                            print SUBSTRING(@dsql, 8000, 12000);
                            print SUBSTRING(@dsql, 12000, 16000);
                            print SUBSTRING(@dsql, 16000, 20000);
                            print SUBSTRING(@dsql, 20000, 24000);
                            print SUBSTRING(@dsql, 24000, 28000);
                            print SUBSTRING(@dsql, 28000, 32000);
                            print SUBSTRING(@dsql, 32000, 36000);
                            print SUBSTRING(@dsql, 36000, 40000);
                        end;
                    insert #indexcolumns (database_id, [schema_name], [object_id], index_id, key_ordinal,
                                          is_included_column, is_descending_key, partition_ordinal,
                                          column_name, system_type_name, max_length, precision, scale, collation_name,
                                          is_nullable, is_identity, is_computed,
                                          is_replicated, is_sparse, is_filestream)
                        exec sp_executesql @dsql;

                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT  ' + CAST(@databaseid as nvarchar(10)) + N' AS database_id,
                        so.object_id,
                        si.index_id,
                        si.type,
                        @i_DatabaseName AS database_name,
                        COALESCE(sc.NAME, ''Unknown'') AS [schema_name],
                        COALESCE(so.name, ''Unknown'') AS [object_name],
                        COALESCE(si.name, ''Unknown'') AS [index_name],
                        CASE    WHEN so.[type] = CAST(''V'' AS CHAR(2)) THEN 1 ELSE 0 END,
                        si.is_unique,
                        si.is_primary_key,
                        CASE when si.type = 3 THEN 1 ELSE 0 END AS is_XML,
                        CASE when si.type = 4 THEN 1 ELSE 0 END AS is_spatial,
                        CASE when si.type = 6 THEN 1 ELSE 0 END AS is_NC_columnstore,
                        CASE when si.type = 5 then 1 else 0 end as is_CX_columnstore,
                        CASE when si.data_space_id = 0 then 1 else 0 end as is_in_memory_oltp,
                        si.is_disabled,
                        si.is_hypothetical,
                        si.is_padded,
                        si.fill_factor,'
                        + case
                              when @sqlserverproductversion not like '9%' then N'
                        CASE WHEN si.filter_definition IS NOT NULL THEN si.filter_definition
                             ELSE N''''
                        END AS filter_definition'
                              else N''''' AS filter_definition' end + N'
                        , ISNULL(us.user_seeks, 0),
                        ISNULL(us.user_scans, 0),
                        ISNULL(us.user_lookups, 0),
                        ISNULL(us.user_updates, 0),
                        us.last_user_seek,
                        us.last_user_scan,
                        us.last_user_lookup,
                        us.last_user_update,
                        so.create_date,
                        so.modify_date
                FROM    ' + QUOTENAME(@databasename) + N'.sys.indexes AS si WITH (NOLOCK)
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.objects AS so WITH (NOLOCK) ON si.object_id = so.object_id
                                               AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                                               AND so.type <> ''TF'' /*Exclude table valued functions*/
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas sc ON so.schema_id = sc.schema_id
                        LEFT JOIN sys.dm_db_index_usage_stats AS us WITH (NOLOCK) ON si.[object_id] = us.[object_id]
                                                                       AND si.index_id = us.index_id
                                                                       AND us.database_id = ' +
                                CAST(@databaseid as nvarchar(10)) + N'
                WHERE    si.[type] IN ( 0, 1, 2, 3, 4, 5, 6 )
                /* Heaps, clustered, nonclustered, XML, spatial, Cluster Columnstore, NC Columnstore */ ' +
                                case
                                    when @tablename is not null
                                        then N' and so.name=' + QUOTENAME(@tablename, N'''') + N' '
                                    else N'' end +
                                case
                                    when (@includeinactiveindexes = 0
                                        and @mode in (0, 4)
                                        and @tablename is null)
                                        then N'AND ( us.user_seeks + us.user_scans + us.user_lookups + us.user_updates ) > 0'
                                    else N''
                                    end
                        + N'OPTION    ( RECOMPILE );
        ';
                    if @dsql is null
                        raiserror ('@dsql is null',16,1);

                    raiserror (N'Inserting data into #IndexSanity',0,1) with nowait;
                    if @debug = 1
                        begin
                            print SUBSTRING(@dsql, 0, 4000);
                            print SUBSTRING(@dsql, 4000, 8000);
                            print SUBSTRING(@dsql, 8000, 12000);
                            print SUBSTRING(@dsql, 12000, 16000);
                            print SUBSTRING(@dsql, 16000, 20000);
                            print SUBSTRING(@dsql, 20000, 24000);
                            print SUBSTRING(@dsql, 24000, 28000);
                            print SUBSTRING(@dsql, 28000, 32000);
                            print SUBSTRING(@dsql, 32000, 36000);
                            print SUBSTRING(@dsql, 36000, 40000);
                        end;
                    insert #indexsanity ([database_id], [object_id], [index_id], [index_type], [database_name],
                                         [schema_name], [object_name],
                                         index_name, is_indexed_view, is_unique, is_primary_key, is_xml, is_spatial,
                                         is_nc_columnstore, is_cx_columnstore, is_in_memory_oltp,
                                         is_disabled, is_hypothetical, is_padded, fill_factor, filter_definition,
                                         user_seeks, user_scans,
                                         user_lookups, user_updates, last_user_seek, last_user_scan, last_user_lookup,
                                         last_user_update,
                                         create_date, modify_date)
                        exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                             @i_databasename = @databasename;


                    raiserror (N'Checking partition count',0,1) with nowait;
                    if @bringthepain = 0 and @skippartitions = 0 and @tablename is null
                        begin
                            /* Count the total number of partitions */
                            set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT @RowcountOUT = SUM(1) FROM ' + QUOTENAME(@databasename) +
                                        '.sys.partitions WHERE partition_number > 1 OPTION    ( RECOMPILE );';
                            exec sp_executesql @dsql, N'@RowcountOUT BIGINT OUTPUT', @rowcountout = @rowcount output;
                            if @rowcount > 100
                                begin
                                    raiserror (N'Setting @SkipPartitions = 1 because > 100 partitions were found. To check them, you must set @BringThePain = 1.',0,1) with nowait;
                                    set @skippartitions = 1;
                                    insert #blitzindexresults (priority, check_id, findings_group, finding, url,
                                                               details, index_definition,
                                                               index_usage_summary, index_size_summary)
                                    values (1, 0,
                                            'Some Checks Were Skipped',
                                            '@SkipPartitions Forced to 1',
                                            'http://FirstResponderKit.org', CAST(@rowcount as nvarchar(50)) +
                                                                            ' partitions found. To analyze them, use @BringThePain = 1.',
                                            'We try to keep things quick - and warning, running @BringThePain = 1 can take tens of minutes.',
                                            '', '');
                                end;
                        end;


                    if (@skippartitions = 0)
                        begin
                            if (select LEFT(@sqlserverproductversion,
                                            CHARINDEX('.', @sqlserverproductversion, 0) - 1)) <=
                               2147483647 --Make change here
                                begin

                                    raiserror (N'Preferring non-2012 syntax with LEFT JOIN to sys.dm_db_index_operational_stats',0,1) with nowait;

                                    --NOTE: If you want to use the newer syntax for 2012+, you'll have to change 2147483647 to 11 on line ~819
                                    --This change was made because on a table with lots of paritions, the OUTER APPLY was crazy slow.
                                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT  ' + CAST(@databaseid as nvarchar(10)) + ' AS database_id,
                                ps.object_id,
								s.name,
                                ps.index_id,
                                ps.partition_number,
                                ps.row_count,
                                ps.reserved_page_count * 8. / 1024. AS reserved_MB,
                                ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
                                ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
                            ' + case
                                                                                                                                                            when @sqlserverproductversion not like '9%'
                                                                                                                                                                then N'par.data_compression_desc '
                                                                                                                                                            else N'null as data_compression_desc ' end + N',
                                SUM(os.leaf_insert_count),
                                SUM(os.leaf_delete_count),
                                SUM(os.leaf_update_count),
                                SUM(os.range_scan_count),
                                SUM(os.singleton_lookup_count),
                                SUM(os.forwarded_fetch_count),
                                SUM(os.lob_fetch_in_pages),
                                SUM(os.lob_fetch_in_bytes),
                                SUM(os.row_overflow_fetch_in_pages),
                                SUM(os.row_overflow_fetch_in_bytes),
                                SUM(os.row_lock_count),
                                SUM(os.row_lock_wait_count),
                                SUM(os.row_lock_wait_in_ms),
                                SUM(os.page_lock_count),
                                SUM(os.page_lock_wait_count),
                                SUM(os.page_lock_wait_in_ms),
                                SUM(os.index_lock_promotion_attempt_count),
                                SUM(os.index_lock_promotion_count),
								SUM(os.page_latch_wait_count),
								SUM(os.page_latch_wait_in_ms),
								SUM(os.page_io_latch_wait_count),
								SUM(os.page_io_latch_wait_in_ms)
                    FROM    ' + QUOTENAME(@databasename) + '.sys.dm_db_partition_stats AS ps
                    JOIN ' + QUOTENAME(@databasename) + '.sys.partitions AS par on ps.partition_id=par.partition_id
                    JOIN ' + QUOTENAME(@databasename) + '.sys.objects AS so ON ps.object_id = so.object_id
                               AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                               AND so.type <> ''TF'' /*Exclude table valued functions*/
					JOIN ' + QUOTENAME(@databasename) + '.sys.schemas AS s ON s.schema_id = so.schema_id
                    LEFT JOIN ' + QUOTENAME(@databasename) + '.sys.dm_db_index_operational_stats('
                                        + CAST(@databaseid as nvarchar(10)) + ', NULL, NULL,NULL) AS os ON
                    ps.object_id=os.object_id and ps.index_id=os.index_id and ps.partition_number=os.partition_number
                    WHERE 1=1
                    ' + case
                                                                                                          when @objectid is not null
                                                                                                              then N'AND so.object_id=' + CAST(@objectid as nvarchar(30)) + N' '
                                                                                                          else N' ' end + '
                    ' + case
                                                                                                                                                      when @filter = 2
                                                                                                                                                          then
                                                                                                                                                              N'AND ps.reserved_page_count * 8./1024. > ' +
                                                                                                                                                              CAST(@filtermb as nvarchar(5)) +
                                                                                                                                                              N' '
                                                                                                                                                      else N' ' end + '
            GROUP BY ps.object_id,
								s.name,
                                ps.index_id,
                                ps.partition_number,
                                ps.row_count,
                                ps.reserved_page_count,
                                ps.lob_reserved_page_count,
                                ps.row_overflow_reserved_page_count,
                            ' + case
                                                                                                                                                                                                          when @sqlserverproductversion not like '9%'
                                                                                                                                                                                                              then N'par.data_compression_desc '
                                                                                                                                                                                                          else N'null as data_compression_desc ' end + N'
			ORDER BY ps.object_id,  ps.index_id, ps.partition_number
            OPTION    ( RECOMPILE );
            ';
                                end;
                            else
                                begin
                                    raiserror (N'Using 2012 syntax to query sys.dm_db_index_operational_stats',0,1) with nowait;
                                    --This is the syntax that will be used if you change 2147483647 to 11 on line ~819.
                                    --If you have a lot of paritions and this suddenly starts running for a long time, change it back.
                                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                        SELECT  ' + CAST(@databaseid as nvarchar(10)) + ' AS database_id,
                                ps.object_id,
								s.name,
                                ps.index_id,
                                ps.partition_number,
                                ps.row_count,
                                ps.reserved_page_count * 8. / 1024. AS reserved_MB,
                                ps.lob_reserved_page_count * 8. / 1024. AS reserved_LOB_MB,
                                ps.row_overflow_reserved_page_count * 8. / 1024. AS reserved_row_overflow_MB,
                                ' + case
                                                                                                                                                                when @sqlserverproductversion not like '9%'
                                                                                                                                                                    then N'par.data_compression_desc '
                                                                                                                                                                else N'null as data_compression_desc' end + N',
                                SUM(os.leaf_insert_count),
                                SUM(os.leaf_delete_count),
                                SUM(os.leaf_update_count),
                                SUM(os.range_scan_count),
                                SUM(os.singleton_lookup_count),
                                SUM(os.forwarded_fetch_count),
                                SUM(os.lob_fetch_in_pages),
                                SUM(os.lob_fetch_in_bytes),
                                SUM(os.row_overflow_fetch_in_pages),
                                SUM(os.row_overflow_fetch_in_bytes),
                                SUM(os.row_lock_count),
                                SUM(os.row_lock_wait_count),
                                SUM(os.row_lock_wait_in_ms),
                                SUM(os.page_lock_count),
                                SUM(os.page_lock_wait_count),
                                SUM(os.page_lock_wait_in_ms),
                                SUM(os.index_lock_promotion_attempt_count),
                                SUM(os.index_lock_promotion_count),
								SUM(os.page_latch_wait_count),
								SUM(os.page_latch_wait_in_ms),
								SUM(os.page_io_latch_wait_count),
								SUM(os.page_io_latch_wait_in_ms)
                        FROM    ' + QUOTENAME(@databasename) + N'.sys.dm_db_partition_stats AS ps
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.partitions AS par on ps.partition_id=par.partition_id
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.objects AS so ON ps.object_id = so.object_id
                                   AND so.is_ms_shipped = 0 /*Exclude objects shipped by Microsoft*/
                                   AND so.type <> ''TF'' /*Exclude table valued functions*/
						JOIN ' + QUOTENAME(@databasename) + '.sys.schemas AS s ON s.schema_id = so.schema_id
                        OUTER APPLY ' + QUOTENAME(@databasename) + N'.sys.dm_db_index_operational_stats('
                                        + CAST(@databaseid as nvarchar(10)) + N', ps.object_id, ps.index_id,ps.partition_number) AS os
                        WHERE 1=1
                        ' + case
                                                                                                              when @objectid is not null
                                                                                                                  then N'AND so.object_id=' + CAST(@objectid as nvarchar(30)) + N' '
                                                                                                              else N' ' end + N'
                        ' + case
                                                                                                                                                              when @filter = 2
                                                                                                                                                                  then
                                                                                                                                                                      N'AND ps.reserved_page_count * 8./1024. > ' +
                                                                                                                                                                      CAST(@filtermb as nvarchar(5)) +
                                                                                                                                                                      N' '
                                                                                                                                                              else N' ' end + '
	            GROUP BY ps.object_id,
								s.name,
                                ps.index_id,
                                ps.partition_number,
                                ps.row_count,
                                ps.reserved_page_count,
                                ps.lob_reserved_page_count,
                                ps.row_overflow_reserved_page_count,
                            ' + case
                                                                                                                                                                                                                  when @sqlserverproductversion not like '9%'
                                                                                                                                                                                                                      then N'par.data_compression_desc '
                                                                                                                                                                                                                  else N'null as data_compression_desc ' end + N'
				ORDER BY ps.object_id,  ps.index_id, ps.partition_number
                OPTION    ( RECOMPILE );
                ';
                                end;

                            if @dsql is null
                                raiserror ('@dsql is null',16,1);

                            raiserror (N'Inserting data into #IndexPartitionSanity',0,1) with nowait;
                            if @debug = 1
                                begin
                                    print SUBSTRING(@dsql, 0, 4000);
                                    print SUBSTRING(@dsql, 4000, 8000);
                                    print SUBSTRING(@dsql, 8000, 12000);
                                    print SUBSTRING(@dsql, 12000, 16000);
                                    print SUBSTRING(@dsql, 16000, 20000);
                                    print SUBSTRING(@dsql, 20000, 24000);
                                    print SUBSTRING(@dsql, 24000, 28000);
                                    print SUBSTRING(@dsql, 28000, 32000);
                                    print SUBSTRING(@dsql, 32000, 36000);
                                    print SUBSTRING(@dsql, 36000, 40000);
                                end;
                            insert #indexpartitionsanity ([database_id],
                                                          [object_id],
                                                          [schema_name],
                                                          index_id,
                                                          partition_number,
                                                          row_count,
                                                          reserved_mb,
                                                          reserved_lob_mb,
                                                          reserved_row_overflow_mb,
                                                          data_compression_desc,
                                                          leaf_insert_count,
                                                          leaf_delete_count,
                                                          leaf_update_count,
                                                          range_scan_count,
                                                          singleton_lookup_count,
                                                          forwarded_fetch_count,
                                                          lob_fetch_in_pages,
                                                          lob_fetch_in_bytes,
                                                          row_overflow_fetch_in_pages,
                                                          row_overflow_fetch_in_bytes,
                                                          row_lock_count,
                                                          row_lock_wait_count,
                                                          row_lock_wait_in_ms,
                                                          page_lock_count,
                                                          page_lock_wait_count,
                                                          page_lock_wait_in_ms,
                                                          index_lock_promotion_attempt_count,
                                                          index_lock_promotion_count,
                                                          page_latch_wait_count,
                                                          page_latch_wait_in_ms,
                                                          page_io_latch_wait_count,
                                                          page_io_latch_wait_in_ms)
                                exec sp_executesql @dsql;

                        end; --End Check For @SkipPartitions = 0


                    raiserror (N'Inserting data into #MissingIndexes',0,1) with nowait;
                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
                SELECT  id.database_id, id.object_id, @i_DatabaseName, sc.[name], so.[name], id.statement , gs.avg_total_user_cost,
                        gs.avg_user_impact, gs.user_seeks, gs.user_scans, gs.unique_compiles,id.equality_columns,
                        id.inequality_columns,id.included_columns
                FROM    sys.dm_db_missing_index_groups ig
                        JOIN sys.dm_db_missing_index_details id ON ig.index_handle = id.index_handle
                        JOIN sys.dm_db_missing_index_group_stats gs ON ig.index_group_handle = gs.group_handle
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.objects so on
                            id.object_id=so.object_id
                        JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas sc on
                            so.schema_id=sc.schema_id
                WHERE    id.database_id = ' + CAST(@databaseid as nvarchar(30)) + '
                ' + case
                                                                                                                                                                                                                                                                  when @objectid is null
                                                                                                                                                                                                                                                                      then N''
                                                                                                                                                                                                                                                                  else N'and id.object_id=' + CAST(@objectid as nvarchar(30))
                        end +
                                N'OPTION (RECOMPILE);';

                    if @dsql is null
                        raiserror ('@dsql is null',16,1);
                    if @debug = 1
                        begin
                            print SUBSTRING(@dsql, 0, 4000);
                            print SUBSTRING(@dsql, 4000, 8000);
                            print SUBSTRING(@dsql, 8000, 12000);
                            print SUBSTRING(@dsql, 12000, 16000);
                            print SUBSTRING(@dsql, 16000, 20000);
                            print SUBSTRING(@dsql, 20000, 24000);
                            print SUBSTRING(@dsql, 24000, 28000);
                            print SUBSTRING(@dsql, 28000, 32000);
                            print SUBSTRING(@dsql, 32000, 36000);
                            print SUBSTRING(@dsql, 36000, 40000);
                        end;
                    insert #missingindexes ([database_id], [object_id], [database_name], [schema_name], [table_name],
                                            [statement], avg_total_user_cost,
                                            avg_user_impact, user_seeks, user_scans, unique_compiles, equality_columns,
                                            inequality_columns, included_columns)
                        exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                             @i_databasename = @databasename;

                    set @dsql = N'
            SELECT DB_ID(N' + QUOTENAME(@databasename, '''') + N') AS [database_id],
			    @i_DatabaseName AS database_name,
				s.name,
                fk_object.name AS foreign_key_name,
                parent_object.[object_id] AS parent_object_id,
                parent_object.name AS parent_object_name,
                referenced_object.[object_id] AS referenced_object_id,
                referenced_object.name AS referenced_object_name,
                fk.is_disabled,
                fk.is_not_trusted,
                fk.is_not_for_replication,
                parent.fk_columns,
                referenced.fk_columns,
                [update_referential_action_desc],
                [delete_referential_action_desc]
            FROM ' + QUOTENAME(@databasename) + N'.sys.foreign_keys fk
            JOIN ' + QUOTENAME(@databasename) + N'.sys.objects fk_object ON fk.object_id=fk_object.object_id
            JOIN ' + QUOTENAME(@databasename) + N'.sys.objects parent_object ON fk.parent_object_id=parent_object.object_id
            JOIN ' + QUOTENAME(@databasename) + N'.sys.objects referenced_object ON fk.referenced_object_id=referenced_object.object_id
			JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas AS s ON fk.schema_id=s.schema_id
            CROSS APPLY ( SELECT  STUFF( (SELECT  N'', '' + c_parent.name AS fk_columns
                                            FROM    ' + QUOTENAME(@databasename) + N'.sys.foreign_key_columns fkc
                                            JOIN ' + QUOTENAME(@databasename) + N'.sys.columns c_parent ON fkc.parent_object_id=c_parent.[object_id]
                                                AND fkc.parent_column_id=c_parent.column_id
                                            WHERE    fk.parent_object_id=fkc.parent_object_id
                                                AND fk.[object_id]=fkc.constraint_object_id
                                            ORDER BY fkc.constraint_column_id
                                    FOR      XML PATH('''') ,
                                              TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''')/*This is how we remove the first comma*/ ) parent ( fk_columns )
            CROSS APPLY ( SELECT  STUFF( (SELECT  N'', '' + c_referenced.name AS fk_columns
                                            FROM    ' + QUOTENAME(@databasename) + N'.sys.    foreign_key_columns fkc
                                            JOIN ' + QUOTENAME(@databasename) + N'.sys.columns c_referenced ON fkc.referenced_object_id=c_referenced.[object_id]
                                                AND fkc.referenced_column_id=c_referenced.column_id
                                            WHERE    fk.referenced_object_id=fkc.referenced_object_id
                                                and fk.[object_id]=fkc.constraint_object_id
                                            ORDER BY fkc.constraint_column_id  /*order by col name, we don''t have anything better*/
                                    FOR      XML PATH('''') ,
                                              TYPE).value(''.'', ''nvarchar(max)''), 1, 1, '''') ) referenced ( fk_columns )
            ' + case
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         when @objectid is not null
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             then
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 'WHERE fk.parent_object_id=' +
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 CAST(@objectid as nvarchar(30)) +
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 N' OR fk.referenced_object_id=' +
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 CAST(@objectid as nvarchar(30)) +
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 N' '
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         else N' ' end + '
            ORDER BY parent_object_name, foreign_key_name
			OPTION (RECOMPILE);';
                    if @dsql is null
                        raiserror ('@dsql is null',16,1);

                    raiserror (N'Inserting data into #ForeignKeys',0,1) with nowait;
                    if @debug = 1
                        begin
                            print SUBSTRING(@dsql, 0, 4000);
                            print SUBSTRING(@dsql, 4000, 8000);
                            print SUBSTRING(@dsql, 8000, 12000);
                            print SUBSTRING(@dsql, 12000, 16000);
                            print SUBSTRING(@dsql, 16000, 20000);
                            print SUBSTRING(@dsql, 20000, 24000);
                            print SUBSTRING(@dsql, 24000, 28000);
                            print SUBSTRING(@dsql, 28000, 32000);
                            print SUBSTRING(@dsql, 32000, 36000);
                            print SUBSTRING(@dsql, 36000, 40000);
                        end;
                    insert #foreignkeys ([database_id], [database_name], [schema_name], foreign_key_name,
                                         parent_object_id, parent_object_name, referenced_object_id,
                                         referenced_object_name,
                                         is_disabled, is_not_trusted, is_not_for_replication, parent_fk_columns,
                                         referenced_fk_columns,
                                         [update_referential_action_desc], [delete_referential_action_desc])
                        exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                             @i_databasename = @databasename;


                    if @skipstatistics = 0 and DB_NAME() = @databasename /* Can only get stats in the current database - see https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1947 */
                        begin
                            if ((PARSENAME(@sqlserverproductversion, 4) >= 12)
                                or (PARSENAME(@sqlserverproductversion, 4) = 11 and
                                    PARSENAME(@sqlserverproductversion, 2) >= 3000)
                                or (PARSENAME(@sqlserverproductversion, 4) = 10 and
                                    PARSENAME(@sqlserverproductversion, 3) = 50 and
                                    PARSENAME(@sqlserverproductversion, 2) >= 2500))
                                begin
                                    raiserror (N'Gathering Statistics Info With Newer Syntax.',0,1) with nowait;
                                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
				SELECT DB_ID(N' + QUOTENAME(@databasename, '''') + N') AS [database_id],
				    @i_DatabaseName AS database_name,
					obj.name AS table_name,
					sch.name AS schema_name,
			        ISNULL(i.name, ''System Or User Statistic'') AS index_name,
			        ca.column_names AS column_names,
			        s.name AS statistics_name,
			        CONVERT(DATETIME, ddsp.last_updated) AS last_statistics_update,
			        DATEDIFF(DAY, ddsp.last_updated, GETDATE()) AS days_since_last_stats_update,
			        ddsp.rows,
			        ddsp.rows_sampled,
			        CAST(ddsp.rows_sampled / ( 1. * NULLIF(ddsp.rows, 0) ) * 100 AS DECIMAL(18, 1)) AS percent_sampled,
			        ddsp.steps AS histogram_steps,
			        ddsp.modification_counter,
			        CASE WHEN ddsp.modification_counter > 0
			             THEN CAST(ddsp.modification_counter / ( 1. * NULLIF(ddsp.rows, 0) ) * 100 AS DECIMAL(18, 1))
			             ELSE ddsp.modification_counter
			        END AS percent_modifications,
			        CASE WHEN ddsp.rows < 500 THEN 500
			             ELSE CAST(( ddsp.rows * .20 ) + 500 AS INT)
			        END AS modifications_before_auto_update,
			        ISNULL(i.type_desc, ''System Or User Statistic - N/A'') AS index_type_desc,
			        CONVERT(DATETIME, obj.create_date) AS table_create_date,
			        CONVERT(DATETIME, obj.modify_date) AS table_modify_date,
					s.no_recompute,
					s.has_filter,
					s.filter_definition
			FROM    ' + QUOTENAME(@databasename) + N'.sys.stats AS s
			JOIN    ' + QUOTENAME(@databasename) + N'.sys.objects obj
			ON      s.object_id = obj.object_id
			JOIN    ' + QUOTENAME(@databasename) + N'.sys.schemas sch
			ON		sch.schema_id = obj.schema_id
			LEFT JOIN    ' + QUOTENAME(@databasename) + N'.sys.indexes AS i
			ON      i.object_id = s.object_id
			        AND i.index_id = s.stats_id
			OUTER APPLY ' + QUOTENAME(@databasename) + N'.sys.dm_db_stats_properties(s.object_id, s.stats_id) AS ddsp
			CROSS APPLY ( SELECT  STUFF((SELECT   '', '' + c.name
						  FROM     ' + QUOTENAME(@databasename) + N'.sys.stats_columns AS sc
						  JOIN     ' + QUOTENAME(@databasename) + N'.sys.columns AS c
						  ON       sc.column_id = c.column_id AND sc.object_id = c.object_id
						  WHERE    sc.stats_id = s.stats_id AND sc.object_id = s.object_id
						  ORDER BY sc.stats_column_id
						  FOR   XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 2, '''')
						) ca (column_names)
			WHERE obj.is_ms_shipped = 0
			OPTION (RECOMPILE);';

                                    if @dsql is null
                                        raiserror ('@dsql is null',16,1);

                                    raiserror (N'Inserting data into #Statistics',0,1) with nowait;
                                    if @debug = 1
                                        begin
                                            print SUBSTRING(@dsql, 0, 4000);
                                            print SUBSTRING(@dsql, 4000, 8000);
                                            print SUBSTRING(@dsql, 8000, 12000);
                                            print SUBSTRING(@dsql, 12000, 16000);
                                            print SUBSTRING(@dsql, 16000, 20000);
                                            print SUBSTRING(@dsql, 20000, 24000);
                                            print SUBSTRING(@dsql, 24000, 28000);
                                            print SUBSTRING(@dsql, 28000, 32000);
                                            print SUBSTRING(@dsql, 32000, 36000);
                                            print SUBSTRING(@dsql, 36000, 40000);
                                        end;
                                    insert #statistics (database_id, database_name, table_name, schema_name, index_name,
                                                        column_names, statistics_name, last_statistics_update,
                                                        days_since_last_stats_update, rows, rows_sampled,
                                                        percent_sampled, histogram_steps, modification_counter,
                                                        percent_modifications, modifications_before_auto_update,
                                                        index_type_desc, table_create_date, table_modify_date,
                                                        no_recompute, has_filter, filter_definition)
                                        exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                                             @i_databasename = @databasename;
                                end;
                            else
                                begin
                                    raiserror (N'Gathering Statistics Info With Older Syntax.',0,1) with nowait;
                                    set @dsql = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
							SELECT DB_ID(N' + QUOTENAME(@databasename, '''') + N') AS [database_id],
							    @i_DatabaseName AS database_name,
								obj.name AS table_name,
								sch.name AS schema_name,
						        ISNULL(i.name, ''System Or User Statistic'') AS index_name,
						        ca.column_names  AS column_names,
						        s.name AS statistics_name,
						        CONVERT(DATETIME, STATS_DATE(s.object_id, s.stats_id)) AS last_statistics_update,
						        DATEDIFF(DAY, STATS_DATE(s.object_id, s.stats_id), GETDATE()) AS days_since_last_stats_update,
						        si.rowcnt,
						        si.rowmodctr,
						        CASE WHEN si.rowmodctr > 0 THEN CAST(si.rowmodctr / ( 1. * NULLIF(si.rowcnt, 0) ) * 100 AS DECIMAL(18, 1))
						             ELSE si.rowmodctr
						        END AS percent_modifications,
						        CASE WHEN si.rowcnt < 500 THEN 500
						             ELSE CAST(( si.rowcnt * .20 ) + 500 AS INT)
						        END AS modifications_before_auto_update,
						        ISNULL(i.type_desc, ''System Or User Statistic - N/A'') AS index_type_desc,
						        CONVERT(DATETIME, obj.create_date) AS table_create_date,
						        CONVERT(DATETIME, obj.modify_date) AS table_modify_date,
								s.no_recompute,
								'
                                        + case
                                              when @sqlserverproductversion not like '9%'
                                                  then N's.has_filter,
									   s.filter_definition'
                                              else N'NULL AS has_filter,
								       NULL AS filter_definition' end
                                        + N'
						FROM    ' + QUOTENAME(@databasename) + N'.sys.stats AS s
						INNER HASH JOIN    ' + QUOTENAME(@databasename) + N'.sys.sysindexes si
						ON      si.name = s.name AND s.object_id = si.id
						INNER HASH JOIN    ' + QUOTENAME(@databasename) + N'.sys.objects obj
						ON      s.object_id = obj.object_id
						INNER HASH JOIN    ' + QUOTENAME(@databasename) + N'.sys.schemas sch
						ON		sch.schema_id = obj.schema_id
						LEFT HASH JOIN ' + QUOTENAME(@databasename) + N'.sys.indexes AS i
						ON      i.object_id = s.object_id
						        AND i.index_id = s.stats_id
						CROSS APPLY ( SELECT  STUFF((SELECT   '', '' + c.name
									  FROM     ' + QUOTENAME(@databasename) + N'.sys.stats_columns AS sc
									  JOIN     ' + QUOTENAME(@databasename) + N'.sys.columns AS c
									  ON       sc.column_id = c.column_id AND sc.object_id = c.object_id
									  WHERE    sc.stats_id = s.stats_id AND sc.object_id = s.object_id
									  ORDER BY sc.stats_column_id
									  FOR   XML PATH(''''), TYPE).value(''.'', ''nvarchar(max)''), 1, 2, '''')
									) ca (column_names)
						WHERE obj.is_ms_shipped = 0
						AND si.rowcnt > 0
						OPTION (RECOMPILE);';

                                    if @dsql is null
                                        raiserror ('@dsql is null',16,1);

                                    raiserror (N'Inserting data into #Statistics',0,1) with nowait;
                                    if @debug = 1
                                        begin
                                            print SUBSTRING(@dsql, 0, 4000);
                                            print SUBSTRING(@dsql, 4000, 8000);
                                            print SUBSTRING(@dsql, 8000, 12000);
                                            print SUBSTRING(@dsql, 12000, 16000);
                                            print SUBSTRING(@dsql, 16000, 20000);
                                            print SUBSTRING(@dsql, 20000, 24000);
                                            print SUBSTRING(@dsql, 24000, 28000);
                                            print SUBSTRING(@dsql, 28000, 32000);
                                            print SUBSTRING(@dsql, 32000, 36000);
                                            print SUBSTRING(@dsql, 36000, 40000);
                                        end;
                                    insert #statistics(database_id, database_name, table_name, schema_name, index_name,
                                                       column_names, statistics_name,
                                                       last_statistics_update, days_since_last_stats_update, rows,
                                                       modification_counter,
                                                       percent_modifications, modifications_before_auto_update,
                                                       index_type_desc, table_create_date, table_modify_date,
                                                       no_recompute, has_filter, filter_definition)
                                        exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                                             @i_databasename = @databasename;
                                end;

                        end;

                    if (PARSENAME(@sqlserverproductversion, 4) >= 10)
                        begin
                            raiserror (N'Gathering Computed Column Info.',0,1) with nowait;
                            set @dsql = N'SELECT DB_ID(@i_DatabaseName) AS [database_id],
							   @i_DatabaseName AS database_name,
   					   		   t.name AS table_name,
   					           s.name AS schema_name,
   					           c.name AS column_name,
   					           cc.is_nullable,
   					           cc.definition,
   					           cc.uses_database_collation,
   					           cc.is_persisted,
   					           cc.is_computed,
   					   		   CASE WHEN cc.definition LIKE ''%|].|[%'' ESCAPE ''|'' THEN 1 ELSE 0 END AS is_function,
   					   		   ''ALTER TABLE '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name) +
   					   		   '' ADD '' + QUOTENAME(c.name) + '' AS '' + cc.definition  +
							   CASE WHEN is_persisted = 1 THEN '' PERSISTED'' ELSE '''' END + '';'' COLLATE DATABASE_DEFAULT AS [column_definition]
   					   FROM    ' + QUOTENAME(@databasename) + N'.sys.computed_columns AS cc
   					   JOIN    ' + QUOTENAME(@databasename) + N'.sys.columns AS c
   					   ON      cc.object_id = c.object_id
   					   		   AND cc.column_id = c.column_id
   					   JOIN    ' + QUOTENAME(@databasename) + N'.sys.tables AS t
   					   ON      t.object_id = cc.object_id
   					   JOIN    ' + QUOTENAME(@databasename) + N'.sys.schemas AS s
   					   ON      s.schema_id = t.schema_id
					   OPTION (RECOMPILE);';

                            if @dsql is null raiserror ('@dsql is null',16,1);

                            insert #computedcolumns
                            (database_id, [database_name], table_name, schema_name, column_name, is_nullable,
                             definition,
                             uses_database_collation, is_persisted, is_computed, is_function, column_definition)
                                exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                                     @i_databasename = @databasename;

                        end;

                    raiserror (N'Gathering Trace Flag Information',0,1) with nowait;
                    insert #tracestatus
                        exec ('DBCC TRACESTATUS(-1) WITH NO_INFOMSGS');

                    if (PARSENAME(@sqlserverproductversion, 4) >= 13)
                        begin
                            raiserror (N'Gathering Temporal Table Info',0,1) with nowait;
                            set @dsql = N'SELECT ' + QUOTENAME(@databasename, '''') + N' AS database_name,
								   DB_ID(N' + QUOTENAME(@databasename, '''') + N') AS [database_id],
								   s.name AS schema_name,
								   t.name AS table_name,
								   oa.hsn as history_schema_name,
								   oa.htn AS history_table_name,
								   c1.name AS start_column_name,
								   c2.name AS end_column_name,
								   p.name AS period_name
							FROM ' + QUOTENAME(@databasename) + N'.sys.periods AS p
							INNER JOIN ' + QUOTENAME(@databasename) + N'.sys.tables AS t
							ON  p.object_id = t.object_id
							INNER JOIN ' + QUOTENAME(@databasename) + N'.sys.columns AS c1
							ON  t.object_id = c1.object_id
							    AND p.start_column_id = c1.column_id
							INNER JOIN ' + QUOTENAME(@databasename) + N'.sys.columns AS c2
							ON  t.object_id = c2.object_id
							    AND p.end_column_id = c2.column_id
							INNER JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas AS s
							ON t.schema_id = s.schema_id
							CROSS APPLY ( SELECT s2.name as hsn, t2.name htn
							              FROM ' + QUOTENAME(@databasename) + N'.sys.tables AS t2
										  INNER JOIN ' + QUOTENAME(@databasename) + N'.sys.schemas AS s2
										  ON t2.schema_id = s2.schema_id
							              WHERE t2.object_id = t.history_table_id
							              AND t2.temporal_type = 1 /*History table*/ ) AS oa
							WHERE t.temporal_type IN ( 2, 4 ) /*BOL currently points to these types, but has no definition for 4*/
							OPTION (RECOMPILE);
							';

                            if @dsql is null
                                raiserror ('@dsql is null',16,1);

                            insert #temporaltables (database_name, database_id, schema_name, table_name,
                                                    history_table_name,
                                                    history_schema_name, start_column_name, end_column_name,
                                                    period_name)
                                exec sp_executesql @dsql;

                            set @dsql = N'SELECT DB_ID(@i_DatabaseName) AS [database_id],
             				   @i_DatabaseName AS database_name,
             		   		   t.name AS table_name,
             		           s.name AS schema_name,
             		           cc.name AS constraint_name,
             		           cc.is_disabled,
             		           cc.definition,
             		           cc.uses_database_collation,
             		           cc.is_not_trusted,
             		   		   CASE WHEN cc.definition LIKE ''%|].|[%'' ESCAPE ''|'' THEN 1 ELSE 0 END AS is_function,
             		   		   ''ALTER TABLE '' + QUOTENAME(s.name) + ''.'' + QUOTENAME(t.name) +
             		   		   '' ADD CONSTRAINT '' + QUOTENAME(cc.name) + '' CHECK '' + cc.definition  + '';'' COLLATE DATABASE_DEFAULT AS [column_definition]
             		   FROM    ' + QUOTENAME(@databasename) + N'.sys.check_constraints AS cc
             		   JOIN    ' + QUOTENAME(@databasename) + N'.sys.tables AS t
             		   ON      t.object_id = cc.parent_object_id
             		   JOIN    ' + QUOTENAME(@databasename) + N'.sys.schemas AS s
             		   ON      s.schema_id = t.schema_id
             		   OPTION (RECOMPILE);';

                            insert #checkconstraints
                            (database_id, [database_name], table_name, schema_name, constraint_name, is_disabled,
                             definition,
                             uses_database_collation, is_not_trusted, is_function, column_definition)
                                exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                                     @i_databasename = @databasename;


                            set @dsql = N'SELECT DB_ID(@i_DatabaseName) AS [database_id],
             				   @i_DatabaseName AS database_name,
                               s.name AS missing_schema_name,
                               t.name AS missing_table_name,
                               i.name AS missing_index_name,
                               c.name AS missing_column_name
                        FROM   ' + QUOTENAME(@databasename) + N'.sys.sql_expression_dependencies AS sed
                        JOIN   ' + QUOTENAME(@databasename) + N'.sys.tables AS t
                            ON t.object_id = sed.referenced_id
                        JOIN   ' + QUOTENAME(@databasename) + N'.sys.schemas AS s
                            ON t.schema_id = s.schema_id
                        JOIN   ' + QUOTENAME(@databasename) + N'.sys.indexes AS i
                            ON i.object_id = sed.referenced_id
                            AND i.index_id = sed.referencing_minor_id
                        JOIN   ' + QUOTENAME(@databasename) + N'.sys.columns AS c
                            ON c.object_id = sed.referenced_id
                            AND c.column_id = sed.referenced_minor_id
                        WHERE  sed.referencing_class = 7
                        AND    sed.referenced_class = 1
                        AND    i.has_filter = 1
                        AND    NOT EXISTS (   SELECT 1/0
                                              FROM   ' + QUOTENAME(@databasename) + N'.sys.index_columns AS ic
                                              WHERE  ic.index_id = sed.referencing_minor_id
                                              AND    ic.column_id = sed.referenced_minor_id
                                              AND    ic.object_id = sed.referenced_id )
                        OPTION(RECOMPILE);'

                            insert #filteredindexes (database_id, database_name, schema_name, table_name, index_name,
                                                     column_name)
                                exec sp_executesql @dsql, @params = N'@i_DatabaseName NVARCHAR(128)',
                                     @i_databasename = @databasename;


                        end;

                end;
            end try
            begin catch
                raiserror (N'Failure populating temp tables.', 0,1) with nowait;

                if @dsql is not null
                    begin
                        set @msg = 'Last @dsql: ' + @dsql;
                        raiserror (@msg, 0, 1) with nowait;
                    end;

                select @msg = @databasename + N' database failed to process. ' + ERROR_MESSAGE(),
                       @errorseverity = ERROR_SEVERITY(),
                       @errorstate = ERROR_STATE();
                raiserror (@msg,@errorseverity, @errorstate ) with nowait;


                while @@trancount > 0
                    rollback;

                return;
            end catch;
            fetch next from c1 into @databasename;
        end;
    deallocate c1;


    ----------------------------------------
--STEP 2: PREP THE TEMP TABLES
--EVERY QUERY AFTER THIS GOES AGAINST TEMP TABLES ONLY.
----------------------------------------

    raiserror (N'Updating #IndexSanity.key_column_names',0,1) with nowait;
update #indexsanity
set key_column_names = d1.key_column_names
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + c.column_name
                                                     + N' {' + system_type_name + N' ' +
                                                 case max_length
                                                     when -1 then N'(max)'
                                                     else
                                                         case
                                                             when system_type_name in (N'char', N'varchar', N'binary', N'varbinary')
                                                                 then N'(' + CAST(max_length as nvarchar(20)) + N')'
                                                             when system_type_name in (N'nchar', N'nvarchar')
                                                                 then N'(' + CAST(max_length / 2 as nvarchar(20)) + N')'
                                                             else ''
                                                             end
                                                     end
                                                     + N'}'
                                                     as col_definition
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.is_included_column = 0 /*Just Keys*/
                                            and c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                                          order by c.object_id, c.index_id, c.key_ordinal
                                          for xml path('') ,type).value('.', 'nvarchar(max)'), 1, 1, ''))
) d1 (key_column_names);

    raiserror (N'Updating #IndexSanity.partition_key_column_name',0,1) with nowait;
update #indexsanity
set partition_key_column_name = d1.partition_key_column_name
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + c.column_name as col_definition
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.partition_ordinal <> 0 /*Just Partitioned Keys*/
                                          order by c.object_id, c.index_id, c.key_ordinal
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))) d1
    (partition_key_column_name);

    raiserror (N'Updating #IndexSanity.key_column_names_with_sort_order',0,1) with nowait;
update #indexsanity
set key_column_names_with_sort_order = d2.key_column_names_with_sort_order
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + c.column_name + case c.is_descending_key
                                                                             when 1 then N' DESC'
                                                                             else N''
    end
                                                     + N' {' + system_type_name + N' ' +
                                                 case max_length
                                                     when -1 then N'(max)'
                                                     else
                                                         case
                                                             when system_type_name in (N'char', N'varchar', N'binary', N'varbinary')
                                                                 then N'(' + CAST(max_length as nvarchar(20)) + N')'
                                                             when system_type_name in (N'nchar', N'nvarchar')
                                                                 then N'(' + CAST(max_length / 2 as nvarchar(20)) + N')'
                                                             else ''
                                                             end
                                                     end
                                                     + N'}'
                                                     as col_definition
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.is_included_column = 0 /*Just Keys*/
                                            and c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                                          order by c.object_id, c.index_id, c.key_ordinal
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))
) d2 (key_column_names_with_sort_order);

    raiserror (N'Updating #IndexSanity.key_column_names_with_sort_order_no_types (for create tsql)',0,1) with nowait;
update #indexsanity
set key_column_names_with_sort_order_no_types = d2.key_column_names_with_sort_order_no_types
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + QUOTENAME(c.column_name) + case c.is_descending_key
                                                                                        when 1 then N' DESC'
                                                                                        else N''
    end as col_definition
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.is_included_column = 0 /*Just Keys*/
                                            and c.key_ordinal > 0 /*Ignore non-key columns, such as partitioning keys*/
                                          order by c.object_id, c.index_id, c.key_ordinal
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))
) d2 (key_column_names_with_sort_order_no_types);

    raiserror (N'Updating #IndexSanity.include_column_names',0,1) with nowait;
update #indexsanity
set include_column_names = d3.include_column_names
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + c.column_name
                                                     + N' {' + system_type_name + N' ' +
                                                 CAST(max_length as nvarchar(50)) + N'}'
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.is_included_column = 1 /*Just includes*/
                                          order by c.column_name /*Order doesn't matter in includes,
                                this is here to make rows easy to compare.*/
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))
) d3 (include_column_names);

    raiserror (N'Updating #IndexSanity.include_column_names_no_types (for create tsql)',0,1) with nowait;
update #indexsanity
set include_column_names_no_types = d3.include_column_names_no_types
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + QUOTENAME(c.column_name)
                                          from #indexcolumns c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.object_id = si.object_id
                                            and c.index_id = si.index_id
                                            and c.is_included_column = 1 /*Just includes*/
                                          order by c.column_name /*Order doesn't matter in includes,
                                this is here to make rows easy to compare.*/
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))
) d3 (include_column_names_no_types);

    raiserror (N'Updating #IndexSanity.count_key_columns and count_include_columns',0,1) with nowait;
update #indexsanity
set count_included_columns = d4.count_included_columns,
    count_key_columns      = d4.count_key_columns
from #indexsanity si
         cross apply (select SUM(case
                                     when is_included_column = 'true' then 1
                                     else 0
    end)                              as count_included_columns,
                             SUM(case
                                     when is_included_column = 'false' and c.key_ordinal > 0 then 1
                                     else 0
                                 end) as count_key_columns
                      from #indexcolumns c
                      where c.database_id = si.database_id
                        and c.schema_name = si.schema_name
                        and c.object_id = si.object_id
                        and c.index_id = si.index_id
) as d4 (count_included_columns, count_key_columns);

    raiserror (N'Updating index_sanity_id on #IndexPartitionSanity',0,1) with nowait;
update #indexpartitionsanity
set index_sanity_id = i.index_sanity_id
from #indexpartitionsanity ps
         join #indexsanity i on ps.[object_id] = i.[object_id]
    and ps.index_id = i.index_id
    and i.database_id = ps.database_id
    and i.schema_name = ps.schema_name;


    raiserror (N'Inserting data into #IndexSanitySize',0,1) with nowait;
insert #indexsanitysize ([index_sanity_id], [database_id], [schema_name], partition_count, total_rows,
                         total_reserved_mb,
                         total_reserved_lob_mb, total_reserved_row_overflow_mb, total_range_scan_count,
                         total_singleton_lookup_count, total_leaf_delete_count, total_leaf_update_count,
                         total_forwarded_fetch_count, total_row_lock_count,
                         total_row_lock_wait_count, total_row_lock_wait_in_ms, avg_row_lock_wait_in_ms,
                         total_page_lock_count, total_page_lock_wait_count, total_page_lock_wait_in_ms,
                         avg_page_lock_wait_in_ms, total_index_lock_promotion_attempt_count,
                         total_index_lock_promotion_count, data_compression_desc,
                         page_latch_wait_count, page_latch_wait_in_ms, page_io_latch_wait_count,
                         page_io_latch_wait_in_ms)
select index_sanity_id,
       ipp.database_id,
       ipp.schema_name,
       COUNT(*),
       SUM(row_count),
       SUM(reserved_mb),
       SUM(reserved_lob_mb),
       SUM(reserved_row_overflow_mb),
       SUM(range_scan_count),
       SUM(singleton_lookup_count),
       SUM(leaf_delete_count),
       SUM(leaf_update_count),
       SUM(forwarded_fetch_count),
       SUM(row_lock_count),
       SUM(row_lock_wait_count),
       SUM(row_lock_wait_in_ms),
       case
           when SUM(row_lock_wait_in_ms) > 0 then
               SUM(row_lock_wait_in_ms) / (1. * SUM(row_lock_wait_count))
           else 0 end as avg_row_lock_wait_in_ms,
       SUM(page_lock_count),
       SUM(page_lock_wait_count),
       SUM(page_lock_wait_in_ms),
       case
           when SUM(page_lock_wait_in_ms) > 0 then
               SUM(page_lock_wait_in_ms) / (1. * SUM(page_lock_wait_count))
           else 0 end as avg_page_lock_wait_in_ms,
       SUM(index_lock_promotion_attempt_count),
       SUM(index_lock_promotion_count),
       LEFT(MAX(data_compression_info.data_compression_rollup), 4000),
       SUM(page_latch_wait_count),
       SUM(page_latch_wait_in_ms),
       SUM(page_io_latch_wait_count),
       SUM(page_io_latch_wait_in_ms)
from #indexpartitionsanity ipp
         /* individual partitions can have distinct compression settings, just roll them into a list here*/
         outer apply (select STUFF((
                                       select N', ' + data_compression_desc
                                       from #indexpartitionsanity ipp2
                                       where ipp.[object_id] = ipp2.[object_id]
                                         and ipp.[index_id] = ipp2.[index_id]
                                         and ipp.database_id = ipp2.database_id
                                         and ipp.schema_name = ipp2.schema_name
                                       order by ipp2.partition_number
                                       for xml path(''),type).value('.', 'nvarchar(max)'), 1, 1,
                                   '')) data_compression_info(data_compression_rollup)
group by index_sanity_id, ipp.database_id, ipp.schema_name
order by index_sanity_id
option ( recompile );

    raiserror (N'Determining index usefulness',0,1) with nowait;
update #missingindexes
set is_low = case
                 when (user_seeks + user_scans) < 5000
                     or unique_compiles = 1
                     then 1
                 else 0
    end;

    raiserror (N'Updating #IndexSanity.referenced_by_foreign_key',0,1) with nowait;
update #indexsanity
set is_referenced_by_foreign_key=1
from #indexsanity s
         join #foreignkeys fk on
        s.object_id = fk.referenced_object_id
        and s.database_id = fk.database_id
        and LEFT(s.key_column_names, LEN(fk.referenced_fk_columns)) = fk.referenced_fk_columns;

    raiserror (N'Update index_secret on #IndexSanity for NC indexes.',0,1) with nowait;
update nc
set secret_columns=
        N'[' +
        case tb.count_key_columns when 0 then '1' else CAST(tb.count_key_columns as nvarchar(10)) end +
        case nc.is_unique when 1 then N' INCLUDE' else N' KEY' end +
        case when tb.count_key_columns > 1 then N'S] ' else N'] ' end +
        case tb.index_id
            when 0 then '[RID]'
            else LTRIM(tb.key_column_names) +
                /* Uniquifiers only needed on non-unique clustereds-- not heaps */
                 case tb.is_unique when 0 then ' [UNIQUIFIER]' else N'' end
            end
  , count_secret_columns=
    case tb.index_id
        when 0 then 1
        else
                tb.count_key_columns +
                case tb.is_unique when 0 then 1 else 0 end
        end
from #indexsanity as nc
         join #indexsanity as tb on nc.object_id = tb.object_id
    and nc.database_id = tb.database_id
    and nc.schema_name = tb.schema_name
    and tb.index_id in (0, 1)
where nc.index_id > 1;

    raiserror (N'Update index_secret on #IndexSanity for heaps and non-unique clustered.',0,1) with nowait;
update tb
set secret_columns= case tb.index_id when 0 then '[RID]' else '[UNIQUIFIER]' end
  , count_secret_columns = 1
from #indexsanity as tb
where tb.index_id = 0 /*Heaps-- these have the RID */
   or (tb.index_id = 1 and tb.is_unique = 0); /* Non-unique CX: has uniquifer (when needed) */


    raiserror (N'Populate #IndexCreateTsql.',0,1) with nowait;
insert #indexcreatetsql (index_sanity_id, create_tsql)
select index_sanity_id,
       ISNULL(
               case index_id
                   when 0 then N'ALTER TABLE ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) + N'.' +
                               QUOTENAME([object_name]) + ' REBUILD;'
                   else
                       case
                           when is_xml = 1 or is_spatial = 1 or is_in_memory_oltp = 1
                               then N'' /* Not even trying for these just yet...*/
                           else
                                   case
                                       when is_primary_key = 1 then
                                               N'ALTER TABLE ' + QUOTENAME([database_name]) + N'.' +
                                               QUOTENAME([schema_name]) +
                                               N'.' + QUOTENAME([object_name]) +
                                               N' ADD CONSTRAINT [' +
                                               index_name +
                                               N'] PRIMARY KEY ' +
                                               case when index_id = 1 then N'CLUSTERED (' else N'(' end +
                                               key_column_names_with_sort_order_no_types + N' )'
                                       when is_cx_columnstore = 1 then
                                               N'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(index_name) +
                                               N' on ' + QUOTENAME([database_name]) + N'.' + QUOTENAME([schema_name]) +
                                               N'.' + QUOTENAME([object_name])
                                       else /*Else not a PK or cx columnstore */
                                               N'CREATE ' +
                                               case when is_unique = 1 then N'UNIQUE ' else N'' end +
                                               case when index_id = 1 then N'CLUSTERED ' else N'' end +
                                               case
                                                   when is_nc_columnstore = 1 then N'NONCLUSTERED COLUMNSTORE '
                                                   else N'' end +
                                               N'INDEX ['
                                               + index_name + N'] ON ' +
                                               QUOTENAME([database_name]) + N'.' +
                                               QUOTENAME([schema_name]) + N'.' + QUOTENAME([object_name]) +
                                               case
                                                   when is_nc_columnstore = 1 then
                                                       N' (' + ISNULL(include_column_names_no_types, '') + N' )'
                                                   else /*Else not columnstore */
                                                           N' (' +
                                                           ISNULL(key_column_names_with_sort_order_no_types, '') + N' )'
                                                           + case
                                                                 when include_column_names_no_types is not null then
                                                                     N' INCLUDE (' + include_column_names_no_types + N')'
                                                                 else N''
                                                               end
                                                   end /*End non-columnstore case */
                                               + case
                                                     when filter_definition <> N'' then N' WHERE ' + filter_definition
                                                     else N'' end
                                       end /*End Non-PK index CASE */
                                   + case
                                         when is_nc_columnstore = 0 and is_cx_columnstore = 0 then
                                                 N' WITH ('
                                                 + N'FILLFACTOR=' + case fill_factor
                                                                        when 0 then N'100'
                                                                        else CAST(fill_factor as nvarchar(5)) end + ', '
                                                 + N'ONLINE=?, SORT_IN_TEMPDB=?, DATA_COMPRESSION=?'
                                                 + N')'
                                         else N'' end
                                   + N';'
                           end /*End non-spatial and non-xml CASE */
                   end, '[Unknown Error]')
           as create_tsql
from #indexsanity;

    raiserror (N'Populate #PartitionCompressionInfo.',0,1) with nowait;
with maps
         as
         (
             select ips.index_sanity_id,
                    ips.partition_number,
                    ips.data_compression_desc,
                    ips.partition_number -
                    ROW_NUMBER() over ( partition by ips.index_sanity_id, ips.data_compression_desc
                        order by ips.partition_number ) as rn
             from #indexpartitionsanity as ips
         )
select *
into #maps
from maps;

with grps
         as
         (
             select MIN(maps.partition_number) as minkey,
                    MAX(maps.partition_number) as maxkey,
                    maps.index_sanity_id,
                    maps.data_compression_desc
             from #maps as maps
             group by maps.rn, maps.index_sanity_id, maps.data_compression_desc
         )
select *
into #grps
from grps;

insert #partitioncompressioninfo (index_sanity_id, partition_compression_detail)
select distinct grps.index_sanity_id,
                SUBSTRING(
                        (STUFF(
                                (select N', ' + N' Partition'
                                            + case
                                                  when grps2.minkey < grps2.maxkey
                                                      then
                                                          + N's ' + CAST(grps2.minkey as nvarchar(10)) + N' - '
                                                          + CAST(grps2.maxkey as nvarchar(10)) + N' use ' +
                                                          grps2.data_compression_desc
                                                  else
                                                          N' ' + CAST(grps2.minkey as nvarchar(10)) + N' uses ' +
                                                          grps2.data_compression_desc
                                            end as partitions
                                 from #grps as grps2
                                 where grps2.index_sanity_id = grps.index_sanity_id
                                 order by grps2.minkey, grps2.maxkey
                                 for xml path(''), type).value('.', 'NVARCHAR(MAX)'), 1, 1, '')), 0,
                        8000) as partition_compression_detail
from #grps as grps;

    raiserror (N'Update #PartitionCompressionInfo.',0,1) with nowait;
update sz
set sz.data_compression_desc = pci.partition_compression_detail
from #indexsanitysize sz
         join #partitioncompressioninfo as pci
              on pci.index_sanity_id = sz.index_sanity_id;

    raiserror (N'Update #IndexSanity for filtered indexes with columns not in the index definition.',0,1) with nowait;
update #indexsanity
set filter_columns_not_in_index = d1.filter_columns_not_in_index
from #indexsanity si
         cross apply (select RTRIM(STUFF((select N', ' + c.column_name as col_definition
                                          from #filteredindexes as c
                                          where c.database_id = si.database_id
                                            and c.schema_name = si.schema_name
                                            and c.table_name = si.object_name
                                            and c.index_name = si.index_name
                                          order by c.index_sanity_id
                                          for xml path('') , type).value('.', 'nvarchar(max)'), 1, 1, ''))) d1
    (filter_columns_not_in_index);


    if @debug = 1
        begin
            select '#IndexSanity' as table_name, * from #indexsanity;
            select '#IndexPartitionSanity' as table_name, * from #indexpartitionsanity;
            select '#IndexSanitySize' as table_name, * from #indexsanitysize;
            select '#IndexColumns' as table_name, * from #indexcolumns;
            select '#MissingIndexes' as table_name, * from #missingindexes;
            select '#ForeignKeys' as table_name, * from #foreignkeys;
            select '#BlitzIndexResults' as table_name, * from #blitzindexresults;
            select '#IndexCreateTsql' as table_name, * from #indexcreatetsql;
            select '#DatabaseList' as table_name, * from #databaselist;
            select '#Statistics' as table_name, * from #statistics;
            select '#PartitionCompressionInfo' as table_name, * from #partitioncompressioninfo;
            select '#ComputedColumns' as table_name, * from #computedcolumns;
            select '#TraceStatus' as table_name, * from #tracestatus;
            select '#CheckConstraints' as table_name, * from #checkconstraints;
            select '#FilteredIndexes' as table_name, * from #filteredindexes;
        end


    ----------------------------------------
--STEP 3: DIAGNOSE THE PATIENT
----------------------------------------


begin try
    ----------------------------------------
--If @TableName is specified, just return information for that table.
--The @Mode parameter doesn't matter if you're looking at a specific table.
----------------------------------------
    if @tablename is not null
        begin
            raiserror (N'@TableName specified, giving detail only on that table.', 0,1) with nowait;

            --We do a left join here in case this is a disabled NC.
            --In that case, it won't have any size info/pages allocated.


            with table_mode_cte as (
                select s.db_schema_object_indexid,
                       s.key_column_names,
                       s.index_definition,
                       ISNULL(s.secret_columns, N'')                                                  as secret_columns,
                       s.fill_factor,
                       s.index_usage_summary,
                       sz.index_op_stats,
                       ISNULL(sz.index_size_summary, '') /*disabled NCs will be null*/                as index_size_summary,
                       partition_compression_detail,
                       ISNULL(sz.index_lock_wait_summary, '')                                         as index_lock_wait_summary,
                       s.is_referenced_by_foreign_key,
                       (select COUNT(*)
                        from #foreignkeys fk
                        where fk.parent_object_id = s.object_id
                          and PATINDEX(fk.parent_fk_columns, s.key_column_names) = 1)                 as fks_covered_by_index,
                       s.last_user_seek,
                       s.last_user_scan,
                       s.last_user_lookup,
                       s.last_user_update,
                       s.create_date,
                       s.modify_date,
                       sz.page_latch_wait_count,
                       CONVERT(varchar(10), (sz.page_latch_wait_in_ms / 1000) / 86400) + ':' +
                       CONVERT(varchar(20), DATEADD(s, (sz.page_latch_wait_in_ms / 1000), 0),
                                            108)                                                      as page_latch_wait_time,
                       sz.page_io_latch_wait_count,
                       CONVERT(varchar(10), (sz.page_io_latch_wait_in_ms / 1000) / 86400) + ':' +
                       CONVERT(varchar(20), DATEADD(s, (sz.page_io_latch_wait_in_ms / 1000), 0),
                                            108)                                                      as page_io_latch_wait_time,
                       ct.create_tsql,
                       case
                           when s.is_primary_key = 1 and s.index_definition <> '[HEAP]'
                               then N'--ALTER TABLE ' + QUOTENAME(s.[database_name]) + N'.' +
                                    QUOTENAME(s.[schema_name]) + N'.' + QUOTENAME(s.[object_name])
                               + N' DROP CONSTRAINT ' + QUOTENAME(s.index_name) + N';'
                           when s.is_primary_key = 0 and s.index_definition <> '[HEAP]'
                               then N'--DROP INDEX ' + QUOTENAME(s.index_name) + N' ON ' +
                                    QUOTENAME(s.[database_name]) + N'.' +
                                    QUOTENAME(s.[schema_name]) + N'.' + QUOTENAME(s.[object_name]) + N';'
                           else N''
                           end                                                                        as drop_tsql,
                       1                                                                              as display_order
                from #indexsanity s
                         left join #indexsanitysize sz on
                    s.index_sanity_id = sz.index_sanity_id
                         left join #indexcreatetsql ct on
                    s.index_sanity_id = ct.index_sanity_id
                         left join #partitioncompressioninfo pci on
                    pci.index_sanity_id = s.index_sanity_id
                where s.[object_id] = @objectid
                union all
                select N'Database ' + QUOTENAME(@databasename) + N' as of ' + CONVERT(nvarchar(16), GETDATE(), 121) +
                       N' (' + @scriptversionname + ')',
                       N'SQL Server First Responder Kit',
                       N'http://FirstResponderKit.org',
                       N'From Your Community Volunteers',
                       null,
                       @daysuptimeinsertvalue,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       null,
                       0 as display_order
            )
            select db_schema_object_indexid     as [Details: db_schema.table.index(indexid)],
                   index_definition             as [Definition: [Property]] ColumnName {datatype maxbytes}],
                   secret_columns               as [Secret Columns],
                   fill_factor                  as [Fillfactor],
                   index_usage_summary          as [Usage Stats],
                   index_op_stats               as [Op Stats],
                   index_size_summary           as [Size],
                   partition_compression_detail as [Compression Type],
                   index_lock_wait_summary      as [Lock Waits],
                   is_referenced_by_foreign_key as [Referenced by FK?],
                   fks_covered_by_index         as [FK Covered by Index?],
                   last_user_seek               as [Last User Seek],
                   last_user_scan               as [Last User Scan],
                   last_user_lookup             as [Last User Lookup],
                   last_user_update             as [Last User Write],
                   create_date                  as [Created],
                   modify_date                  as [Last Modified],
                   page_latch_wait_count        as [Page Latch Wait Count],
                   page_latch_wait_time         as [Page Latch Wait Time (D:H:M:S)],
                   page_io_latch_wait_count     as [Page IO Latch Wait Count],
                   page_io_latch_wait_time      as [Page IO Latch Wait Time (D:H:M:S)],
                   create_tsql                  as [Create TSQL],
                   drop_tsql                    as [Drop TSQL]
            from table_mode_cte
            order by display_order asc, key_column_names asc
            option ( recompile );

            if (select top 1 [object_id] from #missingindexes mi) is not null
                begin
                    ;

                    with create_date as (
                        select i.database_id,
                               i.schema_name,
                               i.[object_id],
                               ISNULL(NULLIF(MAX(DATEDIFF(day, i.create_date, SYSDATETIME())), 0), 1) as create_days
                        from #indexsanity as i
                        group by i.database_id, i.schema_name, i.object_id
                    )
                    select N'Missing index.'                       as finding,
                           N'http://BrentOzar.com/go/Indexaphobia' as url,
                           mi.[statement] +
                           ' Est. Benefit: '
                               + case
                                     when magic_benefit_number >= 922337203685477 then '>= 922,337,203,685,477'
                                     else REPLACE(CONVERT(nvarchar(256), CAST(CAST(
                                             (magic_benefit_number / case
                                                                         when cd.create_days < @daysuptime
                                                                             then cd.create_days
                                                                         else @daysuptime end)
                                         as bigint) as money), 1), '.00', '')
                               end                                 as [Estimated Benefit],
                           missing_index_details                   as [Missing Index Request],
                           index_estimated_impact                  as [Estimated Impact],
                           create_tsql                             as [Create TSQL]
                    from #missingindexes mi
                             left join create_date as cd
                                       on mi.[object_id] = cd.object_id
                                           and mi.database_id = cd.database_id
                                           and mi.schema_name = cd.schema_name
                    where mi.[object_id] = @objectid
                        /* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
                      and (magic_benefit_number /
                           case when cd.create_days < @daysuptime then cd.create_days else @daysuptime end) >= 100000
                    order by magic_benefit_number desc
                    option ( recompile );
                end;
            else
                select 'No missing indexes.' as finding;

            select column_name                                      as [Column Name],
                   (select COUNT(*)
                    from #indexcolumns c2
                    where c2.column_name = c.column_name
                      and c2.key_ordinal is not null)
                       + case
                             when c.index_id = 1 and c.key_ordinal is not null then
                                     -1 + (select COUNT(distinct index_id)
                                           from #indexcolumns c3
                                           where c3.index_id not in (0, 1))
                             else 0 end
                                                                    as [Found In],
                   system_type_name +
                   case max_length
                       when -1 then N' (max)'
                       else
                           case
                               when system_type_name in (N'char', N'varchar', N'binary', N'varbinary')
                                   then N' (' + CAST(max_length as nvarchar(20)) + N')'
                               when system_type_name in (N'nchar', N'nvarchar')
                                   then N' (' + CAST(max_length / 2 as nvarchar(20)) + N')'
                               else ''
                               end
                       end
                                                                    as [Type],
                   case is_computed when 1 then 'yes' else '' end   as [Computed?],
                   max_length                                       as [Length (max bytes)],
                   [precision]                                      as [Prec],
                   [scale]                                          as [Scale],
                   case is_nullable when 1 then 'yes' else '' end   as [Nullable?],
                   case is_identity when 1 then 'yes' else '' end   as [Identity?],
                   case is_replicated when 1 then 'yes' else '' end as [Replicated?],
                   case is_sparse when 1 then 'yes' else '' end     as [Sparse?],
                   case is_filestream when 1 then 'yes' else '' end as [Filestream?],
                   collation_name                                   as [Collation]
            from #indexcolumns as c
            where index_id in (0, 1);

            if (select top 1 parent_object_id from #foreignkeys) is not null
                begin
                    select [database_name] + N':' + parent_object_name + N': ' + foreign_key_name as [Foreign Key],
                           parent_fk_columns                                                      as [Foreign Key Columns],
                           referenced_object_name                                                 as [Referenced Table],
                           referenced_fk_columns                                                  as [Referenced Table Columns],
                           is_disabled                                                            as [Is Disabled?],
                           is_not_trusted                                                         as [Not Trusted?],
                           is_not_for_replication                                                    [Not for Replication?],
                           [update_referential_action_desc]                                       as [Cascading Updates?],
                           [delete_referential_action_desc]                                       as [Cascading Deletes?]
                    from #foreignkeys
                    order by [Foreign Key]
                    option ( recompile );
                end;
            else
                select 'No foreign keys.' as finding;

            /* Show histograms for all stats on this table. More info: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/1900 */
            if EXISTS(select * from sys.all_objects where name = 'dm_db_stats_histogram')
                begin
                    set @dsql = N'SELECT s.name AS [Stat Name], c.name AS [Leading Column Name], hist.step_number AS [Step Number],
                        hist.range_high_key AS [Range High Key], hist.range_rows AS [Range Rows],
                        hist.equal_rows AS [Equal Rows], hist.distinct_range_rows AS [Distinct Range Rows], hist.average_range_rows AS [Average Range Rows],
                        s.auto_created AS [Auto-Created], s.user_created AS [User-Created],
                        props.last_updated AS [Last Updated], s.stats_id AS [StatsID]
                    FROM sys.stats AS s
                    INNER JOIN sys.stats_columns sc ON s.object_id = sc.object_id AND s.stats_id = sc.stats_id AND sc.stats_column_id = 1
                    INNER JOIN sys.columns c ON sc.object_id = c.object_id AND sc.column_id = c.column_id
                    CROSS APPLY sys.dm_db_stats_properties(s.object_id, s.stats_id) AS props
                    CROSS APPLY sys.dm_db_stats_histogram(s.[object_id], s.stats_id) AS hist
                    WHERE s.object_id = @ObjectID
                    ORDER BY s.auto_created, s.user_created, s.name, hist.step_number;';
                    exec sp_executesql @dsql, N'@ObjectID INT', @objectid;
                end


        end;

        --If @TableName is NOT specified...
--Act based on the @Mode and @Filter. (@Filter applies only when @Mode=0 "diagnose")
    else
        begin
            ;
            if @mode in (0, 4) /* DIAGNOSE*/
                begin
                    ;
                    raiserror (N'@Mode=0 or 4, we are diagnosing.', 0,1) with nowait;

                    ----------------------------------------
                    --Multiple Index Personalities: Check_id 0-10
                    ----------------------------------------
                    begin
                        ;

                        --SELECT  [object_id], key_column_names, database_id
                        --                   FROM        #IndexSanity
                        --                   WHERE  index_type IN (1,2) /* Clustered, NC only*/
                        --                        AND is_hypothetical = 0
                        --                        AND is_disabled = 0
                        --                   GROUP BY    [object_id], key_column_names, database_id
                        --                   HAVING    COUNT(*) > 1


                        raiserror ('check_id 1: Duplicate keys', 0,1) with nowait;
                        with duplicate_indexes
                                 as (select [object_id], key_column_names, database_id, [schema_name]
                                     from #indexsanity as ip
                                     where index_type in (1, 2) /* Clustered, NC only*/
                                       and is_hypothetical = 0
                                       and is_disabled = 0
                                       and is_primary_key = 0
                                       and EXISTS(
                                             select 1 / 0
                                             from #indexsanitysize ips
                                             where ip.index_sanity_id = ips.index_sanity_id
                                               and ip.database_id = ips.database_id
                                               and ip.schema_name = ips.schema_name
                                               and ips.total_reserved_mb >= case
                                                                                when (@getalldatabases = 1 or @mode = 0)
                                                                                    then @thresholdmb
                                                                                else ips.total_reserved_mb
                                                 end
                                         )
                                     group by [object_id], key_column_names, database_id, [schema_name]
                                     having COUNT(*) > 1)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 1                                                                             as check_id,
                               ip.index_sanity_id,
                               50                                                                            as priority,
                               'Multiple Index Personalities'                                                as findings_group,
                               'Duplicate keys'                                                              as finding,
                               [database_name]                                                               as [Database Name],
                               N'http://BrentOzar.com/go/duplicateindex'                                     as url,
                               N'Index Name: ' + ip.index_name + N' Table Name: ' + ip.db_schema_object_name as details,
                               ip.index_definition,
                               ip.secret_columns,
                               ip.index_usage_summary,
                               ips.index_size_summary
                        from duplicate_indexes di
                                 join #indexsanity ip on di.[object_id] = ip.[object_id]
                            and ip.database_id = di.database_id
                            and ip.[schema_name] = di.[schema_name]
                            and di.key_column_names = ip.key_column_names
                                 join #indexsanitysize ips on ip.index_sanity_id = ips.index_sanity_id
                            and ip.database_id = ips.database_id
                            and ip.schema_name = ips.schema_name
                            /* WHERE clause limits to only @ThresholdMB or larger duplicate indexes when getting all databases or using PainRelief mode */
                        where ips.total_reserved_mb >= case
                                                           when (@getalldatabases = 1 or @mode = 0) then @thresholdmb
                                                           else ips.total_reserved_mb end
                          and ip.is_primary_key = 0
                        order by ip.object_id, ip.key_column_names_with_sort_order
                        option ( recompile );

                        raiserror ('check_id 2: Keys w/ identical leading columns.', 0,1) with nowait;
                        with borderline_duplicate_indexes
                                 as (select distinct database_id,
                                                     [object_id],
                                                     first_key_column_name,
                                                     key_column_names,
                                                     COUNT([object_id])
                                                           over ( partition by database_id, [object_id], first_key_column_name ) as number_dupes
                                     from #indexsanity
                                     where index_type in (1, 2) /* Clustered, NC only*/
                                       and is_hypothetical = 0
                                       and is_disabled = 0
                                       and is_primary_key = 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 2                                         as check_id,
                               ip.index_sanity_id,
                               60                                        as priority,
                               'Multiple Index Personalities'            as findings_group,
                               'Borderline duplicate keys'               as finding,
                               [database_name]                           as [Database Name],
                               N'http://BrentOzar.com/go/duplicateindex' as url,
                               ip.db_schema_object_indexid               as details,
                               ip.index_definition,
                               ip.secret_columns,
                               ip.index_usage_summary,
                               ips.index_size_summary
                        from #indexsanity as ip
                                 join #indexsanitysize ips on ip.index_sanity_id = ips.index_sanity_id
                        where EXISTS(
                                select di.[object_id]
                                from borderline_duplicate_indexes as di
                                where di.[object_id] = ip.[object_id]
                                  and di.database_id = ip.database_id
                                  and di.first_key_column_name = ip.first_key_column_name
                                  and di.key_column_names <> ip.key_column_names
                                  and di.number_dupes > 1
                            )
                          and ip.is_primary_key = 0
                        order by ip.[schema_name], ip.[object_name], ip.key_column_names, ip.include_column_names
                        option ( recompile );

                    end;
                    ----------------------------------------
                    --Aggressive Indexes: Check_id 10-19
                    ----------------------------------------
                    begin
                        ;

                        raiserror (N'check_id 11: Total lock wait time > 5 minutes (row + page) with long average waits', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 11                                                                       as check_id,
                               i.index_sanity_id,
                               10                                                                       as priority,
                               N'Aggressive '
                                   + case COALESCE((select SUM(1)
                                                    from #indexsanity ime
                                                             inner join #indexsanity iothers
                                                                        on ime.database_id = iothers.database_id
                                                                            and ime.object_id = iothers.object_id
                                                                            and iothers.index_id > 1
                                                    where i.index_sanity_id = ime.index_sanity_id
                                                      and iothers.is_hypothetical = 0
                                                      and iothers.is_disabled = 0
                                                   ), 0)
                                         when 0 then N'Under-Indexing'
                                         when 1 then N'Under-Indexing'
                                         when 2 then N'Under-Indexing'
                                         when 3 then N'Under-Indexing'
                                         when 4 then N'Indexes'
                                         when 5 then N'Indexes'
                                         when 6 then N'Indexes'
                                         when 7 then N'Indexes'
                                         when 8 then N'Indexes'
                                         when 9 then N'Indexes'
                                         else N'Over-Indexing'
                                   end                                                                  as findings_group,
                               N'Total lock wait time > 5 minutes (row + page) with long average waits' as finding,
                               [database_name]                                                          as [Database Name],
                               N'http://BrentOzar.com/go/AggressiveIndexes'                             as url,
                               (i.db_schema_object_indexid + N': ' +
                                sz.index_lock_wait_summary + N' NC indexes on table: ') collate database_default +
                               CAST(COALESCE((select SUM(1)
                                              from #indexsanity ime
                                                       inner join #indexsanity iothers
                                                                  on ime.database_id = iothers.database_id
                                                                      and ime.object_id = iothers.object_id
                                                                      and iothers.index_id > 1
                                              where i.index_sanity_id = ime.index_sanity_id
                                                and iothers.is_hypothetical = 0
                                                and iothers.is_disabled = 0
                                             ), 0)
                                   as nvarchar(30))                                                     as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where (total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) > 300000
                          and (sz.avg_page_lock_wait_in_ms + sz.avg_row_lock_wait_in_ms) > 5000
                        group by i.index_sanity_id, [database_name], i.db_schema_object_indexid,
                                 sz.index_lock_wait_summary, i.index_definition, i.secret_columns,
                                 i.index_usage_summary, sz.index_size_summary, sz.index_sanity_id
                        order by 4, [database_name], 8
                        option ( recompile );

                        raiserror (N'check_id 12: Total lock wait time > 5 minutes (row + page) with short average waits', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 12                                                                        as check_id,
                               i.index_sanity_id,
                               10                                                                        as priority,
                               N'Aggressive '
                                   + case COALESCE((select SUM(1)
                                                    from #indexsanity ime
                                                             inner join #indexsanity iothers
                                                                        on ime.database_id = iothers.database_id
                                                                            and ime.object_id = iothers.object_id
                                                                            and iothers.index_id > 1
                                                    where i.index_sanity_id = ime.index_sanity_id
                                                      and iothers.is_hypothetical = 0
                                                      and iothers.is_disabled = 0
                                                   ), 0)
                                         when 0 then N'Under-Indexing'
                                         when 1 then N'Under-Indexing'
                                         when 2 then N'Under-Indexing'
                                         when 3 then N'Under-Indexing'
                                         when 4 then N'Indexes'
                                         when 5 then N'Indexes'
                                         when 6 then N'Indexes'
                                         when 7 then N'Indexes'
                                         when 8 then N'Indexes'
                                         when 9 then N'Indexes'
                                         else N'Over-Indexing'
                                   end                                                                   as findings_group,
                               N'Total lock wait time > 5 minutes (row + page) with short average waits' as finding,
                               [database_name]                                                           as [Database Name],
                               N'http://BrentOzar.com/go/AggressiveIndexes'                              as url,
                               (i.db_schema_object_indexid + N': ' +
                                sz.index_lock_wait_summary + N' NC indexes on table: ') collate database_default +
                               CAST(COALESCE((select SUM(1)
                                              from #indexsanity ime
                                                       inner join #indexsanity iothers
                                                                  on ime.database_id = iothers.database_id
                                                                      and ime.object_id = iothers.object_id
                                                                      and iothers.index_id > 1
                                              where i.index_sanity_id = ime.index_sanity_id
                                                and iothers.is_hypothetical = 0
                                                and iothers.is_disabled = 0
                                             ), 0)
                                   as nvarchar(30))                                                      as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where (total_row_lock_wait_in_ms + total_page_lock_wait_in_ms) > 300000
                          and (sz.avg_page_lock_wait_in_ms + sz.avg_row_lock_wait_in_ms) < 5000
                        group by i.index_sanity_id, [database_name], i.db_schema_object_indexid,
                                 sz.index_lock_wait_summary, i.index_definition, i.secret_columns,
                                 i.index_usage_summary, sz.index_size_summary, sz.index_sanity_id
                        order by 4, [database_name], 8
                        option ( recompile );

                    end;

                    ----------------------------------------
                    --Index Hoarder: Check_id 20-29
                    ----------------------------------------
                    begin
                        raiserror (N'check_id 20: >=7 NC indexes on any given table. Yes, 7 is an arbitrary number.', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 20                                                                            as check_id,
                               MAX(i.index_sanity_id)                                                        as index_sanity_id,
                               100                                                                           as priority,
                               'Index Hoarder'                                                               as findings_group,
                               'Many NC indexes on a single table'                                           as finding,
                               [database_name]                                                               as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'                                       as url,
                               CAST(COUNT(*) as nvarchar(30)) + ' NC indexes on ' + i.db_schema_object_name  as details,
                               i.db_schema_object_name + ' (' + CAST(COUNT(*) as nvarchar(30)) +
                               ' indexes)'                                                                   as index_definition,
                               ''                                                                            as secret_columns,
                               REPLACE(CONVERT(nvarchar(30), CAST(SUM(total_reads) as money), 1), N'.00', N'') +
                               N' reads (ALL); '
                                   + REPLACE(CONVERT(nvarchar(30), CAST(SUM(user_updates) as money), 1), N'.00', N'') +
                               N' writes (ALL); ',
                               REPLACE(CONVERT(nvarchar(30), CAST(MAX(total_rows) as money), 1), N'.00', N'') +
                               N' rows (MAX)'
                                   + case
                                         when SUM(total_reserved_mb) > 1024 then
                                                 N'; ' + CAST(
                                                     CAST(SUM(total_reserved_mb) / 1024. as numeric(29, 1)) as nvarchar(30)) +
                                                 'GB (ALL)'
                                         when SUM(total_reserved_mb) > 0 then
                                                 N'; ' +
                                                 CAST(CAST(SUM(total_reserved_mb) as numeric(29, 1)) as nvarchar(30)) +
                                                 'MB (ALL)'
                                         else ''
                                   end                                                                       as index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                        where index_id not in (0, 1)
                        group by db_schema_object_name, [i].[database_name]
                        having COUNT(*) >= case
                                               when (@getalldatabases = 1 or @mode = 0)
                                                   then 21
                                               else 7
                            end
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        if @filter = 1 /*@Filter=1 is "ignore unusued" */
                            begin
                                raiserror (N'Skipping checks on unused indexes (21 and 22) because @Filter=1', 0,1) with nowait;
                            end;
                        else /*Otherwise, go ahead and do the checks*/
                            begin
                                raiserror (N'check_id 21: >=5 percent of indexes are unused. Yes, 5 is an arbitrary number.', 0,1) with nowait;
                                declare @percent_nc_indexes_unused numeric(29, 1);
                                declare @nc_indexes_unused_reserved_mb numeric(29, 1);

                                select @percent_nc_indexes_unused = (100.00 * SUM(case
                                                                                      when total_reads = 0
                                                                                          then 1
                                                                                      else 0
                                    end)) / COUNT(*),
                                       @nc_indexes_unused_reserved_mb = SUM(case
                                                                                when total_reads = 0
                                                                                    then sz.total_reserved_mb
                                                                                else 0
                                           end)
                                from #indexsanity i
                                         join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                                where index_id not in (0, 1)
                                  and i.is_unique = 0
                                    /*Skipping tables created in the last week, or modified in past 2 days*/
                                  and i.create_date >= DATEADD(dd, -7, GETDATE())
                                  and i.modify_date > DATEADD(dd, -2, GETDATE())
                                option ( recompile );

                                if @percent_nc_indexes_unused >= 5
                                    insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group,
                                                               finding, [database_name], url, details, index_definition,
                                                               secret_columns, index_usage_summary, index_size_summary)
                                    select 21                                                                     as check_id,
                                           MAX(i.index_sanity_id)                                                 as index_sanity_id,
                                           150                                                                    as priority,
                                           N'Index Hoarder'                                                       as findings_group,
                                           N'More than 5 percent NC indexes are unused'                           as finding,
                                           [database_name]                                                        as [Database Name],
                                           N'http://BrentOzar.com/go/IndexHoarder'                                as url,
                                           CAST(@percent_nc_indexes_unused as nvarchar(30)) + N' percent NC indexes (' +
                                           CAST(COUNT(*) as nvarchar(10)) + N') unused. ' +
                                           N'These take up ' + CAST(@nc_indexes_unused_reserved_mb as nvarchar(30)) +
                                           N'MB of space.'                                                        as details,
                                           i.database_name + ' (' + CAST(COUNT(*) as nvarchar(30)) +
                                           N' indexes)'                                                           as index_definition,
                                           ''                                                                     as secret_columns,
                                           CAST(SUM(total_reads) as nvarchar(256)) + N' reads (ALL); '
                                               + CAST(SUM([user_updates]) as nvarchar(256)) +
                                           N' writes (ALL)'                                                       as index_usage_summary,

                                           REPLACE(CONVERT(nvarchar(30), CAST(MAX([total_rows]) as money), 1), '.00',
                                                   '') + N' rows (MAX)'
                                               + case
                                                     when SUM(total_reserved_mb) > 1024 then
                                                             N'; ' + CAST(
                                                                 CAST(SUM(total_reserved_mb) / 1024. as numeric(29, 1)) as nvarchar(30)) +
                                                             'GB (ALL)'
                                                     when SUM(total_reserved_mb) > 0 then
                                                             N'; ' + CAST(
                                                                 CAST(SUM(total_reserved_mb) as numeric(29, 1)) as nvarchar(30)) +
                                                             'MB (ALL)'
                                                     else ''
                                               end                                                                as index_size_summary
                                    from #indexsanity i
                                             join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                                    where index_id not in (0, 1)
                                      and i.is_unique = 0
                                      and total_reads = 0
                                      and not (@getalldatabases = 1 or @mode = 0)
                                        /*Skipping tables created in the last week, or modified in past 2 days*/
                                      and i.create_date >= DATEADD(dd, -7, GETDATE())
                                      and i.modify_date > DATEADD(dd, -2, GETDATE())
                                    group by i.database_name
                                    option ( recompile );

                                raiserror (N'check_id 22: NC indexes with 0 reads. (Borderline) and >= 10,000 writes', 0,1) with nowait;
                                insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                           [database_name], url, details, index_definition,
                                                           secret_columns, index_usage_summary, index_size_summary)
                                select 22                                      as check_id,
                                       i.index_sanity_id,
                                       100                                     as priority,
                                       N'Index Hoarder'                        as findings_group,
                                       N'Unused NC index with High Writes'     as finding,
                                       [database_name]                         as [Database Name],
                                       N'http://BrentOzar.com/go/IndexHoarder' as url,
                                       N'Reads: 0,'
                                           + N' Writes: '
                                           +
                                       REPLACE(CONVERT(nvarchar(30), CAST((i.user_updates) as money), 1), N'.00', N'')
                                           + N' on: '
                                           + i.db_schema_object_indexid
                                                                               as details,
                                       i.index_definition,
                                       i.secret_columns,
                                       i.index_usage_summary,
                                       sz.index_size_summary
                                from #indexsanity as i
                                         join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                                where i.total_reads = 0
                                  and i.user_updates >= 10000
                                  and i.index_id not in (0, 1) /*NCs only*/
                                  and i.is_unique = 0
                                  and sz.total_reserved_mb >= case
                                                                  when (@getalldatabases = 1 or @mode = 0)
                                                                      then @thresholdmb
                                                                  else sz.total_reserved_mb end
                                order by i.db_schema_object_indexid
                                option ( recompile );
                            end; /*end checks only run when @Filter <> 1*/

                        raiserror (N'check_id 23: Indexes with 7 or more columns. (Borderline)', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 23                                              as check_id,
                               i.index_sanity_id,
                               150                                             as priority,
                               N'Index Hoarder'                                as findings_group,
                               N'Borderline: Wide indexes (7 or more columns)' as finding,
                               [database_name]                                 as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'         as url,
                               CAST(count_key_columns + count_included_columns as nvarchar(10)) + ' columns on '
                                   + i.db_schema_object_indexid                as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where (count_key_columns + count_included_columns) >= 7
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 24: Wide clustered indexes (> 3 columns or > 16 bytes).', 0,1) with nowait;
                        with count_columns as (
                            select database_id,
                                   [object_id],
                                   SUM(case max_length when -1 then 0 else max_length end) as sum_max_length
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                              and key_ordinal > 0
                            group by database_id, object_id
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 24                                                  as check_id,
                               i.index_sanity_id,
                               150                                                 as priority,
                               N'Index Hoarder'                                    as findings_group,
                               N'Wide clustered index (> 3 columns OR > 16 bytes)' as finding,
                               [database_name]                                     as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'             as url,
                               CAST(i.count_key_columns as nvarchar(10)) + N' columns with potential size of '
                                   + CAST(cc.sum_max_length as nvarchar(10))
                                   + N' bytes in clustered index:' + i.db_schema_object_name
                                   + N'. ' +
                               (select CAST(COUNT(*) as nvarchar(23))
                                from #indexsanity i2
                                where i2.[object_id] = i.[object_id]
                                  and i2.database_id = i.database_id
                                  and i2.index_id <> 1
                                  and i2.is_disabled = 0
                                  and i2.is_hypothetical = 0)
                                   + N' NC indexes on the table.'
                                                                                   as details,
                               i.index_definition,
                               secret_columns,
                               i.index_usage_summary,
                               ip.index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and i.database_id = cc.database_id
                        where index_id = 1 /* clustered only */
                          and not (@getalldatabases = 1 or @mode = 0)
                          and (count_key_columns > 3 /*More than three key columns.*/
                            or cc.sum_max_length > 16 /*More than 16 bytes in key */)
                          and i.is_cx_columnstore = 0
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 25: Addicted to nullable columns.', 0,1) with nowait;
                        with count_columns as (
                            select [object_id],
                                   [database_id],
                                   [schema_name],
                                   SUM(case is_nullable when 1 then 0 else 1 end) as non_nullable_columns,
                                   COUNT(*)                                       as total_columns
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                            group by [object_id],
                                     [database_id],
                                     [schema_name]
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 25                                      as check_id,
                               i.index_sanity_id,
                               200                                     as priority,
                               N'Index Hoarder'                        as findings_group,
                               N'Addicted to nulls'                    as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder' as url,
                               i.db_schema_object_name
                                   + N' allows null in ' + CAST((total_columns - non_nullable_columns) as nvarchar(10))
                                   + N' of ' + CAST(total_columns as nvarchar(10))
                                   + N' columns.'                      as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and cc.database_id = ip.database_id
                            and cc.[schema_name] = ip.[schema_name]
                        where i.index_id in (1, 0)
                          and not (@getalldatabases = 1 or @mode = 0)
                          and cc.non_nullable_columns < 2
                          and cc.total_columns > 3
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 26: Wide tables (35+ cols or > 2000 non-LOB bytes).', 0,1) with nowait;
                        with count_columns as (
                            select [object_id],
                                   [database_id],
                                   [schema_name],
                                   SUM(case max_length when -1 then 1 else 0 end)          as count_lob_columns,
                                   SUM(case max_length when -1 then 0 else max_length end) as sum_max_length,
                                   COUNT(*)                                                as total_columns
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                            group by [object_id],
                                     [database_id],
                                     [schema_name]
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 26                                               as check_id,
                               i.index_sanity_id,
                               150                                              as priority,
                               N'Index Hoarder'                                 as findings_group,
                               N'Wide tables: 35+ cols or > 2000 non-LOB bytes' as finding,
                               [database_name]                                  as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'          as url,
                               i.db_schema_object_name
                                   + N' has ' + CAST((total_columns) as nvarchar(10))
                                   + N' total columns with a max possible width of ' +
                               CAST(sum_max_length as nvarchar(10))
                                   + N' bytes.' +
                               case
                                   when count_lob_columns > 0 then CAST((count_lob_columns) as nvarchar(10))
                                       + ' columns are LOB types.'
                                   else ''
                                   end
                                                                                as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and cc.database_id = i.database_id
                            and cc.[schema_name] = i.[schema_name]
                        where i.index_id in (1, 0)
                          and not (@getalldatabases = 1 or @mode = 0)
                          and (cc.total_columns >= 35 or
                               cc.sum_max_length >= 2000)
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 27: Addicted to strings.', 0,1) with nowait;
                        with count_columns as (
                            select [object_id],
                                   [database_id],
                                   [schema_name],
                                   SUM(case
                                           when system_type_name in ('varchar', 'nvarchar', 'char') or max_length = -1
                                               then 1
                                           else 0 end) as string_or_lob_columns,
                                   COUNT(*)            as total_columns
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                            group by [object_id],
                                     [database_id],
                                     [schema_name]
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 27                                                as check_id,
                               i.index_sanity_id,
                               200                                               as priority,
                               N'Index Hoarder'                                  as findings_group,
                               N'Addicted to strings'                            as finding,
                               [database_name]                                   as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'           as url,
                               i.db_schema_object_name
                                   + N' uses string or LOB types for ' + CAST((string_or_lob_columns) as nvarchar(10))
                                   + N' of ' + CAST(total_columns as nvarchar(10))
                                   + N' columns. Check if data types are valid.' as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and cc.database_id = i.database_id
                            and cc.[schema_name] = i.[schema_name]
                                 cross apply (select cc.total_columns - string_or_lob_columns as non_string_or_lob_columns) as calc1
                        where i.index_id in (1, 0)
                          and not (@getalldatabases = 1 or @mode = 0)
                          and calc1.non_string_or_lob_columns <= 1
                          and cc.total_columns > 3
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 28: Non-unique clustered index.', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 28                                      as check_id,
                               i.index_sanity_id,
                               100                                     as priority,
                               N'Index Hoarder'                        as findings_group,
                               N'Non-Unique clustered index'           as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder' as url,
                               N'Uniquifiers will be required! Clustered index: ' + i.db_schema_object_name
                                   + N' and all NC indexes. ' +
                               (select CAST(COUNT(*) as nvarchar(23))
                                from #indexsanity i2
                                where i2.[object_id] = i.[object_id]
                                  and i2.database_id = i.database_id
                                  and i2.index_id <> 1
                                  and i2.is_disabled = 0
                                  and i2.is_hypothetical = 0)
                                   + N' NC indexes on the table.'
                                                                       as details,
                               i.index_definition,
                               secret_columns,
                               i.index_usage_summary,
                               ip.index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                        where index_id = 1 /* clustered only */
                          and not (@getalldatabases = 1 or @mode = 0)
                          and is_unique = 0 /* not unique */
                          and is_cx_columnstore = 0 /* not a clustered columnstore-- no unique option on those */
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 29: NC indexes with 0 reads. (Borderline) and < 10,000 writes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 29                                        as check_id,
                               i.index_sanity_id,
                               150                                       as priority,
                               N'Index Hoarder'                          as findings_group,
                               N'Unused NC index with Low Writes'        as finding,
                               [database_name]                           as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder'   as url,
                               N'0 reads: ' + i.db_schema_object_indexid as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where i.total_reads = 0
                          and i.user_updates < 10000
                          and i.index_id not in (0, 1) /*NCs only*/
                          and i.is_unique = 0
                          and sz.total_reserved_mb >= case
                                                          when (@getalldatabases = 1 or @mode = 0) then @thresholdmb
                                                          else sz.total_reserved_mb end
                            /*Skipping tables created in the last week, or modified in past 2 days*/
                          and i.create_date >= DATEADD(dd, -7, GETDATE())
                          and i.modify_date > DATEADD(dd, -2, GETDATE())
                          and not (@getalldatabases = 1 or @mode = 0)
                        order by i.db_schema_object_indexid
                        option ( recompile );

                    end;
                    ----------------------------------------
                    --Feature-Phobic Indexes: Check_id 30-39
                    ----------------------------------------
                    begin
                        raiserror (N'check_id 30: No indexes with includes', 0,1) with nowait;
                        /* This does not work the way you'd expect with @GetAllDatabases = 1. For details:
               https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/825
            */

                        select database_name,
                               SUM(case when count_included_columns > 0 then 1 else 0 end)                           as number_indexes_with_includes,
                               100. * SUM(case when count_included_columns > 0 then 1 else 0 end) /
                               (1.0 * COUNT(*))                                                                      as percent_indexes_with_includes
                        into #index_includes
                        from #indexsanity
                        where is_hypothetical = 0
                          and is_disabled = 0
                          and not (@getalldatabases = 1 or @mode = 0)
                        group by database_name;

                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 30                                      as check_id,
                               null                                    as index_sanity_id,
                               250                                     as priority,
                               N'Feature-Phobic Indexes'               as findings_group,
                               database_name                           as [Database Name],
                               N'No indexes use includes'              as finding,
                               'http://BrentOzar.com/go/IndexFeatures' as url,
                               N'No indexes use includes'              as details,
                               database_name + N' (Entire database)'   as index_definition,
                               N''                                     as secret_columns,
                               N'N/A'                                  as index_usage_summary,
                               N'N/A'                                  as index_size_summary
                        from #index_includes
                        where number_indexes_with_includes = 0
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 31: < 3 percent of indexes have includes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 31                                                  as check_id,
                               null                                                as index_sanity_id,
                               150                                                 as priority,
                               N'Feature-Phobic Indexes'                           as findings_group,
                               N'Borderline: Includes are used in < 3% of indexes' as findings,
                               database_name                                       as [Database Name],
                               N'http://BrentOzar.com/go/IndexFeatures'            as url,
                               N'Only ' + CAST(percent_indexes_with_includes as nvarchar(20)) +
                               '% of indexes have includes'                        as details,
                               N'Entire database'                                  as index_definition,
                               N''                                                 as secret_columns,
                               N'N/A'                                              as index_usage_summary,
                               N'N/A'                                              as index_size_summary
                        from #index_includes
                        where number_indexes_with_includes > 0
                          and percent_indexes_with_includes <= 3
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 32: filtered indexes and indexed views', 0,1) with nowait;

                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select distinct 32                                                                        as check_id,
                                        null                                                                      as index_sanity_id,
                                        250                                                                       as priority,
                                        N'Feature-Phobic Indexes'                                                 as findings_group,
                                        N'Borderline: No filtered indexes or indexed views exist'                 as finding,
                                        i.database_name                                                           as [Database Name],
                                        N'http://BrentOzar.com/go/IndexFeatures'                                  as url,
                                        N'These are NOT always needed-- but do you know when you would use them?' as details,
                                        i.database_name + N' (Entire database)'                                   as index_definition,
                                        N''                                                                       as secret_columns,
                                        N'N/A'                                                                    as index_usage_summary,
                                        N'N/A'                                                                    as index_size_summary
                        from #indexsanity i
                        where i.database_name not in (
                            select database_name
                            from #indexsanity
                            where filter_definition <> '')
                          and i.database_name not in (
                            select database_name
                            from #indexsanity
                            where is_indexed_view = 1)
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );
                    end;

                    raiserror (N'check_id 33: Potential filtered indexes based on column names.', 0,1) with nowait;

                    insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                               [database_name], url, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary)
                    select 33                                                                                                               as check_id,
                           i.index_sanity_id                                                                                                as index_sanity_id,
                           250                                                                                                              as priority,
                           N'Feature-Phobic Indexes'                                                                                        as findings_group,
                           N'Potential filtered index (based on column name)'                                                               as finding,
                           [database_name]                                                                                                  as [Database Name],
                           N'http://BrentOzar.com/go/IndexFeatures'                                                                         as url,
                           N'A column name in this index suggests it might be a candidate for filtering (is%, %archive%, %active%, %flag%)' as details,
                           i.index_definition,
                           i.secret_columns,
                           i.index_usage_summary,
                           sz.index_size_summary
                    from #indexcolumns ic
                             join #indexsanity i on ic.[object_id] = i.[object_id]
                        and ic.database_id = i.database_id
                        and ic.schema_name = i.schema_name
                        and ic.[index_id] = i.[index_id]
                        and i.[index_id] > 1 /* non-clustered index */
                             join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                    where (column_name like 'is%'
                        or column_name like '%archive%'
                        or column_name like '%active%'
                        or column_name like '%flag%')
                      and not (@getalldatabases = 1 or @mode = 0)
                    option ( recompile );

                    raiserror (N'check_id 34: Filtered index definition columns not in index definition', 0,1) with nowait;

                    insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                               [database_name], url, details, index_definition,
                                               secret_columns, index_usage_summary, index_size_summary)
                    select 34                                        as check_id,
                           i.index_sanity_id,
                           80                                        as priority,
                           N'Forgetful Indexes'                      as findings_group,
                           N'Filter Columns Not In Index Definition' as finding,
                           [database_name]                           as [Database Name],
                           N'http://BrentOzar.com/go/IndexFeatures'  as url,
                           N'The index '
                               + QUOTENAME(i.index_name)
                               + N' on ['
                               + i.db_schema_object_name
                               + N'] has a filter on ['
                               + i.filter_definition
                               + N'] but is missing ['
                               + LTRIM(i.filter_columns_not_in_index)
                               + N'] from the index definition.'
                                                                     as details,
                           i.index_definition,
                           i.secret_columns,
                           i.index_usage_summary,
                           sz.index_size_summary
                    from #indexsanity i
                             join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                    where i.filter_columns_not_in_index is not null
                    order by i.db_schema_object_indexid
                    option ( recompile );

                    ----------------------------------------
                    --Self Loathing Indexes : Check_id 40-49
                    ----------------------------------------
                    begin

                        raiserror (N'check_id 40: Fillfactor in nonclustered 80 percent or less', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 40                                      as check_id,
                               i.index_sanity_id,
                               100                                     as priority,
                               N'Self Loathing Indexes'                as findings_group,
                               N'Low Fill Factor: nonclustered index'  as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing' as url,
                               CAST(fill_factor as nvarchar(10)) + N'% fill factor on ' + db_schema_object_indexid +
                               N'. ' +
                               case
                                   when (last_user_update is null or user_updates < 1)
                                       then N'No writes have been made.'
                                   else
                                           N'Last write was ' + CONVERT(nvarchar(16), last_user_update, 121) +
                                           N' and ' +
                                           CAST(user_updates as nvarchar(25)) + N' updates have been made.'
                                   end
                                                                       as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where index_id > 1
                          and not (@getalldatabases = 1 or @mode = 0)
                          and fill_factor between 1 and 80
                        option ( recompile );

                        raiserror (N'check_id 40: Fillfactor in clustered 80 percent or less', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 40                                      as check_id,
                               i.index_sanity_id,
                               100                                     as priority,
                               N'Self Loathing Indexes'                as findings_group,
                               N'Low Fill Factor: clustered index'     as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing' as url,
                               N'Fill factor on ' + db_schema_object_indexid + N' is ' +
                               CAST(fill_factor as nvarchar(10)) + N'%. ' +
                               case
                                   when (last_user_update is null or user_updates < 1)
                                       then N'No writes have been made.'
                                   else
                                           N'Last write was ' + CONVERT(nvarchar(16), last_user_update, 121) +
                                           N' and ' +
                                           CAST(user_updates as nvarchar(25)) + N' updates have been made.'
                                   end
                                                                       as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                        where index_id = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                          and fill_factor between 1 and 80
                        option ( recompile );


                        raiserror (N'check_id 41: Hypothetical indexes ', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 41                                                 as check_id,
                               i.index_sanity_id,
                               150                                                as priority,
                               N'Self Loathing Indexes'                           as findings_group,
                               N'Hypothetical Index'                              as finding,
                               [database_name]                                    as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'            as url,
                               N'Hypothetical Index: ' + db_schema_object_indexid as details,
                               i.index_definition,
                               i.secret_columns,
                               N''                                                as index_usage_summary,
                               N''                                                as index_size_summary
                        from #indexsanity as i
                        where is_hypothetical = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );


                        raiserror (N'check_id 42: Disabled indexes', 0,1) with nowait;
                        --Note: disabled NC indexes will have O rows in #IndexSanitySize!
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 42                                            as check_id,
                               index_sanity_id,
                               150                                           as priority,
                               N'Self Loathing Indexes'                      as findings_group,
                               N'Disabled Index'                             as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'       as url,
                               N'Disabled Index:' + db_schema_object_indexid as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               'DISABLED'                                    as index_size_summary
                        from #indexsanity as i
                        where is_disabled = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 43: Heaps with forwarded records', 0,1) with nowait;
                        with heaps_cte
                                 as (select [object_id],
                                            [database_id],
                                            [schema_name],
                                            SUM(forwarded_fetch_count) as forwarded_fetch_count,
                                            SUM(leaf_delete_count)     as leaf_delete_count
                                     from #indexpartitionsanity
                                     group by [object_id],
                                              [database_id],
                                              [schema_name]
                                     having SUM(forwarded_fetch_count) > 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 43                                      as check_id,
                               i.index_sanity_id,
                               100                                     as priority,
                               N'Self Loathing Indexes'                as findings_group,
                               N'Heaps with forwarded records'         as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing' as url,
                               case
                                   when h.forwarded_fetch_count >= 922337203685477 then '>= 922,337,203,685,477'
                                   when @daysuptime < 1 then CAST(h.forwarded_fetch_count as nvarchar(256)) +
                                                             N' forwarded fetches against heap: ' +
                                                             db_schema_object_indexid
                                   else REPLACE(CONVERT(nvarchar(256), CAST(CAST(
                                           (h.forwarded_fetch_count /*/@DaysUptime */)
                                       as bigint) as money), 1), '.00', '')
                                   end + N' forwarded fetches per day against heap: '
                                   + db_schema_object_indexid          as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 join heaps_cte h on i.[object_id] = h.[object_id]
                            and i.[database_id] = h.[database_id]
                            and i.[schema_name] = h.[schema_name]
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_id = 0
                          and h.forwarded_fetch_count / @daysuptime > 1000
                          and sz.total_reserved_mb >= case
                                                          when not (@getalldatabases = 1 or @mode = 4) then @thresholdmb
                                                          else sz.total_reserved_mb end
                        option ( recompile );

                        raiserror (N'check_id 49: Heaps with deletes', 0,1) with nowait;
                        with heaps_cte
                                 as (select [object_id],
                                            [database_id],
                                            [schema_name],
                                            SUM(leaf_delete_count) as leaf_delete_count
                                     from #indexpartitionsanity
                                     group by [object_id],
                                              [database_id],
                                              [schema_name]
                                     having SUM(forwarded_fetch_count) < 1000 * @daysuptime /* Only alert about indexes with no forwarded fetches - we already alerted about those in check_id 43 */
                                        and SUM(leaf_delete_count) > 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 49                                      as check_id,
                               i.index_sanity_id,
                               200                                     as priority,
                               N'Self Loathing Indexes'                as findings_group,
                               N'Heaps with deletes'                   as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing' as url,
                               CAST(h.leaf_delete_count as nvarchar(256)) + N' deletes against heap:'
                                   + db_schema_object_indexid          as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 join heaps_cte h on i.[object_id] = h.[object_id]
                            and i.[database_id] = h.[database_id]
                            and i.[schema_name] = h.[schema_name]
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_id = 0
                          and sz.total_reserved_mb >= case
                                                          when not (@getalldatabases = 1 or @mode = 4) then @thresholdmb
                                                          else sz.total_reserved_mb end
                        option ( recompile );

                        raiserror (N'check_id 44: Large Heaps with reads or writes.', 0,1) with nowait;
                        with heaps_cte
                                 as (select [object_id],
                                            [database_id],
                                            [schema_name],
                                            SUM(forwarded_fetch_count) as forwarded_fetch_count,
                                            SUM(leaf_delete_count)     as leaf_delete_count
                                     from #indexpartitionsanity
                                     group by [object_id],
                                              [database_id],
                                              [schema_name]
                                     having SUM(forwarded_fetch_count) > 0
                                         or SUM(leaf_delete_count) > 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 44                                                          as check_id,
                               i.index_sanity_id,
                               100                                                         as priority,
                               N'Self Loathing Indexes'                                    as findings_group,
                               N'Large Active heap'                                        as finding,
                               [database_name]                                             as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'                     as url,
                               N'Should this table be a heap? ' + db_schema_object_indexid as details,
                               i.index_definition,
                               'N/A'                                                       as secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 left join heaps_cte h on i.[object_id] = h.[object_id]
                            and i.[database_id] = h.[database_id]
                            and i.[schema_name] = h.[schema_name]
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_id = 0
                          and (i.total_reads > 0 or i.user_updates > 0)
                          and sz.total_rows >= 100000
                          and h.[object_id] is null /*don't duplicate the prior check.*/
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 45: Medium Heaps with reads or writes.', 0,1) with nowait;
                        with heaps_cte
                                 as (select [object_id],
                                            [database_id],
                                            [schema_name],
                                            SUM(forwarded_fetch_count) as forwarded_fetch_count,
                                            SUM(leaf_delete_count)     as leaf_delete_count
                                     from #indexpartitionsanity
                                     group by [object_id],
                                              [database_id],
                                              [schema_name]
                                     having SUM(forwarded_fetch_count) > 0
                                         or SUM(leaf_delete_count) > 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 45                                                          as check_id,
                               i.index_sanity_id,
                               100                                                         as priority,
                               N'Self Loathing Indexes'                                    as findings_group,
                               N'Medium Active heap'                                       as finding,
                               [database_name]                                             as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'                     as url,
                               N'Should this table be a heap? ' + db_schema_object_indexid as details,
                               i.index_definition,
                               'N/A'                                                       as secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 left join heaps_cte h on i.[object_id] = h.[object_id]
                            and i.[database_id] = h.[database_id]
                            and i.[schema_name] = h.[schema_name]
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_id = 0
                          and (i.total_reads > 0 or i.user_updates > 0)
                          and sz.total_rows >= 10000
                          and sz.total_rows < 100000
                          and h.[object_id] is null /*don't duplicate the prior check.*/
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 46: Small Heaps with reads or writes.', 0,1) with nowait;
                        with heaps_cte
                                 as (select [object_id],
                                            [database_id],
                                            [schema_name],
                                            SUM(forwarded_fetch_count) as forwarded_fetch_count,
                                            SUM(leaf_delete_count)     as leaf_delete_count
                                     from #indexpartitionsanity
                                     group by [object_id],
                                              [database_id],
                                              [schema_name]
                                     having SUM(forwarded_fetch_count) > 0
                                         or SUM(leaf_delete_count) > 0)
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 46                                                          as check_id,
                               i.index_sanity_id,
                               100                                                         as priority,
                               N'Self Loathing Indexes'                                    as findings_group,
                               N'Small Active heap'                                        as finding,
                               [database_name]                                             as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'                     as url,
                               N'Should this table be a heap? ' + db_schema_object_indexid as details,
                               i.index_definition,
                               'N/A'                                                       as secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 left join heaps_cte h on i.[object_id] = h.[object_id]
                            and i.[database_id] = h.[database_id]
                            and i.[schema_name] = h.[schema_name]
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_id = 0
                          and (i.total_reads > 0 or i.user_updates > 0)
                          and sz.total_rows < 10000
                          and h.[object_id] is null /*don't duplicate the prior check.*/
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 47: Heap with a Nonclustered Primary Key', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 47                                                                       as check_id,
                               i.index_sanity_id,
                               100                                                                      as priority,
                               N'Self Loathing Indexes'                                                 as findings_group,
                               N'Heap with a Nonclustered Primary Key'                                  as finding,
                               [database_name]                                                          as [Database Name],
                               N'http://BrentOzar.com/go/SelfLoathing'                                  as url,
                               db_schema_object_indexid + N' is a HEAP with a Nonclustered Primary Key' as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.index_type = 2
                          and i.is_primary_key = 1
                          and EXISTS
                            (
                                select 1 / 0
                                from #indexsanity as isa
                                where i.database_id = isa.database_id
                                  and i.object_id = isa.object_id
                                  and isa.index_id = 0
                            )
                        option ( recompile );

                        raiserror (N'check_id 48: Nonclustered indexes with a bad read to write ratio', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 48                                      as check_id,
                               i.index_sanity_id,
                               100                                     as priority,
                               N'Index Hoarder'                        as findings_group,
                               N'NC index with High Writes:Reads'      as finding,
                               [database_name]                         as [Database Name],
                               N'http://BrentOzar.com/go/IndexHoarder' as url,
                               N'Reads: '
                                   + REPLACE(CONVERT(nvarchar(30), CAST((i.total_reads) as money), 1), N'.00', N'')
                                   + N' Writes: '
                                   + REPLACE(CONVERT(nvarchar(30), CAST((i.user_updates) as money), 1), N'.00', N'')
                                   + N' on: '
                                   + i.db_schema_object_indexid        as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               sz.index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.total_reads > 0 /*Not totally unused*/
                          and i.user_updates >= 10000 /*Decent write activity*/
                          and i.total_reads < 10000
                          and ((i.total_reads * 10) < i.user_updates) /*10x more writes than reads*/
                          and i.index_id not in (0, 1) /*NCs only*/
                          and i.is_unique = 0
                          and sz.total_reserved_mb >= case
                                                          when (@getalldatabases = 1 or @mode = 0) then @thresholdmb
                                                          else sz.total_reserved_mb end
                        order by i.db_schema_object_indexid
                        option ( recompile );

                    end;
                    ----------------------------------------
                    --Indexaphobia
                    --Missing indexes with value >= 5 million: : Check_id 50-59
                    ----------------------------------------
                    begin
                        raiserror (N'check_id 50: Indexaphobia.', 0,1) with nowait;
                        with index_size_cte
                                 as (select i.database_id,
                                            i.schema_name,
                                            i.[object_id],
                                            MAX(i.index_sanity_id)                                                 as index_sanity_id,
                                            ISNULL(NULLIF(MAX(DATEDIFF(day, i.create_date, SYSDATETIME())), 0),
                                                   1)                                                              as create_days,
                                            ISNULL(
                                                        CAST(SUM(case when index_id not in (0, 1) then 1 else 0 end)
                                                            as nvarchar(30)) + N' NC indexes exist (' +
                                                        case
                                                            when SUM(
                                                                         case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end) >
                                                                 1024
                                                                then CAST(CAST(SUM(
                                                                                       case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end) /
                                                                               1024.
                                                                as numeric(29, 1)) as nvarchar(30)) + N'GB); '
                                                            else CAST(SUM(
                                                                    case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end)
                                                                     as nvarchar(30)) + N'MB); '
                                                            end +
                                                        case
                                                            when MAX(sz.[total_rows]) >= 922337203685477
                                                                then '>= 922,337,203,685,477'
                                                            else REPLACE(
                                                                    CONVERT(nvarchar(30), CAST(MAX(sz.[total_rows]) as money), 1),
                                                                    '.00', '')
                                                            end +
                                                        + N' Estimated Rows;'
                                                ,
                                                        N'')                                                       as index_size_summary
                                     from #indexsanity as i
                                              left join #indexsanitysize as sz
                                                        on i.index_sanity_id = sz.index_sanity_id and
                                                           i.database_id = sz.database_id
                                     where i.is_hypothetical = 0
                                       and i.is_disabled = 0
                                     group by i.database_id, i.schema_name, i.[object_id])
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        ,
                        create_tsql
                        ,
                        more_info
                        )

                        select check_id,
                               t.index_sanity_id,
                               t.check_id,
                               t.findings_group,
                               t.finding,
                               t.[Database Name],
                               t.url,
                               t.details,
                               t.[definition],
                               index_estimated_impact,
                               t.index_size_summary,
                               create_tsql,
                               more_info
                        from (
                                 select ROW_NUMBER() over (order by magic_benefit_number desc) as rownum,
                                        50                                                     as check_id,
                                        sz.index_sanity_id,
                                        10                                                     as priority,
                                        N'Indexaphobia'                                        as findings_group,
                                        N'High value missing index'                            as finding,
                                        [database_name]                                        as [Database Name],
                                        N'http://BrentOzar.com/go/Indexaphobia'                as url,
                                        mi.[statement] +
                                        N' Est. benefit per day: ' +
                                        case
                                            when magic_benefit_number >= 922337203685477 then '>= 922,337,203,685,477'
                                            else REPLACE(CONVERT(nvarchar(256), CAST(CAST(
                                                    (magic_benefit_number / @daysuptime)
                                                as bigint) as money), 1), '.00', '')
                                            end                                                as details,
                                        missing_index_details                                  as [definition],
                                        index_estimated_impact,
                                        sz.index_size_summary,
                                        mi.create_tsql,
                                        mi.more_info,
                                        magic_benefit_number,
                                        mi.is_low
                                 from #missingindexes mi
                                          left join index_size_cte sz on mi.[object_id] = sz.object_id
                                     and mi.database_id = sz.database_id
                                     and mi.schema_name = sz.schema_name
                                     /* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
                                 where (@mode = 4 and (magic_benefit_number / case
                                                                                  when sz.create_days < @daysuptime
                                                                                      then sz.create_days
                                                                                  else @daysuptime end) >= 100000)
                                    or (magic_benefit_number / case
                                                                   when sz.create_days < @daysuptime then sz.create_days
                                                                   else @daysuptime end) >= 100000
                             ) as t
                        where t.rownum <= case when (@mode <> 4) then 20 else t.rownum end
                        order by magic_benefit_number desc
                        option ( recompile );


                    end;
                    ----------------------------------------
                    --Abnormal Psychology : Check_id 60-79
                    ----------------------------------------
                    begin
                        raiserror (N'check_id 60: XML indexes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 60                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'XML Indexes'                                as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               N''                                           as index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.is_xml = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 61: Columnstore indexes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 61                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               case
                                   when i.is_nc_columnstore = 1
                                       then N'NC Columnstore Index'
                                   else N'Clustered Columnstore Index'
                                   end                                       as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.is_nc_columnstore = 1
                           or i.is_cx_columnstore = 1
                            and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );


                        raiserror (N'check_id 62: Spatial indexes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 62                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Spatial indexes'                            as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.is_spatial = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 63: Compressed indexes', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 63                                                                         as check_id,
                               i.index_sanity_id,
                               150                                                                        as priority,
                               N'Abnormal Psychology'                                                     as findings_group,
                               N'Compressed indexes'                                                      as finding,
                               [database_name]                                                            as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology'                              as url,
                               i.db_schema_object_indexid + N'. COMPRESSION: ' + sz.data_compression_desc as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')                                          as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where sz.data_compression_desc like '%PAGE%'
                           or sz.data_compression_desc like '%ROW%'
                            and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 64: Partitioned', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 64                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Partitioned indexes'                        as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.partition_key_column_name is not null
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 65: Non-Aligned Partitioned', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 65                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Non-Aligned index on a partitioned table'   as finding,
                               i.[database_name]                             as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanity as iparent on
                                i.[object_id] = iparent.[object_id]
                                and i.database_id = iparent.database_id
                                and i.schema_name = iparent.schema_name
                                and iparent.index_id in (0, 1) /* could be a partitioned heap or clustered table */
                                and iparent.partition_key_column_name is not null /* parent is partitioned*/
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.partition_key_column_name is null
                        option ( recompile );

                        raiserror (N'check_id 66: Recently created tables/indexes (1 week)', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 66                                            as check_id,
                               i.index_sanity_id,
                               200                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Recently created tables/indexes (1 week)'   as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid + N' was created on ' +
                               CONVERT(nvarchar(16), i.create_date, 121) +
                               N'. Tables/indexes which are dropped/created regularly require special methods for index tuning.'
                                                                             as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.create_date >= DATEADD(dd, -7, GETDATE())
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 67: Recently modified tables/indexes (2 days)', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 67                                            as check_id,
                               i.index_sanity_id,
                               200                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Recently modified tables/indexes (2 days)'  as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid + N' was modified on ' +
                               CONVERT(nvarchar(16), i.modify_date, 121) +
                               N'. A large amount of recently modified indexes may mean a lot of rebuilds are occurring each night.'
                                                                             as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.modify_date > DATEADD(dd, -2, GETDATE())
                          and not (@getalldatabases = 1 or @mode = 0)
                          and /*Exclude recently created tables.*/
                            i.create_date < DATEADD(dd, -7, GETDATE())
                        option ( recompile );

                        raiserror (N'check_id 68: Identity columns within 30 percent of the end of range', 0,1) with nowait;
                        -- Allowed Ranges:
                        --int -2,147,483,648 to 2,147,483,647
                        --smallint -32,768 to 32,768
                        --tinyint 0 to 255

                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 68                                            as check_id,
                               i.index_sanity_id,
                               200                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Identity column within ' +
                               CAST(calc1.percent_remaining as nvarchar(256))
                                   + N' percent  end of range'               as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_name + N'.' + QUOTENAME(ic.column_name)
                                   + N' is an identity with type ' + ic.system_type_name
                                   + N', last value of '
                                   + ISNULL((CONVERT(nvarchar(256), CAST(ic.last_value as decimal(38, 0)), 1)), N'NULL')
                                   + N', seed of '
                                   + ISNULL((CONVERT(nvarchar(256), CAST(ic.seed_value as decimal(38, 0)), 1)), N'NULL')
                                   + N', increment of ' + CAST(ic.increment_value as nvarchar(256))
                                   + N', and range of ' +
                               case ic.system_type_name
                                   when 'int' then N'+/- 2,147,483,647'
                                   when 'smallint' then N'+/- 32,768'
                                   when 'tinyint' then N'0 to 255'
                                   else 'unknown'
                                   end
                                                                             as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexcolumns ic on
                                i.object_id = ic.object_id
                                and i.database_id = ic.database_id
                                and i.schema_name = ic.schema_name
                                and i.index_id in (0, 1) /* heaps and cx only */
                                and ic.is_identity = 1
                                and ic.system_type_name in ('tinyint', 'smallint', 'int')
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 cross apply (
                            select CAST(case
                                            when ic.increment_value >= 0
                                                then
                                                case ic.system_type_name
                                                    when 'int' then (2147483647 -
                                                                     (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                    2147483647. * 100
                                                    when 'smallint' then
                                                                (32768 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                32768. * 100
                                                    when 'tinyint' then
                                                                (255 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                255. * 100
                                                    else 999
                                                    end
                                            else --ic.increment_value is negative
                                                case ic.system_type_name
                                                    when 'int' then ABS(-2147483647 -
                                                                        (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                    2147483647. * 100
                                                    when 'smallint' then ABS(-32768 -
                                                                             (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                         32768. * 100
                                                    when 'tinyint' then ABS(
                                                                                0 - (ISNULL(ic.last_value, ic.seed_value) + ic.increment_value)) /
                                                                        255. * 100
                                                    else -1
                                                    end
                                end as numeric(5, 1)) as percent_remaining
                        ) as calc1
                        where i.index_id in (1, 0)
                          and calc1.percent_remaining <= 30
                        union all
                        select 68                                                                 as check_id,
                               i.index_sanity_id,
                               200                                                                as priority,
                               N'Abnormal Psychology'                                             as findings_group,
                               N'Identity column using a negative seed or increment other than 1' as finding,
                               [database_name]                                                    as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology'                      as url,
                               i.db_schema_object_name + N'.' + QUOTENAME(ic.column_name)
                                   + N' is an identity with type ' + ic.system_type_name
                                   + N', last value of '
                                   + ISNULL((CONVERT(nvarchar(256), CAST(ic.last_value as decimal(38, 0)), 1)), N'NULL')
                                   + N', seed of '
                                   + ISNULL((CONVERT(nvarchar(256), CAST(ic.seed_value as decimal(38, 0)), 1)), N'NULL')
                                   + N', increment of ' + CAST(ic.increment_value as nvarchar(256))
                                   + N', and range of ' +
                               case ic.system_type_name
                                   when 'int' then N'+/- 2,147,483,647'
                                   when 'smallint' then N'+/- 32,768'
                                   when 'tinyint' then N'0 to 255'
                                   else 'unknown'
                                   end
                                                                                                  as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexcolumns ic on
                                i.object_id = ic.object_id
                                and i.database_id = ic.database_id
                                and i.schema_name = ic.schema_name
                                and i.index_id in (0, 1) /* heaps and cx only */
                                and ic.is_identity = 1
                                and ic.system_type_name in ('tinyint', 'smallint', 'int')
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                        where i.index_id in (1, 0)
                          and (ic.seed_value < 0 or ic.increment_value <> 1)
                        order by finding, details desc
                        option ( recompile );

                        raiserror (N'check_id 69: Column collation does not match database collation', 0,1) with nowait;
                        with count_columns as (
                            select [object_id],
                                   database_id,
                                   schema_name,
                                   COUNT(*) as column_count
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                              and collation_name <> @collation
                            group by [object_id],
                                     database_id,
                                     schema_name
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 69                                                    as check_id,
                               i.index_sanity_id,
                               150                                                   as priority,
                               N'Abnormal Psychology'                                as findings_group,
                               N'Column collation does not match database collation' as finding,
                               [database_name]                                       as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology'         as url,
                               i.db_schema_object_name
                                   + N' has ' + CAST(column_count as nvarchar(20))
                                   + N' column' + case when column_count > 1 then 's' else '' end
                                   + N' with a different collation than the db collation of '
                                   + @collation                                      as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and cc.database_id = i.database_id
                            and cc.schema_name = i.schema_name
                        where i.index_id in (1, 0)
                          and not (@getalldatabases = 1 or @mode = 0)
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 70: Replicated columns', 0,1) with nowait;
                        with count_columns as (
                            select [object_id],
                                   database_id,
                                   schema_name,
                                   COUNT(*)                                         as column_count,
                                   SUM(case is_replicated when 1 then 1 else 0 end) as replicated_column_count
                            from #indexcolumns ic
                            where index_id in (1, 0) /*Heap or clustered only*/
                            group by object_id,
                                     database_id,
                                     schema_name
                        )
                        insert
                        #blitzindexresults
                        (
                        check_id
                        ,
                        index_sanity_id
                        ,
                        priority
                        ,
                        findings_group
                        ,
                        finding
                        ,
                        [database_name]
                        ,
                        url
                        ,
                        details
                        ,
                        index_definition
                        ,
                        secret_columns
                        ,
                        index_usage_summary
                        ,
                        index_size_summary
                        )
                        select 70                                            as check_id,
                               i.index_sanity_id,
                               200                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Replicated columns'                         as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_name
                                   + N' has ' + CAST(replicated_column_count as nvarchar(20))
                                   + N' out of ' + CAST(column_count as nvarchar(20))
                                   + N' column' + case when column_count > 1 then 's' else '' end
                                   + N' in one or more publications.'
                                                                             as details,
                               i.index_definition,
                               secret_columns,
                               ISNULL(i.index_usage_summary, ''),
                               ISNULL(ip.index_size_summary, '')
                        from #indexsanity i
                                 join #indexsanitysize ip on i.index_sanity_id = ip.index_sanity_id
                                 join count_columns as cc on i.[object_id] = cc.[object_id]
                            and i.database_id = cc.database_id
                            and i.schema_name = cc.schema_name
                        where i.index_id in (1, 0)
                          and replicated_column_count > 0
                          and not (@getalldatabases = 1 or @mode = 0)
                        order by i.db_schema_object_name desc
                        option ( recompile );

                        raiserror (N'check_id 71: Cascading updates or cascading deletes.', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary, more_info)
                        select 71                                            as check_id,
                               null                                          as index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'Cascading Updates or Deletes'               as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               N'Foreign Key ' + foreign_key_name +
                               N' on ' + QUOTENAME(parent_object_name) + N'(' + LTRIM(parent_fk_columns) + N')'
                                   + N' referencing ' + QUOTENAME(referenced_object_name) + N'(' +
                               LTRIM(referenced_fk_columns) + N')'
                                   + N' has settings:'
                                   + case [delete_referential_action_desc]
                                         when N'NO_ACTION' then N''
                                         else N' ON DELETE ' + [delete_referential_action_desc] end
                                   + case [update_referential_action_desc]
                                         when N'NO_ACTION' then N''
                                         else N' ON UPDATE ' + [update_referential_action_desc] end
                                                                             as details,
                               [fk].[database_name]
                                                                             as index_definition,
                               N'N/A'                                        as secret_columns,
                               N'N/A'                                        as index_usage_summary,
                               N'N/A'                                        as index_size_summary,
                               (select top 1 more_info
                                from #indexsanity i
                                where i.object_id = fk.parent_object_id
                                  and i.database_id = fk.database_id
                                  and i.schema_name = fk.schema_name)
                                                                             as more_info
                        from #foreignkeys fk
                        where ([delete_referential_action_desc] <> N'NO_ACTION'
                            or [update_referential_action_desc] <> N'NO_ACTION')
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 72: Columnstore indexes with Trace Flag 834', 0,1) with nowait;
                        if EXISTS(select * from #indexsanity where index_type in (5, 6))
                            and EXISTS(select * from #tracestatus where traceflag = 834 and status = 1)
                            begin
                                insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                           [database_name], url, details, index_definition,
                                                           secret_columns, index_usage_summary, index_size_summary)
                                select 72                                                                                                                        as check_id,
                                       i.index_sanity_id,
                                       150                                                                                                                       as priority,
                                       N'Abnormal Psychology'                                                                                                    as findings_group,
                                       'Columnstore Indexes are being used in conjunction with trace flag 834. Visit the link to see why this can be a bad idea' as finding,
                                       [database_name]                                                                                                           as [Database Name],
                                       N'https://support.microsoft.com/en-us/kb/3210239'                                                                         as url,
                                       i.db_schema_object_indexid                                                                                                as details,
                                       i.index_definition,
                                       i.secret_columns,
                                       i.index_usage_summary,
                                       ISNULL(sz.index_size_summary, '')                                                                                         as index_size_summary
                                from #indexsanity as i
                                         join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                                where i.index_type in (5, 6)
                                option ( recompile );
                            end;

                        raiserror (N'check_id 73: In-Memory OLTP', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 73                                            as check_id,
                               i.index_sanity_id,
                               150                                           as priority,
                               N'Abnormal Psychology'                        as findings_group,
                               N'In-Memory OLTP'                             as finding,
                               [database_name]                               as [Database Name],
                               N'http://BrentOzar.com/go/AbnormalPsychology' as url,
                               i.db_schema_object_indexid                    as details,
                               i.index_definition,
                               i.secret_columns,
                               i.index_usage_summary,
                               ISNULL(sz.index_size_summary, '')             as index_size_summary
                        from #indexsanity as i
                                 join #indexsanitysize sz on i.index_sanity_id = sz.index_sanity_id
                        where i.is_in_memory_oltp = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                    end;

                    ----------------------------------------
                    --Workaholics: Check_id 80-89
                    ----------------------------------------
                    begin

                        raiserror (N'check_id 80: Most scanned indexes (index_usage_stats)', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                            --Workaholics according to index_usage_stats
                            --This isn't perfect: it mentions the number of scans present in a plan
                            --A "scan" isn't necessarily a full scan, but hey, we gotta do the best with what we've got.
                            --in the case of things like indexed views, the operator might be in the plan but never executed
                        select top 5 80 as                                                                            check_id,
                                     i.index_sanity_id as                                                             index_sanity_id,
                                     200 as                                                                           priority,
                                     N'Workaholics' as                                                                findings_group,
                                     N'Scan-a-lots (index_usage_stats)' as                                            finding,
                                     [database_name] as                                                               [Database Name],
                                     N'http://BrentOzar.com/go/Workaholics' as                                        url,
                                     REPLACE(CONVERT(nvarchar(50), CAST(i.user_scans as money), 1), '.00', '')
                                         + N' scans against ' + i.db_schema_object_indexid
                                         + N'. Latest scan: ' + ISNULL(CAST(i.last_user_scan as nvarchar(128)), '?') +
                                     N'. '
                                         + N'ScanFactor=' + CAST(
                                             ((i.user_scans * iss.total_reserved_mb) / 1000000.) as nvarchar(256)) as details,
                                     ISNULL(i.key_column_names_with_sort_order, 'N/A') as                             index_definition,
                                     ISNULL(i.secret_columns, '') as                                                  secret_columns,
                                     i.index_usage_summary as                                                         index_usage_summary,
                                     iss.index_size_summary as                                                        index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize iss on i.index_sanity_id = iss.index_sanity_id
                        where ISNULL(i.user_scans, 0) > 0
                          and not (@getalldatabases = 1 or @mode = 0)
                        order by i.user_scans * iss.total_reserved_mb desc
                        option ( recompile );

                        raiserror (N'check_id 81: Top recent accesses (op stats)', 0,1) with nowait;
                        insert #blitzindexresults (check_id, index_sanity_id, priority, findings_group, finding,
                                                   [database_name], url, details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                            --Workaholics according to index_operational_stats
                            --This isn't perfect either: range_scan_count contains full scans, partial scans, even seeks in nested loop ops
                            --But this can help bubble up some most-accessed tables
                        select top 5 81                                                                          as check_id,
                                     i.index_sanity_id                                                           as index_sanity_id,
                                     200                                                                         as priority,
                                     N'Workaholics'                                                              as findings_group,
                                     N'Top recent accesses (index_op_stats)'                                     as finding,
                                     [database_name]                                                             as [Database Name],
                                     N'http://BrentOzar.com/go/Workaholics'                                      as url,
                                     ISNULL(REPLACE(
                                                    CONVERT(nvarchar(50), CAST(
                                                                                  (iss.total_range_scan_count + iss.total_singleton_lookup_count) as money),
                                                                          1),
                                                    N'.00', N'')
                                                + N' uses of ' + i.db_schema_object_indexid + N'. '
                                                + REPLACE(
                                                    CONVERT(nvarchar(50), CAST(iss.total_range_scan_count as money), 1),
                                                    N'.00', N'') + N' scans or seeks. '
                                                + REPLACE(CONVERT(nvarchar(50),
                                                                  CAST(iss.total_singleton_lookup_count as money), 1),
                                                          N'.00', N'') + N' singleton lookups. '
                                                + N'OpStatsFactor=' + CAST(
                                                    ((((iss.total_range_scan_count + iss.total_singleton_lookup_count) *
                                                       iss.total_reserved_mb)) / 1000000.) as varchar(256)),
                                            '')                                                                  as details,
                                     ISNULL(i.key_column_names_with_sort_order, 'N/A')                           as index_definition,
                                     ISNULL(i.secret_columns, '')                                                as secret_columns,
                                     i.index_usage_summary                                                       as index_usage_summary,
                                     iss.index_size_summary                                                      as index_size_summary
                        from #indexsanity i
                                 join #indexsanitysize iss on i.index_sanity_id = iss.index_sanity_id
                        where (ISNULL(iss.total_range_scan_count, 0) > 0 or
                               ISNULL(iss.total_singleton_lookup_count, 0) > 0)
                          and not (@getalldatabases = 1 or @mode = 0)
                        order by ((iss.total_range_scan_count + iss.total_singleton_lookup_count) *
                                  iss.total_reserved_mb) desc
                        option ( recompile );


                    end;

                    ----------------------------------------
                    --Statistics Info: Check_id 90-99
                    ----------------------------------------
                    begin

                        raiserror (N'check_id 90: Outdated statistics', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 90                                                             as check_id,
                               200                                                            as priority,
                               'Functioning Statistaholics'                                   as findings_group,
                               'Statistic Abandonment Issues',
                               s.database_name,
                               ''                                                             as url,
                               'Statistics on this table were last updated ' +
                               case s.last_statistics_update
                                   when null then N' NEVER '
                                   else CONVERT(nvarchar(20), s.last_statistics_update) +
                                        ' have had ' + CONVERT(nvarchar(100), s.modification_counter) +
                                        ' modifications in that time, which is ' +
                                        CONVERT(nvarchar(100), s.percent_modifications) +
                                        '% of the table.'
                                   end                                                        as details,
                               QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' +
                               QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' +
                               QUOTENAME(s.statistics_name) + '.' + QUOTENAME(s.column_names) as index_definition,
                               'N/A'                                                          as secret_columns,
                               'N/A'                                                          as index_usage_summary,
                               'N/A'                                                          as index_size_summary
                        from #statistics as s
                        where s.last_statistics_update <= CONVERT(datetime, GETDATE() - 7)
                          and s.percent_modifications >= 10.
                          and s.rows >= 10000
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 91: Statistics with a low sample rate', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 91                                                                                                           as check_id,
                               200                                                                                                          as priority,
                               'Functioning Statistaholics'                                                                                 as findings_group,
                               'Antisocial Samples',
                               s.database_name,
                               ''                                                                                                           as url,
                               'Only ' + CONVERT(nvarchar(100), s.percent_sampled) +
                               '% of the rows were sampled during the last statistics update. This may lead to poor cardinality estimates.' as details,
                               QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' +
                               QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' +
                               QUOTENAME(s.statistics_name) + '.' +
                               QUOTENAME(s.column_names)                                                                                    as index_definition,
                               'N/A'                                                                                                        as secret_columns,
                               'N/A'                                                                                                        as index_usage_summary,
                               'N/A'                                                                                                        as index_size_summary
                        from #statistics as s
                        where s.rows_sampled < 1.
                          and s.rows >= 10000
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 92: Statistics with NO RECOMPUTE', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 92                                                                                                                                as check_id,
                               200                                                                                                                               as priority,
                               'Functioning Statistaholics'                                                                                                      as findings_group,
                               'Cyberphobic Samples',
                               s.database_name,
                               ''                                                                                                                                as url,
                               'The statistic ' + QUOTENAME(s.statistics_name) +
                               ' is set to not recompute. This can be helpful if data is really skewed, but harmful if you expect automatic statistics updates.' as details,
                               QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' +
                               QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' +
                               QUOTENAME(s.statistics_name) + '.' +
                               QUOTENAME(s.column_names)                                                                                                         as index_definition,
                               'N/A'                                                                                                                             as secret_columns,
                               'N/A'                                                                                                                             as index_usage_summary,
                               'N/A'                                                                                                                             as index_size_summary
                        from #statistics as s
                        where s.no_recompute = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        raiserror (N'check_id 93: Statistics with filters', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 93                                                                                                     as check_id,
                               200                                                                                                    as priority,
                               'Functioning Statistaholics'                                                                           as findings_group,
                               'Filter Fixation',
                               s.database_name,
                               ''                                                                                                     as url,
                               'The statistic ' + QUOTENAME(s.statistics_name) + ' is filtered on [' +
                               s.filter_definition +
                               ']. It could be part of a filtered index, or just a filtered statistic. This is purely informational.' as details,
                               QUOTENAME(database_name) + '.' + QUOTENAME(s.schema_name) + '.' +
                               QUOTENAME(s.table_name) + '.' + QUOTENAME(s.index_name) + '.' +
                               QUOTENAME(s.statistics_name) + '.' +
                               QUOTENAME(s.column_names)                                                                              as index_definition,
                               'N/A'                                                                                                  as secret_columns,
                               'N/A'                                                                                                  as index_usage_summary,
                               'N/A'                                                                                                  as index_size_summary
                        from #statistics as s
                        where s.has_filter = 1
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                    end;

                    ----------------------------------------
                    --Computed Column Info: Check_id 99-109
                    ----------------------------------------
                    begin

                        raiserror (N'check_id 99: Computed Columns That Reference Functions', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 99                                                                                                                                                      as check_id,
                               50                                                                                                                                                      as priority,
                               'Cold Calculators'                                                                                                                                      as findings_group,
                               'Serial Forcer'                                                                                                                                         as finding,
                               cc.database_name,
                               ''                                                                                                                                                      as url,
                               'The computed column ' + QUOTENAME(cc.column_name) + ' on ' + QUOTENAME(cc.schema_name) +
                               '.' + QUOTENAME(cc.table_name) + ' is based on ' + cc.definition
                                   +
                               '. That indicates it may reference a scalar function, or a CLR function with data access, which can cause all queries and maintenance to run serially.' as details,
                               cc.column_definition,
                               'N/A'                                                                                                                                                   as secret_columns,
                               'N/A'                                                                                                                                                   as index_usage_summary,
                               'N/A'                                                                                                                                                   as index_size_summary
                        from #computedcolumns as cc
                        where cc.is_function = 1
                        option ( recompile );

                        raiserror (N'check_id 100: Computed Columns that are not Persisted.', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 100                     as check_id,
                               200                     as priority,
                               'Cold Calculators'      as findings_group,
                               'Definition Defeatists' as finding,
                               cc.database_name,
                               ''                      as url,
                               'The computed column ' + QUOTENAME(cc.column_name) + ' on ' + QUOTENAME(cc.schema_name) +
                               '.' + QUOTENAME(cc.table_name) +
                               ' is not persisted, which means it will be calculated when a query runs.' +
                               'You can change this with the following command, if the definition is deterministic: ALTER TABLE ' +
                               QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' ALTER COLUMN ' +
                               cc.column_name +
                               ' ADD PERSISTED'        as details,
                               cc.column_definition,
                               'N/A'                   as secret_columns,
                               'N/A'                   as index_usage_summary,
                               'N/A'                   as index_size_summary
                        from #computedcolumns as cc
                        where cc.is_persisted = 0
                          and not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        ----------------------------------------
                        --Temporal Table Info: Check_id 110-119
                        ----------------------------------------
                        raiserror (N'check_id 110: Temporal Tables.', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)

                        select 110               as check_id,
                               200               as priority,
                               'Temporal Tables' as findings_group,
                               'Obsessive Compulsive Tables',
                               t.database_name,
                               ''                as url,
                               'The table ' + QUOTENAME(t.schema_name) + '.' + QUOTENAME(t.table_name) +
                               ' is a temporal table, with rows versioned in '
                                   + QUOTENAME(t.history_schema_name) + '.' + QUOTENAME(t.history_table_name) +
                               ' on History columns ' + QUOTENAME(t.start_column_name) + ' and ' +
                               QUOTENAME(t.end_column_name) + '.'
                                                 as details,
                               ''                as index_definition,
                               'N/A'             as secret_columns,
                               'N/A'             as index_usage_summary,
                               'N/A'             as index_size_summary
                        from #temporaltables as t
                        where not (@getalldatabases = 1 or @mode = 0)
                        option ( recompile );

                        ----------------------------------------
                        --Check Constraint Info: Check_id 120-129
                        ----------------------------------------

                        raiserror (N'check_id 120: Check Constraints That Reference Functions', 0,1) with nowait;
                        insert #blitzindexresults (check_id, priority, findings_group, finding, [database_name], url,
                                                   details, index_definition,
                                                   secret_columns, index_usage_summary, index_size_summary)
                        select 99                                                                                                                                                      as check_id,
                               50                                                                                                                                                      as priority,
                               'Obsessive Constraintive'                                                                                                                               as findings_group,
                               'Serial Forcer'                                                                                                                                         as finding,
                               cc.database_name,
                               'https://www.brentozar.com/archive/2016/01/another-reason-why-scalar-functions-in-computed-columns-is-a-bad-idea/'                                      as url,
                               'The check constraint ' + QUOTENAME(cc.constraint_name) + ' on ' +
                               QUOTENAME(cc.schema_name) + '.' + QUOTENAME(cc.table_name) + ' is based on ' +
                               cc.definition
                                   +
                               '. That indicates it may reference a scalar function, or a CLR function with data access, which can cause all queries and maintenance to run serially.' as details,
                               cc.column_definition,
                               'N/A'                                                                                                                                                   as secret_columns,
                               'N/A'                                                                                                                                                   as index_usage_summary,
                               'N/A'                                                                                                                                                   as index_size_summary
                        from #checkconstraints as cc
                        where cc.is_function = 1
                        option ( recompile );

                    end;

                    raiserror (N'Insert a row to help people find help', 0,1) with nowait;
                    if DATEDIFF(mm, @versiondate, GETDATE()) > 6
                        begin
                            insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                       index_definition,
                                                       index_usage_summary, index_size_summary)
                            values (-1, 0,
                                    'Outdated sp_BlitzIndex', 'sp_BlitzIndex is Over 6 Months Old',
                                    'http://FirstResponderKit.org/',
                                    'Fine wine gets better with age, but this ' + @scriptversionname +
                                    ' is more like bad cheese. Time to get a new one.',
                                    @daysuptimeinsertvalue, N'', N'');
                        end;

                    if EXISTS(select * from #blitzindexresults)
                        begin
                            insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                       index_definition,
                                                       index_usage_summary, index_size_summary)
                            values (-1, 0,
                                    @scriptversionname,
                                    case
                                        when @getalldatabases = 1 then N'All Databases'
                                        else N'Database ' + QUOTENAME(@databasename) + N' as of ' +
                                             CONVERT(nvarchar(16), GETDATE(), 121) end,
                                    N'From Your Community Volunteers', N'http://FirstResponderKit.org',
                                    @daysuptimeinsertvalue, N'', N'');
                        end;
                    else
                        if @mode = 0 or (@getalldatabases = 1 and @mode <> 4)
                            begin
                                insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                           index_definition,
                                                           index_usage_summary, index_size_summary)
                                values (-1, 0,
                                        @scriptversionname,
                                        case
                                            when @getalldatabases = 1 then N'All Databases'
                                            else N'Database ' + QUOTENAME(@databasename) + N' as of ' +
                                                 CONVERT(nvarchar(16), GETDATE(), 121) end,
                                        N'From Your Community Volunteers', N'http://FirstResponderKit.org',
                                        @daysuptimeinsertvalue, N'', N'');
                                insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                           index_definition,
                                                           index_usage_summary, index_size_summary)
                                values (1, 0,
                                        N'No Major Problems Found',
                                        N'Nice Work!',
                                        N'http://FirstResponderKit.org',
                                        N'Consider running with @Mode = 4 in individual databases (not all) for more detailed diagnostics.',
                                        N'The new default Mode 0 only looks for very serious index issues.',
                                        @daysuptimeinsertvalue, N'');

                            end;
                        else
                            begin
                                insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                           index_definition,
                                                           index_usage_summary, index_size_summary)
                                values (-1, 0,
                                        @scriptversionname,
                                        case
                                            when @getalldatabases = 1 then N'All Databases'
                                            else N'Database ' + QUOTENAME(@databasename) + N' as of ' +
                                                 CONVERT(nvarchar(16), GETDATE(), 121) end,
                                        N'From Your Community Volunteers', N'http://FirstResponderKit.org',
                                        @daysuptimeinsertvalue, N'', N'');
                                insert #blitzindexresults (priority, check_id, findings_group, finding, url, details,
                                                           index_definition,
                                                           index_usage_summary, index_size_summary)
                                values (1, 0,
                                        N'No Problems Found',
                                        N'Nice job! Or more likely, you have a nearly empty database.',
                                        N'http://FirstResponderKit.org', 'Time to go read some blog posts.',
                                        @daysuptimeinsertvalue, N'', N'');

                            end;

                    raiserror (N'Returning results.', 0,1) with nowait;

                    /*Return results.*/
                    if (@mode = 0)
                        begin
                            if (@outputtype <> 'NONE')
                                begin
                                    select priority,
                                           ISNULL(br.findings_group, N'') +
                                           case when ISNULL(br.finding, N'') <> N'' then N': ' else N'' end
                                               + br.finding                             as [Finding],
                                           br.[database_name]                           as [Database Name],
                                           br.details                                   as [Details: schema.table.index(indexid)],
                                           br.index_definition                          as [Definition: [Property]] ColumnName {datatype maxbytes}],
                                           ISNULL(br.secret_columns, '')                as [Secret Columns],
                                           br.index_usage_summary                       as [Usage],
                                           br.index_size_summary                        as [Size],
                                           COALESCE(br.more_info, sn.more_info, '')     as [More Info],
                                           br.url,
                                           COALESCE(br.create_tsql, ts.create_tsql, '') as [Create TSQL]
                                    from #blitzindexresults br
                                             left join #indexsanity sn on
                                        br.index_sanity_id = sn.index_sanity_id
                                             left join #indexcreatetsql ts on
                                        br.index_sanity_id = ts.index_sanity_id
                                    where br.check_id in (0, 1, 2, 11, 12, 13,
                                                          22, 34, 43, 47, 48,
                                                          50, 65, 68, 73, 99)
                                    order by br.priority asc, br.check_id asc, br.blitz_result_id asc,
                                             br.findings_group asc
                                    option (recompile);
                                end;

                        end;
                    else
                        if (@mode = 4)
                            if (@outputtype <> 'NONE')
                                begin
                                    select priority,
                                           ISNULL(br.findings_group, N'') +
                                           case when ISNULL(br.finding, N'') <> N'' then N': ' else N'' end
                                               + br.finding                             as [Finding],
                                           br.[database_name]                           as [Database Name],
                                           br.details                                   as [Details: schema.table.index(indexid)],
                                           br.index_definition                          as [Definition: [Property]] ColumnName {datatype maxbytes}],
                                           ISNULL(br.secret_columns, '')                as [Secret Columns],
                                           br.index_usage_summary                       as [Usage],
                                           br.index_size_summary                        as [Size],
                                           COALESCE(br.more_info, sn.more_info, '')     as [More Info],
                                           br.url,
                                           COALESCE(br.create_tsql, ts.create_tsql, '') as [Create TSQL]
                                    from #blitzindexresults br
                                             left join #indexsanity sn on
                                        br.index_sanity_id = sn.index_sanity_id
                                             left join #indexcreatetsql ts on
                                        br.index_sanity_id = ts.index_sanity_id
                                    order by br.priority asc, br.check_id asc, br.blitz_result_id asc,
                                             br.findings_group asc
                                    option (recompile);
                                end;

                end; /* End @Mode=0 or 4 (diagnose)*/
            else
                if (@mode = 1) /*Summarize*/
                    begin
                        --This mode is to give some overall stats on the database.
                        if (@outputtype <> 'NONE')
                            begin
                                raiserror (N'@Mode=1, we are summarizing.', 0,1) with nowait;

                                select DB_NAME(i.database_id)                                              as [Database Name],
                                       CAST((COUNT(*)) as nvarchar(256))                                   as [Number Objects],
                                       CAST(CAST(SUM(sz.total_reserved_mb) /
                                                 1024. as numeric(29, 1)) as nvarchar(500))                as [All GB],
                                       CAST(CAST(SUM(sz.total_reserved_lob_mb) /
                                                 1024. as numeric(29, 1)) as nvarchar(500))                as [LOB GB],
                                       CAST(CAST(SUM(sz.total_reserved_row_overflow_mb) /
                                                 1024. as numeric(29, 1)) as nvarchar(500))                as [Row Overflow GB],
                                       CAST(SUM(case when index_id = 1 then 1 else 0 end) as nvarchar(50)) as [Clustered Tables],
                                       CAST(SUM(case when index_id = 1 then sz.total_reserved_mb else 0 end)
                                           /
                                            1024. as numeric(29, 1))                                       as [Clustered Tables GB],
                                       SUM(case when index_id not in (0, 1) then 1 else 0 end)             as [NC Indexes],
                                       CAST(SUM(case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end)
                                           /
                                            1024. as numeric(29, 1))                                       as [NC Indexes GB],
                                       case
                                           when SUM(
                                                        case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end) >
                                                0 then
                                               CAST(
                                                           SUM(case when index_id in (0, 1) then sz.total_reserved_mb else 0 end)
                                                           / SUM(
                                                                   case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end) as numeric(29, 1))
                                           else 0 end                                                      as [ratio table: NC Indexes],
                                       SUM(case when index_id = 0 then 1 else 0 end)                       as [Heaps],
                                       CAST(SUM(case when index_id = 0 then sz.total_reserved_mb else 0 end)
                                           /
                                            1024. as numeric(29, 1))                                       as [Heaps GB],
                                       SUM(case
                                               when index_id in (0, 1) and partition_key_column_name is not null then 1
                                               else 0 end)                                                 as [Partitioned Tables],
                                       SUM(case
                                               when index_id not in (0, 1) and partition_key_column_name is not null
                                                   then 1
                                               else 0 end)                                                 as [Partitioned NCs],
                                       CAST(SUM(case
                                                    when partition_key_column_name is not null then sz.total_reserved_mb
                                                    else 0 end) /
                                            1024. as numeric(29, 1))                                       as [Partitioned GB],
                                       SUM(case when filter_definition <> '' then 1 else 0 end)            as [Filtered Indexes],
                                       SUM(case when is_indexed_view = 1 then 1 else 0 end)                as [Indexed Views],
                                       MAX(total_rows)                                                     as [Max Row Count],
                                       CAST(MAX(case when index_id in (0, 1) then sz.total_reserved_mb else 0 end)
                                           /
                                            1024. as numeric(29, 1))                                       as [Max Table GB],
                                       CAST(MAX(case when index_id not in (0, 1) then sz.total_reserved_mb else 0 end)
                                           /
                                            1024. as numeric(29, 1))                                       as [Max NC Index GB],
                                       SUM(case
                                               when index_id in (0, 1) and sz.total_reserved_mb > 1024 then 1
                                               else 0 end)                                                 as [Count Tables > 1GB],
                                       SUM(case
                                               when index_id in (0, 1) and sz.total_reserved_mb > 10240 then 1
                                               else 0 end)                                                 as [Count Tables > 10GB],
                                       SUM(case
                                               when index_id in (0, 1) and sz.total_reserved_mb > 102400 then 1
                                               else 0 end)                                                 as [Count Tables > 100GB],
                                       SUM(case
                                               when index_id not in (0, 1) and sz.total_reserved_mb > 1024 then 1
                                               else 0 end)                                                 as [Count NCs > 1GB],
                                       SUM(case
                                               when index_id not in (0, 1) and sz.total_reserved_mb > 10240 then 1
                                               else 0 end)                                                 as [Count NCs > 10GB],
                                       SUM(case
                                               when index_id not in (0, 1) and sz.total_reserved_mb > 102400 then 1
                                               else 0 end)                                                 as [Count NCs > 100GB],
                                       MIN(create_date)                                                    as [Oldest Create Date],
                                       MAX(create_date)                                                    as [Most Recent Create Date],
                                       MAX(modify_date)                                                    as [Most Recent Modify Date],
                                       1                                                                   as [Display Order]
                                from #indexsanity as i
                                         --left join here so we don't lose disabled nc indexes
                                         left join #indexsanitysize as sz
                                                   on i.index_sanity_id = sz.index_sanity_id
                                group by DB_NAME(i.database_id)
                                union all
                                select case
                                           when @getalldatabases = 1 then N'All Databases'
                                           else N'Database ' + N' as of ' + CONVERT(nvarchar(16), GETDATE(), 121) end,
                                       @scriptversionname,
                                       N'From Your Community Volunteers',
                                       N'http://FirstResponderKit.org',
                                       @daysuptimeinsertvalue,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       null,
                                       0 as display_order
                                order by [Display Order] asc
                                option (recompile);
                            end;

                    end; /* End @Mode=1 (summarize)*/
                else
                    if (@mode = 2) /*Index Detail*/
                        begin
                            --This mode just spits out all the detail without filters.
                            --This supports slicing AND dicing in Excel
                            raiserror (N'@Mode=2, here''s the details on existing indexes.', 0,1) with nowait;


                            /* Checks if @OutputServerName is populated with a valid linked server, and that the database name specified is valid */
                            declare @validoutputserver bit;
                            declare @validoutputlocation bit;
                            declare @linkedserverdbcheck nvarchar(2000);
                            declare @validlinkedserverdb int;
                            declare @tmpdbchk table
                                              (
                                                  cnt int
                                              );
                            declare @stringtoexecute nvarchar(max);

                            if @outputservername is not null
                                begin
                                    if (SUBSTRING(@outputtablename, 2, 1) = '#')
                                        begin
                                            raiserror ('Due to the nature of temporary tables, outputting to a linked server requires a permanent table.', 16, 0);
                                        end;
                                    else
                                        if EXISTS(select server_id
                                                  from sys.servers
                                                  where QUOTENAME([name]) = @outputservername)
                                            begin
                                                set @linkedserverdbcheck = 'SELECT 1 WHERE EXISTS (SELECT * FROM ' +
                                                                           @outputservername +
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
                                    if (SUBSTRING(@outputtablename, 2, 2) = '##')
                                        begin
                                            set @stringtoexecute =
                                                    N' IF (OBJECT_ID(''[tempdb].[dbo].@@@OutputTableName@@@'') IS NOT NULL) DROP TABLE @@@OutputTableName@@@';
                                            set @stringtoexecute =
                                                    REPLACE(@stringtoexecute, '@@@OutputTableName@@@', @outputtablename);
                                            exec (@stringtoexecute);

                                            set @outputservername =
                                                    QUOTENAME(CAST(SERVERPROPERTY('ServerName') as nvarchar(128)));
                                            set @outputdatabasename = '[tempdb]';
                                            set @outputschemaname = '[dbo]';
                                            set @validoutputlocation = 1;
                                        end;
                                    else
                                        if (SUBSTRING(@outputtablename, 2, 1) = '#')
                                            begin
                                                raiserror ('Due to the nature of Dymamic SQL, only global (i.e. double pound (##)) temp tables are supported for @OutputTableName', 16, 0);
                                            end;
                                        else
                                            if @outputdatabasename is not null
                                                and @outputschemaname is not null
                                                and @outputtablename is not null
                                                and EXISTS(select *
                                                           from sys.databases
                                                           where QUOTENAME([name]) = @outputdatabasename)
                                                begin
                                                    set @validoutputlocation = 1;
                                                    set @outputservername =
                                                            QUOTENAME(CAST(SERVERPROPERTY('ServerName') as nvarchar(128)));
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

                            if (@validoutputlocation = 0 and @outputtype = 'NONE')
                                begin
                                    raiserror ('Invalid output location and no output asked',12,1);
                                    return;
                                end;

                            /* @OutputTableName lets us export the results to a permanent table */
                            declare @runid uniqueidentifier;
                            set @runid = NEWID();

                            if (@validoutputlocation = 1 and
                                COALESCE(@outputservername, @outputdatabasename, @outputschemaname,
                                         @outputtablename) is not null)
                                begin
                                    declare @tableexists bit;
                                    declare @schemaexists bit;
                                    set @stringtoexecute =
                                            N'SET @SchemaExists = 0;
					SET @TableExists = 0;
					IF EXISTS(SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''@@@OutputSchemaName@@@'')
						SET @SchemaExists = 1
					IF EXISTS (SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''@@@OutputSchemaName@@@'' AND QUOTENAME(TABLE_NAME) = ''@@@OutputTableName@@@'')
						SET @TableExists = 1';

                                    set @stringtoexecute =
                                            REPLACE(@stringtoexecute, '@@@OutputServerName@@@', @outputservername);
                                    set @stringtoexecute =
                                            REPLACE(@stringtoexecute, '@@@OutputDatabaseName@@@', @outputdatabasename);
                                    set @stringtoexecute =
                                            REPLACE(@stringtoexecute, '@@@OutputSchemaName@@@', @outputschemaname);
                                    set @stringtoexecute =
                                            REPLACE(@stringtoexecute, '@@@OutputTableName@@@', @outputtablename);

                                    exec sp_executesql @stringtoexecute,
                                         N'@TableExists BIT OUTPUT, @SchemaExists BIT OUTPUT', @tableexists output,
                                         @schemaexists output;

                                    if @schemaexists = 1
                                        begin
                                            if @tableexists = 0
                                                begin
                                                    set @stringtoexecute =
                                                            N'CREATE TABLE @@@OutputDatabaseName@@@.@@@OutputSchemaName@@@.@@@OutputTableName@@@
										(
											[id] INT IDENTITY(1,1) NOT NULL,
											[run_id] UNIQUEIDENTIFIER,
											[run_datetime] DATETIME,
											[server_name] NVARCHAR(128),
											[database_name] NVARCHAR(128),
											[schema_name] NVARCHAR(128),
											[table_name] NVARCHAR(128),
											[index_name] NVARCHAR(128),
                                            [Drop_Tsql] NVARCHAR(MAX),
                                            [Create_Tsql] NVARCHAR(MAX),
											[index_id] INT,
											[db_schema_object_indexid] NVARCHAR(500),
											[object_type] NVARCHAR(15),
											[index_definition] NVARCHAR(MAX),
											[key_column_names_with_sort_order] NVARCHAR(MAX),
											[count_key_columns] INT,
											[include_column_names] NVARCHAR(MAX),
											[count_included_columns] INT,
											[secret_columns] NVARCHAR(MAX),
											[count_secret_columns] INT,
											[partition_key_column_name] NVARCHAR(MAX),
											[filter_definition] NVARCHAR(MAX),
											[is_indexed_view] BIT,
											[is_primary_key] BIT,
											[is_XML] BIT,
											[is_spatial] BIT,
											[is_NC_columnstore] BIT,
											[is_CX_columnstore] BIT,
											[is_in_memory_oltp] BIT,
											[is_disabled] BIT,
											[is_hypothetical] BIT,
											[is_padded] BIT,
											[fill_factor] INT,
											[is_referenced_by_foreign_key] BIT,
											[last_user_seek] DATETIME,
											[last_user_scan] DATETIME,
											[last_user_lookup] DATETIME,
											[last_user_update] DATETIME,
											[total_reads] BIGINT,
											[user_updates] BIGINT,
											[reads_per_write] MONEY,
											[index_usage_summary] NVARCHAR(200),
											[total_singleton_lookup_count] BIGINT,
											[total_range_scan_count] BIGINT,
											[total_leaf_delete_count] BIGINT,
											[total_leaf_update_count] BIGINT,
											[index_op_stats] NVARCHAR(200),
											[partition_count] INT,
											[total_rows] BIGINT,
											[total_reserved_MB] NUMERIC(29,2),
											[total_reserved_LOB_MB] NUMERIC(29,2),
											[total_reserved_row_overflow_MB] NUMERIC(29,2),
											[index_size_summary] NVARCHAR(300),
											[total_row_lock_count] BIGINT,
											[total_row_lock_wait_count] BIGINT,
											[total_row_lock_wait_in_ms] BIGINT,
											[avg_row_lock_wait_in_ms] BIGINT,
											[total_page_lock_count] BIGINT,
											[total_page_lock_wait_count] BIGINT,
											[total_page_lock_wait_in_ms] BIGINT,
											[avg_page_lock_wait_in_ms] BIGINT,
											[total_index_lock_promotion_attempt_count] BIGINT,
											[total_index_lock_promotion_count] BIGINT,
											[data_compression_desc] NVARCHAR(4000),
						                    [page_latch_wait_count] BIGINT,
								            [page_latch_wait_in_ms] BIGINT,
								            [page_io_latch_wait_count] BIGINT,
								            [page_io_latch_wait_in_ms] BIGINT,
											[create_date] DATETIME,
											[modify_date] DATETIME,
											[more_info] NVARCHAR(500),
											[display_order] INT,
											CONSTRAINT [PK_ID_@@@RunID@@@] PRIMARY KEY CLUSTERED ([id] ASC)
										);';

                                                    set @stringtoexecute = REPLACE(@stringtoexecute,
                                                                                   '@@@OutputDatabaseName@@@',
                                                                                   @outputdatabasename);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputSchemaName@@@', @outputschemaname);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputTableName@@@', @outputtablename);
                                                    set @stringtoexecute = REPLACE(@stringtoexecute, '@@@RunID@@@', @runid);

                                                    if @validoutputserver = 1
                                                        begin
                                                            set @stringtoexecute = REPLACE(@stringtoexecute, '''', '''''');
                                                            exec ('EXEC('''+@stringtoexecute+''') AT ' + @outputservername);
                                                        end;
                                                    else
                                                        begin
                                                            exec (@stringtoexecute);
                                                        end;
                                                end; /* @TableExists = 0 */

                                            set @stringtoexecute =
                                                    N'IF EXISTS(SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = ''@@@OutputSchemaName@@@'')
								AND NOT EXISTS (SELECT * FROM @@@OutputServerName@@@.@@@OutputDatabaseName@@@.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = ''@@@OutputSchemaName@@@'' AND QUOTENAME(TABLE_NAME) = ''@@@OutputTableName@@@'')
								SET @TableExists = 0
							ELSE
								SET @TableExists = 1';

                                            set @tableexists = null;
                                            set @stringtoexecute =
                                                    REPLACE(@stringtoexecute, '@@@OutputServerName@@@', @outputservername);
                                            set @stringtoexecute = REPLACE(@stringtoexecute, '@@@OutputDatabaseName@@@',
                                                                           @outputdatabasename);
                                            set @stringtoexecute =
                                                    REPLACE(@stringtoexecute, '@@@OutputSchemaName@@@', @outputschemaname);
                                            set @stringtoexecute =
                                                    REPLACE(@stringtoexecute, '@@@OutputTableName@@@', @outputtablename);

                                            exec sp_executesql @stringtoexecute, N'@TableExists BIT OUTPUT',
                                                 @tableexists output;

                                            if @tableexists = 1
                                                begin
                                                    set @stringtoexecute =
                                                            N'INSERT @@@OutputServerName@@@.@@@OutputDatabaseName@@@.@@@OutputSchemaName@@@.@@@OutputTableName@@@
										(
											[run_id],
											[run_datetime],
											[server_name],
											[database_name],
											[schema_name],
											[table_name],
											[index_name],
                                            [Drop_Tsql],
                                            [Create_Tsql],
											[index_id],
											[db_schema_object_indexid],
											[object_type],
											[index_definition],
											[key_column_names_with_sort_order],
											[count_key_columns],
											[include_column_names],
											[count_included_columns],
											[secret_columns],
											[count_secret_columns],
											[partition_key_column_name],
											[filter_definition],
											[is_indexed_view],
											[is_primary_key],
											[is_XML],
											[is_spatial],
											[is_NC_columnstore],
											[is_CX_columnstore],
                                            [is_in_memory_oltp],
											[is_disabled],
											[is_hypothetical],
											[is_padded],
											[fill_factor],
											[is_referenced_by_foreign_key],
											[last_user_seek],
											[last_user_scan],
											[last_user_lookup],
											[last_user_update],
											[total_reads],
											[user_updates],
											[reads_per_write],
											[index_usage_summary],
											[total_singleton_lookup_count],
											[total_range_scan_count],
											[total_leaf_delete_count],
											[total_leaf_update_count],
											[index_op_stats],
											[partition_count],
											[total_rows],
											[total_reserved_MB],
											[total_reserved_LOB_MB],
											[total_reserved_row_overflow_MB],
											[index_size_summary],
											[total_row_lock_count],
											[total_row_lock_wait_count],
											[total_row_lock_wait_in_ms],
											[avg_row_lock_wait_in_ms],
											[total_page_lock_count],
											[total_page_lock_wait_count],
											[total_page_lock_wait_in_ms],
											[avg_page_lock_wait_in_ms],
											[total_index_lock_promotion_attempt_count],
											[total_index_lock_promotion_count],
											[data_compression_desc],
						                    [page_latch_wait_count],
								            [page_latch_wait_in_ms],
								            [page_io_latch_wait_count],
								            [page_io_latch_wait_in_ms],
											[create_date],
											[modify_date],
											[more_info],
											[display_order]
										)
									SELECT ''@@@RunID@@@'',
										''@@@GETDATE@@@'',
										''@@@LocalServerName@@@'',
										-- Below should be a copy/paste of the real query
										-- Make sure all quotes are escaped
										i.[database_name] AS [Database Name],
										i.[schema_name] AS [Schema Name],
										i.[object_name] AS [Object Name],
										ISNULL(i.index_name, '''') AS [Index Name],
                                        CASE
						                    WHEN i.is_primary_key = 1 AND i.index_definition <> ''[HEAP]''
							                    THEN N''-ALTER TABLE '' + QUOTENAME(i.[database_name]) + N''.'' + QUOTENAME(i.[schema_name]) + N''.'' + QUOTENAME(i.[object_name]) +
							                         N'' DROP CONSTRAINT '' + QUOTENAME(i.index_name) + N'';''
						                    WHEN i.is_primary_key = 0 AND i.index_definition <> ''[HEAP]''
						                        THEN N''--DROP INDEX ''+ QUOTENAME(i.index_name) + N'' ON '' + QUOTENAME(i.[database_name]) + N''.'' +
							                         QUOTENAME(i.[schema_name]) + N''.'' + QUOTENAME(i.[object_name]) + N'';''
						                ELSE N''''
						                END AS [Drop TSQL],
					                    CASE
						                    WHEN i.index_definition = ''[HEAP]'' THEN N''''
					                            ELSE N''--'' + ict.create_tsql END AS [Create TSQL],
										CAST(i.index_id AS NVARCHAR(10))AS [Index ID],
										db_schema_object_indexid AS [Details: schema.table.index(indexid)],
										CASE    WHEN index_id IN ( 1, 0 ) THEN ''TABLE''
											ELSE ''NonClustered''
											END AS [Object Type],
										LEFT(index_definition,4000) AS [Definition: [Property]] ColumnName {datatype maxbytes}],
										ISNULL(LTRIM(key_column_names_with_sort_order), '''') AS [Key Column Names With Sort],
										ISNULL(count_key_columns, 0) AS [Count Key Columns],
										ISNULL(include_column_names, '''') AS [Include Column Names],
										ISNULL(count_included_columns,0) AS [Count Included Columns],
										ISNULL(secret_columns,'''') AS [Secret Column Names],
										ISNULL(count_secret_columns,0) AS [Count Secret Columns],
										ISNULL(partition_key_column_name, '''') AS [Partition Key Column Name],
										ISNULL(filter_definition, '''') AS [Filter Definition],
										is_indexed_view AS [Is Indexed View],
										is_primary_key AS [Is Primary Key],
										is_XML AS [Is XML],
										is_spatial AS [Is Spatial],
										is_NC_columnstore AS [Is NC Columnstore],
										is_CX_columnstore AS [Is CX Columnstore],
										is_in_memory_oltp AS [Is In-Memory OLTP],
										is_disabled AS [Is Disabled],
										is_hypothetical AS [Is Hypothetical],
										is_padded AS [Is Padded],
										fill_factor AS [Fill Factor],
										is_referenced_by_foreign_key AS [Is Reference by Foreign Key],
										last_user_seek AS [Last User Seek],
										last_user_scan AS [Last User Scan],
										last_user_lookup AS [Last User Lookup],
										last_user_update AS [Last User Update],
										total_reads AS [Total Reads],
										user_updates AS [User Updates],
										reads_per_write AS [Reads Per Write],
										index_usage_summary AS [Index Usage],
										sz.total_singleton_lookup_count AS [Singleton Lookups],
										sz.total_range_scan_count AS [Range Scans],
										sz.total_leaf_delete_count AS [Leaf Deletes],
										sz.total_leaf_update_count AS [Leaf Updates],
										sz.index_op_stats AS [Index Op Stats],
										sz.partition_count AS [Partition Count],
										sz.total_rows AS [Rows],
										sz.total_reserved_MB AS [Reserved MB],
										sz.total_reserved_LOB_MB AS [Reserved LOB MB],
										sz.total_reserved_row_overflow_MB AS [Reserved Row Overflow MB],
										sz.index_size_summary AS [Index Size],
										sz.total_row_lock_count AS [Row Lock Count],
										sz.total_row_lock_wait_count AS [Row Lock Wait Count],
										sz.total_row_lock_wait_in_ms AS [Row Lock Wait ms],
										sz.avg_row_lock_wait_in_ms AS [Avg Row Lock Wait ms],
										sz.total_page_lock_count AS [Page Lock Count],
										sz.total_page_lock_wait_count AS [Page Lock Wait Count],
										sz.total_page_lock_wait_in_ms AS [Page Lock Wait ms],
										sz.avg_page_lock_wait_in_ms AS [Avg Page Lock Wait ms],
										sz.total_index_lock_promotion_attempt_count AS [Lock Escalation Attempts],
										sz.total_index_lock_promotion_count AS [Lock Escalations],
										sz.data_compression_desc AS [Data Compression],
						                sz.page_latch_wait_count,
								        sz.page_latch_wait_in_ms,
								        sz.page_io_latch_wait_count,
								        sz.page_io_latch_wait_in_ms,
										i.create_date AS [Create Date],
										i.modify_date AS [Modify Date],
										more_info AS [More Info],
										1 AS [Display Order]
									FROM #IndexSanity AS i
									LEFT JOIN #IndexSanitySize AS sz ON i.index_sanity_id = sz.index_sanity_id
                                    LEFT JOIN #IndexCreateTsql AS ict  ON i.index_sanity_id = ict.index_sanity_id
									ORDER BY [Database Name], [Schema Name], [Object Name], [Index ID]
									OPTION (RECOMPILE);';

                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputServerName@@@', @outputservername);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputDatabaseName@@@',
                                                                    @outputdatabasename);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputSchemaName@@@', @outputschemaname);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@OutputTableName@@@', @outputtablename);
                                                    set @stringtoexecute = REPLACE(@stringtoexecute, '@@@RunID@@@', @runid);
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@GETDATE@@@', GETDATE());
                                                    set @stringtoexecute =
                                                            REPLACE(@stringtoexecute, '@@@LocalServerName@@@',
                                                                    CAST(SERVERPROPERTY('ServerName') as nvarchar(128)));
                                                    exec (@stringtoexecute);
                                                end; /* @TableExists = 1 */
                                            else
                                                raiserror ('Creation of the output table failed.', 16, 0);
                                        end; /* @TableExists = 0 */
                                    else
                                        raiserror (N'Invalid schema name, data could not be saved.', 16, 0);
                                end; /* @ValidOutputLocation = 1 */
                            else

                                if (@outputtype <> 'NONE')
                                    begin
                                        select i.[database_name]                                   as [Database Name],
                                               i.[schema_name]                                     as [Schema Name],
                                               i.[object_name]                                     as [Object Name],
                                               ISNULL(i.index_name, '')                            as [Index Name],
                                               CAST(i.index_id as nvarchar(10))                    as [Index ID],
                                               db_schema_object_indexid                            as [Details: schema.table.index(indexid)],
                                               case
                                                   when index_id in (1, 0) then 'TABLE'
                                                   else 'NonClustered'
                                                   end                                             as [Object Type],
                                               index_definition                                    as [Definition: [Property]] ColumnName {datatype maxbytes}],
                                               ISNULL(LTRIM(key_column_names_with_sort_order), '') as [Key Column Names With Sort],
                                               ISNULL(count_key_columns, 0)                        as [Count Key Columns],
                                               ISNULL(include_column_names, '')                    as [Include Column Names],
                                               ISNULL(count_included_columns, 0)                   as [Count Included Columns],
                                               ISNULL(secret_columns, '')                          as [Secret Column Names],
                                               ISNULL(count_secret_columns, 0)                     as [Count Secret Columns],
                                               ISNULL(partition_key_column_name, '')               as [Partition Key Column Name],
                                               ISNULL(filter_definition, '')                       as [Filter Definition],
                                               is_indexed_view                                     as [Is Indexed View],
                                               is_primary_key                                      as [Is Primary Key],
                                               is_xml                                              as [Is XML],
                                               is_spatial                                          as [Is Spatial],
                                               is_nc_columnstore                                   as [Is NC Columnstore],
                                               is_cx_columnstore                                   as [Is CX Columnstore],
                                               is_in_memory_oltp                                   as [Is In-Memory OLTP],
                                               is_disabled                                         as [Is Disabled],
                                               is_hypothetical                                     as [Is Hypothetical],
                                               is_padded                                           as [Is Padded],
                                               fill_factor                                         as [Fill Factor],
                                               is_referenced_by_foreign_key                        as [Is Reference by Foreign Key],
                                               last_user_seek                                      as [Last User Seek],
                                               last_user_scan                                      as [Last User Scan],
                                               last_user_lookup                                    as [Last User Lookup],
                                               last_user_update                                    as [Last User Update],
                                               total_reads                                         as [Total Reads],
                                               user_updates                                        as [User Updates],
                                               reads_per_write                                     as [Reads Per Write],
                                               index_usage_summary                                 as [Index Usage],
                                               sz.total_singleton_lookup_count                     as [Singleton Lookups],
                                               sz.total_range_scan_count                           as [Range Scans],
                                               sz.total_leaf_delete_count                          as [Leaf Deletes],
                                               sz.total_leaf_update_count                          as [Leaf Updates],
                                               sz.index_op_stats                                   as [Index Op Stats],
                                               sz.partition_count                                  as [Partition Count],
                                               sz.total_rows                                       as [Rows],
                                               sz.total_reserved_mb                                as [Reserved MB],
                                               sz.total_reserved_lob_mb                            as [Reserved LOB MB],
                                               sz.total_reserved_row_overflow_mb                   as [Reserved Row Overflow MB],
                                               sz.index_size_summary                               as [Index Size],
                                               sz.total_row_lock_count                             as [Row Lock Count],
                                               sz.total_row_lock_wait_count                        as [Row Lock Wait Count],
                                               sz.total_row_lock_wait_in_ms                        as [Row Lock Wait ms],
                                               sz.avg_row_lock_wait_in_ms                          as [Avg Row Lock Wait ms],
                                               sz.total_page_lock_count                            as [Page Lock Count],
                                               sz.total_page_lock_wait_count                       as [Page Lock Wait Count],
                                               sz.total_page_lock_wait_in_ms                       as [Page Lock Wait ms],
                                               sz.avg_page_lock_wait_in_ms                         as [Avg Page Lock Wait ms],
                                               sz.total_index_lock_promotion_attempt_count         as [Lock Escalation Attempts],
                                               sz.total_index_lock_promotion_count                 as [Lock Escalations],
                                               sz.page_latch_wait_count                            as [Page Latch Wait Count],
                                               sz.page_latch_wait_in_ms                            as [Page Latch Wait ms],
                                               sz.page_io_latch_wait_count                         as [Page IO Latch Wait Count],
                                               sz.page_io_latch_wait_in_ms                         as [Page IO Latch Wait ms],
                                               sz.total_forwarded_fetch_count                      as [Forwarded Fetches],
                                               sz.data_compression_desc                            as [Data Compression],
                                               i.create_date                                       as [Create Date],
                                               i.modify_date                                       as [Modify Date],
                                               more_info                                           as [More Info],
                                               case
                                                   when i.is_primary_key = 1 and i.index_definition <> '[HEAP]'
                                                       then N'--ALTER TABLE ' + QUOTENAME(i.[database_name]) + N'.' +
                                                            QUOTENAME(i.[schema_name]) + N'.' +
                                                            QUOTENAME(i.[object_name])
                                                       + N' DROP CONSTRAINT ' + QUOTENAME(i.index_name) + N';'
                                                   when i.is_primary_key = 0 and i.index_definition <> '[HEAP]'
                                                       then N'--DROP INDEX ' + QUOTENAME(i.index_name) + N' ON ' +
                                                            QUOTENAME(i.[database_name]) + N'.' +
                                                            QUOTENAME(i.[schema_name]) + N'.' +
                                                            QUOTENAME(i.[object_name]) + N';'
                                                   else N''
                                                   end                                             as [Drop TSQL],
                                               case
                                                   when i.index_definition = '[HEAP]' then N''
                                                   else N'--' + ict.create_tsql end                as [Create TSQL],
                                               1                                                   as [Display Order]
                                        from #indexsanity as i --left join here so we don't lose disabled nc indexes
                                                 left join #indexsanitysize as sz on i.index_sanity_id = sz.index_sanity_id
                                                 left join #indexcreatetsql as ict on i.index_sanity_id = ict.index_sanity_id
                                        order by [Database Name], [Schema Name], [Object Name], [Index ID]
                                        option (recompile);
                                    end;


                        end; /* End @Mode=2 (index detail)*/
                    else
                        if (@mode = 3) /*Missing index Detail*/
                            begin
                                if (@outputtype <> 'NONE')
                                    begin
                                        ;
                                        with create_date as (
                                            select i.database_id,
                                                   i.schema_name,
                                                   i.[object_id],
                                                   ISNULL(NULLIF(MAX(DATEDIFF(day, i.create_date, SYSDATETIME())), 0),
                                                          1) as create_days
                                            from #indexsanity as i
                                            group by i.database_id, i.schema_name, i.object_id
                                        )
                                        select mi.database_name          as [Database Name],
                                               mi.[schema_name]          as [Schema],
                                               mi.table_name             as [Table],
                                               CAST((mi.magic_benefit_number / case
                                                                                   when cd.create_days < @daysuptime
                                                                                       then cd.create_days
                                                                                   else @daysuptime end) as bigint)
                                                                         as [Magic Benefit Number],
                                               mi.missing_index_details  as [Missing Index Details],
                                               mi.avg_total_user_cost    as [Avg Query Cost],
                                               mi.avg_user_impact        as [Est Index Improvement],
                                               mi.user_seeks             as [Seeks],
                                               mi.user_scans             as [Scans],
                                               mi.unique_compiles        as [Compiles],
                                               mi.equality_columns       as [Equality Columns],
                                               mi.inequality_columns     as [Inequality Columns],
                                               mi.included_columns       as [Included Columns],
                                               mi.index_estimated_impact as [Estimated Impact],
                                               mi.create_tsql            as [Create TSQL],
                                               mi.more_info              as [More Info],
                                               1                         as [Display Order],
                                               mi.is_low
                                        from #missingindexes as mi
                                                 left join create_date as cd
                                                           on mi.[object_id] = cd.object_id
                                                               and mi.database_id = cd.database_id
                                                               and mi.schema_name = cd.schema_name
                                            /* Minimum benefit threshold = 100k/day of uptime OR since table creation date, whichever is lower*/
                                        where (mi.magic_benefit_number / case
                                                                             when cd.create_days < @daysuptime
                                                                                 then cd.create_days
                                                                             else @daysuptime end) >= 100000
                                        union all
                                        select @scriptversionname,
                                               N'From Your Community Volunteers',
                                               N'http://FirstResponderKit.org',
                                               100000000000,
                                               @daysuptimeinsertvalue,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               null,
                                               0    as [Display Order],
                                               null as is_low
                                        order by [Display Order] asc, [Magic Benefit Number] desc
                                        option (recompile);
                                    end;

                                if (@bringthepain = 1
                                    and @databasename is not null
                                    and @getalldatabases = 0)
                                    begin

                                        exec sp_BlitzCache @sortorder = 'sp_BlitzIndex', @databasename = @databasename,
                                             @bringthepain = 1, @queryfilter = 'statement', @hidesummary = 1;

                                    end;


                            end; /* End @Mode=3 (index detail)*/
        end;
end try
begin catch
    raiserror (N'Failure analyzing temp tables.', 0,1) with nowait;

    select @msg = ERROR_MESSAGE(), @errorseverity = ERROR_SEVERITY(), @errorstate = ERROR_STATE();

    raiserror (@msg,
        @errorseverity,
        @errorstate
        );

    while @@trancount > 0
        rollback;

    return;
end catch;
go
