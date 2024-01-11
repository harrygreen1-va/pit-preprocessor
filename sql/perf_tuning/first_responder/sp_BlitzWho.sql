if OBJECT_ID('dbo.sp_BlitzWho') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzWho AS RETURN 0;')
go

alter procedure dbo.sp_blitzwho @help tinyint = 0,
                                @showsleepingspids tinyint = 0,
                                @expertmode bit = 0,
                                @debug bit = 0,
                                @outputdatabasename nvarchar(256) = null,
                                @outputschemaname nvarchar(256) = null,
                                @outputtablename nvarchar(256) = null,
                                @outputtableretentiondays tinyint = 3,
                                @minelapsedseconds int = 0,
                                @mincputime int = 0,
                                @minlogicalreads int = 0,
                                @minphysicalreads int = 0,
                                @minwrites int = 0,
                                @mintempdbmb int = 0,
                                @minrequestedmemorykb int = 0,
                                @minblockingseconds int = 0,
                                @checkdateoverride datetimeoffset = null,
                                @version varchar(30) = null output,
                                @versiondate datetime = null output,
                                @versioncheckmode bit = 0
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
sp_BlitzWho from http://FirstResponderKit.org

This script gives you a snapshot of everything currently executing on your SQL Server.

To learn more, visit http://FirstResponderKit.org where you can download new
versions for free, watch training videos on how it works, get more info on
the findings, contribute your own code, and more.

Known limitations of this version:
 - Only Microsoft-supported versions of SQL Server. Sorry, 2005 and 2000.
 - Outputting to table is only supported with SQL Server 2012 and higher.
 - If @OutputDatabaseName and @OutputSchemaName are populated, the database and
   schema must already exist. We will not create them, only the table.
   
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

/* Get the major and minor build numbers */
    declare @productversion nvarchar(128)
        ,@productversionmajor decimal(10, 2)
        ,@productversionminor decimal(10, 2)
        ,@platform nvarchar(8) /* Azure or NonAzure are acceptable */ = (select case when @@VERSION like '%Azure%' then N'Azure' else N'NonAzure' end as [Platform])
        ,@enhanceflag bit = 0
        ,@blockingcheck nvarchar(max)
        ,@stringtoselect nvarchar(max)
        ,@stringtoexecute nvarchar(max)
        ,@outputtablecleanupdate date
        ,@sessionwaits bit = 0
        ,@sessionwaitssql nvarchar(max) =
        N'LEFT JOIN ( SELECT DISTINCT
                               wait.session_id ,
                               ( SELECT TOP  5 waitwait.wait_type + N'' (''
                                    + CAST(MAX(waitwait.wait_time_ms) AS NVARCHAR(128))
                                    + N'' ms), ''
                                FROM   sys.dm_exec_session_wait_stats AS waitwait
                                WHERE  waitwait.session_id = wait.session_id
                                GROUP BY  waitwait.wait_type
                                HAVING SUM(waitwait.wait_time_ms) > 5
                                ORDER BY 1
                                FOR
                                XML PATH('''') ) AS session_wait_info
                       FROM sys.dm_exec_session_wait_stats AS wait ) AS wt2
       ON   s.session_id = wt2.session_id
       LEFT JOIN sys.dm_exec_query_stats AS session_stats
       ON   r.sql_handle = session_stats.sql_handle
               AND r.plan_handle = session_stats.plan_handle
         AND r.statement_start_offset = session_stats.statement_start_offset
         AND r.statement_end_offset = session_stats.statement_end_offset'
        ,@querystatsxmlselect nvarchar(max) = N' CAST(COALESCE(qs_live.query_plan, ''<?No live query plan available. To turn on live plans, see https://www.BrentOzar.com/go/liveplans ?>'') AS XML) AS live_query_plan , '
        ,@querystatsxmlsql nvarchar(max) = N'OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live'
        ,@objectfullname nvarchar(2000);


    set @productversion = CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));
    select @productversionmajor = SUBSTRING(@productversion, 1, CHARINDEX('.', @productversion) + 1),
           @productversionminor = PARSENAME(CONVERT(varchar(32), @productversion), 2)
    if EXISTS(select *
              from sys.all_columns
              where object_id = OBJECT_ID('sys.dm_exec_query_statistics_xml') and name = 'query_plan')
        begin
            set @querystatsxmlselect =
                    N' CAST(COALESCE(qs_live.query_plan, ''<?No live query plan available. To turn on live plans, see https://www.BrentOzar.com/go/liveplans ?>'') AS XML) AS live_query_plan , ';
            set @querystatsxmlsql = N'OUTER APPLY sys.dm_exec_query_statistics_xml(s.session_id) qs_live';
        end
    else
        begin
            set @querystatsxmlselect = N' NULL AS live_query_plan , ';
            set @querystatsxmlsql = N' ';
        end

    select @outputdatabasename = QUOTENAME(@outputdatabasename),
           @outputschemaname = QUOTENAME(@outputschemaname),
           @outputtablename = QUOTENAME(@outputtablename);

    if @outputdatabasename is not null and @outputschemaname is not null and @outputtablename is not null
        and EXISTS(select *
                   from sys.databases
                   where QUOTENAME([name]) = @outputdatabasename)
        begin
            set @expertmode = 1;
            /* Force ExpertMode when we're logging to table */

            /* Create the table if it doesn't exist */
            set @stringtoexecute = N'USE '
                + @outputdatabasename
                + N'; IF EXISTS(SELECT * FROM '
                + @outputdatabasename
                + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                + @outputschemaname
                + N''') AND NOT EXISTS (SELECT * FROM '
                + @outputdatabasename
                + N'.INFORMATION_SCHEMA.TABLES WHERE QUOTENAME(TABLE_SCHEMA) = '''
                + @outputschemaname + N''' AND QUOTENAME(TABLE_NAME) = '''
                + @outputtablename + N''') CREATE TABLE '
                + @outputschemaname + N'.'
                + @outputtablename
                + N'(';
            set @stringtoexecute = @stringtoexecute + N'
	ID INT IDENTITY(1,1) NOT NULL,
	ServerName NVARCHAR(128) NOT NULL,
	CheckDate DATETIMEOFFSET NOT NULL,
	[elapsed_time] [varchar](41) NULL,
	[session_id] [smallint] NOT NULL,
	[database_name] [nvarchar](128) NULL,
	[query_text] [nvarchar](max) NULL,
	[query_plan] [xml] NULL,
	[live_query_plan] [xml] NULL,
	[query_cost] [float] NULL,
	[status] [nvarchar](30) NOT NULL,
	[wait_info] [nvarchar](max) NULL,
	[top_session_waits] [nvarchar](max) NULL,
	[blocking_session_id] [smallint] NULL,
	[open_transaction_count] [int] NULL,
	[is_implicit_transaction] [int] NOT NULL,
	[nt_domain] [nvarchar](128) NULL,
	[host_name] [nvarchar](128) NULL,
	[login_name] [nvarchar](128) NOT NULL,
	[nt_user_name] [nvarchar](128) NULL,
	[program_name] [nvarchar](128) NULL,
	[fix_parameter_sniffing] [nvarchar](150) NULL,
	[client_interface_name] [nvarchar](32) NULL,
	[login_time] [datetime] NOT NULL,
	[start_time] [datetime] NULL,
	[request_time] [datetime] NULL,
	[request_cpu_time] [int] NULL,
	[request_logical_reads] [bigint] NULL,
	[request_writes] [bigint] NULL,
	[request_physical_reads] [bigint] NULL,
	[session_cpu] [int] NOT NULL,
	[session_logical_reads] [bigint] NOT NULL,
	[session_physical_reads] [bigint] NOT NULL,
	[session_writes] [bigint] NOT NULL,
	[tempdb_allocations_mb] [decimal](38, 2) NULL,
	[memory_usage] [int] NOT NULL,
	[estimated_completion_time] [bigint] NULL,
	[percent_complete] [real] NULL,
	[deadlock_priority] [int] NULL,
	[transaction_isolation_level] [varchar](33) NOT NULL,
	[degree_of_parallelism] [smallint] NULL,
	[last_dop] [bigint] NULL,
	[min_dop] [bigint] NULL,
	[max_dop] [bigint] NULL,
	[last_grant_kb] [bigint] NULL,
	[min_grant_kb] [bigint] NULL,
	[max_grant_kb] [bigint] NULL,
	[last_used_grant_kb] [bigint] NULL,
	[min_used_grant_kb] [bigint] NULL,
	[max_used_grant_kb] [bigint] NULL,
	[last_ideal_grant_kb] [bigint] NULL,
	[min_ideal_grant_kb] [bigint] NULL,
	[max_ideal_grant_kb] [bigint] NULL,
	[last_reserved_threads] [bigint] NULL,
	[min_reserved_threads] [bigint] NULL,
	[max_reserved_threads] [bigint] NULL,
	[last_used_threads] [bigint] NULL,
	[min_used_threads] [bigint] NULL,
	[max_used_threads] [bigint] NULL,
	[grant_time] [varchar](20) NULL,
	[requested_memory_kb] [bigint] NULL,
	[grant_memory_kb] [bigint] NULL,
	[is_request_granted] [varchar](39) NOT NULL,
	[required_memory_kb] [bigint] NULL,
	[query_memory_grant_used_memory_kb] [bigint] NULL,
	[ideal_memory_kb] [bigint] NULL,
	[is_small] [bit] NULL,
	[timeout_sec] [int] NULL,
	[resource_semaphore_id] [smallint] NULL,
	[wait_order] [varchar](20) NULL,
	[wait_time_ms] [varchar](20) NULL,
	[next_candidate_for_memory_grant] [varchar](3) NOT NULL,
	[target_memory_kb] [bigint] NULL,
	[max_target_memory_kb] [varchar](30) NULL,
	[total_memory_kb] [bigint] NULL,
	[available_memory_kb] [bigint] NULL,
	[granted_memory_kb] [bigint] NULL,
	[query_resource_semaphore_used_memory_kb] [bigint] NULL,
	[grantee_count] [int] NULL,
	[waiter_count] [int] NULL,
	[timeout_error_count] [bigint] NULL,
	[forced_grant_count] [varchar](30) NULL,
	[workload_group_name] [sysname] NULL,
	[resource_pool_name] [sysname] NULL,
	[context_info] [varchar](128) NULL,
	[query_hash] [binary](8) NULL,
	[query_plan_hash] [binary](8) NULL,
	[sql_handle] [varbinary] (64) NULL,
	[plan_handle] [varbinary] (64) NULL,
	[statement_start_offset] INT NULL,
	[statement_end_offset] INT NULL,
	JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50)),
	PRIMARY KEY CLUSTERED (ID ASC));';
            if @debug = 1
                begin
                    print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 0, 8000))
                    print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 8000, 16000))
                end
            exec (@stringtoexecute);

            /* If the table doesn't have the new JoinKey computed column, add it. See Github #2162. */
            set @objectfullname = @outputdatabasename + N'.' + @outputschemaname + N'.' + @outputtablename;
            set @stringtoexecute = N'IF NOT EXISTS (SELECT * FROM ' + @outputdatabasename + N'.sys.all_columns
		WHERE object_id = (OBJECT_ID(''' + @objectfullname + N''')) AND name = ''JoinKey'')
		ALTER TABLE ' + @objectfullname + N' ADD JoinKey AS ServerName + CAST(CheckDate AS NVARCHAR(50));';
            exec (@stringtoexecute);

            /* Delete history older than @OutputTableRetentionDays */
            set @outputtablecleanupdate = CAST((DATEADD(day, -1 * @outputtableretentiondays, GETDATE())) as date);
            set @stringtoexecute = N' IF EXISTS(SELECT * FROM '
                + @outputdatabasename
                + N'.INFORMATION_SCHEMA.SCHEMATA WHERE QUOTENAME(SCHEMA_NAME) = '''
                + @outputschemaname + N''') DELETE '
                + @outputdatabasename + '.'
                + @outputschemaname + '.'
                + @outputtablename
                + N' WHERE ServerName = @SrvName AND CheckDate < @CheckDate;';
            if @debug = 1
                begin
                    print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 0, 8000))
                    print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 8000, 16000))
                end
            exec sp_executesql @stringtoexecute,
                 N'@SrvName NVARCHAR(128), @CheckDate date',
                 @@SERVERNAME, @outputtablecleanupdate;

        end

    select @blockingcheck = N'SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
   
						DECLARE @blocked TABLE 
								(
								    dbid SMALLINT NOT NULL,
								    last_batch DATETIME NOT NULL,
								    open_tran SMALLINT NOT NULL,
								    sql_handle BINARY(20) NOT NULL,
								    session_id SMALLINT NOT NULL,
								    blocking_session_id SMALLINT NOT NULL,
								    lastwaittype NCHAR(32) NOT NULL,
								    waittime BIGINT NOT NULL,
								    cpu INT NOT NULL,
								    physical_io BIGINT NOT NULL,
								    memusage INT NOT NULL
								); 
						
						INSERT @blocked ( dbid, last_batch, open_tran, sql_handle, session_id, blocking_session_id, lastwaittype, waittime, cpu, physical_io, memusage )
						SELECT
							sys1.dbid, sys1.last_batch, sys1.open_tran, sys1.sql_handle, 
							sys2.spid AS session_id, sys2.blocked AS blocking_session_id, sys2.lastwaittype, sys2.waittime, sys2.cpu, sys2.physical_io, sys2.memusage
						FROM sys.sysprocesses AS sys1
						JOIN sys.sysprocesses AS sys2
						ON sys1.spid = sys2.blocked;';

    if @productversionmajor > 9 and @productversionmajor < 11
        begin
            /* Think of the StringToExecute as starting with this, but we'll set this up later depending on whether we're doing an insert or a select:
            SELECT @StringToExecute = N'SELECT  GETDATE() AS run_date ,
            */
            set @stringtoexecute = N'COALESCE( CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
			       s.session_id ,
						    COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid), ''N/A'') AS database_name,
			       ISNULL(SUBSTRING(dest.text,
			            ( query_stats.statement_start_offset / 2 ) + 1,
			            ( ( CASE query_stats.statement_end_offset
			               WHEN -1 THEN DATALENGTH(dest.text)
			               ELSE query_stats.statement_end_offset
			             END - query_stats.statement_start_offset )
			              / 2 ) + 1), dest.text) AS query_text ,
			       derp.query_plan ,
						    qmg.query_cost ,										   		   
						    s.status ,
			       COALESCE(wt.wait_info, RTRIM(blocked.lastwaittype) + '' ('' + CONVERT(VARCHAR(10), 
						    blocked.waittime) + '')'' ) AS wait_info ,											
						    CASE WHEN r.blocking_session_id <> 0 AND blocked.session_id IS NULL 
							     THEN r.blocking_session_id
							     WHEN r.blocking_session_id <> 0 AND s.session_id <> blocked.blocking_session_id 
							     THEN blocked.blocking_session_id
							    ELSE NULL 
						    END AS blocking_session_id , 
			       COALESCE(r.open_transaction_count, blocked.open_tran) AS open_transaction_count ,
						    CASE WHEN EXISTS (  SELECT 1 
               FROM sys.dm_tran_active_transactions AS tat
               JOIN sys.dm_tran_session_transactions AS tst
               ON tst.transaction_id = tat.transaction_id
               WHERE tat.name = ''implicit_transaction''
               AND s.session_id = tst.session_id 
               )  THEN 1 
            ELSE 0 
          END AS is_implicit_transaction ,
					     s.nt_domain ,
			       s.host_name ,
			       s.login_name ,
			       s.nt_user_name ,'
            if @platform = 'NonAzure'
                begin
                    set @stringtoexecute +=
                        N'program_name = COALESCE((
                         SELECT REPLACE(program_name,Substring(program_name,30,34),''"''+j.name+''"'')
                         FROM msdb.dbo.sysjobs j WHERE Substring(program_name,32,32) = CONVERT(char(32),CAST(j.job_id AS binary(16)),2)
                         ),s.program_name)'
                end
            else
                begin
                    set @stringtoexecute += N's.program_name'
                end

            if @expertmode = 1
                begin
                    set @stringtoexecute +=
                        N',
                                 ''DBCC FREEPROCCACHE ('' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + '');'' AS fix_parameter_sniffing,
                        s.client_interface_name ,
                        s.login_time ,
                        r.start_time ,
                        qmg.request_time ,
                                 COALESCE(r.cpu_time, s.cpu_time) AS request_cpu_time,
                        COALESCE(r.logical_reads, s.logical_reads) AS request_logical_reads,
                        COALESCE(r.writes, s.writes) AS request_writes,
                        COALESCE(r.reads, s.reads) AS request_physical_reads ,
                        s.cpu_time AS session_cpu,
                        s.logical_reads AS session_logical_reads,
                        s.reads AS session_physical_reads ,
                        s.writes AS session_writes,
                                 tempdb_allocations.tempdb_allocations_mb,
                        s.memory_usage ,
                        r.estimated_completion_time ,
                                 r.percent_complete ,
                        r.deadlock_priority ,
                        CASE
                          WHEN s.transaction_isolation_level = 0 THEN ''Unspecified''
                          WHEN s.transaction_isolation_level = 1 THEN ''Read Uncommitted''
                          WHEN s.transaction_isolation_level = 2 AND EXISTS (SELECT 1 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE s.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed Snapshot Isolation''
                                   WHEN s.transaction_isolation_level = 2 AND NOT EXISTS (SELECT 1 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE s.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed''
                          WHEN s.transaction_isolation_level = 3 THEN ''Repeatable Read''
                          WHEN s.transaction_isolation_level = 4 THEN ''Serializable''
                          WHEN s.transaction_isolation_level = 5 THEN ''Snapshot''
                          ELSE ''WHAT HAVE YOU DONE?''
                        END AS transaction_isolation_level ,
                                 qmg.dop AS degree_of_parallelism ,
                        COALESCE(CAST(qmg.grant_time AS VARCHAR(20)), ''N/A'') AS grant_time ,
                        qmg.requested_memory_kb ,
                        qmg.granted_memory_kb AS grant_memory_kb,
                        CASE WHEN qmg.grant_time IS NULL THEN ''N/A''
                 WHEN qmg.requested_memory_kb < qmg.granted_memory_kb
                          THEN ''Query Granted Less Than Query Requested''
                          ELSE ''Memory Request Granted''
                        END AS is_request_granted ,
                        qmg.required_memory_kb ,
                        qmg.used_memory_kb AS query_memory_grant_used_memory_kb,
                        qmg.ideal_memory_kb ,
                        qmg.is_small ,
                        qmg.timeout_sec ,
                        qmg.resource_semaphore_id ,
                        COALESCE(CAST(qmg.wait_order AS VARCHAR(20)), ''N/A'') AS wait_order ,
                        COALESCE(CAST(qmg.wait_time_ms AS VARCHAR(20)),
                           ''N/A'') AS wait_time_ms ,
                        CASE qmg.is_next_candidate
                          WHEN 0 THEN ''No''
                          WHEN 1 THEN ''Yes''
                          ELSE ''N/A''
                        END AS next_candidate_for_memory_grant ,
                        qrs.target_memory_kb ,
                        COALESCE(CAST(qrs.max_target_memory_kb AS VARCHAR(20)),
                           ''Small Query Resource Semaphore'') AS max_target_memory_kb ,
                        qrs.total_memory_kb ,
                        qrs.available_memory_kb ,
                        qrs.granted_memory_kb ,
                        qrs.used_memory_kb AS query_resource_semaphore_used_memory_kb,
                        qrs.grantee_count ,
                        qrs.waiter_count ,
                        qrs.timeout_error_count ,
                        COALESCE(CAST(qrs.forced_grant_count AS VARCHAR(20)),
                           ''Small Query Resource Semaphore'') AS forced_grant_count,
                                 wg.name AS workload_group_name ,
                                 rp.name AS resource_pool_name,
                                  CONVERT(VARCHAR(128), r.context_info)  AS context_info
                                 '
                end /* IF @ExpertMode = 1 */

            set @stringtoexecute +=
                    N'FROM sys.dm_exec_sessions AS s
                         LEFT JOIN sys.dm_exec_requests AS r
                         ON   r.session_id = s.session_id
                         LEFT JOIN ( SELECT DISTINCT
                              wait.session_id ,
                              ( SELECT waitwait.wait_type + N'' (''
                                 + CAST(MAX(waitwait.wait_duration_ms) AS NVARCHAR(128))
                                 + N'' ms) ''
                                FROM   sys.dm_os_waiting_tasks AS waitwait
                                WHERE  waitwait.session_id = wait.session_id
                                GROUP BY  waitwait.wait_type
                                ORDER BY  SUM(waitwait.wait_duration_ms) DESC
                              FOR
                                XML PATH('''') ) AS wait_info
                            FROM sys.dm_os_waiting_tasks AS wait ) AS wt
                         ON   s.session_id = wt.session_id
                         LEFT JOIN sys.dm_exec_query_stats AS query_stats
                         ON   r.sql_handle = query_stats.sql_handle
                                    AND r.plan_handle = query_stats.plan_handle
                           AND r.statement_start_offset = query_stats.statement_start_offset
                           AND r.statement_end_offset = query_stats.statement_end_offset
                         LEFT JOIN sys.dm_exec_query_memory_grants qmg
                         ON   r.session_id = qmg.session_id
                                    AND r.request_id = qmg.request_id
                         LEFT JOIN sys.dm_exec_query_resource_semaphores qrs
                         ON   qmg.resource_semaphore_id = qrs.resource_semaphore_id
                                 AND qmg.pool_id = qrs.pool_id
                            LEFT JOIN sys.resource_governor_workload_groups wg
                            ON 		s.group_id = wg.group_id
                            LEFT JOIN sys.resource_governor_resource_pools rp
                            ON		wg.pool_id = rp.pool_id
                            OUTER APPLY (
                                            SELECT TOP 1
                                            b.dbid, b.last_batch, b.open_tran, b.sql_handle,
                                            b.session_id, b.blocking_session_id, b.lastwaittype, b.waittime
                                            FROM @blocked b
                                            WHERE (s.session_id = b.session_id
                                                    OR s.session_id = b.blocking_session_id)
                                        ) AS blocked
                            OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, blocked.sql_handle)) AS dest
                         OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS derp
                            OUTER APPLY (
                                    SELECT CONVERT(DECIMAL(38,2), SUM( (((tsu.user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8) / 1024.)) ) AS tempdb_allocations_mb
                                    FROM sys.dm_db_task_space_usage tsu
                                    WHERE tsu.request_id = r.request_id
                                    AND tsu.session_id = r.session_id
                                    AND tsu.session_id = s.session_id
                            ) as tempdb_allocations
                         WHERE s.session_id <> @@SPID
                            AND s.host_name IS NOT NULL
                            '
                    + case
                          when @showsleepingspids = 0 then
                              N' AND COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid)) IS NOT NULL'
                          when @showsleepingspids = 1 then
                              N' OR COALESCE(r.open_transaction_count, blocked.open_tran) >= 1'
                          else N'' end;
        end /* IF @ProductVersionMajor > 9 and @ProductVersionMajor < 11 */

    if @productversionmajor >= 11
        begin
            select @enhanceflag =
                   case
                       when @productversionmajor = 11 and @productversionminor >= 6020 then 1
                       when @productversionmajor = 12 and @productversionminor >= 5000 then 1
                       when @productversionmajor = 13 and @productversionminor >= 1601 then 1
                       when @productversionmajor > 13 then 1
                       else 0
                       end


            if OBJECT_ID('sys.dm_exec_session_wait_stats') is not null
                begin
                    set @sessionwaits = 1
                end

            /* Think of the StringToExecute as starting with this, but we'll set this up later depending on whether we're doing an insert or a select:
            SELECT @StringToExecute = N'SELECT  GETDATE() AS run_date ,
            */
            select @stringtoexecute = N'COALESCE( CONVERT(VARCHAR(20), (ABS(r.total_elapsed_time) / 1000) / 86400) + '':'' + CONVERT(VARCHAR(20), (DATEADD(SECOND, (r.total_elapsed_time / 1000), 0) + DATEADD(MILLISECOND, (r.total_elapsed_time % 1000), 0)), 114), CONVERT(VARCHAR(20), DATEDIFF(SECOND, s.last_request_start_time, GETDATE()) / 86400) + '':'' + CONVERT(VARCHAR(20), DATEADD(SECOND, DATEDIFF(SECOND, s.last_request_start_time, GETDATE()), 0), 114) ) AS [elapsed_time] ,
			       s.session_id ,
						    COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid), ''N/A'') AS database_name,
			       ISNULL(SUBSTRING(dest.text,
			            ( query_stats.statement_start_offset / 2 ) + 1,
			            ( ( CASE query_stats.statement_end_offset
			               WHEN -1 THEN DATALENGTH(dest.text)
			               ELSE query_stats.statement_end_offset
			             END - query_stats.statement_start_offset )
			              / 2 ) + 1), dest.text) AS query_text ,
			       derp.query_plan ,'
                + @querystatsxmlselect
                + '
			       qmg.query_cost ,
			       s.status ,
			       COALESCE(wt.wait_info, RTRIM(blocked.lastwaittype) + '' ('' + CONVERT(VARCHAR(10), blocked.waittime) + '')'' ) AS wait_info ,'
                +
                                      case @sessionwaits
                                          when 1
                                              then + N'SUBSTRING(wt2.session_wait_info, 0, LEN(wt2.session_wait_info) ) AS top_session_waits ,'
                                          else N' NULL AS top_session_waits ,'
                                          end
                +
                                      N'CASE WHEN r.blocking_session_id <> 0 AND blocked.session_id IS NULL
                                             THEN r.blocking_session_id
                                             WHEN r.blocking_session_id <> 0 AND s.session_id <> blocked.blocking_session_id
                                             THEN blocked.blocking_session_id
                                             ELSE NULL
                                        END AS blocking_session_id,
                             COALESCE(r.open_transaction_count, blocked.open_tran) AS open_transaction_count ,
                                      CASE WHEN EXISTS (  SELECT 1
                         FROM sys.dm_tran_active_transactions AS tat
                         JOIN sys.dm_tran_session_transactions AS tst
                         ON tst.transaction_id = tat.transaction_id
                         WHERE tat.name = ''implicit_transaction''
                         AND s.session_id = tst.session_id
                         )  THEN 1
                      ELSE 0
                    END AS is_implicit_transaction ,
                                   s.nt_domain ,
                             s.host_name ,
                             s.login_name ,
                             s.nt_user_name ,'
            if @platform = 'NonAzure'
                begin
                    set @stringtoexecute +=
                        N'program_name = COALESCE((
                         SELECT REPLACE(program_name,Substring(program_name,30,34),''"''+j.name+''"'')
                         FROM msdb.dbo.sysjobs j WHERE Substring(program_name,32,32) = CONVERT(char(32),CAST(j.job_id AS binary(16)),2)
                         ),s.program_name)'
                end
            else
                begin
                    set @stringtoexecute += N's.program_name'
                end

            if @expertmode = 1 /* We show more columns in expert mode, so the SELECT gets longer */
                begin
                    set @stringtoexecute +=
                            N', ''DBCC FREEPROCCACHE ('' + CONVERT(NVARCHAR(128), r.plan_handle, 1) + '');'' AS fix_parameter_sniffing,
                        s.client_interface_name ,
                        s.login_time ,
                        r.start_time ,
                        qmg.request_time ,
                                COALESCE(r.cpu_time, s.cpu_time) AS request_cpu_time,
                        COALESCE(r.logical_reads, s.logical_reads) AS request_logical_reads,
                        COALESCE(r.writes, s.writes) AS request_writes,
                        COALESCE(r.reads, s.reads) AS request_physical_reads ,
                        s.cpu_time AS session_cpu,
                        s.logical_reads AS session_logical_reads,
                        s.reads AS session_physical_reads ,
                        s.writes AS session_writes,
                                tempdb_allocations.tempdb_allocations_mb,
                        s.memory_usage ,
                        r.estimated_completion_time ,
                                r.percent_complete ,
                        r.deadlock_priority ,
                                CASE
                            WHEN s.transaction_isolation_level = 0 THEN ''Unspecified''
                            WHEN s.transaction_isolation_level = 1 THEN ''Read Uncommitted''
                            WHEN s.transaction_isolation_level = 2 AND EXISTS (SELECT 1 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE s.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed Snapshot Isolation''
                                    WHEN s.transaction_isolation_level = 2 AND NOT EXISTS (SELECT 1 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE s.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed''
                            WHEN s.transaction_isolation_level = 3 THEN ''Repeatable Read''
                            WHEN s.transaction_isolation_level = 4 THEN ''Serializable''
                            WHEN s.transaction_isolation_level = 5 THEN ''Snapshot''
                            ELSE ''WHAT HAVE YOU DONE?''
                        END AS transaction_isolation_level ,
                                qmg.dop AS degree_of_parallelism ,						'
                            +
                            case @enhanceflag
                                when 1 then N'query_stats.last_dop,
        query_stats.min_dop,
        query_stats.max_dop,
        query_stats.last_grant_kb,
        query_stats.min_grant_kb,
        query_stats.max_grant_kb,
        query_stats.last_used_grant_kb,
        query_stats.min_used_grant_kb,
        query_stats.max_used_grant_kb,
        query_stats.last_ideal_grant_kb,
        query_stats.min_ideal_grant_kb,
        query_stats.max_ideal_grant_kb,
        query_stats.last_reserved_threads,
        query_stats.min_reserved_threads,
        query_stats.max_reserved_threads,
        query_stats.last_used_threads,
        query_stats.min_used_threads,
        query_stats.max_used_threads,'
                                else N' NULL AS last_dop,
        NULL AS min_dop,
        NULL AS max_dop,
        NULL AS last_grant_kb,
        NULL AS min_grant_kb,
        NULL AS max_grant_kb,
        NULL AS last_used_grant_kb,
        NULL AS min_used_grant_kb,
        NULL AS max_used_grant_kb,
        NULL AS last_ideal_grant_kb,
        NULL AS min_ideal_grant_kb,
        NULL AS max_ideal_grant_kb,
        NULL AS last_reserved_threads,
        NULL AS min_reserved_threads,
        NULL AS max_reserved_threads,
        NULL AS last_used_threads,
        NULL AS min_used_threads,
        NULL AS max_used_threads,'
                                end

                    set @stringtoexecute +=
                        N'
                COALESCE(CAST(qmg.grant_time AS VARCHAR(20)), ''Memory Not Granted'') AS grant_time ,
                qmg.requested_memory_kb ,
                qmg.granted_memory_kb AS grant_memory_kb,
                CASE WHEN qmg.grant_time IS NULL THEN ''N/A''
                WHEN qmg.requested_memory_kb < qmg.granted_memory_kb
                    THEN ''Query Granted Less Than Query Requested''
                    ELSE ''Memory Request Granted''
                END AS is_request_granted ,
                qmg.required_memory_kb ,
                qmg.used_memory_kb AS query_memory_grant_used_memory_kb,
                qmg.ideal_memory_kb ,
                qmg.is_small ,
                qmg.timeout_sec ,
                qmg.resource_semaphore_id ,
                COALESCE(CAST(qmg.wait_order AS VARCHAR(20)), ''N/A'') AS wait_order ,
                COALESCE(CAST(qmg.wait_time_ms AS VARCHAR(20)),
                    ''N/A'') AS wait_time_ms ,
                CASE qmg.is_next_candidate
                    WHEN 0 THEN ''No''
                    WHEN 1 THEN ''Yes''
                    ELSE ''N/A''
                END AS next_candidate_for_memory_grant ,
                qrs.target_memory_kb ,
                COALESCE(CAST(qrs.max_target_memory_kb AS VARCHAR(20)),
                    ''Small Query Resource Semaphore'') AS max_target_memory_kb ,
                qrs.total_memory_kb ,
                qrs.available_memory_kb ,
                qrs.granted_memory_kb ,
                qrs.used_memory_kb AS query_resource_semaphore_used_memory_kb,
                qrs.grantee_count ,
                qrs.waiter_count ,
                qrs.timeout_error_count ,
                COALESCE(CAST(qrs.forced_grant_count AS VARCHAR(20)),
                ''Small Query Resource Semaphore'') AS forced_grant_count,
                wg.name AS workload_group_name,
                rp.name AS resource_pool_name,
                CONVERT(VARCHAR(128), r.context_info)  AS context_info,
                r.query_hash, r.query_plan_hash, r.sql_handle, r.plan_handle, r.statement_start_offset, r.statement_end_offset '
                end /* IF @ExpertMode = 1 */

            set @stringtoexecute +=
                    N' FROM sys.dm_exec_sessions AS s
                    LEFT JOIN sys.dm_exec_requests AS r
                                    ON   r.session_id = s.session_id
                    LEFT JOIN ( SELECT DISTINCT
                                        wait.session_id ,
                                        ( SELECT waitwait.wait_type + N'' (''
                                            + CAST(MAX(waitwait.wait_duration_ms) AS NVARCHAR(128))
                                            + N'' ms) ''
                                        FROM   sys.dm_os_waiting_tasks AS waitwait
                                        WHERE  waitwait.session_id = wait.session_id
                                        GROUP BY  waitwait.wait_type
                                        ORDER BY  SUM(waitwait.wait_duration_ms) DESC
                                        FOR
                                        XML PATH('''') ) AS wait_info
                                    FROM sys.dm_os_waiting_tasks AS wait ) AS wt
                                    ON   s.session_id = wt.session_id
                    LEFT JOIN sys.dm_exec_query_stats AS query_stats
                    ON   r.sql_handle = query_stats.sql_handle
                            AND r.plan_handle = query_stats.plan_handle
                        AND r.statement_start_offset = query_stats.statement_start_offset
                        AND r.statement_end_offset = query_stats.statement_end_offset
                    '
                    +
                    case @sessionwaits
                        when 1 then @sessionwaitssql
                        else N''
                        end
                    +
                    N'
                    LEFT JOIN sys.dm_exec_query_memory_grants qmg
                    ON   r.session_id = qmg.session_id
                            AND r.request_id = qmg.request_id
                    LEFT JOIN sys.dm_exec_query_resource_semaphores qrs
                    ON   qmg.resource_semaphore_id = qrs.resource_semaphore_id
                            AND qmg.pool_id = qrs.pool_id
                    LEFT JOIN sys.resource_governor_workload_groups wg
                    ON 		s.group_id = wg.group_id
                    LEFT JOIN sys.resource_governor_resource_pools rp
                    ON		wg.pool_id = rp.pool_id
                    OUTER APPLY (
                            SELECT TOP 1
                            b.dbid, b.last_batch, b.open_tran, b.sql_handle,
                            b.session_id, b.blocking_session_id, b.lastwaittype, b.waittime
                            FROM @blocked b
                            WHERE (s.session_id = b.session_id
                                    OR s.session_id = b.blocking_session_id)
                        ) AS blocked
                    OUTER APPLY sys.dm_exec_sql_text(COALESCE(r.sql_handle, blocked.sql_handle)) AS dest
                    OUTER APPLY sys.dm_exec_query_plan(r.plan_handle) AS derp
                    OUTER APPLY (
                            SELECT CONVERT(DECIMAL(38,2), SUM( (((tsu.user_objects_alloc_page_count - user_objects_dealloc_page_count) * 8) / 1024.)) ) AS tempdb_allocations_mb
                            FROM sys.dm_db_task_space_usage tsu
                            WHERE tsu.request_id = r.request_id
                            AND tsu.session_id = r.session_id
                            AND tsu.session_id = s.session_id
                    ) as tempdb_allocations
                    '
                    + @querystatsxmlsql
                    +
                    N'
                    WHERE s.session_id <> @@SPID
                    AND s.host_name IS NOT NULL
                    '
                    + case
                          when @showsleepingspids = 0 then
                              N' AND COALESCE(DB_NAME(r.database_id), DB_NAME(blocked.dbid)) IS NOT NULL'
                          when @showsleepingspids = 1 then
                              N' OR COALESCE(r.open_transaction_count, blocked.open_tran) >= 1'
                          else N'' end;


        end /* IF @ProductVersionMajor >= 11  */

    if (@minelapsedseconds + @mincputime + @minlogicalreads + @minphysicalreads + @minwrites + @mintempdbmb +
        @minrequestedmemorykb + @minblockingseconds) > 0
        begin
            /* They're filtering for something, so set up a where clause that will let any (not all combined) of the min triggers work: */
            set @stringtoexecute += N' AND (1 = 0 ';
            if @minelapsedseconds > 0
                set @stringtoexecute += N' OR ABS(COALESCE(r.total_elapsed_time,0)) / 1000 >= ' +
                                        CAST(@minelapsedseconds as nvarchar(20));
            if @mincputime > 0
                set @stringtoexecute += N' OR COALESCE(r.cpu_time, s.cpu_time,0) / 1000 >= ' +
                                        CAST(@mincputime as nvarchar(20));
            if @minlogicalreads > 0
                set @stringtoexecute += N' OR COALESCE(r.logical_reads, s.logical_reads,0) >= ' +
                                        CAST(@minlogicalreads as nvarchar(20));
            if @minphysicalreads > 0
                set @stringtoexecute += N' OR COALESCE(s.reads,0) >= ' + CAST(@minphysicalreads as nvarchar(20));
            if @minwrites > 0
                set @stringtoexecute += N' OR COALESCE(r.writes, s.writes,0) >= ' + CAST(@minwrites as nvarchar(20));
            if @mintempdbmb > 0
                set @stringtoexecute += N' OR COALESCE(tempdb_allocations.tempdb_allocations_mb,0) >= ' +
                                        CAST(@mintempdbmb as nvarchar(20));
            if @minrequestedmemorykb > 0
                set @stringtoexecute += N' OR COALESCE(qmg.requested_memory_kb,0) >= ' +
                                        CAST(@minrequestedmemorykb as nvarchar(20));
            /* Blocking is a little different - we're going to return ALL of the queries if we meet the blocking threshold. */
            if @minblockingseconds > 0
                set @stringtoexecute += N' OR (SELECT SUM(waittime / 1000) FROM @blocked) >= ' +
                                        CAST(@minblockingseconds as nvarchar(20));
            set @stringtoexecute += N' ) ';
        end

    set @stringtoexecute +=
        N' ORDER BY 2 DESC
        ';


    if @outputdatabasename is not null and @outputschemaname is not null and @outputtablename is not null
        and EXISTS(select *
                   from sys.databases
                   where QUOTENAME([name]) = @outputdatabasename)
        begin
            set @stringtoexecute = N'USE '
                + @outputdatabasename + N'; '
                + @blockingcheck +
                                   + ' INSERT INTO '
                + @outputschemaname + N'.'
                + @outputtablename
                + N'(ServerName
	,CheckDate
	,[elapsed_time]
	,[session_id]
	,[database_name]
	,[query_text]
	,[query_plan]'
                + case when @productversionmajor >= 11 then N',[live_query_plan]' else N'' end + N'
	,[query_cost]
	,[status]
	,[wait_info]'
                + case when @productversionmajor >= 11 then N',[top_session_waits]' else N'' end + N'
	,[blocking_session_id]
	,[open_transaction_count]
	,[is_implicit_transaction]
	,[nt_domain]
	,[host_name]
	,[login_name]
	,[nt_user_name]
	,[program_name]
	,[fix_parameter_sniffing]
	,[client_interface_name]
	,[login_time]
	,[start_time]
	,[request_time]
	,[request_cpu_time]
	,[request_logical_reads]
	,[request_writes]
	,[request_physical_reads]
	,[session_cpu]
	,[session_logical_reads]
	,[session_physical_reads]
	,[session_writes]
	,[tempdb_allocations_mb]
	,[memory_usage]
	,[estimated_completion_time]
	,[percent_complete]
	,[deadlock_priority]
	,[transaction_isolation_level]
	,[degree_of_parallelism]'
                + case
                      when @productversionmajor >= 11 then N'
	,[last_dop]
	,[min_dop]
	,[max_dop]
	,[last_grant_kb]
	,[min_grant_kb]
	,[max_grant_kb]
	,[last_used_grant_kb]
	,[min_used_grant_kb]
	,[max_used_grant_kb]
	,[last_ideal_grant_kb]
	,[min_ideal_grant_kb]
	,[max_ideal_grant_kb]
	,[last_reserved_threads]
	,[min_reserved_threads]
	,[max_reserved_threads]
	,[last_used_threads]
	,[min_used_threads]
	,[max_used_threads]'
                      else N'' end + N'
	,[grant_time]
	,[requested_memory_kb]
	,[grant_memory_kb]
	,[is_request_granted]
	,[required_memory_kb]
	,[query_memory_grant_used_memory_kb]
	,[ideal_memory_kb]
	,[is_small]
	,[timeout_sec]
	,[resource_semaphore_id]
	,[wait_order]
	,[wait_time_ms]
	,[next_candidate_for_memory_grant]
	,[target_memory_kb]
	,[max_target_memory_kb]
	,[total_memory_kb]
	,[available_memory_kb]
	,[granted_memory_kb]
	,[query_resource_semaphore_used_memory_kb]
	,[grantee_count]
	,[waiter_count]
	,[timeout_error_count]
	,[forced_grant_count]
	,[workload_group_name]
	,[resource_pool_name]
	,[context_info]'
                + case
                      when @productversionmajor >= 11 then N'
	,[query_hash]
	,[query_plan_hash]
	,[sql_handle]
	,[plan_handle]
	,[statement_start_offset]
	,[statement_end_offset]'
                      else N'' end + N'
) 
	SELECT @@SERVERNAME, COALESCE(@CheckDateOverride, SYSDATETIMEOFFSET()) AS CheckDate , '
                + @stringtoexecute;
        end
    else
        set @stringtoexecute = @blockingcheck + N' SELECT  GETDATE() AS run_date , ' + @stringtoexecute;

/* If the server has > 50GB of memory, add a max grant hint to avoid getting a giant grant */
    if (@productversionmajor = 11 and @productversionminor >= 6020)
        or (@productversionmajor = 12 and @productversionminor >= 5000)
        or (@productversionmajor >= 13)
           and 50000000 < (select cntr_value
                           from sys.dm_os_performance_counters
                           where object_name like '%:Memory Manager%'
                             and counter_name like 'Target Server Memory (KB)%')
        begin
            set @stringtoexecute = @stringtoexecute + N' OPTION (MAX_GRANT_PERCENT = 1, RECOMPILE) ';
        end
    else
        begin
            set @stringtoexecute = @stringtoexecute + N' OPTION (RECOMPILE) ';
        end

/* Be good: */
    set @stringtoexecute = @stringtoexecute + N' ; ';


    if @debug = 1
        begin
            print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 0, 8000))
            print CONVERT(varchar(8000), SUBSTRING(@stringtoexecute, 8000, 16000))
        end

    exec sp_executesql @stringtoexecute,
         N'@CheckDateOverride DATETIMEOFFSET',
         @checkdateoverride;

end
go
