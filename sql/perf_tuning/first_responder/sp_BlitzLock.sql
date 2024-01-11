if OBJECT_ID('dbo.sp_BlitzLock') is null
    exec ('CREATE PROCEDURE dbo.sp_BlitzLock AS RETURN 0;');
go

alter procedure dbo.sp_blitzlock(@top int = 2147483647,
                                 @databasename nvarchar(256) = null,
                                 @startdate datetime = '19000101',
                                 @enddate datetime = '99991231',
                                 @objectname nvarchar(1000) = null,
                                 @storedprocname nvarchar(1000) = null,
                                 @appname nvarchar(256) = null,
                                 @hostname nvarchar(256) = null,
                                 @loginname nvarchar(256) = null,
                                 @eventsessionpath varchar(256) = 'system_health*.xel',
                                 @victimsonly bit = 0,
                                 @debug bit = 0,
                                 @help bit = 0,
                                 @version varchar(30) = null output,
                                 @versiondate datetime = null output,
                                 @versioncheckmode bit = 0,
                                 @outputdatabasename nvarchar(256) = null,
                                 @outputschemaname nvarchar(256) = 'dbo', --ditto as below
                                 @outputtablename nvarchar(256) = 'BlitzLock' --put a standard here no need to check later in the script
)
    with recompile
as
begin

    set nocount on;
    set transaction isolation level read uncommitted;

    select @version = '2.97', @versiondate = '20200712';


    if (@versioncheckmode = 1)
        begin
            return;
        end;
    if @help = 1
        print '
	/*
	sp_BlitzLock from http://FirstResponderKit.org
	
	This script checks for and analyzes deadlocks from the system health session or a custom extended event path

	Variables you can use:
		@Top: Use if you want to limit the number of deadlocks to return.
			  This is ordered by event date ascending

		@DatabaseName: If you want to filter to a specific database

		@StartDate: The date you want to start searching on.

		@EndDate: The date you want to stop searching on.

		@ObjectName: If you want to filter to a specific able. 
					 The object name has to be fully qualified ''Database.Schema.Table''

		@StoredProcName: If you want to search for a single stored proc
					 The proc name has to be fully qualified ''Database.Schema.Sproc''
		
		@AppName: If you want to filter to a specific application
		
		@HostName: If you want to filter to a specific host
		
		@LoginName: If you want to filter to a specific login

		@EventSessionPath: If you want to point this at an XE session rather than the system health session.
	
		@OutputDatabaseName: If you want to output information to a specific database
		@OutputSchemaName: Specify a schema name to output information to a specific Schema
		@OutputTableName: Specify table name to to output information to a specific table
	
	To learn more, visit http://FirstResponderKit.org where you can download new
	versions for free, watch training videos on how it works, get more info on
	the findings, contribute your own code, and more.

	Known limitations of this version:
	 - Only SQL Server 2012 and newer is supported
	 - If your tables have weird characters in them (https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references) you may get errors trying to parse the XML.
	   I took a long look at this one, and:
		1) Trying to account for all the weird places these could crop up is a losing effort. 
		2) Replace is slow af on lots of XML.
	- Your mom.




	Unknown limitations of this version:
	 - None.  (If we knew them, they would be known. Duh.)


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


    declare @productversion nvarchar(128);
    declare @productversionmajor float;
    declare @productversionminor int;

    set @productversion = CAST(SERVERPROPERTY('ProductVersion') as nvarchar(128));

    select @productversionmajor = SUBSTRING(@productversion, 1, CHARINDEX('.', @productversion) + 1),
           @productversionminor = PARSENAME(CONVERT(varchar(32), @productversion), 2);


    if @productversionmajor < 11.0
        begin
            raiserror (
                'sp_BlitzLock will throw a bunch of angry errors on versions of SQL Server earlier than 2012.',
                0,
                1) with nowait;
            return;
        end;

    if ((select SERVERPROPERTY('EDITION')) = 'SQL Azure'
        and
        LOWER(@eventsessionpath) not like 'http%')
        begin
            raiserror (
                'The default storage path doesn''t work in Azure SQLDB/Managed instances.
You need to use an Azure storage account, and the path has to look like this: https://StorageAccount.blob.core.windows.net/Container/FileName.xel',
                0,
                1) with nowait;
            return;
        end;


    if @top is null
        set @top = 2147483647;

    if @startdate is null
        set @startdate = '19000101';

    if @enddate is null
        set @enddate = '99991231';


    if OBJECT_ID('tempdb..#deadlock_data') is not null
        drop table #deadlock_data;

    if OBJECT_ID('tempdb..#deadlock_process') is not null
        drop table #deadlock_process;

    if OBJECT_ID('tempdb..#deadlock_stack') is not null
        drop table #deadlock_stack;

    if OBJECT_ID('tempdb..#deadlock_resource') is not null
        drop table #deadlock_resource;

    if OBJECT_ID('tempdb..#deadlock_owner_waiter') is not null
        drop table #deadlock_owner_waiter;

    if OBJECT_ID('tempdb..#deadlock_findings') is not null
        drop table #deadlock_findings;

    create table #deadlock_findings
    (
        id int identity (1, 1) primary key clustered,
        check_id int not null,
        database_name nvarchar(256),
        object_name nvarchar(1000),
        finding_group nvarchar(100),
        finding nvarchar(4000)
    );

    declare @d varchar(40), @stringtoexecute nvarchar(4000),@stringtoexecuteparams nvarchar(500),@r nvarchar(200),@outputtablefindings nvarchar(100);
    declare @servername nvarchar(256)
    declare @outputdatabasecheck bit;
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    set @outputtablefindings = '[BlitzLockFindings]'
    set @servername = (select @@ServerName)
    if (@outputdatabasename is not null)
        begin
            --if databaseName is set do some sanity checks and put [] around def.
            if ((select name from sys.databases where name = @outputdatabasename) is null) --if database is invalid raiserror and set bitcheck
                begin
                    raiserror ('Database Name for output of table is invalid please correct, Output to Table will not be preformed', 0, 1, @d) with nowait;
                    set @outputdatabasecheck = -1 -- -1 invalid/false, 0 = good/true
                end
            else
                begin
                    set @outputdatabasecheck = 0
                    select @stringtoexecute = N'select @r = name from ' + '' + @outputdatabasename +
                                              '' + '.sys.objects where type_desc=''USER_TABLE'' and name=' + '''' +
                                              @outputtablename + '''',
                           @stringtoexecuteparams =
                           N'@OutputDatabaseName NVARCHAR(200),@OutputTableName NVARCHAR(200),@r NVARCHAR(200) OUTPUT'
                    exec sp_executesql @stringtoexecute, @stringtoexecuteparams, @outputdatabasename, @outputtablename,
                         @r output
                    --put covers around all before.
                    select @outputdatabasename = QUOTENAME(@outputdatabasename),
                           @outputtablename = QUOTENAME(@outputtablename),
                           @outputschemaname = QUOTENAME(@outputschemaname)
                    if (@r is null) --if it is null there is no table, create it from above execution
                        begin
                            select @stringtoexecute =
                                   N'use ' + @outputdatabasename + ';create table ' + @outputschemaname + '.' +
                                   @outputtablename + ' (
							ServerName NVARCHAR(256),
							deadlock_type NVARCHAR(256),
							event_date datetime,
							database_name NVARCHAR(256),
							deadlock_group NVARCHAR(256),
							query XML,
							object_names XML,
							isolation_level NVARCHAR(256),
							owner_mode NVARCHAR(256),
							waiter_mode NVARCHAR(256),
							transaction_count bigint,
							login_name NVARCHAR(256),
							host_name NVARCHAR(256),
							client_app NVARCHAR(256),
							wait_time BIGINT,
							priority smallint,
							log_used BIGINT,
							last_tran_started datetime,
							last_batch_started datetime,
							last_batch_completed datetime,
							transaction_name NVARCHAR(256),
							owner_waiter_type NVARCHAR(256),
							owner_activity NVARCHAR(256),
							owner_waiter_activity NVARCHAR(256),
							owner_merging NVARCHAR(256),
							owner_spilling NVARCHAR(256),
							owner_waiting_to_close NVARCHAR(256),
							waiter_waiter_type NVARCHAR(256),
							waiter_owner_activity NVARCHAR(256),
							waiter_waiter_activity NVARCHAR(256),
							waiter_merging NVARCHAR(256),
							waiter_spilling NVARCHAR(256),
							waiter_waiting_to_close NVARCHAR(256),
							deadlock_graph XML)',
                                   @stringtoexecuteparams =
                                   N'@OutputDatabaseName NVARCHAR(200),@OutputSchemaName NVARCHAR(100),@OutputTableName NVARCHAR(200)'
                            exec sp_executesql @stringtoexecute, @stringtoexecuteparams, @outputdatabasename,
                                 @outputschemaname, @outputtablename
                            --table created.
                            select @stringtoexecute = N'select @r = name from ' + '' + @outputdatabasename +
                                                      '' +
                                                      '.sys.objects where type_desc=''USER_TABLE'' and name=''BlitzLockFindings''',
                                   @stringtoexecuteparams = N'@OutputDatabaseName NVARCHAR(200),@r NVARCHAR(200) OUTPUT'
                            exec sp_executesql @stringtoexecute, @stringtoexecuteparams, @outputdatabasename, @r output
                            if (@r is null) --if table does not excist
                                begin
                                    select @outputtablefindings = N'[BlitzLockFindings]',
                                           @stringtoexecute =
                                           N'use ' + @outputdatabasename + ';create table ' + @outputschemaname + '.' +
                                           @outputtablefindings + ' (
								ServerName NVARCHAR(256),
								check_id INT, 
								database_name NVARCHAR(256), 
								object_name NVARCHAR(1000), 
								finding_group NVARCHAR(100), 
								finding NVARCHAR(4000))',
                                           @stringtoexecuteparams =
                                           N'@OutputDatabaseName NVARCHAR(200),@OutputSchemaName NVARCHAR(100),@OutputTableFindings NVARCHAR(200)'
                                    exec sp_executesql @stringtoexecute, @stringtoexecuteparams, @outputdatabasename,
                                         @outputschemaname, @outputtablefindings

                                end

                        end
                    --create synonym for deadlockfindings.
                    if ((select name
                         from sys.objects
                         where name = 'DeadlockFindings' and type_desc = 'SYNONYM') is not null)
                        begin
                            raiserror ('found synonym', 0, 1) with nowait;
                            drop synonym deadlockfindings;
                        end
                    set @stringtoexecute =
                                'CREATE SYNONYM DeadlockFindings FOR ' + @outputdatabasename + '.' + @outputschemaname +
                                '.' + @outputtablefindings;
                    exec sp_executesql @stringtoexecute

                    --create synonym for deadlock table.
                    if ((select name from sys.objects where name = 'DeadLockTbl' and type_desc = 'SYNONYM') is not null)
                        begin
                            drop synonym deadlocktbl;
                        end
                    set @stringtoexecute =
                                'CREATE SYNONYM DeadLockTbl FOR ' + @outputdatabasename + '.' + @outputschemaname +
                                '.' + @outputtablename;
                    exec sp_executesql @stringtoexecute

                end
        end


    create table #t
    (
        id int not null
    );

    /* WITH ROWCOUNT doesn't work on Amazon RDS - see: https://github.com/BrentOzarULTD/SQL-Server-First-Responder-Kit/issues/2037 */
    if LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') as varchar(8000)), 8) <> 'EC2AMAZ-'
        and LEFT(CAST(SERVERPROPERTY('MachineName') as varchar(8000)), 8) <> 'EC2AMAZ-'
        and LEFT(CAST(SERVERPROPERTY('ServerName') as varchar(8000)), 8) <> 'EC2AMAZ-'
        and db_id('rdsadmin') is null
        begin
            update statistics #t with rowcount = 100000000, pagecount = 100000000;
        end

    /*Grab the initial set of XML to parse*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Grab the initial set of XML to parse at %s', 0, 1, @d) with nowait;
    with xml
             as (select CONVERT(xml, event_data) as deadlock_xml
                 from sys.fn_xe_file_target_read_file(@eventsessionpath, null, null, null))
    select top (@top) ISNULL(xml.deadlock_xml, '') as deadlock_xml
    into #deadlock_data
    from xml
             left join #t as t
                       on 1 = 1
    where xml.deadlock_xml.value('(/event/@name)[1]', 'VARCHAR(256)') = 'xml_deadlock_report'
      and CONVERT(datetime,
                  SWITCHOFFSET(CONVERT(datetimeoffset, xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime')),
                               DATENAME(tzoffset, SYSDATETIMEOFFSET()))) > @startdate
      and CONVERT(datetime,
                  SWITCHOFFSET(CONVERT(datetimeoffset, xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime')),
                               DATENAME(tzoffset, SYSDATETIMEOFFSET()))) < @enddate
    order by xml.deadlock_xml.value('(/event/@timestamp)[1]', 'datetime') desc
    option ( recompile );


    /*Parse process and input buffer XML*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse process and input buffer XML %s', 0, 1, @d) with nowait;
    select q.event_date,
           q.victim_id,
           q.deadlock_graph,
           q.id,
           q.database_id,
           q.priority,
           q.log_used,
           q.wait_resource,
           q.wait_time,
           q.transaction_name,
           q.last_tran_started,
           q.last_batch_started,
           q.last_batch_completed,
           q.lock_mode,
           q.transaction_count,
           q.client_app,
           q.host_name,
           q.login_name,
           q.isolation_level,
           q.process_xml,
           ISNULL(ca2.ib.query('.'), '') as input_buffer
    into #deadlock_process
    from (select dd.deadlock_xml,
                 CONVERT(datetime2(7), SWITCHOFFSET(CONVERT(datetimeoffset, dd.event_date),
                                                    DATENAME(tzoffset, SYSDATETIMEOFFSET()))) as event_date,
                 dd.victim_id,
                 dd.deadlock_graph,
                 ca.dp.value('@id', 'NVARCHAR(256)')                                          as id,
                 ca.dp.value('@currentdb', 'BIGINT')                                          as database_id,
                 ca.dp.value('@priority', 'SMALLINT')                                         as priority,
                 ca.dp.value('@logused', 'BIGINT')                                            as log_used,
                 ca.dp.value('@waitresource', 'NVARCHAR(256)')                                as wait_resource,
                 ca.dp.value('@waittime', 'BIGINT')                                           as wait_time,
                 ca.dp.value('@transactionname', 'NVARCHAR(256)')                             as transaction_name,
                 ca.dp.value('@lasttranstarted', 'DATETIME2(7)')                              as last_tran_started,
                 ca.dp.value('@lastbatchstarted', 'DATETIME2(7)')                             as last_batch_started,
                 ca.dp.value('@lastbatchcompleted', 'DATETIME2(7)')                           as last_batch_completed,
                 ca.dp.value('@lockMode', 'NVARCHAR(256)')                                    as lock_mode,
                 ca.dp.value('@trancount', 'BIGINT')                                          as transaction_count,
                 ca.dp.value('@clientapp', 'NVARCHAR(256)')                                   as client_app,
                 ca.dp.value('@hostname', 'NVARCHAR(256)')                                    as host_name,
                 ca.dp.value('@loginname', 'NVARCHAR(256)')                                   as login_name,
                 ca.dp.value('@isolationlevel', 'NVARCHAR(256)')                              as isolation_level,
                 ISNULL(ca.dp.query('.'), '')                                                 as process_xml
          from (select d1.deadlock_xml,
                       d1.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2')                             as event_date,
                       d1.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]',
                                             'NVARCHAR(256)')                                                  as victim_id,
                       d1.deadlock_xml.query('/event/data/value/deadlock')                                     as deadlock_graph
                from #deadlock_data as d1) as dd
                   cross apply dd.deadlock_xml.nodes('//deadlock/process-list/process') as ca(dp)
          where (ca.dp.value('@currentdb', 'BIGINT') = DB_ID(@databasename) or @databasename is null)
            and (ca.dp.value('@clientapp', 'NVARCHAR(256)') = @appname or @appname is null)
            and (ca.dp.value('@hostname', 'NVARCHAR(256)') = @hostname or @hostname is null)
            and (ca.dp.value('@loginname', 'NVARCHAR(256)') = @loginname or @loginname is null)
         ) as q
             cross apply q.deadlock_xml.nodes('//deadlock/process-list/process/inputbuf') as ca2(ib)
    option ( recompile );


    /*Parse execution stack XML*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse execution stack XML %s', 0, 1, @d) with nowait;
    select distinct dp.id,
                    dp.event_date,
                    ca.dp.value('@procname', 'NVARCHAR(1000)') as proc_name,
                    ca.dp.value('@sqlhandle', 'NVARCHAR(128)') as sql_handle
    into #deadlock_stack
    from #deadlock_process as dp
             cross apply dp.process_xml.nodes('//executionStack/frame') as ca(dp)
    where (ca.dp.value('@procname', 'NVARCHAR(256)') = @storedprocname or @storedprocname is null)
    option ( recompile );


    /*Grab the full resource list*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Grab the full resource list %s', 0, 1, @d) with nowait;
    select dd.deadlock_xml.value('(event/@timestamp)[1]', 'DATETIME2')                             as event_date,
           dd.deadlock_xml.value('(//deadlock/victim-list/victimProcess/@id)[1]', 'NVARCHAR(256)') as victim_id,
           ISNULL(ca.dp.query('.'), '')                                                            as resource_xml
    into #deadlock_resource
    from #deadlock_data as dd
             cross apply dd.deadlock_xml.nodes('//deadlock/resource-list') as ca(dp)
    option ( recompile );


    /*Parse object locks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse object locks %s', 0, 1, @d) with nowait;
    select distinct ca.event_date,
                    ca.database_id,
                    ca.object_name,
                    ca.lock_mode,
                    ca.index_name,
                    w.l.value('@id', 'NVARCHAR(256)')   as waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') as waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)')   as owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') as owner_mode,
                    N'OBJECT'                           as lock_type
    into #deadlock_owner_waiter
    from (
             select dr.event_date,
                    ca.dr.value('@dbid', 'BIGINT')              as database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') as object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)')       as lock_mode,
                    ca.dr.value('@indexname', 'NVARCHAR(256)')  as index_name,
                    ca.dr.query('.')                            as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/objectlock') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    where (ca.object_name = @objectname or @objectname is null)
    option ( recompile );


    /*Parse page locks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse page locks %s', 0, 1, @d) with nowait;
    insert #deadlock_owner_waiter with (tablockx)
    select distinct ca.event_date,
                    ca.database_id,
                    ca.object_name,
                    ca.lock_mode,
                    ca.index_name,
                    w.l.value('@id', 'NVARCHAR(256)')   as waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') as waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)')   as owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') as owner_mode,
                    N'PAGE'                             as lock_type
    from (
             select dr.event_date,
                    ca.dr.value('@dbid', 'BIGINT')              as database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') as object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)')       as lock_mode,
                    ca.dr.value('@indexname', 'NVARCHAR(256)')  as index_name,
                    ca.dr.query('.')                            as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/pagelock') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    option ( recompile );


    /*Parse key locks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse key locks %s', 0, 1, @d) with nowait;
    insert #deadlock_owner_waiter with (tablockx)
    select distinct ca.event_date,
                    ca.database_id,
                    ca.object_name,
                    ca.lock_mode,
                    ca.index_name,
                    w.l.value('@id', 'NVARCHAR(256)')   as waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') as waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)')   as owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') as owner_mode,
                    N'KEY'                              as lock_type
    from (
             select dr.event_date,
                    ca.dr.value('@dbid', 'BIGINT')              as database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') as object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)')       as lock_mode,
                    ca.dr.value('@indexname', 'NVARCHAR(256)')  as index_name,
                    ca.dr.query('.')                            as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/keylock') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    option ( recompile );


    /*Parse RID locks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse RID locks %s', 0, 1, @d) with nowait;
    insert #deadlock_owner_waiter with (tablockx)
    select distinct ca.event_date,
                    ca.database_id,
                    ca.object_name,
                    ca.lock_mode,
                    ca.index_name,
                    w.l.value('@id', 'NVARCHAR(256)')   as waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') as waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)')   as owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') as owner_mode,
                    N'RID'                              as lock_type
    from (
             select dr.event_date,
                    ca.dr.value('@dbid', 'BIGINT')              as database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') as object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)')       as lock_mode,
                    ca.dr.value('@indexname', 'NVARCHAR(256)')  as index_name,
                    ca.dr.query('.')                            as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/ridlock') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    option ( recompile );


    /*Parse row group locks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse row group locks %s', 0, 1, @d) with nowait;
    insert #deadlock_owner_waiter with (tablockx)
    select distinct ca.event_date,
                    ca.database_id,
                    ca.object_name,
                    ca.lock_mode,
                    ca.index_name,
                    w.l.value('@id', 'NVARCHAR(256)')   as waiter_id,
                    w.l.value('@mode', 'NVARCHAR(256)') as waiter_mode,
                    o.l.value('@id', 'NVARCHAR(256)')   as owner_id,
                    o.l.value('@mode', 'NVARCHAR(256)') as owner_mode,
                    N'ROWGROUP'                         as lock_type
    from (
             select dr.event_date,
                    ca.dr.value('@dbid', 'BIGINT')              as database_id,
                    ca.dr.value('@objectname', 'NVARCHAR(256)') as object_name,
                    ca.dr.value('@mode', 'NVARCHAR(256)')       as lock_mode,
                    ca.dr.value('@indexname', 'NVARCHAR(256)')  as index_name,
                    ca.dr.query('.')                            as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/rowgrouplock') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    option ( recompile );

    update d
    set d.index_name = d.object_name
        + '.HEAP'
    from #deadlock_owner_waiter as d
    where lock_type in (N'HEAP', N'RID')
    option (recompile);

    /*Parse parallel deadlocks*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Parse parallel deadlocks %s', 0, 1, @d) with nowait;
    select distinct ca.id,
                    ca.event_date,
                    ca.wait_type,
                    ca.node_id,
                    ca.waiter_type,
                    ca.owner_activity,
                    ca.waiter_activity,
                    ca.merging,
                    ca.spilling,
                    ca.waiting_to_close,
                    w.l.value('@id', 'NVARCHAR(256)') as waiter_id,
                    o.l.value('@id', 'NVARCHAR(256)') as owner_id
    into #deadlock_resource_parallel
    from (
             select dr.event_date,
                    ca.dr.value('@id', 'NVARCHAR(256)')             as id,
                    ca.dr.value('@WaitType', 'NVARCHAR(256)')       as wait_type,
                    ca.dr.value('@nodeId', 'BIGINT')                as node_id,
                 /* These columns are in 2017 CU5 ONLY */
                    ca.dr.value('@waiterType', 'NVARCHAR(256)')     as waiter_type,
                    ca.dr.value('@ownerActivity', 'NVARCHAR(256)')  as owner_activity,
                    ca.dr.value('@waiterActivity', 'NVARCHAR(256)') as waiter_activity,
                    ca.dr.value('@merging', 'NVARCHAR(256)')        as merging,
                    ca.dr.value('@spilling', 'NVARCHAR(256)')       as spilling,
                    ca.dr.value('@waitingToClose', 'NVARCHAR(256)') as waiting_to_close,
                 /*                                    */
                    ca.dr.query('.')                                as dr
             from #deadlock_resource as dr
                      cross apply dr.resource_xml.nodes('//resource-list/exchangeEvent') as ca(dr)
         ) as ca
             cross apply ca.dr.nodes('//waiter-list/waiter') as w(l)
             cross apply ca.dr.nodes('//owner-list/owner') as o(l)
    option ( recompile );


    /*Get rid of parallel noise*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Get rid of parallel noise %s', 0, 1, @d) with nowait;
    with c
             as
             (
                 select *, ROW_NUMBER() over ( partition by drp.owner_id, drp.waiter_id order by drp.event_date ) as rn
                 from #deadlock_resource_parallel as drp
             )
    delete
    from c
    where c.rn > 1
    option ( recompile );


    /*Get rid of nonsense*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Get rid of nonsense %s', 0, 1, @d) with nowait;
    delete dow
    from #deadlock_owner_waiter as dow
    where dow.owner_id = dow.waiter_id
    option ( recompile );

    /*Add some nonsense*/
    alter table #deadlock_process
        add waiter_mode nvarchar(256),
            owner_mode nvarchar(256),
            is_victim as CONVERT(bit, case when id = victim_id then 1 else 0 end);

    /*Update some nonsense*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Update some nonsense part 1 %s', 0, 1, @d) with nowait;
    update dp
    set dp.owner_mode = dow.owner_mode
    from #deadlock_process as dp
             join #deadlock_owner_waiter as dow
                  on dp.id = dow.owner_id
                      and dp.event_date = dow.event_date
    where dp.is_victim = 0
    option ( recompile );

    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Update some nonsense part 2 %s', 0, 1, @d) with nowait;
    update dp
    set dp.waiter_mode = dow.waiter_mode
    from #deadlock_process as dp
             join #deadlock_owner_waiter as dow
                  on dp.victim_id = dow.waiter_id
                      and dp.event_date = dow.event_date
    where dp.is_victim = 1
    option ( recompile );

    /*Get Agent Job and Step names*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Get Agent Job and Step names %s', 0, 1, @d) with nowait;
    select *,
           CONVERT(uniqueidentifier,
                   CONVERT(xml, '').value('xs:hexBinary(substring(sql:column("x.job_id"), 0) )', 'BINARY(16)')
               ) as job_id_guid
    into #agent_job
    from (
             select dp.event_date,
                    dp.victim_id,
                    dp.id,
                    dp.database_id,
                    dp.client_app,
                    SUBSTRING(dp.client_app,
                              CHARINDEX('0x', dp.client_app) + LEN('0x'),
                              32
                        ) as job_id,
                    SUBSTRING(dp.client_app,
                              CHARINDEX(': Step ', dp.client_app) + LEN(': Step '),
                              CHARINDEX(')', dp.client_app, CHARINDEX(': Step ', dp.client_app))
                                  - (CHARINDEX(': Step ', dp.client_app)
                                  + LEN(': Step '))
                        ) as step_id
             from #deadlock_process as dp
             where dp.client_app like 'SQLAgent - %'
         ) as x
    option ( recompile );


    alter table #agent_job
        add job_name nvarchar(256),
            step_name nvarchar(256);

    if SERVERPROPERTY('EngineEdition') not in (5, 6) /* Azure SQL DB doesn't support querying jobs */
        and not (LEFT(CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') as varchar(8000)), 8) =
                 'EC2AMAZ-' /* Neither does Amazon RDS Express Edition */
            and LEFT(CAST(SERVERPROPERTY('MachineName') as varchar(8000)), 8) = 'EC2AMAZ-'
            and LEFT(CAST(SERVERPROPERTY('ServerName') as varchar(8000)), 8) = 'EC2AMAZ-'
            and db_id('rdsadmin') is not null
            and EXISTS(select *
                       from master.sys.all_objects
                       where name in
                             ('rds_startup_tasks', 'rds_help_revlogin', 'rds_hexadecimal', 'rds_failover_tracking',
                              'rds_database_tracking', 'rds_track_change'))
            )
        begin
            set @stringtoexecute = N'UPDATE aj
                    SET  aj.job_name = j.name, 
                         aj.step_name = s.step_name
		            FROM msdb.dbo.sysjobs AS j
		            JOIN msdb.dbo.sysjobsteps AS s 
                        ON j.job_id = s.job_id
                    JOIN #agent_job AS aj
                        ON  aj.job_id_guid = j.job_id
                        AND aj.step_id = s.step_id
						OPTION ( RECOMPILE );';
            exec (@stringtoexecute);
        end

    update dp
    set dp.client_app =
            case
                when dp.client_app like N'SQLAgent - %'
                    then N'SQLAgent - Job: '
                    + aj.job_name
                    + N' Step: '
                    + aj.step_name
                else dp.client_app
                end
    from #deadlock_process as dp
             join #agent_job as aj
                  on dp.event_date = aj.event_date
                      and dp.victim_id = aj.victim_id
                      and dp.id = aj.id
    option ( recompile );

    /*Begin checks based on parsed values*/

    /*Check 1 is deadlocks by database*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 1 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 1                       as check_id,
           DB_NAME(dp.database_id) as database_name,
           '-'                     as object_name,
           'Total database locks'  as finding_group,
           'This database had '
               + CONVERT(nvarchar(20), COUNT_BIG(distinct dp.event_date))
               + ' deadlocks.'
    from #deadlock_process as dp
    where 1 = 1
      and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
      and (dp.event_date >= @startdate or @startdate is null)
      and (dp.event_date < @enddate or @enddate is null)
      and (dp.client_app = @appname or @appname is null)
      and (dp.host_name = @hostname or @hostname is null)
      and (dp.login_name = @loginname or @loginname is null)
    group by DB_NAME(dp.database_id)
    option ( recompile );

    /*Check 2 is deadlocks by object*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 2 objects %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 2                                           as check_id,
           ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') as database_name,
           ISNULL(dow.object_name, 'UNKNOWN')          as object_name,
           'Total object deadlocks'                    as finding_group,
           'This object was involved in '
               + CONVERT(nvarchar(20), COUNT_BIG(distinct dow.event_date))
               + ' deadlock(s).'
    from #deadlock_owner_waiter as dow
    where 1 = 1
      and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
      and (dow.event_date >= @startdate or @startdate is null)
      and (dow.event_date < @enddate or @enddate is null)
      and (dow.object_name = @objectname or @objectname is null)
    group by DB_NAME(dow.database_id), dow.object_name
    option ( recompile );

    /*Check 2 continuation, number of locks per index*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 2 indexes %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 2                                           as check_id,
           ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') as database_name,
           dow.index_name                              as index_name,
           'Total index deadlocks'                     as finding_group,
           'This index was involved in '
               + CONVERT(nvarchar(20), COUNT_BIG(distinct dow.event_date))
               + ' deadlock(s).'
    from #deadlock_owner_waiter as dow
    where 1 = 1
      and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
      and (dow.event_date >= @startdate or @startdate is null)
      and (dow.event_date < @enddate or @enddate is null)
      and (dow.object_name = @objectname or @objectname is null)
      and dow.lock_type not in (N'HEAP', N'RID')
    group by DB_NAME(dow.database_id), dow.index_name
    option ( recompile );


    /*Check 2 continuation, number of locks per heap*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 2 heaps %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 2                                           as check_id,
           ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') as database_name,
           dow.index_name                              as index_name,
           'Total heap deadlocks'                      as finding_group,
           'This heap was involved in '
               + CONVERT(nvarchar(20), COUNT_BIG(distinct dow.event_date))
               + ' deadlock(s).'
    from #deadlock_owner_waiter as dow
    where 1 = 1
      and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
      and (dow.event_date >= @startdate or @startdate is null)
      and (dow.event_date < @enddate or @enddate is null)
      and (dow.object_name = @objectname or @objectname is null)
      and dow.lock_type in (N'HEAP', N'RID')
    group by DB_NAME(dow.database_id), dow.index_name
    option ( recompile );


    /*Check 3 looks for Serializable locking*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 3 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 3                       as check_id,
           DB_NAME(dp.database_id) as database_name,
           '-'                     as object_name,
           'Serializable locking'  as finding_group,
           'This database has had ' +
           CONVERT(nvarchar(20), COUNT_BIG(*)) +
           ' instances of serializable deadlocks.'
                                   as finding
    from #deadlock_process as dp
    where dp.isolation_level like 'serializable%'
      and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
      and (dp.event_date >= @startdate or @startdate is null)
      and (dp.event_date < @enddate or @enddate is null)
      and (dp.client_app = @appname or @appname is null)
      and (dp.host_name = @hostname or @hostname is null)
      and (dp.login_name = @loginname or @loginname is null)
    group by DB_NAME(dp.database_id)
    option ( recompile );


    /*Check 4 looks for Repeatable Read locking*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 4 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 4                         as check_id,
           DB_NAME(dp.database_id)   as database_name,
           '-'                       as object_name,
           'Repeatable Read locking' as finding_group,
           'This database has had ' +
           CONVERT(nvarchar(20), COUNT_BIG(*)) +
           ' instances of repeatable read deadlocks.'
                                     as finding
    from #deadlock_process as dp
    where dp.isolation_level like 'repeatable read%'
      and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
      and (dp.event_date >= @startdate or @startdate is null)
      and (dp.event_date < @enddate or @enddate is null)
      and (dp.client_app = @appname or @appname is null)
      and (dp.host_name = @hostname or @hostname is null)
      and (dp.login_name = @loginname or @loginname is null)
    group by DB_NAME(dp.database_id)
    option ( recompile );


    /*Check 5 breaks down app, host, and login information*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 5 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 5                              as check_id,
           DB_NAME(dp.database_id)        as database_name,
           '-'                            as object_name,
           'Login, App, and Host locking' as finding_group,
           'This database has had ' +
           CONVERT(nvarchar(20), COUNT_BIG(distinct dp.event_date)) +
           ' instances of deadlocks involving the login ' +
           ISNULL(dp.login_name, 'UNKNOWN') +
           ' from the application ' +
           ISNULL(dp.client_app, 'UNKNOWN') +
           ' on host ' +
           ISNULL(dp.host_name, 'UNKNOWN')
                                          as finding
    from #deadlock_process as dp
    where 1 = 1
      and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
      and (dp.event_date >= @startdate or @startdate is null)
      and (dp.event_date < @enddate or @enddate is null)
      and (dp.client_app = @appname or @appname is null)
      and (dp.host_name = @hostname or @hostname is null)
      and (dp.login_name = @loginname or @loginname is null)
    group by DB_NAME(dp.database_id), dp.login_name, dp.client_app, dp.host_name
    option ( recompile );


    /*Check 6 breaks down the types of locks (object, page, key, etc.)*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 6 %s', 0, 1, @d) with nowait;
    with lock_types as (
        select DB_NAME(dp.database_id)                                              as database_name,
               dow.object_name,
               SUBSTRING(dp.wait_resource, 1, CHARINDEX(':', dp.wait_resource) - 1) as lock,
               CONVERT(nvarchar(20), COUNT_BIG(distinct dp.id))                     as lock_count
        from #deadlock_process as dp
                 join #deadlock_owner_waiter as dow
                      on dp.id = dow.owner_id
                          and dp.event_date = dow.event_date
        where 1 = 1
          and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
          and (dp.event_date >= @startdate or @startdate is null)
          and (dp.event_date < @enddate or @enddate is null)
          and (dp.client_app = @appname or @appname is null)
          and (dp.host_name = @hostname or @hostname is null)
          and (dp.login_name = @loginname or @loginname is null)
          and (dow.object_name = @objectname or @objectname is null)
          and dow.object_name is not null
        group by DB_NAME(dp.database_id), SUBSTRING(dp.wait_resource, 1, CHARINDEX(':', dp.wait_resource) - 1),
                 dow.object_name
    )
    insert
    #deadlock_findings
    with (tablockx)
    (
    check_id
    ,
    database_name
    ,
    object_name
    ,
    finding_group
    ,
    finding
    )
    select distinct 6                          as check_id,
                    lt.database_name,
                    lt.object_name,
                    'Types of locks by object' as finding_group,
                    'This object has had ' +
                    STUFF((select distinct N', ' + lt2.lock_count + ' ' + lt2.lock
                           from lock_types as lt2
                           where lt2.database_name = lt.database_name
                             and lt2.object_name = lt.object_name
                           for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'')
                        + ' locks'
    from lock_types as lt
    option ( recompile );


    /*Check 7 gives you more info queries for sp_BlitzCache & BlitzQueryStore*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 7 part 1 %s', 0, 1, @d) with nowait;
    with deadlock_stack as (
        select distinct ds.id,
                        ds.proc_name,
                        ds.event_date,
                        PARSENAME(ds.proc_name, 3)                                                                 as database_name,
                        PARSENAME(ds.proc_name, 2)                                                                 as schema_name,
                        PARSENAME(ds.proc_name, 1)                                                                 as proc_only_name,
                        '''' + STUFF((select distinct N',' + ds2.sql_handle
                                      from #deadlock_stack as ds2
                                      where ds2.id = ds.id
                                        and ds2.event_date = ds.event_date
                                      for xml path(N''), type).value(N'.[1]', N'NVARCHAR(MAX)'), 1, 1, N'') +
                        ''''                                                                                       as sql_handle_csv
        from #deadlock_stack as ds
        group by PARSENAME(ds.proc_name, 3),
                 PARSENAME(ds.proc_name, 2),
                 PARSENAME(ds.proc_name, 1),
                 ds.id,
                 ds.proc_name,
                 ds.event_date
    )
    insert
    #deadlock_findings
    with (tablockx)
    (
    check_id
    ,
    database_name
    ,
    object_name
    ,
    finding_group
    ,
    finding
    )
    select distinct 7                                           as check_id,
                    ISNULL(DB_NAME(dow.database_id), 'UNKNOWN') as database_name,
                    ds.proc_name                                as object_name,
                    'More Info - Query'                         as finding_group,
                    'EXEC sp_BlitzCache ' +
                    case
                        when ds.proc_name = 'adhoc'
                            then ' @OnlySqlHandles = ' + ds.sql_handle_csv
                        else '@StoredProcName = ' +
                             QUOTENAME(ds.proc_only_name, '''')
                        end +
                    ';'                                         as finding
    from deadlock_stack as ds
             join #deadlock_owner_waiter as dow
                  on dow.owner_id = ds.id
                      and dow.event_date = ds.event_date
    where 1 = 1
      and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
      and (dow.event_date >= @startdate or @startdate is null)
      and (dow.event_date < @enddate or @enddate is null)
      and (dow.object_name = @storedprocname or @storedprocname is null)
    option ( recompile );

    if @productversionmajor >= 13
        begin
            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Check 7 part 2 %s', 0, 1, @d) with nowait;
            with deadlock_stack as (
                select distinct ds.id,
                                ds.sql_handle,
                                ds.proc_name,
                                ds.event_date,
                                PARSENAME(ds.proc_name, 3) as database_name,
                                PARSENAME(ds.proc_name, 2) as schema_name,
                                PARSENAME(ds.proc_name, 1) as proc_only_name
                from #deadlock_stack as ds
            )
            insert
            #deadlock_findings
            with (tablockx)
            (
            check_id
            ,
            database_name
            ,
            object_name
            ,
            finding_group
            ,
            finding
            )
            select distinct 7                        as check_id,
                            DB_NAME(dow.database_id) as database_name,
                            ds.proc_name             as object_name,
                            'More Info - Query'      as finding_group,
                            'EXEC sp_BlitzQueryStore '
                                + '@DatabaseName = '
                                + QUOTENAME(ds.database_name, '''')
                                + ', '
                                + '@StoredProcName = '
                                + QUOTENAME(ds.proc_only_name, '''')
                                + ';'                as finding
            from deadlock_stack as ds
                     join #deadlock_owner_waiter as dow
                          on dow.owner_id = ds.id
                              and dow.event_date = ds.event_date
            where ds.proc_name <> 'adhoc'
              and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
              and (dow.event_date >= @startdate or @startdate is null)
              and (dow.event_date < @enddate or @enddate is null)
              and (dow.object_name = @storedprocname or @storedprocname is null)
            option ( recompile );
        end;


    /*Check 8 gives you stored proc deadlock counts*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 8 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 8                       as check_id,
           DB_NAME(dp.database_id) as database_name,
           ds.proc_name,
           'Stored Procedure Deadlocks',
           'The stored procedure '
               + PARSENAME(ds.proc_name, 2)
               + '.'
               + PARSENAME(ds.proc_name, 1)
               + ' has been involved in '
               + CONVERT(nvarchar(10), COUNT_BIG(distinct ds.id))
               + ' deadlocks.'
    from #deadlock_stack as ds
             join #deadlock_process as dp
                  on dp.id = ds.id
                      and ds.event_date = dp.event_date
    where ds.proc_name <> 'adhoc'
      and (ds.proc_name = @storedprocname or @storedprocname is null)
      and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
      and (dp.event_date >= @startdate or @startdate is null)
      and (dp.event_date < @enddate or @enddate is null)
      and (dp.client_app = @appname or @appname is null)
      and (dp.host_name = @hostname or @hostname is null)
      and (dp.login_name = @loginname or @loginname is null)
    group by DB_NAME(dp.database_id), ds.proc_name
    option (recompile);


    /*Check 9 gives you more info queries for sp_BlitzIndex */
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 9 %s', 0, 1, @d) with nowait;
    with bi as (
        select distinct dow.object_name,
                        PARSENAME(dow.object_name, 3) as database_name,
                        PARSENAME(dow.object_name, 2) as schema_name,
                        PARSENAME(dow.object_name, 1) as table_name
        from #deadlock_owner_waiter as dow
        where 1 = 1
          and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
          and (dow.event_date >= @startdate or @startdate is null)
          and (dow.event_date < @enddate or @enddate is null)
          and (dow.object_name = @objectname or @objectname is null)
          and dow.object_name is not null
    )
    insert
    #deadlock_findings
    with (tablockx)
    (
    check_id
    ,
    database_name
    ,
    object_name
    ,
    finding_group
    ,
    finding
    )
    select 9                   as check_id,
           bi.database_name,
           bi.schema_name + '.' + bi.table_name,
           'More Info - Table' as finding_group,
           'EXEC sp_BlitzIndex ' +
           '@DatabaseName = ' + QUOTENAME(bi.database_name, '''') +
           ', @SchemaName = ' + QUOTENAME(bi.schema_name, '''') +
           ', @TableName = ' + QUOTENAME(bi.table_name, '''') +
           ';'                 as finding
    from bi
    option ( recompile );

    /*Check 10 gets total deadlock wait time per object*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 10 %s', 0, 1, @d) with nowait;
    with chopsuey as (
        select distinct PARSENAME(dow.object_name, 3)                                                      as database_name,
                        dow.object_name,
                        CONVERT(varchar(10), (SUM(distinct dp.wait_time) / 1000) / 86400)                  as wait_days,
                        CONVERT(varchar(20), DATEADD(second, (SUM(distinct dp.wait_time) / 1000), 0),
                                             108)                                                          as wait_time_hms
        from #deadlock_owner_waiter as dow
                 join #deadlock_process as dp
                      on (dp.id = dow.owner_id or dp.victim_id = dow.waiter_id)
                          and dp.event_date = dow.event_date
        where 1 = 1
          and (DB_NAME(dow.database_id) = @databasename or @databasename is null)
          and (dow.event_date >= @startdate or @startdate is null)
          and (dow.event_date < @enddate or @enddate is null)
          and (dow.object_name = @objectname or @objectname is null)
          and (dp.client_app = @appname or @appname is null)
          and (dp.host_name = @hostname or @hostname is null)
          and (dp.login_name = @loginname or @loginname is null)
        group by PARSENAME(dow.object_name, 3), dow.object_name
    )
    insert
    #deadlock_findings
    with (tablockx)
    (
    check_id
    ,
    database_name
    ,
    object_name
    ,
    finding_group
    ,
    finding
    )
    select 10                                        as check_id,
           cs.database_name,
           cs.object_name,
           'Total object deadlock wait time'         as finding_group,
           'This object has had '
               + CONVERT(varchar(10), cs.wait_days)
               + ':' + CONVERT(varchar(20), cs.wait_time_hms, 108)
               + ' [d/h/m/s] of deadlock wait time.' as finding
    from chopsuey as cs
    where cs.object_name is not null
    option ( recompile );

    /*Check 11 gets total deadlock wait time per database*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 11 %s', 0, 1, @d) with nowait;
    with wait_time as (
        select DB_NAME(dp.database_id)            as database_name,
               SUM(CONVERT(bigint, dp.wait_time)) as total_wait_time_ms
        from #deadlock_process as dp
        where 1 = 1
          and (DB_NAME(dp.database_id) = @databasename or @databasename is null)
          and (dp.event_date >= @startdate or @startdate is null)
          and (dp.event_date < @enddate or @enddate is null)
          and (dp.client_app = @appname or @appname is null)
          and (dp.host_name = @hostname or @hostname is null)
          and (dp.login_name = @loginname or @loginname is null)
        group by DB_NAME(dp.database_id)
    )
    insert
    #deadlock_findings
    with (tablockx)
    (
    check_id
    ,
    database_name
    ,
    object_name
    ,
    finding_group
    ,
    finding
    )
    select 11                                  as check_id,
           wt.database_name,
           '-'                                 as object_name,
           'Total database deadlock wait time' as finding_group,
           'This database has had '
               + CONVERT(varchar(10), (SUM(distinct wt.total_wait_time_ms) / 1000) / 86400)
               + ':' + CONVERT(varchar(20), DATEADD(second, (SUM(distinct wt.total_wait_time_ms) / 1000), 0), 108)
               + ' [d/h/m/s] of deadlock wait time.'
    from wait_time as wt
    group by wt.database_name
    option ( recompile );

    /*Check 12 gets total deadlock wait time for SQL Agent*/
    set @d = CONVERT(varchar(40), GETDATE(), 109);
    raiserror ('Check 12 %s', 0, 1, @d) with nowait;
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    select 12,
           DB_NAME(aj.database_id),
           'SQLAgent - Job: '
               + aj.job_name
               + ' Step: '
               + aj.step_name,
           'Agent Job Deadlocks',
           RTRIM(COUNT(*)) + ' deadlocks from this Agent Job and Step'
    from #agent_job as aj
    group by DB_NAME(aj.database_id), aj.job_name, aj.step_name
    option ( recompile );

    /*Thank you goodnight*/
    insert #deadlock_findings with (tablockx)
        (check_id, database_name, object_name, finding_group, finding)
    values (-1,
            N'sp_BlitzLock ' + CAST(CONVERT(datetime, @versiondate, 102) as varchar(100)),
            N'SQL Server First Responder Kit',
            N'http://FirstResponderKit.org/',
            N'To get help or add your own contributions, join us at http://FirstResponderKit.org.');


    /*Results*/
    /*Break in case of emergency*/
    --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_process (event_date, id);
    --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_resource_parallel (event_date, owner_id);
    --CREATE CLUSTERED INDEX cx_whatever ON #deadlock_owner_waiter (event_date, owner_id, waiter_id);
    if (@outputdatabasecheck = 0)
        begin

            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Results 1 %s', 0, 1, @d) with nowait;
            with deadlocks
                     as (select N'Regular Deadlock'                                                            as deadlock_type,
                                dp.event_date,
                                dp.id,
                                dp.victim_id,
                                dp.database_id,
                                dp.priority,
                                dp.log_used,
                                dp.wait_resource collate database_default                                      as wait_resource,
                                CONVERT(
                                    xml,
                                        STUFF((select distinct NCHAR(10)
                                                                   + N' <object>'
                                                                   + ISNULL(c.object_name, N'')
                                                                   +
                                                               N'</object> ' collate database_default as object_name
                                               from #deadlock_owner_waiter as c
                                               where (dp.id = c.owner_id
                                                   or dp.victim_id = c.waiter_id)
                                                 and CONVERT(date, dp.event_date) = CONVERT(date, c.event_date)
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(4000)'),
                                              1, 1,
                                              N''))                                                            as object_names,
                                dp.wait_time,
                                dp.transaction_name,
                                dp.last_tran_started,
                                dp.last_batch_started,
                                dp.last_batch_completed,
                                dp.lock_mode,
                                dp.transaction_count,
                                dp.client_app,
                                dp.host_name,
                                dp.login_name,
                                dp.isolation_level,
                                dp.process_xml.value('(//process/inputbuf/text())[1]',
                                                     'NVARCHAR(MAX)')                                          as inputbuf,
                                ROW_NUMBER() over ( partition by dp.event_date, dp.id order by dp.event_date ) as dn,
                                DENSE_RANK() over ( order by dp.event_date )                                   as en,
                                ROW_NUMBER() over ( partition by dp.event_date order by dp.event_date ) - 1    as qn,
                                dp.is_victim,
                                ISNULL(dp.owner_mode, '-')                                                     as owner_mode,
                                null                                                                           as owner_waiter_type,
                                null                                                                           as owner_activity,
                                null                                                                           as owner_waiter_activity,
                                null                                                                           as owner_merging,
                                null                                                                           as owner_spilling,
                                null                                                                           as owner_waiting_to_close,
                                ISNULL(dp.waiter_mode, '-')                                                    as waiter_mode,
                                null                                                                           as waiter_waiter_type,
                                null                                                                           as waiter_owner_activity,
                                null                                                                           as waiter_waiter_activity,
                                null                                                                           as waiter_merging,
                                null                                                                           as waiter_spilling,
                                null                                                                           as waiter_waiting_to_close,
                                dp.deadlock_graph
                         from #deadlock_process as dp
                         where dp.victim_id is not null

                         union all

                         select N'Parallel Deadlock'                                                           as deadlock_type,
                                dp.event_date,
                                dp.id,
                                dp.victim_id,
                                dp.database_id,
                                dp.priority,
                                dp.log_used,
                                dp.wait_resource collate database_default,
                                CONVERT(xml, N'parallel_deadlock' collate database_default)                    as object_names,
                                dp.wait_time,
                                dp.transaction_name,
                                dp.last_tran_started,
                                dp.last_batch_started,
                                dp.last_batch_completed,
                                dp.lock_mode,
                                dp.transaction_count,
                                dp.client_app,
                                dp.host_name,
                                dp.login_name,
                                dp.isolation_level,
                                dp.process_xml.value('(//process/inputbuf/text())[1]',
                                                     'NVARCHAR(MAX)')                                          as inputbuf,
                                ROW_NUMBER() over ( partition by dp.event_date, dp.id order by dp.event_date ) as dn,
                                DENSE_RANK() over ( order by dp.event_date )                                   as en,
                                ROW_NUMBER() over ( partition by dp.event_date order by dp.event_date ) - 1    as qn,
                                1                                                                              as is_victim,
                                cao.wait_type collate database_default                                         as owner_mode,
                                cao.waiter_type                                                                as owner_waiter_type,
                                cao.owner_activity                                                             as owner_activity,
                                cao.waiter_activity                                                            as owner_waiter_activity,
                                cao.merging                                                                    as owner_merging,
                                cao.spilling                                                                   as owner_spilling,
                                cao.waiting_to_close                                                           as owner_waiting_to_close,
                                caw.wait_type collate database_default                                         as waiter_mode,
                                caw.waiter_type                                                                as waiter_waiter_type,
                                caw.owner_activity                                                             as waiter_owner_activity,
                                caw.waiter_activity                                                            as waiter_waiter_activity,
                                caw.merging                                                                    as waiter_merging,
                                caw.spilling                                                                   as waiter_spilling,
                                caw.waiting_to_close                                                           as waiter_waiting_to_close,
                                dp.deadlock_graph
                         from #deadlock_process as dp
                                  cross apply (select top 1 *
                                               from #deadlock_resource_parallel as drp
                                               where drp.owner_id = dp.id
                                                 and drp.wait_type = 'e_waitPipeNewRow'
                                               order by drp.event_date) as cao
                                  cross apply (select top 1 *
                                               from #deadlock_resource_parallel as drp
                                               where drp.owner_id = dp.id
                                                 and drp.wait_type = 'e_waitPipeGetRow'
                                               order by drp.event_date) as caw
                         where dp.victim_id is null
                           and dp.login_name is not null)
            insert
            into deadlocktbl (servername,
                              deadlock_type,
                              event_date,
                              database_name,
                              deadlock_group,
                              query,
                              object_names,
                              isolation_level,
                              owner_mode,
                              waiter_mode,
                              transaction_count,
                              login_name,
                              host_name,
                              client_app,
                              wait_time,
                              priority,
                              log_used,
                              last_tran_started,
                              last_batch_started,
                              last_batch_completed,
                              transaction_name,
                              owner_waiter_type,
                              owner_activity,
                              owner_waiter_activity,
                              owner_merging,
                              owner_spilling,
                              owner_waiting_to_close,
                              waiter_waiter_type,
                              waiter_owner_activity,
                              waiter_waiter_activity,
                              waiter_merging,
                              waiter_spilling,
                              waiter_waiting_to_close,
                              deadlock_graph)
            select @servername,
                   d.deadlock_type,
                   d.event_date,
                   DB_NAME(d.database_id)                                                as database_name,
                   'Deadlock #'
                       + CONVERT(nvarchar(10), d.en)
                       + ', Query #'
                       + case when d.qn = 0 then N'1' else CONVERT(nvarchar(10), d.qn) end
                       + case when d.is_victim = 1 then ' - VICTIM' else '' end
                                                                                         as deadlock_group,
                   CONVERT(xml, N'<inputbuf><![CDATA[' + d.inputbuf + N']]></inputbuf>') as query,
                   d.object_names,
                   d.isolation_level,
                   d.owner_mode,
                   d.waiter_mode,
                   d.transaction_count,
                   d.login_name,
                   d.host_name,
                   d.client_app,
                   d.wait_time,
                   d.priority,
                   d.log_used,
                   d.last_tran_started,
                   d.last_batch_started,
                   d.last_batch_completed,
                   d.transaction_name,
                /*These columns will be NULL for regular (non-parallel) deadlocks*/
                   d.owner_waiter_type,
                   d.owner_activity,
                   d.owner_waiter_activity,
                   d.owner_merging,
                   d.owner_spilling,
                   d.owner_waiting_to_close,
                   d.waiter_waiter_type,
                   d.waiter_owner_activity,
                   d.waiter_waiter_activity,
                   d.waiter_merging,
                   d.waiter_spilling,
                   d.waiter_waiting_to_close,
                   d.deadlock_graph
            from deadlocks as d
            where d.dn = 1
              and (is_victim = @victimsonly or @victimsonly = 0)
              and d.en < case when d.deadlock_type = N'Parallel Deadlock' then 2 else 2147483647 end
              and (DB_NAME(d.database_id) = @databasename or @databasename is null)
              and (d.event_date >= @startdate or @startdate is null)
              and (d.event_date < @enddate or @enddate is null)
              and (CONVERT(nvarchar(max), d.object_names) like '%' + @objectname + '%' or @objectname is null)
              and (d.client_app = @appname or @appname is null)
              and (d.host_name = @hostname or @hostname is null)
              and (d.login_name = @loginname or @loginname is null)
            order by d.event_date, is_victim desc
            option ( recompile );

            drop synonym deadlocktbl;
            --done insert into blitzlock table going to insert into findings table first create synonym.

            --	RAISERROR('att deadlock findings', 0, 1) WITH NOWAIT;


            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Findings %s', 0, 1, @d) with nowait;

            insert into deadlockfindings (servername, check_id, database_name, object_name, finding_group, finding)
            select @servername, df.check_id, df.database_name, df.object_name, df.finding_group, df.finding
            from #deadlock_findings as df
            order by df.check_id
            option ( recompile );

            drop synonym deadlockfindings; --done with inserting.
        end
    else --Output to database is not set output to client app
        begin
            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Results 1 %s', 0, 1, @d) with nowait;
            with deadlocks
                     as (select N'Regular Deadlock'                                                            as deadlock_type,
                                dp.event_date,
                                dp.id,
                                dp.victim_id,
                                dp.database_id,
                                dp.priority,
                                dp.log_used,
                                dp.wait_resource collate database_default                                      as wait_resource,
                                CONVERT(
                                    xml,
                                        STUFF((select distinct NCHAR(10)
                                                                   + N' <object>'
                                                                   + ISNULL(c.object_name, N'')
                                                                   +
                                                               N'</object> ' collate database_default as object_name
                                               from #deadlock_owner_waiter as c
                                               where (dp.id = c.owner_id
                                                   or dp.victim_id = c.waiter_id)
                                                 and CONVERT(date, dp.event_date) = CONVERT(date, c.event_date)
                                               for xml path(N''), type).value(N'.[1]', N'NVARCHAR(4000)'),
                                              1, 1,
                                              N''))                                                            as object_names,
                                dp.wait_time,
                                dp.transaction_name,
                                dp.last_tran_started,
                                dp.last_batch_started,
                                dp.last_batch_completed,
                                dp.lock_mode,
                                dp.transaction_count,
                                dp.client_app,
                                dp.host_name,
                                dp.login_name,
                                dp.isolation_level,
                                dp.process_xml.value('(//process/inputbuf/text())[1]',
                                                     'NVARCHAR(MAX)')                                          as inputbuf,
                                ROW_NUMBER() over ( partition by dp.event_date, dp.id order by dp.event_date ) as dn,
                                DENSE_RANK() over ( order by dp.event_date )                                   as en,
                                ROW_NUMBER() over ( partition by dp.event_date order by dp.event_date ) - 1    as qn,
                                dp.is_victim,
                                ISNULL(dp.owner_mode, '-')                                                     as owner_mode,
                                null                                                                           as owner_waiter_type,
                                null                                                                           as owner_activity,
                                null                                                                           as owner_waiter_activity,
                                null                                                                           as owner_merging,
                                null                                                                           as owner_spilling,
                                null                                                                           as owner_waiting_to_close,
                                ISNULL(dp.waiter_mode, '-')                                                    as waiter_mode,
                                null                                                                           as waiter_waiter_type,
                                null                                                                           as waiter_owner_activity,
                                null                                                                           as waiter_waiter_activity,
                                null                                                                           as waiter_merging,
                                null                                                                           as waiter_spilling,
                                null                                                                           as waiter_waiting_to_close,
                                dp.deadlock_graph
                         from #deadlock_process as dp
                         where dp.victim_id is not null

                         union all

                         select N'Parallel Deadlock'                                                           as deadlock_type,
                                dp.event_date,
                                dp.id,
                                dp.victim_id,
                                dp.database_id,
                                dp.priority,
                                dp.log_used,
                                dp.wait_resource collate database_default,
                                CONVERT(xml, N'parallel_deadlock' collate database_default)                    as object_names,
                                dp.wait_time,
                                dp.transaction_name,
                                dp.last_tran_started,
                                dp.last_batch_started,
                                dp.last_batch_completed,
                                dp.lock_mode,
                                dp.transaction_count,
                                dp.client_app,
                                dp.host_name,
                                dp.login_name,
                                dp.isolation_level,
                                dp.process_xml.value('(//process/inputbuf/text())[1]',
                                                     'NVARCHAR(MAX)')                                          as inputbuf,
                                ROW_NUMBER() over ( partition by dp.event_date, dp.id order by dp.event_date ) as dn,
                                DENSE_RANK() over ( order by dp.event_date )                                   as en,
                                ROW_NUMBER() over ( partition by dp.event_date order by dp.event_date ) - 1    as qn,
                                1                                                                              as is_victim,
                                cao.wait_type collate database_default                                         as owner_mode,
                                cao.waiter_type                                                                as owner_waiter_type,
                                cao.owner_activity                                                             as owner_activity,
                                cao.waiter_activity                                                            as owner_waiter_activity,
                                cao.merging                                                                    as owner_merging,
                                cao.spilling                                                                   as owner_spilling,
                                cao.waiting_to_close                                                           as owner_waiting_to_close,
                                caw.wait_type collate database_default                                         as waiter_mode,
                                caw.waiter_type                                                                as waiter_waiter_type,
                                caw.owner_activity                                                             as waiter_owner_activity,
                                caw.waiter_activity                                                            as waiter_waiter_activity,
                                caw.merging                                                                    as waiter_merging,
                                caw.spilling                                                                   as waiter_spilling,
                                caw.waiting_to_close                                                           as waiter_waiting_to_close,
                                dp.deadlock_graph
                         from #deadlock_process as dp
                                  outer apply (select top 1 *
                                               from #deadlock_resource_parallel as drp
                                               where drp.owner_id = dp.id
                                                 and drp.wait_type = 'e_waitPipeNewRow'
                                               order by drp.event_date) as cao
                                  outer apply (select top 1 *
                                               from #deadlock_resource_parallel as drp
                                               where drp.owner_id = dp.id
                                                 and drp.wait_type = 'e_waitPipeGetRow'
                                               order by drp.event_date) as caw
                         where dp.victim_id is null
                           and dp.login_name is not null
                )
            select d.deadlock_type,
                   d.event_date,
                   DB_NAME(d.database_id)                                                as database_name,
                   'Deadlock #'
                       + CONVERT(nvarchar(10), d.en)
                       + ', Query #'
                       + case when d.qn = 0 then N'1' else CONVERT(nvarchar(10), d.qn) end
                       + case when d.is_victim = 1 then ' - VICTIM' else '' end
                                                                                         as deadlock_group,
                   CONVERT(xml, N'<inputbuf><![CDATA[' + d.inputbuf + N']]></inputbuf>') as query,
                   d.object_names,
                   d.isolation_level,
                   d.owner_mode,
                   d.waiter_mode,
                   d.transaction_count,
                   d.login_name,
                   d.host_name,
                   d.client_app,
                   d.wait_time,
                   d.priority,
                   d.log_used,
                   d.last_tran_started,
                   d.last_batch_started,
                   d.last_batch_completed,
                   d.transaction_name,
                /*These columns will be NULL for regular (non-parallel) deadlocks*/
                   d.owner_waiter_type,
                   d.owner_activity,
                   d.owner_waiter_activity,
                   d.owner_merging,
                   d.owner_spilling,
                   d.owner_waiting_to_close,
                   d.waiter_waiter_type,
                   d.waiter_owner_activity,
                   d.waiter_waiter_activity,
                   d.waiter_merging,
                   d.waiter_spilling,
                   d.waiter_waiting_to_close,
                   d.deadlock_graph
            from deadlocks as d
            where d.dn = 1
              and (is_victim = @victimsonly or @victimsonly = 0)
              and d.en < case when d.deadlock_type = N'Parallel Deadlock' then 2 else 2147483647 end
              and (DB_NAME(d.database_id) = @databasename or @databasename is null)
              and (d.event_date >= @startdate or @startdate is null)
              and (d.event_date < @enddate or @enddate is null)
              and (CONVERT(nvarchar(max), d.object_names) like '%' + @objectname + '%' or @objectname is null)
              and (d.client_app = @appname or @appname is null)
              and (d.host_name = @hostname or @hostname is null)
              and (d.login_name = @loginname or @loginname is null)
            order by d.event_date, is_victim desc
            option ( recompile );

            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Findings %s', 0, 1, @d) with nowait;
            select df.check_id, df.database_name, df.object_name, df.finding_group, df.finding
            from #deadlock_findings as df
            order by df.check_id
            option ( recompile );

            set @d = CONVERT(varchar(40), GETDATE(), 109);
            raiserror ('Done %s', 0, 1, @d) with nowait;
        end --done with output to client app.


    if @debug = 1
        begin

            select '#deadlock_data' as table_name, *
            from #deadlock_data as dd
            option ( recompile );

            select '#deadlock_resource' as table_name, *
            from #deadlock_resource as dr
            option ( recompile );

            select '#deadlock_resource_parallel' as table_name, *
            from #deadlock_resource_parallel as drp
            option ( recompile );

            select '#deadlock_owner_waiter' as table_name, *
            from #deadlock_owner_waiter as dow
            option ( recompile );

            select '#deadlock_process' as table_name, *
            from #deadlock_process as dp
            option ( recompile );

            select '#deadlock_stack' as table_name, *
            from #deadlock_stack as ds
            option ( recompile );

        end; -- End debug

end; --Final End

go
