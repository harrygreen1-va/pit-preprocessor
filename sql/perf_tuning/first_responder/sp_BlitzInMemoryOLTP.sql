declare @msg nvarchar(max) = N'';

-- Must be a compatible, on-prem version of SQL (2014+)
if ((select SERVERPROPERTY('EDITION')) <> 'SQL Azure'
    and (select PARSENAME(CONVERT(nvarchar(128), SERVERPROPERTY('PRODUCTVERSION')), 4)) < 12
       )
    -- or Azure Database (not Azure Data Warehouse), running at database compat level 120+
    or ((select SERVERPROPERTY('EDITION')) = 'SQL Azure'
        and (select SERVERPROPERTY('ENGINEEDITION')) = 5
        and (select [compatibility_level]
             from sys.databases
             where [name] = DB_NAME()) < 120
       )
    begin
        select @msg = N'Sorry, sp_BlitzInMemoryOLTP doesn''t work on versions of SQL prior to 2014.' +
                      REPLICATE(CHAR(13), 7933);
        print @msg;
        return;
    end;


if OBJECT_ID('dbo.sp_BlitzInMemoryOLTP', 'P') is null
    execute ('CREATE PROCEDURE dbo.sp_BlitzInMemoryOLTP AS SELECT 1;');
go

alter procedure dbo.sp_blitzinmemoryoltp(@instancelevelonly bit = 0
                                        , @dbname nvarchar(4000) = N'ALL'
                                        , @tablename nvarchar(4000) = null
                                        , @debug bit = 0
                                        , @version varchar(30) = null output
                                        , @versiondate datetime = null output
                                        , @versioncheckmode bit = 0)
/*
.SYNOPSIS
    Get detailed information about In-Memory SQL Server objects

.DESCRIPTION
    Get detailed information about In-Memory SQL Server objects
    Tested on SQL Server: 2014, 2016, 2017
    tested on Azure SQL Database
    NOT tested on Azure Managed Instances

.PARAMETER @instanceLevelOnly
    Only check instance In-Memory related information

.PARAMETER @dbName
    Check database In-Memory objects for specified database

.PARAMETER @tableName
    Check database In-Memory objects for specified tablename

.PARAMETER @debug
    Only PRINT dynamic sql statements without executing it

.EXAMPLE
    EXEC sp_BlitzInMemoryOLTP;
    -- Get all In-memory information

.EXAMPLE
    EXEC sp_BlitzInMemoryOLTP @dbName = N'ಠ ಠ';
    -- Get In-memory information for database with name ಠ ಠ

.EXAMPLE
    EXEC sp_BlitzInMemoryOLTP @instanceLevelOnly = 1;
    -- Get only instance In-Memory information

.EXAMPLE
    EXEC sp_BlitzInMemoryOLTP @debug = 1;
    -- PRINT dynamic sql statements without executing it

.LICENSE MIT
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

.NOTE
    Author: Ned Otter
    Version: 2.0
    Original link: http://nedotter.com/archive/2017/10/in-memory-oltp-diagnostic-script/
    Release Link: https://github.com/ktaranov/sqlserver-kit/blob/master/Stored_Procedure/dbo.sp_BlitzInMemoryOLTP.sql
    Main Contributors: Ned Otter, Konstantin Taranov, Aleksey Nagorskiy

*/
as
declare @scriptversion varchar(30);
select @scriptversion = '1.8', @versiondate = '20200712';

    if (@versioncheckmode = 1)
        begin
            set @version = @scriptversion;
            return;
        end;

begin try

    set nocount on;

    declare @runningonazuresqldb bit = 0;

    declare @crlf varchar(10) = CHAR(10);

    declare @edition nvarchar(max) = CAST(SERVERPROPERTY('Edition') as nvarchar(128))
        , @errormessage nvarchar(512);

    declare @mssqlversion int = CONVERT(int, SERVERPROPERTY('ProductMajorVersion'));

    if @debug = 1 print ('--@MSSQLVersion = ' + CAST(@mssqlversion as varchar(30)));

    /*
    ###################################################
        if we get here, we are running at least SQL 2014, but that version 
        only runs In-Memory if we are using Enterprise Edition

        NOTE: Azure SQL database changes this equation
    ###################################################
    */

    /*
        SERVERPROPERTY('EngineEdition')
         1 = Personal or Desktop Engine (Not available in SQL Server 2005 and later versions.)
        ,2 = Standard (This is returned for Standard, Web, and Business Intelligence.)
        ,3 = Enterprise (This is returned for Evaluation, Developer, and both Enterprise editions.)
        ,4 = Express (This is returned for Express, Express with Tools and Express with Advanced Services)
        ,5 = SQL Database
        ,6 = SQL Data Warehouse
        ,8 = Managed Instance
    */

    select @runningonazuresqldb =
           case
               when SERVERPROPERTY('EngineEdition') in (5, 6, 8) then 1
               else 0
               end;

    -- Database level: we are running SQL Database or SQL Data Warehouse, but this specific database does not support XTP
    if (@runningonazuresqldb = 1 and DatabasePropertyEx(DB_NAME(), 'IsXTPSupported') = 0)
        begin
            set @errormessage = 'For Azure SQL Database, In-Memory OLTP is only suppported on the Premium tier';
            throw 55001, @errormessage, 1;
        end;

    -- not on Azure, so we need to check versions/Editions
    -- SQL 2014 only supports XTP on Enterprise edition
    if (SERVERPROPERTY('EngineEdition') in (2, 4)) and @mssqlversion = 12 and
       (@edition not like 'Enterprise%' and @edition not like 'Developer%')
        begin
            set @errormessage = CONCAT(
                    'For SQL 2014, In-Memory OLTP is only suppported on Enterprise Edition. You are running SQL Server edition: ',
                    @edition);
            throw 55002, @errormessage, 1;
        end;

    -- We're not running on Azure, so we need to check versions/Editions
    -- SQL 2016 non-Enterprise only supports XTP after SP1
    declare @buildstring varchar(4) = CONVERT(varchar(4), SERVERPROPERTY('ProductBuild'));

    if (SERVERPROPERTY('EngineEdition') in (2, 4)) and @mssqlversion = 13 and (@buildstring < 4001)
        -- 13.0.4001.0 is the minimum build for XTP support
        begin
            set @errormessage = 'For SQL 2016, In-Memory OLTP is only suppported on non-Enterprise Edition as of SP1';
            throw 55003, @errormessage, 1;
        end;

    /*
    ######################################################################################################################
        DATABASE LEVEL
    ######################################################################################################################
    */

    declare @resultsdatabaselayout table
                                   (
                                       [object] nvarchar(max),
                                       databasename nvarchar(max),
                                       filegroupname nvarchar(max),
                                       filename nvarchar(max),
                                       [containerName/fileType] nvarchar(max),
                                       name nvarchar(max),
                                       iscontainer nvarchar(max),
                                       filegroupdescription nvarchar(max),
                                       filegroupstate nvarchar(max),
                                       sizekb nvarchar(max),
                                       sizemb nvarchar(max),
                                       sizegb nvarchar(max),
                                       totalsizemb nvarchar(max)
                                   );

    declare @resultsnativemodulecount table
                                      (
                                          [object] nvarchar(max),
                                          databasename nvarchar(max),
                                          [Number of modules] int
                                      );

    declare @resultsinmemtables table
                                (
                                    [object] nvarchar(max),
                                    databasename nvarchar(max),
                                    tablename nvarchar(max),
                                    [rowCount] int,
                                    durability_desc nvarchar(max),
                                    temporal_type_desc nvarchar(max),
                                    memoryallocatedfortablekb nvarchar(max),
                                    memoryusedbytablekb nvarchar(max),
                                    memoryallocatedforindexeskb nvarchar(max),
                                    memoryusedbyindexeskb nvarchar(max)
                                );

    declare @resultsindexes table
                            (
                                [object] nvarchar(max),
                                databasename nvarchar(max),
                                tablename nvarchar(max),
                                indexname nvarchar(max),
                                memory_consumer_id int,
                                consumertype nvarchar(max),
                                description nvarchar(max),
                                allocations int,
                                allocatedbytesmb nvarchar(max),
                                usedbytesmb nvarchar(max)
                            );

    declare @resultshashbuckets table
                                (
                                    [object] nvarchar(max),
                                    databasename nvarchar(max),
                                    [Schema] nvarchar(max),
                                    tablename nvarchar(max),
                                    indexname nvarchar(max),
                                    totalbucketcount bigint,
                                    emptybucketcount bigint,
                                    emptybucketpercent int,
                                    avg_chainlength int,
                                    max_chainlength bigint,
                                    [Free buckets status] nvarchar(max),
                                    [avg_chain_length status] bigint
                                );

    declare @resultsindexcount table
                               (
                                   [object] nvarchar(max),
                                   databasename nvarchar(max),
                                   tablename nvarchar(max),
                                   indexcount int
                               );

    declare @resultsnativemodules table
                                  (
                                      [object] nvarchar(max),
                                      name nvarchar(max),
                                      databasename nvarchar(max),
                                      [type] nvarchar(max),
                                      [definition] nvarchar(max)
                                  );

    declare @resultsnativeloaded table
                                 (
                                     [object] nvarchar(max),
                                     databasename nvarchar(max),
                                     modulename nvarchar(max),
                                     object_id int
                                 );

    declare @resultstemporal table
                             (
                                 [object] nvarchar(256),
                                 databasename nvarchar(max),
                                 temporaltableschema nvarchar(max),
                                 temporaltablename nvarchar(max),
                                 internalhistorytablename nvarchar(max),
                                 allocatedbytesforinternalhistorytable bigint,
                                 usedbytesforinternalhistorytable bigint
                             );

    declare @resultsmemoryconsumerforlobs table
                                          (
                                              [object] nvarchar(max),
                                              databasename nvarchar(max),
                                              tablename nvarchar(max),
                                              columnname nvarchar(max),
                                              typedescription nvarchar(max),
                                              memoryconsumertypedescription nvarchar(max),
                                              memoryconsumerdescription nvarchar(max),
                                              allocatedbytes int,
                                              usedbytes int
                                          );

    declare @resultstabletypes table
                               (
                                   [object] nvarchar(max),
                                   databasename nvarchar(max),
                                   [Schema] nvarchar(max),
                                   [Name] nvarchar(max)
                               );

    declare @resultsnativemodulestats table
                                      (
                                          [object] nvarchar(max),
                                          databasename nvarchar(max),
                                          object_id int,
                                          object_name nvarchar(max),
                                          cached_time datetime,
                                          last_execution_time datetime,
                                          execution_count int,
                                          total_worker_time int,
                                          last_worker_time int,
                                          min_worker_time int,
                                          max_worker_time int,
                                          total_elapsed_time int,
                                          last_elapsed_time int,
                                          min_elapsed_time int,
                                          max_elapsed_time int

                                      );

    declare @resultsxtp_storage_percent table
                                        (
                                            databasename nvarchar(max),
                                            end_time datetime,
                                            xtp_storage_percent decimal(5, 2)

                                        );

    create table #resultscontainerdetails
    (
        [object] nvarchar(256),
        databasename nvarchar(256),
        containername nvarchar(256),
        container_id bigint,
        sizemb nvarchar(256),
        filecount int
    );

    create table #resultscontainerfiledetails
    (
        [object] nvarchar(256),
        databasename nvarchar(256),
        containername nvarchar(256),
        container_id bigint,
        filetype nvarchar(256),
        filestate nvarchar(256),
        sizebytes nvarchar(256),
        sizegb nvarchar(256),
        filecount int,
        filegroupstate nvarchar(256)
    );

    create table #resultscontainerfilesummary
    (
        [object] nvarchar(256),
        databasename nvarchar(256),
        filetype nvarchar(256),
        filestate nvarchar(256),
        sizebytes nvarchar(256),
        sizemb nvarchar(256),
        filecount int,
        filegroupstate nvarchar(256)
    );

    if OBJECT_ID('tempdb..#inmemDatabases') is not null drop table #inmemdatabases;

    /*
        -- IF we are searching for a specific @tablename, it could exist in >1 database. 
        -- This is the point at which we should filter, but it might require dynamic SQL, 
        -- or deleting database names that don't have an object where the name matches. 

    */

    select QUOTENAME(name)                       as name
         , database_id
         , ROW_NUMBER() over (order by name asc) as rownumber
    into #inmemdatabases
    from sys.databases
    where name not in ('master', 'model', 'tempdb', 'distribution', 'msdb', 'SSISDB')
      and 1 =
          case
              when @runningonazuresqldb = 1 then 1
              when @runningonazuresqldb = 0 and name = @dbname then 1
              when @runningonazuresqldb = 0 and @dbname = N'ALL' then 1
              else 0
              end
      and state_desc = 'ONLINE';

    declare @sql nvarchar(max) = ''

    declare @counter int = 1
        , @maxrows int = (select COUNT(*) from #inmemdatabases);

    while @counter <= @maxrows
        begin

            --IF @debug = 1 PRINT('--@counter = ' + CAST(@counter AS VARCHAR(30)) + ';' + @crlf);

            if @tablename is not null
                select @sql =
                       CONCAT
                           (
                               'DELETE #inmemDatabases '
                           , 'WHERE UPPER(name) = '
                           , ''''
                           , UPPER(name)
                           , ''''
                           , ' AND NOT EXISTS ('
                           , 'SELECT *
                FROM '
                           , name
                           , '.sys.objects
            WHERE UPPER(name) = '
                           , ''''
                           , UPPER(@tablename)
                           , ''''
                           , ' AND UPPER(type) = ''U'')'
                           )
                from #inmemdatabases
                where rownumber = @counter;

            if @debug = 1 print (@sql);
            exec (@sql)

            select @counter += 1;
        end;

    alter table #inmemdatabases
        add newrownumber int identity

    if @debug = 1
        select 'All ONLINE user databases' as alldatabases
             , name
             , database_id
        from #inmemdatabases;

    if @dbname is null and @instancelevelonly = 0
        begin
            set @errormessage = '@dbName IS NULL, please specify database name or ALL';
            throw 55004, @errormessage, 1;
            return;
        end;

    if (@dbname is not null and @dbname <> N'ALL')
        and (not EXISTS(select 1 from #inmemdatabases where name = QUOTENAME(@dbname)) and @instancelevelonly = 0)
        begin
            set @errormessage = N'Database [' + @dbname + N'] not found in sys.databases!!!' + @crlf +
                                N'Did you add N if your database has a unicode name?' + @crlf +
                                N'Try to exec this: EXEC sp_BlitzInMemoryOLTP @dbName = N''ಠ ಠ_Your_Unicode_DB_Name_ಠ ಠ''';
            throw 55005, @errormessage, 1;
            return;
        end;

    if @dbname = 'ALL' and not EXISTS(select 1 from #inmemdatabases)
        begin
            set @errormessage = 'ALL was specified, but no memory-optimized databases were found';
            throw 55006, @errormessage, 1;
            return;
        end;

    -- we can't reference sys.dm_os_loaded_modules if we're on Azure SQL DB
    if @runningonazuresqldb = 0
        begin
            if OBJECT_ID('tempdb..#moduleSplit') is not null drop table #modulesplit;

            create table #modulesplit
            (
                rownumber int identity primary key,
                value nvarchar(max) null
            );

            declare @loadedmodules table
                                   (
                                       rownumber int identity primary key,
                                       name nvarchar(max) null
                                   );

            insert @loadedmodules
            (
                name
            )
            select name
            from sys.dm_os_loaded_modules as a
            where description = 'XTP Native DLL'
              and PATINDEX('%[_]p[_]%', name) > 0;

            declare @maxloadedmodules int = (select COUNT(*) from @loadedmodules);
            declare @modulecounter int = 1;
            declare @loadedmodulename nvarchar(max) = '';

            set @modulecounter = 1;

            while @modulecounter <= @maxloadedmodules
                begin

                    select @loadedmodulename = name
                    from @loadedmodules
                    where rownumber = @modulecounter;

                    declare @xml xml
                        , @delimiter nvarchar(10);
                    set @delimiter = '_';
                    set @xml = CAST(('<X>' + REPLACE(@loadedmodulename, @delimiter, '</X><X>') + '</X>') as xml);

                    insert #modulesplit
                    (
                        value
                    )
                    select c.value('.', 'NVARCHAR(1000)') as value
                    from @xml.nodes('X') as x(c);

                    select @modulecounter += 1;

                end;
        end;

    if @instancelevelonly = 0
        begin

            /*
            ####################################################
                Determine which databases are memory-optimized
                NOTE: if we are running on Azure SQL DB, we need
                to verify in-memory capability without joining to
                sys.filegroups
            ####################################################
            */
            select @maxrows = (select COUNT(*) from #inmemdatabases);
            select @counter = 1

            select @sql = ''

            while @counter <= @maxrows
                begin

                    --IF @debug = 1 PRINT('--@counter = ' + CAST(@counter AS VARCHAR(30)) + ';' + @crlf);

                    if @counter = 1
                        begin
                            select @sql += ';WITH InMemDatabases AS (';
                        end;

                    select @sql +=
                           case
                               when @counter = 1 then '' -- there is exactly 1 database for the entire instance
                               else @crlf + ' UNION ALL ' + @crlf
                               end;

                    select @sql +=
                           case
                               when @runningonazuresqldb = 0 then
                                   CONCAT
                                       (
                                           @crlf
                                       , 'SELECT DISTINCT '
                                       , 'N'''
                                       , name
                                       , ''' AS databaseName,' + @crlf
                                       , database_id
                                       , ' AS database_id' + @crlf + ' FROM '
                                       , name
                                       , '.sys.database_files' + @crlf + ' INNER JOIN '
                                       , name
                                       , '.sys.filegroups ON database_files.data_space_id = filegroups.data_space_id '
                                       , 'WHERE filegroups.type = '
                                       , ''''
                                       , 'FX'
                                       , ''''
                                       )
                               else
                                   -- if we arrive here and we're running on Azure SQL DB, then the database inherently supports In-Memory OLTP
                                   CONCAT
                                       (
                                           @crlf
                                       , 'SELECT '
                                       , 'N'''
                                       , name
                                       , ''' AS databaseName,' + @crlf
                                       , database_id
                                       , ' AS database_id' + @crlf
                                       )
                               end
                    from #inmemdatabases
                    where newrownumber = @counter;

                    select @counter += 1;
                end;

            --IF @debug = 1 PRINT(@sql);


            -- post-processing
            select @sql +=
                   CONCAT
                       (
                           ')'
                       , @crlf
                       , 'SELECT InMemDatabases.*, sys.databases.log_reuse_wait_desc'
                       , @crlf
                       , 'FROM InMemDatabases '
                       , @crlf
                       , 'INNER JOIN sys.databases ON '
                       , 'QUOTENAME(sys.databases.name) = InMemDatabases.databaseName;'
                       );

            if @debug = 1
                print ('--Determine which databases are memory-optimized' + @crlf + @sql + @crlf);

            declare @rowcount int = (select COUNT(*) from #inmemdatabases);

            if @rowcount <> 0
                begin

                    if OBJECT_ID('tempdb..#MemoryOptimizedDatabases') is not null drop table #memoryoptimizeddatabases;

                    create table #memoryoptimizeddatabases
                    (
                        rownumber int identity,
                        dbname nvarchar(256) not null,
                        database_id int null,
                        log_reuse_wait_desc nvarchar(256)
                    );

                    insert #memoryoptimizeddatabases
                    ( dbname
                    , database_id
                    , log_reuse_wait_desc)
                        execute sp_executesql @sql;

                    --IF @debug = 1 PRINT(@sql + @crlf);
                    --ELSE
                    --BEGIN
                    select 'Memory-optimized database(s)' as databases
                         , dbname
                         , database_id
                         , log_reuse_wait_desc
                    from #memoryoptimizeddatabases
                    order by dbname;
                    --END;
                end;

            if OBJECT_ID('tempdb..#NativeModules') is not null drop table #nativemodules;

            create table #nativemodules
            (
                modulekey int identity not null,
                moduleid int not null,
                modulename nvarchar(256) not null,
                collectionstatus bit null
            );

            select @sql = '';
            declare @dbcounter int = 1;
            select @maxrows = COUNT(*) from #memoryoptimizeddatabases;
            declare @databaseid int = 1;


            /*
            ###################################################
                This is the loop that processes each db
            ###################################################
            */

            while @dbcounter <= @maxrows
                begin

                    select 'now processing database: ' + dbname as status
                    from #memoryoptimizeddatabases
                    where rownumber = @dbcounter;

                    /*
                    ###################################################
                        List memory-optimized tables in this database
                    ###################################################
                    */
                    select @sql =
                           CONCAT
                               (
                                   'SELECT '
                               , '''Memory optimized tables'''
                               , ' AS [object],'
                               , ' N'''
                               , dbname
                               , ''' AS databaseName'
                               , ', b.name AS tableName
                    , p.rows AS [rowCount]
                    ,durability_desc '
                               , case
                                     when @mssqlversion >= 13 then ', temporal_type_desc '
                                     else ',NULL AS temporal_type_desc' end
                               , ', FORMAT(memory_allocated_for_table_kb, ''###,###,###'') AS memoryAllocatedForTableKB
                    ,FORMAT(memory_used_by_table_kb, ''###,###,###'') AS memoryUsedByTableKB
                    ,FORMAT(memory_allocated_for_indexes_kb, ''###,###,###'') AS memoryAllocatedForIndexesKB
                    ,FORMAT(memory_used_by_indexes_kb, ''###,###,###'') AS memoryUsedByIndexesKB
                     FROM '
                               , dbname
                               , '.sys.dm_db_xtp_table_memory_stats a'
                               , ' INNER JOIN '
                               , dbname
                               , '.sys.tables b ON b.object_id = a.object_id'
                               , ' INNER JOIN '
                               , dbname
                               , '.sys.partitions p'
                               , ' ON p.[object_id] = b.[object_id]'
                               , ' INNER JOIN '
                               , dbname
                               , '.sys.schemas s'
                               , ' ON b.[schema_id] = s.[schema_id]'
                               , ' WHERE p.index_id = 2'
                               )
                    from #memoryoptimizeddatabases
                    where rownumber = @dbcounter;

                    if @tablename is not null
                        begin
                            select @sql += CONCAT(' AND b.name = ', '''', @tablename, '''');
                        end;

                    if @debug = 1
                        print ('--List memory-optimized tables in this database' + @crlf + @sql + @crlf);
                    else
                        begin

                            delete @resultsinmemtables
                            insert @resultsinmemtables
                                execute sp_executesql @sql;

                            if EXISTS(select 1 from @resultsinmemtables)
                                select * from @resultsinmemtables;

                        end;

                    /*
                    ##############################################################
                        List indexes on memory-optimized tables in this database
                    ##############################################################
                    */
                    select @sql =
                           CONCAT
                               (
                                   'SELECT '
                               , '''List indexes on memory-optimized tables in this database'' AS [object],'
                               , ' N'''
                               , dbname
                               , ''''
                               , ' AS databaseName
                        ,t.name AS tableName
                        ,i.name AS indexName
                        ,c.memory_consumer_id
                        ,c.memory_consumer_type_desc AS consumerType
                        ,c.memory_consumer_desc AS description
                        ,c.allocation_count AS allocations
                        ,FORMAT(c.allocated_bytes / 1024.0, ''###,###,###,###'') AS allocatedBytesMB
                        ,FORMAT(c.used_bytes / 1024.00, ''###,###,###,###.###'') AS usedBytesMB
                    FROM '
                               , dbname
                               , '.sys.dm_db_xtp_memory_consumers c
                    INNER JOIN '
                               , dbname
                               , '.sys.tables t ON t.object_id = c.object_id'
                               , case
                                     when @mssqlversion > 12 then ' INNER JOIN sys.memory_optimized_tables_internal_attributes a ON a.object_id = c.object_id
                                                                            AND a.xtp_object_id = c.xtp_object_id'
                                     else null end
                               , @crlf + ' LEFT JOIN '
                               , dbname
                               , '.sys.indexes i ON c.object_id = i.object_id
                                                    AND c.index_id = i.index_id '
                               , case when @mssqlversion > 12 then 'AND a.minor_id = 0' else null end
                               , @crlf + ' WHERE t.type = '
                               , '''u'''
                               , '   AND t.is_memory_optimized = 1 '
                               , ' AND i.index_id IS NOT NULL'
                               )
                    from #memoryoptimizeddatabases
                    where rownumber = @dbcounter;

                    if @tablename is not null
                        begin
                            select @sql += CONCAT(' AND t.name = ', '''', @tablename, '''');
                        end;

                    select @sql += ' ORDER BY tableName, indexName;'

                    if @debug = 1
                        print ('--List indexes on memory-optimized tables in this database' + @crlf + @sql + @crlf);
                    else
                        begin
                            delete @resultsindexes
                            insert @resultsindexes
                                execute sp_executesql @sql;

                            if EXISTS(select 1 from @resultsindexes)
                                select * from @resultsindexes;

                        end;

                    /*
                    #########################################################
                        verify avg_chain_length for HASH indexes

                        From BOL:

                        Empty buckets:
                            33% is a good target value, but a larger percentage (even 90%) is usually fine.
                            When the bucket count equals the number of distinct key values, approximately 33% of the buckets are empty.
                            A value below 10% is too low.

                        Chains within buckets:
                            An average chain length of 1 is ideal in case there are no duplicate index key values. Chain lengths up to 10 are usually acceptable.
                            If the average chain length is greater than 10, and the empty bucket percent is greater than 10%,
                            the data has so many duplicates that a hash index might not be the most appropriate type.

                    #########################################################
                    */

                    select @sql =
                           CONCAT
                               (
                                   'SELECT '
                               , '''avg_chain_length for HASH indexes'''
                               , ' AS [object],'''
                               , dbname
                               , ''''
                               , ' AS databaseName'
                               , ', sch.name AS [Schema] '
                               , ', t.name AS tableName
                         ,i.name AS [indexName]
                         ,h.total_bucket_count AS totalBucketCount
                         ,h.empty_bucket_count AS emptyBucketCount
                         ,FLOOR((CAST(h.empty_bucket_count AS FLOAT) / h.total_bucket_count) * 100) AS [emptybBucketPercent]
                         ,h.avg_chain_length AS avg_ChainLength
                         ,h.max_chain_length AS maxChainLength
                         ,IIF(FLOOR((CAST(h.empty_bucket_count AS FLOAT) / h.total_bucket_count) * 100) < 33, ''Free buckets % is low!'', '''') AS [Free buckets status]
                         ,IIF(h.avg_chain_length > 10 AND FLOOR((CAST(h.empty_bucket_count AS FLOAT) / h.total_bucket_count) * 100) > 10, ''avg_chain_length has many collisions!'', '''') AS [avg_chain_length status]
                     FROM '
                               , dbname
                               , '.sys.dm_db_xtp_hash_index_stats AS h
                    INNER JOIN '
                               , dbname
                               , '.sys.indexes AS i ON h.object_id = i.object_id AND h.index_id = i.index_id'
                               , case
                                     when @mssqlversion > 12 then
                                         CONCAT(' INNER JOIN ', dbname,
                                                '.sys.memory_optimized_tables_internal_attributes ia ON h.xtp_object_id = ia.xtp_object_id')
                                     else null end
                               , ' INNER JOIN '
                               , dbname
                               , '.sys.tables t ON h.object_id = t.object_id'
                               , ' INNER JOIN '
                               , dbname
                               , '.sys.schemas sch ON sch.schema_id = t.schema_id '
                               , case when @mssqlversion > 12 then 'WHERE ia.type = 1' else null end
                               )
                    from #memoryoptimizeddatabases
                    where rownumber = @dbcounter;

                    if @tablename is not null
                        begin
                            select @sql += CONCAT(' AND t.name = ', '''', @tablename, '''');
                        end;

                    select @sql += ' ORDER BY sch.name
                                     ,t.name
                                     ,i.name;';

                    if @debug = 1
                        print ('--Verify avg_chain_length for HASH indexes' + @crlf + @sql + @crlf);
                    else
                        begin

                            delete @resultshashbuckets
                            insert @resultshashbuckets
                                execute sp_executesql @sql;
                            ;
                            if EXISTS(select 1 from @resultshashbuckets)
                                select * from @resultshashbuckets;

                        end;


                    /*
                    #########################################################
                        Count of indexes per table in this database
                    #########################################################
                    */

                    select @sql =
                           CONCAT
                               (
                                   'SELECT '
                               , '''Number of indexes per table'' AS [object],'
                               , ' N'''
                               , dbname
                               , ''''
                               , ' AS databaseName
                    ,t.name AS tableName
                    ,COUNT(DISTINCT i.index_id) AS indexCount
                    FROM '
                               , dbname
                               , '.sys.dm_db_xtp_memory_consumers c
                    INNER JOIN '
                               , dbname
                               , '.sys.tables t ON t.object_id = c.object_id'
                               , case
                                     when @mssqlversion > 12 then
                                         CONCAT(' INNER JOIN ', dbname, '.sys.memory_optimized_tables_internal_attributes a ON a.object_id = c.object_id
                                                                        AND a.xtp_object_id = c.xtp_object_id')
                                     else null end
                               , ' LEFT JOIN '
                               , dbname
                               , '.sys.indexes i ON c.object_id = i.object_id
                                                    AND c.index_id = i.index_id '
                               , case when @mssqlversion > 12 then ' AND a.minor_id = 0' else null end
                               , ' WHERE t.type = '
                               , '''u'''
                               , '   AND t.is_memory_optimized = 1 '
                               , ' AND i.index_id IS NOT NULL'
                               --,' GROUP BY t.name
                               --    ORDER BY t.name'
                               )
                    from #memoryoptimizeddatabases
                    where rownumber = @dbcounter;

                    if @tablename is not null
                        begin
                            select @sql += CONCAT(' AND t.name = ', '''', @tablename, '''');
                        end;

                    select @sql +=
                           ' GROUP BY t.name
                             ORDER BY t.name';

                    if @debug = 1
                        print ('--Count of indexes per table in this database' + @crlf + @sql + @crlf);
                    else
                        begin

                            delete @resultsindexcount
                            insert @resultsindexcount
                                execute sp_executesql @sql;

                            if EXISTS(select 1 from @resultsindexcount)
                                select * from @resultsindexcount;

                        end;


                    /*
                    #####################################################
                        List natively compiled modules in this database
                    #####################################################
                    */
                    /*

                        FN = SQL scalar function
                        IF = SQL inline table-valued function
                        TF = SQL table-valued-function
                        TR = SQL DML trigger
                    */

                    if @tablename is null
                        begin

                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT ''Natively compiled modules'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName
                         ,A.name
                         ,CASE A.type
                            WHEN ''FN'' THEN ''Function''
                            WHEN ''P'' THEN ''Procedure''
                            WHEN ''TR'' THEN ''Trigger''
                           END AS type
                         ,B.definition AS [definition]
                         FROM '
                                       , dbname
                                       , '.sys.all_objects AS A
                         INNER JOIN '
                                       , dbname
                                       , '.sys.sql_modules AS B ON B.object_id = A.object_id
                        WHERE UPPER(B.definition) LIKE ''%NATIVE_COMPILATION%''
                        AND UPPER(A.name) <> ''SP_BLITZINMEMORYOLTP''
                        ORDER BY A.type, A.name'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--List natively compiled modules in this database' + @crlf + @sql + @crlf);
                            else
                                begin

                                    delete @resultsnativemodules
                                    insert @resultsnativemodules
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultsnativemodules)
                                        select * from @resultsnativemodules;

                                end;
                        end;

                    /*
                    #####################################################
                        List *loaded* natively compiled modules in this database, i.e. executed at least 1x
                    #####################################################
                    */

                    /*
                        the format for checkpoint files changed from SQL 2014 to SQL 2016

                        SQL 2014 format:
                        database_id = 5
                        object_id = 309576141

                        H:\SQLDATA\xtp\5\xtp_p_5_309576141.dll

                        SQL 2016+ format
                        database_id = 9
                        object_id = 1600880920

                        H:\SQLDATA\xtp\9\xtp_p_9_1600880920_185048689287400_1.dll

                        the following code should handle all versions
                    */

                    -- NOTE: disabling this for Azure SQL DB
                    if @tablename is null and @runningonazuresqldb = 0
                        begin
                            select @sql =
                                   CONCAT
                                       (
                                           ';WITH nativeModuleObjectID AS
                                            (
                                               SELECT DISTINCT REPLACE(value, ''.dll'', '''') AS object_id
                                               FROM #moduleSplit
                                               WHERE rowNumber % '
                                       , case
                                             when @mssqlversion = 12 then ' 4 = 0'
                                             else ' 6 = 4' -- @MSSQLVersion >= 13
                                               end
                                       , ')'
                                       );

                            select @sql +=
                                   CONCAT
                                       (
                                           'SELECT ''Loaded natively compiled modules'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName
                       ,name AS moduleName
                       ,procedures.object_id
                        FROM '
                                       , dbname
                                       , '.sys.all_sql_modules
                        INNER JOIN '
                                       , dbname
                                       , '.sys.procedures ON procedures.object_id = all_sql_modules.object_id
                        INNER JOIN nativeModuleObjectID ON nativeModuleObjectID.object_id = procedures.object_id'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--List loaded natively compiled modules in this database (@MSSQLVersion >= 13)' +
                                       @crlf + @sql + @crlf);
                            else
                                begin

                                    delete @resultsnativeloaded
                                    insert @resultsnativeloaded
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultsnativeloaded)
                                        select * from @resultsnativeloaded;

                                end;
                        end;

                    /*
                    #########################################################
                        Count of natively compiled modules in this database
                    #########################################################
                    */
                    if @tablename is null
                        begin
                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT ''Count of natively compiled modules'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ' '''
                                       , ' AS databaseName
                        , COUNT(*) AS [Number of modules]
                        FROM '
                                       , dbname
                                       , '.sys.all_sql_modules
                         INNER JOIN '
                                       , dbname
                                       , '.sys.procedures ON procedures.object_id = all_sql_modules.object_id
                        WHERE uses_native_compilation = 1
                        ORDER BY 1'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--Count of natively compiled modules in this database' + @crlf + @sql + @crlf);
                            else
                                begin

                                    delete @resultsnativemodulecount
                                    insert @resultsnativemodulecount
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultsnativemodulecount where [Number of modules] > 0)
                                        select * from @resultsnativemodulecount;

                                end;
                        end;

                    /*
                    ############################################################
                        Display memory consumption for temporal/internal tables
                    ############################################################
                    */

                    -- temporal is supported in SQL 2016+
                    if @mssqlversion >= 13
                        begin

                            select @sql =
                                   CONCAT
                                       (
                                           ';WITH InMemoryTemporalTables
                                           AS
                                           (
                                               SELECT '
                                       , ''''
                                       , 'In-Memory Temporal Tables'
                                       , ''''
                                       , 'AS object,'
                                       , 'N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName'
                                       , ',sch.name AS temporalTableSchema
                                  ,T1.OBJECT_ID AS temporalTableObjectId
                                  ,IT.OBJECT_ID AS internalTableObjectId
                                  ,T1.name AS temporalTableName
                                  ,IT.Name AS internalHistoryTableName
                            FROM '
                                       , dbname
                                       , '.sys.internal_tables IT
                            INNER JOIN '
                                       , dbname
                                       , '.sys.tables T1 ON IT.parent_OBJECT_ID = T1.OBJECT_ID
                            INNER JOIN '
                                       , dbname
                                       , '.sys.schemas sch ON sch.schema_id = T1.schema_id
                            WHERE T1.is_memory_optimized = 1 
                              AND T1.temporal_type = 2
                        )
                        ,DetailedConsumption
                        AS
                        (
                            SELECT object
                                  ,databaseName
                                  ,temporalTableSchema
                                  ,T.temporalTableName
                                  ,T.internalHistoryTableName
                                  ,CASE
                                      WHEN C.object_id = T.temporalTableObjectId
                                      THEN ''Temporal Table''
                                      ELSE ''Internal Table''
                                   END AS ConsumedBy
                                  ,C.allocated_bytes
                                  ,C.used_bytes
                            FROM '
                                       , dbname
                                       , '.sys.dm_db_xtp_memory_consumers C
                            INNER JOIN InMemoryTemporalTables T
                            ON C.object_id = T.temporalTableObjectId OR C.object_id = T.internalTableObjectId
                            WHERE C.allocated_bytes > 0
                              AND C.object_id <> T.temporalTableObjectId
                        )
                        SELECT DISTINCT object
                              ,databaseName
                              ,temporalTableSchema
                              ,temporalTableName
                              ,internalHistoryTableName
                              ,SUM(allocated_bytes) OVER (PARTITION BY temporalTableName ORDER BY temporalTableName) AS allocatedBytesForInternalHistoryTable
                              ,SUM(used_bytes) OVER (PARTITION BY temporalTableName ORDER BY temporalTableName) AS usedBytesForInternalHistoryTable
                        FROM DetailedConsumption'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @tablename is not null
                                begin
                                    select @sql += CONCAT(' WHERE temporalTableName = ', '''', @tablename, '''');
                                end;

                            if @debug = 1
                                print ('--Display memory consumption for temporal/internal tables' + @crlf + @sql +
                                       @crlf);
                            else
                                begin

                                    delete @resultstemporal
                                    insert @resultstemporal
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultstemporal)
                                        select * from @resultstemporal;

                                end;
                        end;
                    -- display memory consumption for temporal/internal tables

                    /*
                    #########################################################
                        Display memory structures for LOB columns (off-row)
                        for SQL 2016+
                    #########################################################
                    */

                    if @mssqlversion >= 13
                        begin

                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT DISTINCT '
                                       , '''LOB/Off-row data '' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName'
                                       , ', OBJECT_NAME(a.object_id) AS tableName
                        ,cols.name AS columnName
                        ,a.type_desc AS typeDescription
                        ,c.memory_consumer_type_desc AS memoryConsumerTypeDescription
                        ,c.memory_consumer_desc AS memoryConsumerDescription
                        ,c.allocated_bytes AS allocatedBytes
                        ,c.used_bytes AS usedBytes
                         FROM '
                                       , dbname
                                       , '.sys.dm_db_xtp_memory_consumers c
                        INNER JOIN '
                                       , dbname
                                       , '.sys.memory_optimized_tables_internal_attributes a ON a.object_id = c.object_id
                                                                                AND a.xtp_object_id = c.xtp_object_id '
                                       , ' INNER JOIN '
                                       , dbname
                                       , '.sys.objects AS b ON b.object_id = a.object_id '
                                       , ' INNER JOIN '
                                       , dbname
                                       , '.sys.syscolumns AS cols ON cols.id = b.object_id
                           WHERE a.type_desc = '
                                       , ''''
                                       , 'INTERNAL OFF-ROW DATA TABLE'
                                       , ''''
                                       , ' AND c.memory_consumer_desc = ''Table heap'''
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @tablename is not null
                                begin
                                    select @sql += CONCAT(' AND OBJECT_NAME(a.object_id) = ', '''', @tablename, '''');
                                end;

                            select @sql += ' ORDER BY databaseName, tableName, columnName';

                            if @debug = 1
                                print ('--Display memory structures for LOB columns (off-row)' + @crlf + @sql + @crlf);
                            else
                                begin

                                    delete @resultsmemoryconsumerforlobs
                                    insert @resultsmemoryconsumerforlobs
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultsmemoryconsumerforlobs)
                                        select * from @resultsmemoryconsumerforlobs;

                                end;

                        end;

                    /*
                    #######################################################
                        Display memory-optimized table types
                    #######################################################
                    */
                    if @tablename is null
                        begin
                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT '
                                       , '''Memory optimized table types'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''' AS databaseName,'
                                       , 'SCHEMA_NAME(tt.schema_id) AS [Schema]
                              ,tt.name AS [Name]
                        FROM '
                                       , dbname
                                       , '.sys.table_types AS tt
                        INNER JOIN '
                                       , dbname
                                       , '.sys.schemas AS stt ON stt.schema_id = tt.schema_id
                        WHERE tt.is_memory_optimized = 1
                        ORDER BY [Schema], tt.name '
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--Display memory-optimized table types' + @crlf + @sql + @crlf);
                            else
                                begin

                                    delete @resultstabletypes
                                    insert @resultstabletypes
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultstabletypes)
                                        select * from @resultstabletypes;

                                end;

                        end;

                    /*
                    ##################################################################
                        ALL database files, including container name, size, location
                    ##################################################################
                    */

                    if @tablename is null
                        begin
                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT '
                                       , '''Database layout'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName'
                                       , ',filegroups.name AS fileGroupName
                          ,physical_name AS fileName
                          ,database_files.name AS [Name]
                          ,filegroups.type AS fileGroupType
                          ,IsContainer = IIF(filegroups.type = ''FX'', ''Yes'', ''No'')
                          ,filegroups.type_desc AS fileGroupDescription
                          ,database_files.state_desc AS fileGroupState
                          ,FORMAT(database_files.size * CONVERT(BIGINT, 8192) / 1024, ''###,###,###,###'') AS sizeKB
                          ,FORMAT(database_files.size * CONVERT(BIGINT, 8192) / 1048576.0, ''###,###,###,###'') AS sizeMB
                          ,FORMAT(database_files.size * CONVERT(BIGINT, 8192) / 1073741824.0, ''###,###,###,###.##'') AS sizeGB
                          ,FORMAT(SUM(database_files.size / 128.0) OVER(), ''###,###,###,###'') AS totalSizeMB
                        FROM '
                                       , dbname
                                       , '.sys.database_files
                        LEFT JOIN '
                                       , dbname
                                       , '.sys.filegroups ON database_files.data_space_id = filegroups.data_space_id
                        ORDER BY filegroups.type, filegroups.name, database_files.name'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--ALL database files, including container name, size, location' + @crlf + @sql +
                                       @crlf);
                            else
                                begin
                                    delete @resultsdatabaselayout

                                    insert @resultsdatabaselayout
                                        execute sp_executesql @sql;

                                    if EXISTS(select 1 from @resultsdatabaselayout)
                                        select * from @resultsdatabaselayout;

                                end;

                            /*
                            ##################################################################
                                container name, size, number of files
                            ##################################################################
                            */

                            delete #resultscontainerdetails;

                            select @sql =
                                   CONCAT
                                       (
                                           ';WITH ContainerDetails AS
                                           (
                                                   SELECT '
                                       , ' container_id
                                   ,SUM(ISNULL(file_size_in_bytes, 0)) AS sizeinBytes
                                   ,COUNT(*) AS fileCount
                                   ,MAX(container_guid) AS container_guid
                             FROM '
                                       , dbname
                                       , '.sys.dm_db_xtp_checkpoint_files
                             GROUP BY container_id
                         )
                         INSERT #resultsContainerDetails
                         SELECT 
                              ''Container details by container name'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName
                             ,database_files.name AS containerName
                             ,ContainerDetails.container_id
                             ,FORMAT(ContainerDetails.sizeinBytes / 1048576., ''###,###,###'') AS sizeMB
                             ,ContainerDetails.fileCount
                         FROM ContainerDetails
                         INNER JOIN '
                                       , dbname
                                       ,
                                           '.sys.database_files ON ContainerDetails.container_guid = database_files.file_guid'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--container name, size, number of files' + @crlf + @sql + @crlf);
                            else
                                begin
                                    execute sp_executesql @sql;
                                    if EXISTS(select 1 from #resultscontainerdetails)
                                        select * from #resultscontainerdetails
                                end

                            /*
                            ##################################################################
                                container file summary
                            ##################################################################
                            */

                            delete #resultscontainerfilesummary;

                            select @sql =
                                   CONCAT
                                       (
                                           ';WITH ContainerFileSummary AS
                                           (
                                                   SELECT '
                                       , '
                                   SUM(ISNULL(file_size_in_bytes, 0)) AS sizeinBytes
                                  ,MAX(ISNULL(file_type_desc, '''')) AS fileType
                                  ,COUNT(*) AS fileCount
                                  ,MAX(state_desc) AS fileState
                                  ,MAX(container_guid) AS container_guid
                            FROM '
                                       , dbname
                                       , '.sys.dm_db_xtp_checkpoint_files
                            GROUP BY file_type_desc, state_desc
                        )
                        INSERT #resultsContainerFileSummary
                        SELECT 
                             ''Container details by fileType and fileState'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName
                            ,ContainerFileSummary.fileType
                            ,ContainerFileSummary.fileState
                            ,FORMAT(ContainerFileSummary.sizeinBytes, ''###,###,###'') AS sizeBytes
                            ,FORMAT(ContainerFileSummary.sizeinBytes / 1048576., ''###,###,###'') AS sizeMB
                            ,ContainerFileSummary.fileCount
                            ,database_files.state_desc AS fileGroupState
                            FROM ContainerFileSummary
                        INNER JOIN '
                                       , dbname
                                       ,
                                           '.sys.database_files ON ContainerFileSummary.container_guid = database_files.file_guid'
                                       , ' ORDER BY ContainerFileSummary.fileType, ContainerFileSummary.fileState;'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--container file summary' + @crlf + @sql + @crlf);
                            else
                                begin
                                    execute sp_executesql @sql;
                                    if EXISTS(select 1 from #resultscontainerfilesummary)
                                        select * from #resultscontainerfilesummary;
                                end;

                            /*
                            ##################################################################
                                container file details
                            ##################################################################
                            */

                            delete #resultscontainerfiledetails;

                            select @sql =
                                   CONCAT
                                       (
                                           ';WITH ContainerFileDetails AS
                                           (
                                              SELECT
                                              container_id
                                             ,SUM(ISNULL(file_size_in_bytes, 0)) AS sizeinBytes
                                             ,MAX(ISNULL(file_type_desc, '''')) AS fileType
                                             ,COUNT(*) AS fileCount
                                             ,MAX(state_desc) AS fileState
                                             ,MAX(container_guid) AS container_guid
                                           FROM '
                                       , dbname
                                       , '.sys.dm_db_xtp_checkpoint_files
                            GROUP BY container_id, file_type_desc, state_desc
                        )
                        INSERT #resultsContainerFileDetails
                        SELECT '
                                       ,
                                           '''Container file details by container_id, fileType and fileState'' AS [object],'
                                       , ' N'''
                                       , dbname
                                       , ''''
                                       , ' AS databaseName
                        ,database_files.name AS containerName
                        ,ContainerFileDetails.container_id
                        ,ContainerFileDetails.fileType
                        ,ContainerFileDetails.fileState
                        ,FORMAT(ContainerFileDetails.sizeinBytes, ''###,###,###'') AS sizeBytes
                        ,FORMAT(ContainerFileDetails.sizeinBytes / 1048576., ''###,###,###'') AS sizeGB
                        ,ContainerFileDetails.fileCount
                        ,database_files.state_desc AS fileGroupState
                        FROM ContainerFileDetails
                        INNER JOIN '
                                       , dbname
                                       ,
                                           '.sys.database_files ON ContainerFileDetails.container_guid = database_files.file_guid'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            if @debug = 1
                                print ('--container details' + @crlf + @sql + @crlf);
                            else
                                begin
                                    execute sp_executesql @sql;
                                    if EXISTS(select 1 from #resultscontainerfiledetails)
                                        select * from #resultscontainerfiledetails;
                                end;
                        end;
                    /*

                    ###########################################################
                        Report on whether or not execution statistics
                        for natively compiled procedures is enabled
                    ###########################################################
                    */

                    if EXISTS(select 1 from #nativemodules)
                        begin
                            select @sql =
                                   CONCAT
                                       (
                                           'INSERT #NativeModules
                                           (
                                                moduleID
                                               ,moduleName
                                           )
                                           SELECT '
                                       , dbname
                                       , '.sys.all_sql_modules.Object_ID AS ObjectID
                        ,name AS moduleName
                        FROM '
                                       , dbname
                                       , '.sys.all_sql_modules
                         INNER JOIN '
                                       , dbname
                                       , '.sys.procedures ON procedures.object_id = all_sql_modules.object_id'
                                       , ' WHERE uses_native_compilation = 1'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            execute sp_executesql @sql;

                            delete @resultsnativemodulestats;

                            select @sql =
                                   CONCAT
                                       (
                                           'SELECT ''Native modules that have exec stats enabled'' AS object'
                                       , ','
                                       , ' N'''
                                       , dbname
                                       , ''' AS databaseName
                            ,object_id
                            ,OBJECT_NAME(object_id) AS ''object name''
                            ,cached_time
                            ,last_execution_time
                            ,execution_count
                            ,total_worker_time
                            ,last_worker_time
                            ,min_worker_time
                            ,max_worker_time
                            ,total_elapsed_time
                            ,last_elapsed_time
                            ,min_elapsed_time
                            ,max_elapsed_time
                        FROM '
                                       , 'sys.dm_exec_procedure_stats
                        WHERE database_id = DB_ID()
                        AND object_id IN (SELECT object_id FROM sys.sql_modules WHERE uses_native_compilation = 1)
                        ORDER BY total_worker_time DESC;'
                                       )
                            from #memoryoptimizeddatabases
                            where rownumber = @dbcounter;

                            insert @resultsnativemodulestats
                                execute sp_executesql @sql;

                            if @debug = 1
                                print ('--Native modules with execution status' + @crlf + @sql + @crlf);
                            else
                                if EXISTS(select 1 from @resultsnativemodulestats)
                                    select * from @resultsnativemodulestats;

                        end; --IF EXISTS (SELECT 1 FROM #NativeModules)

                    else
                        begin
                            print '--No modules found that have collection stats enabled';
                        end;

                    if @runningonazuresqldb = 1
                        begin

                            delete @resultsxtp_storage_percent;

                            insert @resultsxtp_storage_percent
                            ( databasename
                            , end_time
                            , xtp_storage_percent)
                            select DB_NAME() as databasename
                                 , end_time
                                 , xtp_storage_percent
                            from sys.dm_db_resource_stats
                            where xtp_storage_percent > 0;

                            if EXISTS(select 1 from @resultsxtp_storage_percent)
                                begin
                                    select databasename
                                         , 'xtp_storage_percent in descending order' as object
                                         , end_time
                                         , xtp_storage_percent
                                    from @resultsxtp_storage_percent
                                    order by end_time desc;
                                end;

                            select                  DB_NAME() as databasename
                                 , dbscopedconfig = 'XTP_PROCEDURE_EXECUTION_STATISTICS enabled:'
                                 , status         = case when value = 1 then 'Yes' else 'No' end
                            from sys.database_scoped_configurations
                            where UPPER(name) = 'XTP_PROCEDURE_EXECUTION_STATISTICS';

                            select                  DB_NAME() as databasename
                                 , dbscopedconfig = 'XTP_QUERY_EXECUTION_STATISTICS enabled:'
                                 , status         = case when value = 1 then 'Yes' else 'No' end
                            from sys.database_scoped_configurations
                            where UPPER(name) = 'XTP_QUERY_EXECUTION_STATISTICS';
                        end;

                    select @dbcounter += 1;

                end; -- This is the loop that processes each database

        end; -- IF @instanceLevelOnly = 0


    if OBJECT_ID('#NativeModules', 'U') is not null drop table #nativemodules;

    /*
    ######################################################################################################################
        INSTANCE LEVEL
    ######################################################################################################################
    */


    /*
    ###################################################
        Because SQL 2016/SP1 brings In-Memory OLTP to 
        editions other Enterprise, we must check
        @@version
    ###################################################
    */

    if @instancelevelonly = 1 and @mssqlversion >= 12
        begin

            select @@version as version;

            select name
                 , value        as configvalue
                 , value_in_use as runvalue
            from sys.configurations
            where UPPER(name) like 'MAX SERVER MEMORY%'
            order by name
            option (recompile);

            -- from Mark Wilkinson
            /*
                If memory is being used it should be in here.

                Memory that is reported as being consumed here for XTP was missing in
                the other XTP DMVs. We should simply look to see what the highest consumer is.

                SELECT * FROM sys.dm_xtp_system_memory_consumers

                SELECT * FROM sys.dm_db_xtp_memory_consumers

            */

            select 'dm_os_memory_clerks, DETAILS' as object
                 , type
                 , name
                 , pages_kb
                 , virtual_memory_reserved_kb
                 , virtual_memory_committed_kb
                 , awe_allocated_kb
                 , shared_memory_reserved_kb
                 , shared_memory_committed_kb
            from sys.dm_os_memory_clerks;

            select 'dm_os_memory_clerks, SUMMARY by XTP type' as object
                 , type                                       as object_type
                 , SUM(pages_kb) / 1024.0 / 1024.0            as pages_mb
            from sys.dm_os_memory_clerks
            where type like '%XTP%'
            group by type;

            declare @xtp_system_memory_consumers table
                                                 (
                                                     object_type nvarchar(64),
                                                     pagesallocatedmb bigint,
                                                     pagesusedmb bigint
                                                 );

            insert @xtp_system_memory_consumers
            ( object_type
            , pagesallocatedmb
            , pagesusedmb)
            select memory_consumer_type_desc              as object_type,
                   SUM(allocated_bytes) / 1024.0 / 1024.0 as pagesallocatedmb
                    ,
                   SUM(allocated_bytes) / 1024.0 / 1024.0 as pagesusedmb
            from sys.dm_xtp_system_memory_consumers
            group by memory_consumer_type_desc
            order by memory_consumer_type_desc;

            if EXISTS(select 1 from @xtp_system_memory_consumers)
                select 'xtp_system_memory_consumers' as object
                     , object_type
                     , pagesallocatedmb
                     , pagesusedmb
                from @xtp_system_memory_consumers;

            -- sys.dm_os_sys_info not supported on Azure SQL Database
            if @runningonazuresqldb = 0
                begin
                    select 'Committed Target memory'                                    as object
                         , FORMAT(committed_target_kb, '###,###,###,###,###')           as committedtargetkb
                         , FORMAT(committed_target_kb / 1024, '###,###,###,###,###')    as committedtargetmb
                         , FORMAT(committed_target_kb / 1048576, '###,###,###,###,###') as committedtargetgb
                    from sys.dm_os_sys_info;
                end

            if OBJECT_ID('#TraceFlags', 'U') is not null drop table #traceflags;

            create table #traceflags
            (
                traceflag int not null,
                status tinyint not null,
                global tinyint not null,
                session tinyint not null
            );

            set @sql = 'DBCC TRACESTATUS';

            insert #traceflags
                execute sp_executesql @sql;

            if @debug = 1
                print (@crlf + @sql + @crlf);

            declare @msg nvarchar(max);

            if EXISTS(select 1 from #traceflags where traceflag = 10316) -- allows custom indexing on hidden staging table for temporal tables
                begin

                    select @msg = 'TraceFlag 10316 is enabled';

                    select @msg
                         , traceflag
                         , status
                         , global
                         , session
                    from #traceflags
                    where traceflag = 10316
                    order by traceflag;

                end;

            /*
            #############################################################################################
                Verify if collection statistics are enabled for:
                1. specific native modules
                2. all native modules (instance-wide config)

                Having collection statistics enabled can severely impact performance of native modules.
            #############################################################################################
            */

            -- instance level
            declare @instancecollectionstatus bit;

            if @runningonazuresqldb = 0
                begin

                    exec sys.sp_xtp_control_query_exec_stats
                         @old_collection_value = @instancecollectionstatus output;

                    select case
                               when @instancecollectionstatus = 1 then 'YES'
                               else 'NO'
                               end as [instance-level collection of execution statistics for Native Modules enabled];
                end;
            else
                begin
                    -- repeating this from the database section if we are running @instanceLevelOnly = 1

                    delete @resultsxtp_storage_percent;

                    insert @resultsxtp_storage_percent
                    ( databasename
                    , end_time
                    , xtp_storage_percent)
                    select DB_NAME() as databasename
                         , end_time
                         , xtp_storage_percent
                    from sys.dm_db_resource_stats
                    where xtp_storage_percent > 0;

                    if EXISTS(select 1 from @resultsxtp_storage_percent)
                        begin
                            select databasename
                                 , 'xtp_storage_percent in descending order' as object
                                 , end_time
                                 , xtp_storage_percent
                            from @resultsxtp_storage_percent
                            order by end_time desc;
                        end;

                    select                  DB_NAME() as databasename
                         , dbscopedconfig = 'XTP_PROCEDURE_EXECUTION_STATISTICS enabled:'
                         , status         = case when value = 1 then 'Yes' else 'No' end
                    from sys.database_scoped_configurations
                    where UPPER(name) = 'XTP_PROCEDURE_EXECUTION_STATISTICS';

                    select                  DB_NAME() as databasename
                         , dbscopedconfig = 'XTP_QUERY_EXECUTION_STATISTICS enabled:'
                         , status         = case when value = 1 then 'Yes' else 'No' end
                    from sys.database_scoped_configurations
                    where UPPER(name) = 'XTP_QUERY_EXECUTION_STATISTICS';
                end;

            /*
            ####################################################################################
                List any databases that are bound to resource pools

                NOTE #1: if there are memory optimized databases that do NOT appear
                in this list, they consume memory from the 'default' pool, where
                all other SQL Server memory is allocated from.

                If the memory-optimized footprint grows, from either addition of rows,
                or row versions, it can put pressure on the buffer pool, cause it to shrink,
                and affect performance for harddrive-based tables.

                NOTE #2: if you want to bind a memory-optimized database to resource pool,
                the database must be taken OFFLINE/ONINE for the binding to take effect.
                This will cause all durable data to be removed from memory, and re(streamed)
                from checkpoint file pairs.

            ####################################################################################
            */

            if EXISTS(
                    select 1
                    from sys.databases d
                             inner join sys.dm_resource_governor_resource_pools as pools
                                        on pools.pool_id = d.resource_pool_id
                )
                select 'Resource pool'                                                          as [object]
                     , pools.name                                                               as poolname
                     , d.name                                                                   as databasename
                     , min_memory_percent                                                       as minmemorypercent
                     , max_memory_percent                                                       as maxmemorypercent
                     , used_memory_kb / 1024                                                    as usedmemorymb
                     , max_memory_kb / 1024                                                     as maxmemorymb
                     , FORMAT(((used_memory_kb * 1.0) / (max_memory_kb * 1.0) * 100), '###.##') as percentused
                     , target_memory_kb / 1024                                                  as targetmemorymb
                from sys.databases d
                         inner join sys.dm_resource_governor_resource_pools as pools
                                    on pools.pool_id = d.resource_pool_id
                order by poolname, databasename;

                /*
                ###########################################################
                    Memory breakdown
                ###########################################################
                */

                ;
            with clerksaggregated as
                (
                    select clerks.[type]                 as clerktype
                         , CONVERT(char(20)
                        , SUM(clerks.pages_kb) / 1024.0) as clerktypeusagemb
                    from sys.dm_os_memory_clerks as clerks with (nolock)
                    where clerks.pages_kb <> 0
                      and clerks.type in ('MEMORYCLERK_SQLBUFFERPOOL', 'MEMORYCLERK_XTP')
                    group by clerks.[type]
                )
               , clerksaggregatedstring as
                (
                    select clerktype
                         , clerktypeusagemb
                         , PATINDEX('%.%', clerktypeusagemb) as decimalpoint
                    from clerksaggregated
                )
            select clerktype
                 , memusagemb =
                case
                    when decimalpoint > 1 then SUBSTRING(clerktypeusagemb, 1, PATINDEX('%.%', clerktypeusagemb) - 1)
                    else clerktypeusagemb
                    end
            from clerksaggregatedstring;

            declare @dm_os_memory_clerks table
                                         (
                                             clerk_type nvarchar(60),
                                             name nvarchar(256),
                                             memory_node_id smallint,
                                             pages_mb bigint
                                         );

            insert @dm_os_memory_clerks
            ( clerk_type
            , name
            , memory_node_id
            , pages_mb)
                -- total memory allocated for in-memory engine
            select type            as clerk_type
                 , name
                 , memory_node_id
                 , pages_kb / 1024 as pages_mb
            from sys.dm_os_memory_clerks
            where type like '%xtp%';

            if EXISTS(select 1 from @dm_os_memory_clerks)
                select *
                from @dm_os_memory_clerks;


            /*
            #################################################################
                Oldest xtp transactions, they might prevent
                garbage collection from cleaning up row versions
            #################################################################
            */

            declare @dm_db_xtp_transactions table
                                            (
                                                [object] nvarchar(256),
                                                xtp_transaction_id bigint,
                                                transaction_id bigint,
                                                session_id smallint,
                                                begin_tsn bigint,
                                                end_tsn bigint,
                                                state_desc nvarchar(64),
                                                result_desc nvarchar(64)
                                            );

            insert @dm_db_xtp_transactions
            ( object
            , xtp_transaction_id
            , transaction_id
            , session_id
            , begin_tsn
            , end_tsn
            , state_desc
            , result_desc)
            select top 10 'Oldest xtp transactions' as [object]
                        , xtp_transaction_id
                        , transaction_id
                        , session_id
                        , begin_tsn
                        , end_tsn
                        , state_desc
                        , result_desc
            from sys.dm_db_xtp_transactions
            order by begin_tsn desc;

            if EXISTS(select 1 from @dm_db_xtp_transactions)
                select *
                from @dm_db_xtp_transactions;

            /*
            #################################################################
                Is event notification defined at the serverdb level?
                If so, errors will be generated, as EN is not
                supported for memory-optimized objects, and causes problems
            #################################################################
            */

            if EXISTS(
                    select 1
                    from sys.event_notifications
                )
                begin
                    select 'Event notifications are listed below';
                    select *
                    from sys.event_notifications;
                end;
        end; -- @instanceLevelOnly = 1 AND @MSSQLVersion >= 12

    select 'Thanks for using sp_BlitzInMemoryOLTP!'                                           as [Thanks],
           'From Your Community Volunteers'                                                   as [From],
           'http://FirstResponderKit.org'                                                     as [At],
           'We hope you found this tool useful. Current version: '
               + @scriptversion + ' released on ' + CONVERT(nvarchar(30), @versiondate) + '.' as [Version];


end try
begin catch
    throw
    print 'Error: ' + CONVERT(varchar(50), ERROR_NUMBER()) +
          ', Severity: ' + CONVERT(varchar(5), ERROR_SEVERITY()) +
          ', State: ' + CONVERT(varchar(5), ERROR_STATE()) +
          ', Procedure: ' + ISNULL(ERROR_PROCEDURE(), '-') +
          ', Line: ' + CONVERT(varchar(5), ERROR_LINE()) +
          ', User name: ' + CONVERT(sysname, CURRENT_USER);
    print ERROR_MESSAGE();
end catch;
go
