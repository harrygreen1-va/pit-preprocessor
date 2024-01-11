if OBJECT_ID('dbo.sp_ineachdb') is null
    exec ('CREATE PROCEDURE dbo.sp_ineachdb AS RETURN 0')
go

alter procedure [dbo].[sp_ineachdb]
    -- mssqltips.com/sqlservertip/5694/execute-a-command-in-the-context-of-each-database-in-sql-server--part-2/
    @command nvarchar(max) = null,
    @replace_character nchar(1) = N'?',
    @print_dbname bit = 0,
    @select_dbname bit = 0,
    @print_command bit = 0,
    @print_command_only bit = 0,
    @suppress_quotename bit = 0, -- use with caution
    @system_only bit = 0,
    @user_only bit = 0,
    @name_pattern nvarchar(300) = N'%',
    @database_list nvarchar(max) = null,
    @exclude_pattern nvarchar(300) = null,
    @exclude_list nvarchar(max) = null,
    @recovery_model_desc nvarchar(120) = null,
    @compatibility_level tinyint = null,
    @state_desc nvarchar(120) = N'ONLINE',
    @is_read_only bit = 0,
    @is_auto_close_on bit = null,
    @is_auto_shrink_on bit = null,
    @is_broker_enabled bit = null,
    @user_access nvarchar(128) = null,
    @help bit = 0,
    @version varchar(30) = null output,
    @versiondate datetime = null output,
    @versioncheckmode bit = 0
-- WITH EXECUTE AS OWNER – maybe not a great idea, depending on the security your system
as
begin
    set nocount on;

    select @version = '2.97', @versiondate = '20200712';

    if (@versioncheckmode = 1)
        begin
            return;
        end;
    if @help = 1
        begin

            print '
		/*
			sp_ineachdb from http://FirstResponderKit.org
			
			This script will execute a command against multiple databases.
		
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
			
			Copyright (c) 2019 Brent Ozar Unlimited
		
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

            return -1;
        end

    declare @exec nvarchar(150),
        @sx nvarchar(18) = N'.sys.sp_executesql',
        @db sysname,
        @dbq sysname,
        @cmd nvarchar(max),
        @thisdb sysname,
        @cr char(2) = CHAR(13) + CHAR(10);

    declare @sqlversion as tinyint = (@@microsoftversion / 0x1000000) & 0xff -- Stores the SQL Server Version Number(8(2000),9(2005),10(2008 & 2008R2),11(2012),12(2014),13(2016),14(2017))
    declare @servername as sysname = CONVERT(sysname, SERVERPROPERTY('ServerName')) -- Stores the SQL Server Instance name.

    create table #ineachdb
    (
        id int,
        name nvarchar(512)
    );

    if @database_list > N''
        -- comma-separated list of potentially valid/invalid/quoted/unquoted names
        begin
            ;
            with n(n) as (select 1 union all select n + 1 from n where n < 4000),
                 names as
                     (
                         select name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@database_list, n,
                                                                       CHARINDEX(N',', @database_list + N',', n) - n),
                                                             1)))
                         from n
                         where n <= LEN(@database_list)
                           and SUBSTRING(N',' + @database_list, n, 1) = N','
                     )
            insert
            #ineachdb
            (
            id
            ,
            name
            )
            select d.database_id, d.name
            from sys.databases as d
            where EXISTS(select 1 from names where name = d.name)
            option (maxrecursion 0);
        end
    else
        begin
            insert #ineachdb(id, name) select database_id, name from sys.databases;
        end

    -- first, let's delete any that have been explicitly excluded
    if @exclude_list > N''
        -- comma-separated list of potentially valid/invalid/quoted/unquoted names
        -- exclude trumps include
        begin
            ;
            with n(n) as (select 1 union all select n + 1 from n where n < 4000),
                 names as
                     (
                         select name = LTRIM(RTRIM(PARSENAME(SUBSTRING(@exclude_list, n,
                                                                       CHARINDEX(N',', @exclude_list + N',', n) - n),
                                                             1)))
                         from n
                         where n <= LEN(@exclude_list)
                           and SUBSTRING(N',' + @exclude_list, n, 1) = N','
                     )
            delete d
            from #ineachdb as d
                     inner join names
                                on names.name = d.name
            option (maxrecursion 0);
        end

    -- next, let's delete any that *don't* match various criteria passed in
    delete dbs
    from #ineachdb as dbs
    where (@system_only = 1 and id not in (1, 2, 3, 4))
       or (@user_only = 1 and id in (1, 2, 3, 4))
       or name not like @name_pattern
       or name like @exclude_pattern
       or EXISTS
        (
            select 1
            from sys.databases as d
            where d.database_id = dbs.id
              and not
                (
                            recovery_model_desc = COALESCE(@recovery_model_desc, recovery_model_desc)
                        and compatibility_level = COALESCE(@compatibility_level, compatibility_level)
                        and is_read_only = COALESCE(@is_read_only, is_read_only)
                        and is_auto_close_on = COALESCE(@is_auto_close_on, is_auto_close_on)
                        and is_auto_shrink_on = COALESCE(@is_auto_shrink_on, is_auto_shrink_on)
                        and is_broker_enabled = COALESCE(@is_broker_enabled, is_broker_enabled)
                    )
        );

    -- if a user access is specified, remove any that are NOT in that state
    if @user_access in (N'SINGLE_USER', N'MULTI_USER', N'RESTRICTED_USER')
        begin
            delete #ineachdb
            where CONVERT(nvarchar(128), DATABASEPROPERTYEX(name, 'UserAccess')) <> @user_access;
        end

    -- finally, remove any that are not *fully* online or we can't access
    delete dbs
    from #ineachdb as dbs
    where EXISTS
              (
                  select 1
                  from sys.databases
                  where database_id = dbs.id
                    and (
                              @state_desc = N'ONLINE' and
                              (
                                      [state] & 992 <> 0 -- inaccessible
                                      or state_desc <> N'ONLINE' -- not online
                                      or HAS_DBACCESS(name) = 0 -- don't have access
                                      or
                                      DATABASEPROPERTYEX(name, 'Collation') is null -- not fully online. See "status" here:
                                  -- https://docs.microsoft.com/en-us/sql/t-sql/functions/databasepropertyex-transact-sql
                                  )
                          or (@state_desc <> N'ONLINE' and state_desc <> @state_desc)
                      )
              );

    -- from Andy Mallon / First Responders Kit. Make sure that if we're an
-- AG secondary, we skip any database where allow connections is off
    if @sqlversion >= 11
        delete dbs
        from #ineachdb as dbs
        where EXISTS
                  (
                      select 1
                      from sys.dm_hadr_database_replica_states as drs
                               inner join sys.availability_replicas as ar
                                          on ar.replica_id = drs.replica_id
                               inner join sys.dm_hadr_availability_group_states ags
                                          on ags.group_id = ar.group_id
                      where drs.database_id = dbs.id
                        and ar.secondary_role_allow_connections = 0
                        and ags.primary_replica <> @servername
                  );

    -- Well, if we deleted them all...
    if not EXISTS(select 1 from #ineachdb)
        begin
            raiserror (N'No databases to process.', 1, 0);
            return;
        end

    -- ok, now, let's go through what we have left
    declare dbs cursor local fast_forward
        for select DB_NAME(id), QUOTENAME(DB_NAME(id))
            from #ineachdb;

    open dbs;

    fetch next from dbs into @db, @dbq;

    declare @msg1 nvarchar(512) = N'Could not run against %s : %s.',
        @msg2 nvarchar(max);

    while @@FETCH_STATUS <> -1
        begin
            set @thisdb = case when @suppress_quotename = 1 then @db else @dbq end;
            set @cmd = REPLACE(@command, @replace_character, REPLACE(@thisdb, '''', ''''''));

            begin try
                if @print_dbname = 1
                    begin
                        print N'/* ' + @thisdb + N' */';
                    end

                if @select_dbname = 1
                    begin
                        select [ineachdb current database] = @thisdb;
                    end

                if 1 in (@print_command, @print_command_only)
                    begin
                        print N'/* For ' + @thisdb + ': */' + @cr + @cr + @cmd + @cr + @cr;
                    end

                if COALESCE(@print_command_only, 0) = 0
                    begin
                        set @exec = @dbq + @sx;
                        exec @exec @cmd;
                    end
            end try
            begin catch
                set @msg2 = ERROR_MESSAGE();
                raiserror (@msg1, 1, 0, @db, @msg2);
            end catch

            fetch next from dbs into @db, @dbq;
        end

    close dbs;
    deallocate dbs;
end
go
