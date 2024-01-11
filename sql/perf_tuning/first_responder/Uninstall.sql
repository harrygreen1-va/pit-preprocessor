--First Responder Kit Uninstaller Script

--Configuration Parameters

declare @alldatabases bit = 0; --Flip this bit to 1 if you want to uninstall the scripts from all the databases, not only the current one
declare @printonly bit = 0;
--Flip this bit to 1 if you want to print the drop commands only without executing

--End Configuration
--Variables

set nocount on;
declare @sql nvarchar(max) = N'';

if OBJECT_ID('tempdb.dbo.#ToDelete') is not null
    drop table #todelete;

select 'sp_AllNightLog' as procedurename
into #todelete
union
select 'sp_AllNightLog_Setup' as procedurename
union
select 'sp_Blitz' as procedurename
union
select 'sp_BlitzBackups' as procedurename
union
select 'sp_BlitzCache' as procedurename
union
select 'sp_BlitzFirst' as procedurename
union
select 'sp_BlitzInMemoryOLTP' as procedurename
union
select 'sp_BlitzIndex' as procedurename
union
select 'sp_BlitzLock' as procedurename
union
select 'sp_BlitzQueryStore' as procedurename
union
select 'sp_BlitzWho' as procedurename
union
select 'sp_DatabaseRestore' as procedurename
union
select 'sp_foreachdb' as procedurename
union
select 'sp_ineachdb' as procedurename

--End Variables

if (@alldatabases = 0)
    begin

        select @sql += N'DROP PROCEDURE dbo.' + d.procedurename + ';' + CHAR(10)
        from sys.procedures p
                 join #todelete d on d.procedurename = p.name;

    end
else
    begin

        declare @dbname sysname;
        declare @innersql nvarchar(max);

        declare c cursor local fast_forward
            for select QUOTENAME([name])
                from sys.databases
                where [state] = 0;

        open c;

        fetch next from c into @dbname;

        while(@@FETCH_STATUS = 0)
            begin

                set @innersql = N'    SELECT @SQL += N''USE  ' + @dbname + N';' + NCHAR(10) + N'DROP PROCEDURE dbo.'' + D.ProcedureName + '';'' + NCHAR(10)
        FROM ' + @dbname + N'.sys.procedures P
        JOIN #ToDelete D ON D.ProcedureName = P.name COLLATE DATABASE_DEFAULT';

                exec sp_executesql @innersql, N'@SQL nvarchar(max) OUTPUT', @sql = @sql output;

                fetch next from c into @dbname;

            end

        close c;
        deallocate c;

    end

print @sql;

if (@printonly = 0)
    exec sp_executesql @sql
