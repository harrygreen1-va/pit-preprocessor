# PIT ETL preprocessor and postprocessor

Build:

```bash
mvn clean package assembly:single -Dtest=PlainTestSuite
```

Unit tests:

```bash
mvn -Dtest=PlainTestSuite test
```

## How to configure SQL server to get additional truncation info

* Install the latest CU
* DBCC TRACEON( 460, -1)

What is the version?
select @@VERSION

https://blogs.msdn.microsoft.com/sql_server_team/string-or-binary-data-would-be-truncated-replacing-the-infamous-error-8152/
https://support.microsoft.com/en-us/help/3177312/sql-server-2016-build-versions
https://sqlserverbuilds.blogspot.com/
https://www.sqlservercentral.com/articles/enable-trace-flags-in-sql-server