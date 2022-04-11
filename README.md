# Test Connection Retry Logic

## Run Test

Create a `.env` file starting from the provided `.env.template` and specify a connection string to a database that will be used to test the connection resiliency.

**WARNING**: The selected database will go through a cycle of SLO change. The SLO change will happen every 3 minutes, and will move the database from `GP_Gen5_2` to `GP_Gen5_4` back and forth.

To run the test just run `dotnet run` passing the name of the test to be run:

- `noretry`: test without retry logic, using only the "Idle Connection" retry logic. Ref: [.NET SqlConnection parameters for connection retry](https://docs.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-common-connectivity-issues#net-sqlconnection-parameters-for-connection-retry)
- `good`: run the test using code adopting best pratices 
- `bad`: run the test using code not follwing best pratices

for example:

```
dotnet run -- good
```

## Notes

Then test program will execute the following query in an infinite loop:

```sql
declare @result sysname; 
select @result = cast(databasepropertyex(db_name(), 'ServiceObjective' ) as sysname); 
waitfor delay '00:00:01.000'; 
select @result as SLO;

```
and will report *every minute* information on average execution time (`EA`) and how many executions (`EC`) where done in a minute.

The test program has a generic exception handler that will catch **all** unhandled exception, and will simply log them, then the loop will restart.

A SLO change will be issued every 3 minutes.

