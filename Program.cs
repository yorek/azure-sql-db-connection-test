using System;
using System.Threading;
using System.Collections.Generic;
using System.Timers;
using Microsoft.Data.SqlClient;
using DotNetEnv;
using System.Diagnostics;
using Dapper;
using Polly;
using System.ComponentModel;

namespace Azure.SQLDB.Samples.Connection
{
    class Program
    {
        static volatile string requestedSLO = "N/A";
        static volatile string detectedSLO = "N/A";
        static int querySleepTime = 50; //Msec
        static readonly string query = $@"
            declare @result sysname; 
            select @result = cast(databasepropertyex(db_name(), 'ServiceObjective' ) as sysname) + '@' + @@servername; 
            waitfor delay '00:00:00.050'; 
            select @result as SLO;
        ";

        static void Log(string message, bool newLine = true)
        {
            StackTrace stackTrace = new StackTrace();
            var caller = stackTrace.GetFrame(1).GetMethod().Name;
            if (caller.Contains("Log")) caller = stackTrace.GetFrame(2).GetMethod().Name;
            var logMessage = $"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] {caller} > {message}";
            if (newLine)
                Console.WriteLine(logMessage);
            else
                Console.Write(logMessage);
        }

        static void Main(string[] args)
        {
            Log("Starting...");
            Env.Load();

            if (args.GetLength(0) != 1)
            {
                Console.WriteLine("Please specify which test you want to run");
                Console.WriteLine("eg: dotnet run -- [noretry|native|polly]");
                return;
            }

            // And then request a SLO change every "x" seconds
            //SLOChangerTimer(2 * 60).Start();

            // Request a SLO change right now
            //SLOChange();

            Log("Creating connection...");
            var connectionString = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");
            var csb = new SqlConnectionStringBuilder(connectionString);
    
            Log($"Connection timeout: {csb.ConnectTimeout}");
            Log($"Connection retry interval: {csb.ConnectRetryInterval}");
            Log($"Connection retry count: {csb.ConnectRetryCount}");

            Log($"Running: {args[0]}");
            Log("Starting loop. (CTRL+C to end)");

            if (args[0] == "noretry") TestNoRetryLogic(csb);
            else if (args[0] == "native") TestNative(csb);
            else if (args[0] == "polly") TestPolly(csb);
            else Log("Unknown option. Terminating.");
        }

        private static void SLOChange()
        {
            var cs = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");
            var cnn = new SqlConnection(cs);
            try
            {
                Log($"Starting SLO change...");
                cnn.Open();
                var slo = cnn.ExecuteScalar<string>("select databasepropertyex(db_name(), 'ServiceObjective' ) as SLO");
                var newSLO = slo.ToLower() == "gp_gen5_2" ? "GP_Gen5_4" : "GP_Gen5_2";
                Log($"Changing from {slo} to {newSLO}...");
                cnn.Execute($"alter database [{cnn.Database}] modify (service_objective = '{newSLO}')");
                Log($"Request sent.");
                requestedSLO = newSLO;
            }
            catch (Exception ex)
            {
                LogExceptions("WARNING!!! Unhandled Exception trapped while changing SLO", ex);
            }
            finally
            {
                cnn.Close();
            }
        }

        private static System.Timers.Timer SLOChangerTimer(int secs)
        {
            Log("Setting up SLO changer...");
            Log($"SLO changer will change SLO every {secs} seconds");
            var t = new System.Timers.Timer(secs * 1000);
            t.Elapsed += (_, e) => SLOChange();

            return t;
        }

        static void TestNoRetryLogic(SqlConnectionStringBuilder csb)
        {
            while (true)
            {
                try
                {
                    using (var conn = new SqlConnection(csb.ConnectionString))
                    {
                        var cmd = new SqlCommand(query, conn);
                        conn.Open();
                        detectedSLO = (string)cmd.ExecuteScalar();
                        conn.Close();
                        Log($"Detected SLO: {detectedSLO}. Requested SLO: {requestedSLO}");
                    }
                }
                catch (Exception ex)
                {
                    LogExceptions("WARNING!!! Unhandled Exception trapped while running test", ex);
                }

                Thread.Sleep(querySleepTime);
            }
        }

        static void TestNative(SqlConnectionStringBuilder csb)
        {
            var options = new SqlRetryLogicOption()
            {
                NumberOfTries = 5,
                DeltaTime = TimeSpan.FromSeconds(5),
                MaxTimeInterval = TimeSpan.FromSeconds(60),
                TransientErrors = new int[] { 0, 64, 40615 }
            };

            EventHandler<SqlRetryingEventArgs> retryEvent = (object s, SqlRetryingEventArgs e) =>
            {
                detectedSLO = "N/A";

                foreach (var ex in e.Exceptions)
                {
                    Log($"Retrying called due to {ex.GetType().Name} - {(ex as SqlException).Number} - {ex.Message}");
                }

                Log($"Retrying (Retry Count: {e.RetryCount + 1}, Retry Delay: {e.Delay})... ");
            };

            var cnnProvider = SqlConfigurableRetryFactory.CreateFixedRetryProvider(options);
            cnnProvider.Retrying += retryEvent;

            var cmdProvider = SqlConfigurableRetryFactory.CreateFixedRetryProvider(options);
            cmdProvider.Retrying += retryEvent;

            while (true)
            {
                try
                {
                    using (var cnn = new SqlConnection(csb.ConnectionString))
                    {
                        var cmd = new SqlCommand(query, cnn);
                        cnn.RetryLogicProvider = cnnProvider;
                        cnn.Open();
                        detectedSLO = cmdProvider.Execute<string>(null, () =>
                        {
                            detectedSLO = (string)cmd.ExecuteScalar();
                            return detectedSLO;
                        });
                        cnn.Close();
                        Log($"Detected SLO: {detectedSLO}. Requested SLO: {requestedSLO}");
                    }
                }
                catch (Exception ex)
                {
                    LogExceptions("WARNING!!! Unhandled Exception trapped while running test", ex);
                }

                Thread.Sleep(querySleepTime);
            }
        }

        static void TestPolly(SqlConnectionStringBuilder csb)
        {
            var p = Policy
                .Handle<SqlException>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                .Or<TimeoutException>()
                .OrInner<Win32Exception>(SqlServerTransientExceptionDetector.ShouldRetryOn)
                .WaitAndRetry(5, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)),
                    (exception, timeSpan, context) =>
                    {
                        LogExceptions("Retry called due to SqlException", exception);
                        Log($"(Retry count: {context.Count + 1}), Retry delay was: {timeSpan.TotalSeconds} secs ");
                    });

            var database = "N/A";
            var server = "N/A";
            var sw = new Stopwatch();            
            while (true)            
            {
                sw.Start();
                try
                {
                    p.Execute(() =>
                    {
                        using (var conn = new SqlConnection(csb.ConnectionString))
                        {                            
                            var cmd = new SqlCommand(query, conn);                            
                            conn.Open();
                            var result = (string)cmd.ExecuteScalar();
                            var resultItems = result.Split('@');
                            detectedSLO = resultItems[0];
                            server = resultItems[1];
                            database = conn.Database;
                            conn.Close();                            
                        }
                    });
                }
                catch (Exception ex)
                {
                    LogExceptions("WARNING!!! Unhandled Exception trapped in the execution loop", ex);
                }
                finally {
                    sw.Stop();                    
                    Log($"{database}@{server}, SLO: {detectedSLO}, Query Execution Time: {sw.ElapsedMilliseconds}");
                    sw.Reset();
                }

                Thread.Sleep(querySleepTime);
            }
        }

        static void LogExceptions(string message, Exception ex)
        {
            Log(message);
            var cx = ex;
            string prefix = "";
            while (cx != null)
            {
                if (cx is SqlException)
                {
                    var sx = cx as SqlException;
                    Log($"[{prefix}{sx.GetType().Name}] Number: {sx.Number}, Code: {sx.ErrorCode}, Message: {sx.Message}");
                }
                else
                {
                    Log($"[{prefix}{cx.GetType().Name}] " + cx.Message);
                }
                cx = cx.InnerException;
                if (cx != null) prefix += "-";
            }
        }
    }

    public static class SqlServerTransientExceptionDetector
    {
        // Error list created from: 
        // - https://github.com/dotnet/efcore/blob/main/src/EFCore.SqlServer/Storage/Internal/SqlServerTransientExceptionDetector.cs
        // - https://docs.microsoft.com/en-us/dotnet/api/microsoft.data.sqlclient.sqlconfigurableretryfactory?view=sqlclient-dotnet-standard-4.1
        // Manually added also
        // 0, 18456
        private static List<int> transientErrorNumbers = new List<int> { 
            233, 997, 921, 669, 617, 601, 121, 64, 20, 0,
            1203, 1204, 1205, 1222, 1221,
            1807,
            3966, 3960, 3935,
            4060, 4221, 
            8651, 8645,
            9515,
            14355,            
            10929, 10928, 10060, 10054, 10053, 10936, 10929, 10928, 10922, 
            17197,
            18456,
            20041,
            41839, 41325, 41305, 41302, 41301, 40143, 40613, 40501, 40540, 40197, 49918, 49919, 49920 
        };

        public static bool ShouldRetryOn(Exception ex)
        {
            if (ex is SqlException sqlException)
            {
                foreach (SqlError err in sqlException.Errors)
                {
                    if (transientErrorNumbers.Contains(err.Number)) return true;
                }

                return false;
            }

            return ex is TimeoutException;
        }
    }
}
