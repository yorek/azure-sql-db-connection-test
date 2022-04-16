using System;
using System.Threading;
using System.Collections.Generic;
using System.Timers;
using Microsoft.Data.SqlClient;
using DotNetEnv;
using System.Diagnostics;
using Dapper;
using Polly;

namespace Azure.SQLDB.Samples.Connection
{
    class Program
    {
        static volatile string detectedSLO = "N/A";
        static volatile string databaseName = "N/A";
        static volatile int executionTime = 0;
        static volatile int executionCount = 0;

        static readonly string query = "declare @result sysname; select @result = cast(databasepropertyex(db_name(), 'ServiceObjective' ) as sysname); waitfor delay '00:00:01.000'; select @result as SLO;";

        static void Log(string message)
        {
            StackTrace stackTrace = new StackTrace();
            var caller = stackTrace.GetFrame(1).GetMethod().Name;
            if (caller.Contains("Log")) caller = stackTrace.GetFrame(2).GetMethod().Name;
            Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] {caller} > {message}");
        }

        static void Main(string[] args)
        {
            Log("Starting...");
            Env.Load();

            if (args.GetLength(0) != 1)
            {
                Console.WriteLine("Please specify which test you want to run");
                Console.WriteLine("eg: dotnet run -- [noretry|good|bad]");
                return;
            }

            Monitor(60).Start();

            SLOChanger(3 * 60).Start();

            Log("Creating connection...");
            var connectionString = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");
            var csb = new SqlConnectionStringBuilder(connectionString);
            databaseName = $"{csb.DataSource}@{csb.InitialCatalog}";
            Log($"Connection timeout: {csb.ConnectTimeout}");
            Log($"Connection retry interval: {csb.ConnectRetryInterval}");
            Log($"Connection retry count: {csb.ConnectRetryCount}");

            Log($"Running: {args[0]}");
            Log("Starting loop. (CTRL+C to end)");

            if (args[0] == "noretry") TestNoRetryLogic(csb);
            else if (args[0] == "bad") TestBadCode(csb);
            else if (args[0] == "good") TestGoodCode(csb);
            else if (args[0] == "polly") TestPolly(csb);
            else Log("Unknown option. Terminating.");
        }

        private static System.Timers.Timer SLOChanger(int secs)
        {
            Log("Setting up SLO changer...");
            Log($"SLO changer will change SLO every {secs} seconds");
            var t = new System.Timers.Timer(secs * 1000);
            t.Elapsed += (_, e) =>
            {
                var cs = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");
                var cnn = new SqlConnection(cs);
                try
                {
                    Log($"Starting SLO change...");
                    cnn.Open();
                    var slo = cnn.ExecuteScalar<string>("select databasepropertyex(db_name(), 'ServiceObjective' ) as SLO");
                    var newSlo = slo.ToLower() == "gp_gen5_2" ? "GP_Gen5_4" : "GP_Gen5_2";
                    Log($"Changing from {slo} to {newSlo}...");
                    cnn.Execute($"alter database [{cnn.Database}] modify (service_objective = '{newSlo}')");
                    Log($"Request sent.");
                }
                catch (Exception ex)
                {
                    LogExceptions("Exception while changing SLO", ex);
                }
                finally
                {
                    cnn.Close();
                }
            };

            return t;
        }

        static System.Timers.Timer Monitor(int secs)
        {
            Log("Setting up monitor...");
            Log($"Monitor will report every {secs} seconds");
            var t = new System.Timers.Timer(secs * 1000);
            t.Elapsed += (_, e) =>
            {
                int ec = Interlocked.Exchange(ref executionCount, 0);
                int et = Interlocked.Exchange(ref executionTime, 0);
                double ea = (double)et / (double)ec;
                Log($"DB: {databaseName} - SLO: {detectedSLO} - EA: {ea:000.000} - EC: {ec}");
            };

            return t;
        }

        static void TestNoRetryLogic(SqlConnectionStringBuilder csb)
        {
            int waitMsec = 50;

            Stopwatch sw = new Stopwatch();
            while (true)
            {
                try
                {
                    using (var conn = new SqlConnection(csb.ConnectionString))
                    {
                        var cmd = new SqlCommand(query, conn);
                        sw.Start();
                        conn.Open();
                        //Thread.Sleep(waitMsec);
                        detectedSLO = (string)cmd.ExecuteScalar();
                        sw.Stop();
                        Interlocked.Add(ref executionCount, 1);
                        Interlocked.Add(ref executionTime, (int)sw.ElapsedMilliseconds);
                        sw.Reset();
                    }
                }
                catch (Exception ex)
                {
                    LogExceptions("Exception trapped while running test", ex);
                }

                Thread.Sleep(waitMsec);
            }
        }

        static void TestGoodCode(SqlConnectionStringBuilder csb)
        {
            int waitMsec = 50;

            var options = new SqlRetryLogicOption()
            {
                NumberOfTries = 5,
                DeltaTime = TimeSpan.FromSeconds(5),
                MaxTimeInterval = TimeSpan.FromSeconds(60),
                TransientErrors = new int[] {0,64,40615}
            };

            var cnnProvider = SqlConfigurableRetryFactory.CreateFixedRetryProvider(options);
            var cmdProvider = SqlConfigurableRetryFactory.CreateFixedRetryProvider(options);

            cnnProvider.Retrying += (object s, SqlRetryingEventArgs e) =>
            {
                detectedSLO = "N/A";

                foreach (var ex in e.Exceptions)
                {
                    Log($"Retrying called in connection opening due to error {(ex as SqlException).Number} - {ex.Message}");
                }

                Log($"Retrying (Retry Count: {e.RetryCount}, Retry Delay: {e.Delay})... ");
            };

            cmdProvider.Retrying += (object s, SqlRetryingEventArgs e) =>
            {
                detectedSLO = "N/A";

                foreach (var ex in e.Exceptions)
                {
                    Log($"Retrying called in command execution due to error {(ex as SqlException).Number} - {ex.Message}");
                }

                Log($"Retrying (Retry Count: {e.RetryCount}, Retry Delay: {e.Delay})... ");
            };

            Stopwatch sw = new Stopwatch();
            while (true)
            {
                // try
                // {
                    using (var conn = new SqlConnection(csb.ConnectionString))
                    {
                        conn.RetryLogicProvider = cnnProvider;
                        var cmd = new SqlCommand(query, conn);
                        cmd.RetryLogicProvider = cmdProvider;
                        sw.Start();
                        conn.Open();
                        //Thread.Sleep(waitMsec);
                        detectedSLO = (string)cmd.ExecuteScalar();
                        sw.Stop();
                        Interlocked.Add(ref executionCount, 1);
                        Interlocked.Add(ref executionTime, (int)sw.ElapsedMilliseconds);
                        sw.Reset();
                    }
                // }
                // catch (Exception ex)
                // {
                //     LogExceptions("Exception trapped while running test", ex);
                // }

                Thread.Sleep(waitMsec);
            }
        }

        static void TestBadCode(SqlConnectionStringBuilder csb)
        {
            int waitMsec = 50;

            var options = new SqlRetryLogicOption()
            {
                NumberOfTries = 5,
                DeltaTime = TimeSpan.FromSeconds(5),
                MaxTimeInterval = TimeSpan.FromSeconds(20)
            };

            var provider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(options);
            provider.Retrying += (object s, SqlRetryingEventArgs e) =>
            {
                detectedSLO = "N/A";

                foreach (var ex in e.Exceptions)
                {
                    Log($"Retrying called due to error {(ex as SqlException).Number}");
                }

                Log($"Retrying (Retry Count: {e.RetryCount}, Retry Delay: {e.Delay})... ");
            };

            Stopwatch sw = new Stopwatch();
            using (var conn = new SqlConnection(csb.ConnectionString))
            {
                conn.RetryLogicProvider = provider;
                var cmd = new SqlCommand(query, conn);
                cmd.RetryLogicProvider = provider;
                while (true)
                {
                    try
                    {
                        sw.Start();
                        conn.Open();
                        detectedSLO = (string)cmd.ExecuteScalar();
                        conn.Close();
                        sw.Stop();
                        Interlocked.Add(ref executionCount, 1);
                        Interlocked.Add(ref executionTime, (int)sw.ElapsedMilliseconds);
                        sw.Reset();
                    }
                    catch (Exception ex)
                    {
                        LogExceptions("Exception trapped while running test", ex);
                    }
                    Thread.Sleep(waitMsec);
                }
            }
        }

        
        static void TestPolly(SqlConnectionStringBuilder csb)
        {
            int waitMsec = 50;

            var p = Policy
                .Handle<SqlException>()
                .RetryForever(onRetry: (e, c) =>
                {                    
                    detectedSLO = "N/A";

                    LogExceptions("Retry called due to SqlException", e);

                    Log($"Retrying (Retry Count: {c.Count})... ");                    
                });

            Stopwatch sw = new Stopwatch();
            while (true)
            {
                try
                {
                    p.Execute(() => {
                        using (var conn = new SqlConnection(csb.ConnectionString))
                        {
                            var cmd = new SqlCommand(query, conn);
                            sw.Start();
                            conn.Open();
                            detectedSLO = (string)cmd.ExecuteScalar();
                            sw.Stop();
                            Interlocked.Add(ref executionCount, 1);
                            Interlocked.Add(ref executionTime, (int)sw.ElapsedMilliseconds);
                            sw.Reset();
                        }
                    });
                }
                catch (Exception ex)
                {
                    LogExceptions("Exception trapped while running test", ex);
                }

                Thread.Sleep(waitMsec);
            }
        }
        
        static void LogExceptions(string message, Exception ex)
        {
            Log(message);
            var cx = ex;
            while (cx != null)
            {
                if (cx is SqlException)
                {
                    var sx = cx as SqlException;
                    Log($"Number: {sx.Number}, Message: {sx.Message}");
                }
                else
                {
                    Log(cx.Message);
                }
                cx = cx.InnerException;
            }
        }
    }
}
