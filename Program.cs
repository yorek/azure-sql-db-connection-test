using System;
using System.Threading;
using System.Collections.Generic;
using System.Timers;
using Microsoft.Data.SqlClient;
using DotNetEnv;

namespace Azure.SQLDB.Samples.Connection
{
    class Program
    {
        static volatile string detectedSLO = "N/A";
        static volatile string databaseName = "N/A";
        static void Main(string[] args)
        {
            Env.Load();

            Console.WriteLine("Setting up retry logic...");
            var options = new SqlRetryLogicOption()
            {
                NumberOfTries = 5,
                DeltaTime = TimeSpan.FromSeconds(10),
                MaxTimeInterval = TimeSpan.FromSeconds(20),
                TransientErrors = new List<int>() {0, 35, 64}                        
            };

            var provider = SqlConfigurableRetryFactory.CreateExponentialRetryProvider(options);
            provider.Retrying += (_, e) => {
                databaseName = "N/A";
                detectedSLO = "N/A";
                Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] Retrying...");
                foreach(var ex in e.Exceptions)
                {
                    Console.WriteLine(ex.Message);
                }
            };

            Console.WriteLine("Setting up monitor...");
            var t = new System.Timers.Timer(1000);
            t.Elapsed += (_, e) => {
                Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}] DB: {databaseName} - SLO: {detectedSLO}");
            };
            t.Start();

            Console.WriteLine("Creating connection...");
            var connectionString = Environment.GetEnvironmentVariable("AZURE_CONNECTION_STRING");
            var csb = new SqlConnectionStringBuilder(connectionString);
            var conn = new SqlConnection(csb.ConnectionString);
            databaseName = $"{csb.DataSource}@{csb.InitialCatalog}";
            conn.RetryLogicProvider = provider;
            Console.WriteLine($"Connection timeout: {conn.ConnectionTimeout}");

            Console.WriteLine("Starting loop. (CTRL+C to end)");
            var cmd = new SqlCommand("select databasepropertyex(db_name(), 'ServiceObjective' ) as SLO ", conn);
            cmd.RetryLogicProvider = provider;
            while(true)
            {
                conn.Open();
                detectedSLO = (string)cmd.ExecuteScalar();
                conn.Close();
                
                Thread.Sleep(50);
            }           
        }
    }
}
