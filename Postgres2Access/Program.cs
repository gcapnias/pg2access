using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Postgres2Access
{
    class Program
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        private static DbConnectionStringBuilder cnStringyBuilder = null;
        private static Options options = null;

        static void Main(string[] args)
        {
            log4net.Config.XmlConfigurator.Configure();
            _log.Info("Application is starting...");

            options = new Options();
            var result = Parser.Default.ParseArguments<Options>(args).WithParsed(o => { options = o; });

            if (options.Tablenames != null && options.Tablenames.Count() > 0)
            {
                foreach (string tablename in options.Tablenames)
                {
                    ProcessTable(tablename);
                }
            }
            else
            {
                Console.Out.WriteLine();
                Console.Out.WriteLine("Please specify a table name to export...");
            }

            _log.Info("Application has ended!");
        }

        private static void ProcessTable(string tblName)
        {
            if (string.IsNullOrEmpty(tblName))
            {
                return;
            }

            string filename = $"{tblName}.accdb".ToLower().Replace(" ", "_");
            FileInfo fi = new FileInfo(filename);
            if (!File.Exists(fi.FullName))
            {
                File.Delete(fi.FullName);
            }

            DbProviderFactory pf = DbProviderFactories.GetFactory("Npgsql");
            cnStringyBuilder = pf.CreateConnectionStringBuilder();
            cnStringyBuilder.Add("Host", options.Host);
            cnStringyBuilder.Add("Database", options.Database);
            cnStringyBuilder.Add("Username", options.Username);
            cnStringyBuilder.Add("Password", options.Password);

            DataTable dt = new DataTable(tblName);

            using (DbConnection cn = pf.CreateConnection())
            {
                try
                {
                    cn.ConnectionString = cnStringyBuilder.ConnectionString;

                    using (DbCommand cmd = cn.CreateCommand())
                    {
                        cmd.CommandText = $"SELECT * FROM {tblName};";
                        cmd.CommandType = CommandType.Text;

                        cn.Open();
                        dt.Load(cmd.ExecuteReader());
                        cn.Close();
                    }
                }
                catch (Exception ex)
                {
                    _log.Fatal(ex);

                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.Error.WriteLine(ex.Message);
                    Console.Error.WriteLine();
                    Console.ForegroundColor = ConsoleColor.White;

                    return;
                }
            }

            Console.WriteLine($"Exporting table '{tblName}'...");

            AccessExport ae = new AccessExport();
            ae.ExportDataTable(dt, tblName, filename);

        }


    }

    public class Options
    {
        [Value(0, Min = 1, Required = true, HelpText = "Database tables to export.")]
        public IEnumerable<string> Tablenames { get; set; }

        [Option('v', "verbose", Required = false, HelpText = "Set output to verbose messages.")]
        public bool Verbose { get; set; }

        [Option('h', "host", Required = false, Default = "localhost", HelpText = "database name")]
        public string Host { get; set; }

        [Option('d', "database", Required = false, Default = "postgres", HelpText = "database name")]
        public string Database { get; set; }

        [Option('u', "username", Required = false, Default = "postgres", HelpText = "database name")]
        public string Username { get; set; }

        [Option('p', "password", Required = false, Default = "postgres", HelpText = "database name")]
        public string Password { get; set; }

        [Usage(ApplicationAlias = "PostgreSQL2Access")]
        public static IEnumerable<Example> Examples
        {
            get
            {
                return new List<Example>() {
                    new Example("Export tables", new Options {
                        Tablenames = new string[] {"ota_2001_2011", "ota_2012", "ota_2013", "ota_2014" }
                    }),
                    new Example("Export tables", new Options {
                        Database = "ota",
                        Tablenames = new string[] { "ota_2011" }
                    }),
                };
            }
        }
    }

}
