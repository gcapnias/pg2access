using CommandLine;
using CommandLine.Text;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Access2PostgreSQL
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

            if (options.DbFilenames != null && options.DbFilenames.Count() > 0)
            {
                foreach (string filename in options.DbFilenames)
                {
                    ProcessTable(filename);
                }
            }
            else
            {
                Console.Out.WriteLine();
                Console.Out.WriteLine("Please specify an MS Access database to import...");
            }

            _log.Info("Application has ended!");
        }

        private static void ProcessTable(string filename)
        {
            if (string.IsNullOrEmpty(filename))
            {
                return;
            }

            FileInfo fi = new FileInfo(filename);
            if (!File.Exists(fi.FullName))
            {
                return;
            }

            string tblName = fi.Name.Replace(fi.Extension, "").ToLower().Replace(" ", "_");
            DbProviderFactory pf = DbProviderFactories.GetFactory("System.Data.OleDb");
            DataTable dt = new DataTable(tblName);
            using (DbConnection cn = pf.CreateConnection())
            {
                try
                {
                    cn.ConnectionString = BuildConnectionString(fi.FullName);

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

            Console.WriteLine($"Importing table '{tblName}'...");

            PostgreSQLExport pe = new PostgreSQLExport();
            pe.ExportDataTable(dt, tblName, options);
        }

        private static string BuildConnectionString(string path)
        {
            string result = null;

            int providerIndex = 0;
            using (OleDbDataReader reader = OleDbEnumerator.GetRootEnumerator())
            {
                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        if (reader.GetName(i).Equals("SOURCES_NAME"))
                        {
                            string provider = reader.GetString(i);
                            _log.Debug($"Provider: {provider}");

                            switch (provider)
                            {
                                case "Microsoft.Jet.OLEDB.4.0":
                                    if (providerIndex < 1)
                                    {
                                        result = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};Persist Security Info=False", path);
                                        providerIndex = 1;
                                    }
                                    break;

                                case "Microsoft.ACE.OLEDB.12.0":
                                    if (providerIndex < 2)
                                    {
                                        result = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Persist Security Info=False", path);
                                        providerIndex = 2;
                                    }
                                    break;

                                case "Microsoft.ACE.OLEDB.14.0":
                                    if (providerIndex < 3)
                                    {
                                        result = string.Format("Provider=Microsoft.ACE.OLEDB.14.0;Data Source={0};Persist Security Info=False", path);
                                        providerIndex = 3;
                                    }
                                    break;

                                case "Microsoft.ACE.OLEDB.15.0":
                                    if (providerIndex < 4)
                                    {
                                        result = string.Format("Provider=Microsoft.ACE.OLEDB.15.0;Data Source={0};Persist Security Info=False", path);
                                        providerIndex = 4;
                                    }
                                    break;

                                case "Microsoft.ACE.OLEDB.16.0":
                                    if (providerIndex < 5)
                                    {
                                        result = string.Format("Provider=Microsoft.ACE.OLEDB.16.0;Data Source={0};Persist Security Info=False", path);
                                        providerIndex = 5;
                                    }
                                    break;

                                default:
                                    break;
                            }
                        }
                    }
                }

            }

            FileInfo fi = new FileInfo(path);
            if (fi.Extension.ToLower().Equals(".accdb") && providerIndex < 2)
            {
                throw new InvalidConstraintException("No suitable Access Engine installed in workstation.");
            }

            return result;
        }

    }

    public class Options
    {
        [Value(0, Min = 1, Required = true, HelpText = "MS Access database to import.")]
        public IEnumerable<string> DbFilenames { get; set; }

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

        [Usage(ApplicationAlias = "Access2PostgreSQL")]
        public static IEnumerable<Example> Examples
        {
            get
            {
                return new List<Example>() {
                    new Example("Import tables", new Options {
                        DbFilenames = new string[] {"ota_2001_2011.accdb", "ota_2012.accdb", "ota_2013.accdb", "ota_2014.accdb" }
                    }),
                    new Example("Import tables", new Options {
                        Database = "ota",
                        DbFilenames = new string[] { "ota_2011.accdb" }
                    }),
                };
            }
        }
    }

}
