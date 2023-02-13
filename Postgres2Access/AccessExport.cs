using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OleDb;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Postgres2Access
{
    public class AccessExport
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static int tableRecordCount = 0;
        static int rowCount = 0;

        public AccessExport()
        {
        }

        public void ExportDataTable(DataTable table, string tblName, string filename)
        {
            try
            {
                SaveEmptyDb(filename);

                DbProviderFactory f = DbProviderFactories.GetFactory("System.Data.OleDb");
                using (DbConnection cn = f.CreateConnection())
                {
                    string connectionString = BuildConnectionString(filename);
                    cn.ConnectionString = connectionString;

                    DbCommand cmd = cn.CreateCommand();
                    cmd.CommandText = GetTableDdl(table, tblName);
                    cmd.CommandType = CommandType.Text;

                    cn.Open();
                    cmd.ExecuteNonQuery();
                    cn.Close();

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    int currentRow = 0;

                    #region ' Using Datatable '

                    DbCommand selectCommand = cn.CreateCommand();
                    selectCommand.CommandText = $"SELECT * FROM [{tblName}]";
                    selectCommand.CommandType = CommandType.Text;

                    DbDataAdapter dataAdapter = f.CreateDataAdapter();
                    dataAdapter.SelectCommand = selectCommand;
                    dataAdapter.AddRowUpdatedHandler((sender, e) =>
                    {
                        rowCount = rowCount + e.RowCount;

                        int percent = rowCount * 100 / tableRecordCount;
                        ProgressBarUtility.WriteProgressBar(percent, true);
                    });

                    DbCommandBuilder commandBuilder = f.CreateCommandBuilder();
                    commandBuilder.DataAdapter = dataAdapter;
                    commandBuilder.QuotePrefix = "[";
                    commandBuilder.QuoteSuffix = "]";

                    DbCommand insertCommand = commandBuilder.GetInsertCommand(true);
                    insertCommand.Connection = cn;
                    insertCommand.UpdatedRowSource = UpdateRowSource.None;
                    dataAdapter.InsertCommand = insertCommand;

                    DataTable dt = new DataTable(tblName);
                    dataAdapter.Fill(dt);

                    Console.Write("Reading source database: ");
                    ProgressBarUtility.WriteProgressBar(0, false);
                    foreach (DataRow row in table.Rows)
                    {
                        dt.Rows.Add(row.ItemArray);
                        currentRow++;

                        int percent = currentRow * 100 / table.Rows.Count;
                        ProgressBarUtility.WriteProgressBar(percent, true);
                    }
                    Console.WriteLine();

                    rowCount = 0;
                    tableRecordCount = table.Rows.Count;
                    Console.Write("Exporting to database  : ");
                    ProgressBarUtility.WriteProgressBar(0, false);
                    dataAdapter.Update(dt);

                    #endregion

                    sw.Stop();
                    Console.WriteLine();
                    Console.WriteLine(string.Format("{0} rows processed in {1:mm\\:ss\\:fff}.", currentRow + 1, sw.Elapsed));
                }
            }
            catch (Exception ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.Out.WriteLine(ex.Message);
                Console.ForegroundColor = ConsoleColor.White;
                Console.Out.WriteLine();
            }
        }

        private string GetTableDdl(DataTable table, string tblName)
        {
            StringBuilder sb = new StringBuilder();
            Dictionary<string, int> fieldLengths = new Dictionary<string, int>();

            string IdColumn = null;
            bool has_autoincrement = table.Columns.Cast<DataColumn>().Where(c => c.AutoIncrement == true).Count() > 0;
            if (!has_autoincrement)
            {
                string[] columnNames = table.Columns.Cast<DataColumn>()
                    .Where(c => c.ColumnName.ToLower().Equals("id"))
                    .Select(c => c.ColumnName)
                    .ToArray();

                if (columnNames.Length > 0)
                {
                    IdColumn = columnNames[0];
                }
                else
                {
                    IdColumn = table.Columns.Cast<DataColumn>()
                        .Where(c => c.ColumnName.ToLower().EndsWith("id", StringComparison.InvariantCultureIgnoreCase))
                        .OrderBy(c => c.Ordinal)
                        .Select(c => c.ColumnName)
                        .FirstOrDefault();
                }
            }

            sb.AppendLine($"CREATE TABLE [{tblName}] (");
            for (int i = 0; i < table.Columns.Count; i++)
            {
                string sanitizedName = Regex.Replace(table.Columns[i].ColumnName, @"[\.]", string.Empty);

                sb.Append("[" + sanitizedName + "] ");

                if (table.Columns[i].AutoIncrement || table.Columns[i].ColumnName.Equals(IdColumn, StringComparison.InvariantCultureIgnoreCase))
                {
                    sb.Append("AUTOINCREMENT PRIMARY KEY");
                }
                else
                {
                    string columnType = table.Columns[i].DataType.ToString();
                    switch (columnType)
                    {
                        case "System.Boolean":
                            sb.Append("BIT");
                            break;

                        case "System.Byte":
                            sb.Append("UNSIGNED BYTE");
                            break;

                        case "System.Int16":
                            sb.Append("SHORT");
                            break;

                        case "System.Int32":
                            sb.Append("LONG");
                            break;

                        case "System.Int64":
                            sb.Append("CURRENCY");
                            break;

                        case "System.Decimal":
                            sb.Append("DECIMAL");
                            break;

                        case "System.DateTime":
                            sb.Append("DATETIME");
                            break;

                        case "System.Single":
                            sb.Append("SINGLE");
                            break;

                        case "System.Double":
                            sb.Append("DOUBLE");
                            break;

                        case "System.String":
                        default:
                            int maxLength = table.Columns[i].MaxLength < 0 ? table.AsEnumerable().Select(r => r.Field<string>(i)?.Length ?? 0).Max() : table.Columns[i].MaxLength;
                            sb.Append(table.Columns[i].MaxLength == 0 || maxLength <= 255 ? "VARCHAR" : "LONGTEXT");

                            break;
                    }

                    if (!table.Columns[i].AllowDBNull)
                    {
                        sb.Append(" NOT NULL");
                    }
                }

                if (i < table.Columns.Count - 1)
                {
                    sb.AppendLine(",");
                }
            }

            sb.AppendLine(")");

            return sb.ToString();
        }

        private bool SaveEmptyDb(string filename)
        {
            bool result = false;

            FileInfo fi = new FileInfo(filename);
            Assembly assembly = Assembly.GetExecutingAssembly();
            Stream resource = null;

            switch (fi.Extension.ToLower())
            {
                case ".mdb":
                    resource = assembly.GetManifestResourceStream("Postgres2Access.Resources.Empty2003.mdb");
                    break;

                case ".accdb":
                    resource = assembly.GetManifestResourceStream("Postgres2Access.Resources.Empty2007.accdb");
                    break;

                default:
                    resource = null;
                    break;
            }

            if (resource != null)
            {
                using (var fileStream = new FileStream(filename, FileMode.Create, FileAccess.Write))
                {
                    resource.CopyTo(fileStream);

                    result = true;
                }
            }

            return result;
        }

        private string GetSqlInsert(DataTable table)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append($"INSERT INTO [{table}] (");

            for (int i = 0; i < table.Columns.Count; i++)
            {
                sb.Append(string.Format("[{0}]", table.Columns[i].ColumnName));

                if (i < table.Columns.Count - 1)
                {
                    sb.Append(",");
                }
            }

            sb.Append(") VALUES (");

            for (int i = 0; i < table.Columns.Count; i++)
            {
                //string sanitizedName = Regex.Replace(table.Columns[i].ColumnName, @"[\.]", string.Empty);

                //string columnType = table.Columns[i].DataType.ToString();
                //switch (columnType)
                //{
                //    case "System.DateTime":
                //    case "System.String":
                //        sb.AppendFormat("'#{0}#'", i);
                //        break;

                //    default:
                //        sb.AppendFormat("#{0}#", i);
                //        break;
                //}

                sb.AppendFormat("#{0}#", i);

                if (i < table.Columns.Count - 1)
                {
                    sb.Append(",");
                }
            }

            sb.Append(");");

            return sb.ToString();
        }

        private string GetSqlInsertInto(DataTable table)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append($"INSERT INTO [{table}] (");

            for (int i = 0; i < table.Columns.Count; i++)
            {
                sb.Append(string.Format("[{0}]", table.Columns[i].ColumnName));

                if (i < table.Columns.Count - 1)
                {
                    sb.Append(", ");
                }
            }

            sb.Append(") ");

            return sb.ToString();
        }

        private string GetSqlSelect(DataTable table)
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("SELECT TOP 1 ");

            for (int i = 0; i < table.Columns.Count; i++)
            {
                sb.AppendFormat("#{0}#", i);
                sb.Append(string.Format(" AS [{0}]", table.Columns[i].ColumnName));

                if (i < table.Columns.Count - 1)
                {
                    sb.Append(", ");
                }
            }

            sb.Append($" FROM [{table}]");

            return sb.ToString();
        }

        private string FillSqlCommand(DataRow row, string sqlCommand)
        {
            CultureInfo enUS = new CultureInfo("en-US");
            StringBuilder template = new StringBuilder(sqlCommand);
            int counter = 0;
            Dictionary<string, string> valuesDictionary = new Dictionary<string, string>();

            foreach (object item in row.ItemArray)
            {
                string columnType = item.GetType().ToString();
                switch (columnType)
                {
                    case "System.DateTime":
                        valuesDictionary.Add(string.Format("#{0}#", counter++), item == null ? "NULL" : string.Format("#{0:yyyy-MM-dd HH:mm:ss}#", ((DateTime)item)));
                        break;

                    case "System.DBNull":
                        string dataType = row.Table.Columns[counter].DataType.ToString();
                        bool allowDBNull = row.Table.Columns[counter].AllowDBNull;
                        switch (dataType)
                        {
                            case "System.String":
                                valuesDictionary.Add(string.Format("#{0}#", counter++), allowDBNull ? "NULL" : "\"\"");
                                break;

                            case "System.Int32":
                                valuesDictionary.Add(string.Format("#{0}#", counter++), "0");
                                break;

                            default:
                                valuesDictionary.Add(string.Format("#{0}#", counter++), "NULL");
                                break;
                        }
                        break;

                    case "System.Int32":
                        valuesDictionary.Add(string.Format("#{0}#", counter++), item == null ? "NULL" : string.Format(enUS, "{0:0}", item));
                        break;

                    case "System.Double":
                        valuesDictionary.Add(string.Format("#{0}#", counter++), item == null ? "NULL" : string.Format(enUS, "{0:0.0}", item));
                        break;

                    case "System.String":
                        string value = item.ToString();
                        value = value.ToString().Replace("\"", "\"\"");
                        valuesDictionary.Add(string.Format("#{0}#", counter++), string.IsNullOrEmpty(item.ToString().Trim()) ? "\"\"" : string.Format("\"{0}\"", value));
                        break;

                    default:
                        valuesDictionary.Add(string.Format("#{0}#", counter++), item == null ? "NULL" : string.Format(enUS, "{0}", item));
                        break;
                }
            }

            foreach (string valueKey in valuesDictionary.Keys)
            {
                template = template.Replace(valueKey, valuesDictionary[valueKey]);
            }

            return template.ToString();
        }

        public event EventHandler<EventArgs> RowProccesed = delegate { };
        protected virtual void OnRowProccesed(EventArgs e)
        {
            RowProccesed(this, e);
        }

        public static string BuildConnectionString(string path)
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

    public static class ProgressBarUtility
    {
        static int last_percent;
        const char _block = '█';
        const string _twirl = "-\\|/";
        const int _size = 32;

        public static string backspaces
        {
            get { return new string('\b', _size + 7); }
        }

        public static void WriteProgressBar(int percent, bool update = false)
        {
            if (last_percent == percent && percent > 0)
            {
                return;
            }

            last_percent = percent;

            if (update)
            {
                Console.Write(backspaces);
            }
            Console.Write("[");

            var p = (int)((percent * _size / 100f) + .5f);
            for (var i = 0; i < _size; ++i)
            {
                char c = i >= p ? ' ' : _block;
                Console.Write(c);
            }
            Console.Write("] {0,3:##0}%", percent);
        }

        public static void WriteProgress(int progress, bool update = false)
        {
            if (update)
            {
                Console.Write("\b");
            }
            Console.Write(_twirl[progress % _twirl.Length]);
        }
    }

}
