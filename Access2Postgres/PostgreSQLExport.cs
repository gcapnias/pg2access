using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Access2PostgreSQL
{
    public class PostgreSQLExport
    {
        private static readonly log4net.ILog _log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        static int tableRecordCount = 0;
        static int rowCount = 0;

        public PostgreSQLExport()
        {

        }

        public void ExportDataTable(DataTable table, string tblName)
        {
            try
            {
                DbProviderFactory f = DbProviderFactories.GetFactory(ConfigurationManager.ConnectionStrings["Default"].ProviderName);
                using (DbConnection cn = f.CreateConnection())
                {
                    string connectionString = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;
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
                    selectCommand.CommandText = $"SELECT * FROM public.{tblName}";
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
                    commandBuilder.QuotePrefix = "\"";
                    commandBuilder.QuoteSuffix = "\"";

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

            string IdColumn = table.Columns.Cast<DataColumn>()
                .Where(c => c.AutoIncrement == true)
                    .OrderBy(c => c.Ordinal)
                    .Select(c => c.ColumnName)
                    .FirstOrDefault();

            if (string.IsNullOrEmpty(IdColumn))
            {
                IdColumn = table.Columns.Cast<DataColumn>()
                    .Where(c => c.ColumnName.ToLower().Equals("id"))
                    .OrderBy(c => c.Ordinal)
                    .Select(c => c.ColumnName)
                    .FirstOrDefault();

                if (string.IsNullOrEmpty(IdColumn))
                {
                    IdColumn = table.Columns.Cast<DataColumn>()
                        .Where(c => c.ColumnName.ToLower().EndsWith("id", StringComparison.InvariantCultureIgnoreCase))
                        .OrderBy(c => c.Ordinal)
                        .Select(c => c.ColumnName)
                        .FirstOrDefault();
                }
            }

            if (!string.IsNullOrEmpty(IdColumn))
            {
                IdColumn = Regex.Replace(IdColumn, @"[\.]", string.Empty);
                IdColumn = IdColumn.ToLower().Replace(" ", "_");
            }

            sb.AppendLine($"DROP TABLE IF EXISTS public.{tblName};");
            sb.AppendLine();
            sb.AppendLine($"CREATE TABLE IF NOT EXISTS public.{tblName} (");
            for (int i = 0; i < table.Columns.Count; i++)
            {
                string columnName = Regex.Replace(table.Columns[i].ColumnName, @"[\.]", string.Empty);
                columnName = columnName.ToLower().Replace(" ", "_");
                sb.Append("\t" + columnName + " ");

                if (table.Columns[i].ColumnName.Equals(IdColumn, StringComparison.InvariantCultureIgnoreCase))
                {
                    string columnType = table.Columns[i].DataType.ToString();
                    switch (columnType)
                    {
                        case "System.Int16":
                        case "System.Int32":
                            sb.Append("SERIAL");
                            break;

                        case "System.Int64":
                        case "System.Decimal":
                        case "System.Single":
                        case "System.Double":
                            sb.Append("BIGSERIAL");
                            break;

                        case "System.String":
                        default:
                            sb.Append("VARCHAR");
                            break;
                    }
                }
                else
                {
                    string columnType = table.Columns[i].DataType.ToString();
                    switch (columnType)
                    {
                        case "System.Boolean":
                            sb.Append("BOOLEAN");
                            break;

                        case "System.Byte":
                            sb.Append("BYTE");
                            break;

                        case "System.Int16":
                            sb.Append("SMALLINT");
                            break;

                        case "System.Int32":
                            sb.Append("INT");
                            break;

                        case "System.Int64":
                            sb.Append("BIGINT");
                            break;

                        case "System.Decimal":
                            sb.Append("DECIMAL");
                            break;

                        case "System.DateTime":
                            sb.Append("DATE");
                            break;

                        case "System.Single":
                            sb.Append("REAL");
                            break;

                        case "System.Double":
                            sb.Append("DOUBLE");
                            break;

                        case "System.String":
                        default:
                            int maxLength = table.Columns[i].MaxLength < 0 ? table.AsEnumerable().Select(r => r.Field<string>(i)?.Length ?? 0).Max() : table.Columns[i].MaxLength;
                            sb.Append(table.Columns[i].MaxLength == 0 ? "VARCHAR" : $"VARCHAR({maxLength})");

                            break;
                    }

                    if (!table.Columns[i].AllowDBNull)
                    {
                        sb.Append(" NOT NULL");
                    }
                }

                if (i == table.Columns.Count - 1 && !string.IsNullOrEmpty(IdColumn))
                {
                    sb.AppendLine(",");
                }
                else
                {
                    sb.AppendLine(",");
                }
            }

            if (!string.IsNullOrEmpty(IdColumn))
            {
                sb.AppendLine($"\tCONSTRAINT {tblName}_pkey PRIMARY KEY ({IdColumn})");
            }

            sb.AppendLine(");");
            sb.AppendLine();
            sb.AppendLine($"ALTER TABLE IF EXISTS public.{tblName} OWNER to postgres;");

            return sb.ToString();
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
