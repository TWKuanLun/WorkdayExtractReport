using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;
using ExtractSolution.Model;

namespace ExtractSolution
{
    class ExtractReportToDB
    {
        private readonly string TenantID;
        private readonly string UserName;
        private readonly string Password;
        private readonly string ReportAccount;
        private readonly string ReportName;
        private readonly string ConnectionString;
        private readonly string ReportArgsString;
        public ExtractReportToDB(string tenantID, string username, string password, string reportAccount, string reportName, string connectionString, string reportArgsString)
        {
            TenantID = tenantID;
            UserName = username;
            Password = password;
            ReportAccount = reportAccount;
            ReportName = reportName;
            ConnectionString = connectionString;
            ReportArgsString = reportArgsString;
        }
        public void Run()
        {
            try
            {
                //Download XML Report
                using (MyWebClient client = new MyWebClient())
                {
                    client.Headers.Add("user-agent", "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.2; .NET CLR 1.0.3705;)");
                    client.Credentials = new NetworkCredential(UserName, Password);
                    client.DownloadFile($"https://wd3-impl-services1.workday.com/ccx/service/customreport2/{TenantID}/{ReportAccount}/{ReportName}?{ReportArgsString}format=simplexml", $"{ReportName}.xml");
                }

                //var dataSet = new Extract_Supervisory_OrganizationsDataSet();
                var dataSetType = Type.GetType($"ExtractSolution.Model.{ReportName}DataSet");
                var dataSet = Activator.CreateInstance(dataSetType);
                //dataSet.ReadXml($"{reportName}.xml", XmlReadMode.ReadSchema);
                var readXmlMethods = dataSetType.GetMethods(BindingFlags.Public | BindingFlags.Instance);
                var readXmlMethod = readXmlMethods.Where(x => x.Name == "ReadXml").Single(x => {
                    var parameters = x.GetParameters();
                    return parameters.Length == 2 && parameters.Any(y => y.Name == "fileName") && parameters.Any(y => y.Name == "mode");
                });

                object[] parametersArray = new object[] { $"{ReportName}.xml", XmlReadMode.ReadSchema };
                readXmlMethod.Invoke(dataSet, parametersArray);

                //var columns = dataSet.Report_Entry.Columns;
                #region Check Table Exist 
                var tables = new DataTable();
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (SqlDataAdapter data = new SqlDataAdapter($"SELECT t.name, s.name FROM sys.tables t LEFT JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = '{ReportName}' AND s.name = 'Report'", connection))
                    {
                        data.Fill(tables);
                    }
                }
                if (tables.Rows.Count > 0)
                {
                    using (var connection = new SqlConnection(ConnectionString))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand($"DROP TABLE [Report].[{ReportName}]", connection))
                        {
                            var result = command.ExecuteNonQuery();
                        }
                    }
                }
                #endregion
                var tablesArray = ((DataSet)dataSet).Tables.Cast<DataTable>().Where(x => x.TableName != "Report_Data").ToArray();

                //Add schema
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand($"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'Report') BEGIN EXEC('CREATE SCHEMA Report') END", connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }

                var columnNameMapping = new Dictionary<string, Dictionary<string, (string newColumnName, DataTable table, DataColumn column)>>();
                #region Create Table
                StringBuilder createTableSQL = new StringBuilder($"CREATE TABLE [Report].[{ReportName}] (");
                foreach (var table in tablesArray)
                {
                    foreach (DataColumn column in table.Columns)
                    {
                        //var name = dataSet.Report_Entry.Availability_DateColumn.ColumnName;
                        //var type = dataSet.Report_Entry.Availability_DateColumn.DataType;
                        if (column.ColumnName == "Report_Entry_Id" || column.ColumnName == "Report_Data_Id")
                        {
                            continue;
                        }
                        ColumnSQL(table, column, columnNameMapping);
                    }
                }
                foreach (var table in columnNameMapping)
                {
                    foreach (var column in table.Value)
                    {
                        createTableSQL.Append($"[{column.Value.newColumnName}] {GetSQLType(column.Value.column)} {AutoIncrement(column.Value.column)} {AllowDBNull(column.Value.column)}, ");
                    }
                }
                createTableSQL.Remove(createTableSQL.Length - 2, 2);
                createTableSQL.Append(");");
                using (var connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    using (SqlCommand command = new SqlCommand(createTableSQL.ToString(), connection))
                    {
                        var result = command.ExecuteNonQuery();
                    }
                }
                #endregion

                var Row_ColumnNameDict = new List<ReportTable>();
                foreach(var table in tablesArray)
                {
                    Row_ColumnNameDict.Add(ConvertToReportTable(table, columnNameMapping));
                }
                var reportEntry = Row_ColumnNameDict.SingleOrDefault(x => x.TableName == "Report_Entry");
                var arrayTables = Row_ColumnNameDict.Where(x => x.TableName != "Report_Entry");
                #region Save Array Table Data
                IEnumerable<Dictionary<string, object>> query = reportEntry.Data;

                foreach (var table in arrayTables)
                {
                    query = from entryRow in query
                            join otherRow in table.Data on entryRow["Report_Entry_Id"].ToString() equals otherRow["Report_Entry_Id"].ToString() into otherGroup
                            from newOtherRow in otherGroup.DefaultIfEmpty()
                            select MergeDict(entryRow, newOtherRow);
                }
                var allData = query.ToArray();
                #endregion

                #region Insert Data

                var newColumnNameList = columnNameMapping.SelectMany(x => x.Value.Select(y => (newColumnName: y.Value.newColumnName, type: y.Value.column.DataType))).ToArray();
                //SQL "INSERT INTO" once insert limit 1000 rows
                var _1000Count = Math.Ceiling((double)allData.Length / 1000);
                for (int i = 0; i < _1000Count; i++)
                {
                    StringBuilder insertDataSQL = new StringBuilder($"INSERT INTO [Report].[{ReportName}] (");

                    for (int j = 0; j < newColumnNameList.Length; j++)
                    {
                        insertDataSQL.Append($"[{newColumnNameList[j].newColumnName}], ");
                    }
                    insertDataSQL.Remove(insertDataSQL.Length - 2, 2);
                    insertDataSQL.Append(") ");

                    for (int j = 0; j < 1000; j++)
                    {
                        var index = i * 1000 + j;
                        if (index >= allData.Length)
                            break;

                        insertDataSQL.Append($"SELECT ");
                        for (int k = 0; k < newColumnNameList.Length; k++)
                        {
                            
                            if (!allData[index].ContainsKey(newColumnNameList[k].newColumnName))
                            {
                                insertDataSQL.Append($"{GetValueByType(DBNull.Value, newColumnNameList[k].type)}, ");
                            }
                            else
                            {
                                insertDataSQL.Append($"{GetValueByType(allData[index][newColumnNameList[k].newColumnName], newColumnNameList[k].type)}, ");
                            }
                        }
                        insertDataSQL.Remove(insertDataSQL.Length - 2, 2);
                        insertDataSQL.Append($" UNION ALL ");
                    }
                    insertDataSQL.Remove(insertDataSQL.Length - 10, 10);
                    insertDataSQL.Append($";");

                    using (var connection = new SqlConnection(ConnectionString))
                    {
                        connection.Open();
                        using (SqlCommand command = new SqlCommand(insertDataSQL.ToString(), connection))
                        {
                            command.CommandTimeout = 0;
                            var result = command.ExecuteNonQuery();
                        }
                    }
                }
                #endregion
                //列出 test1 的第 4 筆資料
                //Console.WriteLine(dataSet.Report_Entry.Rows[3][dataSet.Report_Entry.Availability_DateColumn]);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        private void ColumnSQL(DataTable table, DataColumn column, Dictionary<string, Dictionary<string, (string newColumnName, DataTable table, DataColumn column)>> columnNameMapping)
        {
            //[A] table [a] column, [B] table [a] column => [A] table [A a] column, [B] table [B a] column
            if (columnNameMapping.Any(x => x.Value.Any(y => y.Key == column.ColumnName)))
            {
                #region 把ColumnName跟這個Column一樣的全部都改成tablename columnname
                var needChangeNameColumn = columnNameMapping.Where(x => x.Value.Any(y => y.Value.newColumnName == column.ColumnName));
                foreach (var pair in needChangeNameColumn)
                {
                    var temp = columnNameMapping[pair.Key][column.ColumnName];
                    columnNameMapping[pair.Key][column.ColumnName] = ($"{pair.Key} {column.ColumnName}", temp.table, temp.column);
                }
                #endregion
                if (!columnNameMapping.ContainsKey(table.TableName))
                    columnNameMapping.Add(table.TableName, new Dictionary<string, (string newColumnName, DataTable table, DataColumn column)>());
                columnNameMapping[table.TableName].Add(column.ColumnName, ($"{table.TableName} {column.ColumnName}", table, column));
            }
            else
            {
                if (!columnNameMapping.ContainsKey(table.TableName))
                    columnNameMapping.Add(table.TableName, new Dictionary<string, (string newColumnName, DataTable table, DataColumn column)>());
                columnNameMapping[table.TableName].Add(column.ColumnName, (column.ColumnName, table, column));
            }
            //sql.Append($"[{columnNameMapping[table.TableName][column.ColumnName]}] {GetSQLType(column)} {AutoIncrement(column)} {AllowDBNull(column)}, ");
        }
        private string GetSQLType(DataColumn column)
        {
            Type type = column.DataType;
            if (type == typeof(int))
            {
                return "int";
            }
            else if (type == typeof(long))
            {
                return "bigint";
            }
            else if (type == typeof(short))
            {
                return "smallint";
            }
            else if (type == typeof(byte))
            {
                return "tinyint";
            }
            else if (type == typeof(decimal))
            {
                return "decimal";
            }
            else if (type == typeof(DateTime))
            {
                return "datetime";
            }
            else if (type == typeof(bool))
            {
                return "bit";
            }
            else
            {
                return string.Format("nvarchar({0})", column.MaxLength == -1 ? "max" : column.MaxLength.ToString());
            }
        }
        private string AutoIncrement(DataColumn column)
        {
            if (column.AutoIncrement)
                return $"IDENTITY({column.AutoIncrementSeed.ToString()},{column.AutoIncrementStep.ToString()})";
            return "";
        }
        private string AllowDBNull(DataColumn column)
        {
            if (!column.AllowDBNull)
                return "NOT NULL";
            return "";
        }
        private string GetValueByType(object value, Type type)
        {
            if (value == DBNull.Value)
            {
                return "NULL";
            }

            if (type == typeof(int) || type == typeof(long) || type == typeof(short) || type == typeof(byte) || type == typeof(decimal))
            {
                return value.ToString();
            }
            else if (type == typeof(DateTime))
            {
                var temp = Convert.ToDateTime(value);
                if (temp.Year < 1000)//if continuous_service_date is NULL simplexml will get 0001-01-03, don't know why
                {
                    return "NULL";
                }
                return $"'{temp.ToString("yyyy-MM-dd HH:mm:ss")}'";
            }
            else if (type == typeof(bool))
            {
                return $"'{value}'";
            }
            else
            {
                return $"N'{value.ToString().Replace("'", "''")}'";
            }
        }
        private Dictionary<string, object> MergeDict(Dictionary<string, object> a, Dictionary<string, object> b)
        {
            var result = new Dictionary<string, object>();
            if (a != null)
            {
                foreach (var pair in a)
                {
                    result.Add(pair.Key, pair.Value);
                }
            }
            if (b != null)
            {
                foreach (var pair in b)
                {
                    if (pair.Key == "Report_Entry_Id")
                        continue;
                    result.Add(pair.Key, pair.Value);
                }
            }
            return result;
        }

        private ReportTable ConvertToReportTable(DataTable table, Dictionary<string, Dictionary<string, (string newColumnName, DataTable table, DataColumn column)>> columnNameMapping)
        {
            var result = new ReportTable();
            result.TableName = table.TableName;
            var rows = new List<Dictionary<string, object>>();
            foreach(DataRow row in table.Rows)
            {
                var oneData = new Dictionary<string, object>();
                foreach (DataColumn column in table.Columns)
                {
                    var newColumnName = column.ColumnName;
                    if (columnNameMapping[table.TableName].ContainsKey(column.ColumnName))
                    {
                        newColumnName = columnNameMapping[table.TableName][column.ColumnName].newColumnName;
                    }
                    oneData.Add(newColumnName, row[column.ColumnName]);
                }
                rows.Add(oneData);
            }
            result.Data = rows;
            return result;
        }
    }
}