using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using XsdToDataSetTool;

namespace ExtractTask
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var tenantId = args[0];
            var userName = args[1];
            var password = args[2];
            var xsdExePath = args[3];
            var msBuildPath = args[4];
            var connectionString = args[5];
            var sqlCondition = args[6];

            var reportListTable = new DataTable();
            using (var connection = new SqlConnection(connectionString))
            {
                connection.Open();
                using (SqlDataAdapter data = new SqlDataAdapter($"SELECT ReportName, ReportArgs, CreateBy FROM [Workday_Report_Info_List] {sqlCondition}", connection))
                {
                    data.Fill(reportListTable);
                }
            }

            foreach(DataRow reportInfo in reportListTable.Rows)
            {
                var reportName = reportInfo["ReportName"].ToString();
                var createBy = reportInfo["CreateBy"].ToString();

                await GenerateCSByXSD(tenantId, userName, password, createBy, reportName, xsdExePath);
            }

            await ExecuteAsync("MsBuild", msBuildPath, $"/t:rebuild /p:DeployOnBuild=true /p:Configuration=Release \"..\\..\\..\\ExtractSolution\\ExtractSolution.csproj\"");

            foreach (DataRow reportInfo in reportListTable.Rows)
            {
                var reportName = reportInfo["ReportName"].ToString();
                var reportArgs = reportInfo["ReportArgs"].ToString();
                var createBy = reportInfo["CreateBy"].ToString();

                var exePath = "\"..\\..\\..\\ExtractSolution\\bin\\Release\\ExtractSolution.exe\"";

                await ExecuteAsync(reportName, exePath, $"\"{tenantId}\" \"{userName}\" \"{password}\" \"{createBy}\" \"{reportName}\" \"{connectionString}\" \"{reportArgs}\"");
            }
        }
        public async static Task GenerateCSByXSD(string tenantId, string userName, string password, string reportAccount, string reportName, string xsdExePath)
        {
            await XsdToDataSetTool.Program.Main(new string[] {
                tenantId,
                userName,
                password,
                reportAccount,
                reportName,
                xsdExePath
            });
        }
        public static async Task ExecuteAsync(string executeWhat, string executablePath, string args)
        {
            using (var process = new Process())
            {
                // configure process
                process.StartInfo.FileName = executablePath;
                process.StartInfo.UseShellExecute = false;
                process.StartInfo.CreateNoWindow = true;
                process.StartInfo.Arguments = args;
                process.StartInfo.RedirectStandardOutput = true;
                // run process asynchronously
                await process.RunAsync(executeWhat);
                // do stuff with results
                Console.WriteLine($"{executeWhat} Process finished running at {process.ExitTime} with exit code {process.ExitCode}");
            };// dispose process
        }
    }
    public static class ExtProcess
    {
        public static Task RunAsync(this Process process, string executeWhat)
        {
            var tcs = new TaskCompletionSource<object>();
            process.EnableRaisingEvents = true;
            process.Exited += (s, e) => tcs.TrySetResult(null);
            // not sure on best way to handle false being returned
            if (!process.Start()) tcs.SetException(new Exception($"{executeWhat} Failed to start process."));
            while (!process.StandardOutput.EndOfStream)
            {
                string line = process.StandardOutput.ReadLine();
                Console.WriteLine(line);
            }
            return tcs.Task;
        }
    }
}
