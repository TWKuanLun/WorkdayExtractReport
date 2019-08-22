namespace ExtractSolution
{
    class Program
    {
        static void Main(string[] args)
        {
            var tenantId = args[0];
            var userName = args[1];
            var password = args[2];
            var reportCreateByAcccount = args[3];
            var reportName = args[4];
            var connectionString = args[5];
            var reportArgsString = args[6];
            var extractReportToDB = new ExtractReportToDB(tenantId, userName, password, reportCreateByAcccount, reportName, connectionString, reportArgsString);
            extractReportToDB.Run();
        }
    }
}
