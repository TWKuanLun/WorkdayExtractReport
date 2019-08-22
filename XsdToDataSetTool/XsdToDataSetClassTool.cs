using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using System.Xml;

namespace XsdToDataSetTool
{
    class XsdToDataSetClassTool
    {
        private readonly string TenantID;
        private readonly string UserName;
        private readonly string Password;
        private readonly string ReportAccount;
        private readonly string ReportName;
        private readonly string XsdExePath;
        private readonly string OutputPath;
        private readonly string Namespace;
        public XsdToDataSetClassTool(string tenantId, string username, string password, string reportAccount, string reportName, string xsdExePath, string outputPath, string @namespace)
        {
            TenantID = tenantId;
            UserName = username;
            Password = password;
            ReportAccount = reportAccount;
            ReportName = reportName;
            //"C:\\Program Files (x86)\\Microsoft SDKs\\Windows\\v10.0A\\bin\\NETFX 4.6.1 Tools\\xsd.exe"
            XsdExePath = xsdExePath;
            OutputPath = outputPath;
            Namespace = @namespace;
        }
        public async Task Run()
        {
            try
            {
                Stopwatch sw = new Stopwatch();
                sw.Start();
                //Download xsd
                using (WebClient client = new WebClient())
                {
                    client.Credentials = new NetworkCredential(UserName, Password);
                    await client.DownloadFileTaskAsync($"https://wd3-impl-services1.workday.com/ccx/service/customreport2/{TenantID}/{ReportAccount}/{ReportName}?xsds", $"{ReportName}.xsd");
                }

                //add id for dataset className
                XmlDocument doc = new XmlDocument();
                doc.Load($"{ReportName}.xsd");
                XmlAttribute idAttr = doc.CreateAttribute("id");
                idAttr.Value = $"{ReportName}DataSet";
                doc.ChildNodes[1].Attributes.Append(idAttr);
                doc.Save($"{ReportName}.xsd");

                //xsd to dataset class
                await ExecuteAsync(XsdExePath, $"/o:{OutputPath} /d /l:CS {ReportName}.xsd /eld /n:{Namespace}");
                ChangeCsproj();
                sw.Stop();
                Console.WriteLine(sw.ElapsedMilliseconds);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }
        public async Task ExecuteAsync(string executablePath, string args)
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
                await process.RunAsync();
                // do stuff with results
                Console.WriteLine($"Process finished running at {process.ExitTime} with exit code {process.ExitCode}");
            };// dispose process
        }
        internal void ChangeCsproj()
        {
            XmlDocument doc = new XmlDocument();
            doc.Load($"..\\..\\..\\ExtractSolution\\ExtractSolution.csproj");
            var compileItemGroup = doc.GetElementsByTagName("ItemGroup").Cast<XmlNode>()
                .Single(x => x.ChildNodes.Cast<XmlNode>().Any(y => y.Name == "Compile"));

            if(compileItemGroup.Cast<XmlNode>().Any(x => x.Attributes.Cast<XmlAttribute>().Any(y => y.Value == $"Model\\{ReportName}.cs")))
            {
                return;
            }

            //Add .cs to csproj
            XmlElement elem = doc.CreateElement("Compile");
            XmlAttribute includeAttr = doc.CreateAttribute("Include");
            includeAttr.Value = $"Model\\{ReportName}.cs";
            elem.Attributes.Append(includeAttr);
            elem.RemoveAttribute("xmlns");

            compileItemGroup.AppendChild(elem);
            doc.Save($"..\\..\\..\\ExtractSolution\\ExtractSolution.csproj");
        }
    }
}
