using System;
using System.Net;
using System.Threading;

namespace ExtractSolution
{
    public class MyWebClient : WebClient
    {
        protected override WebRequest GetWebRequest(Uri uri)
        {
            WebRequest w = base.GetWebRequest(uri);
            w.Timeout = Timeout.Infinite;
            return w;
        }
    }
}
