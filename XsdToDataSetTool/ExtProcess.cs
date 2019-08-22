using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace XsdToDataSetTool
{
    public static class ExtProcess
    {
        public static Task RunAsync(this Process process)
        {
            var tcs = new TaskCompletionSource<object>();
            process.EnableRaisingEvents = true;
            process.Exited += (s, e) => tcs.TrySetResult(null);
            // not sure on best way to handle false being returned
            if (!process.Start()) tcs.SetException(new Exception("Failed to start process."));
            while (!process.StandardOutput.EndOfStream)
            {
                string line = process.StandardOutput.ReadLine();
                Console.WriteLine(line);
            }
            return tcs.Task;
        }
    }
}
