using System;
using System.Threading.Tasks;

namespace Shared
{
    class Program
    {
        static void Main(string[] args)
        {
            MongoContext mc = new MongoContext("pls");
            JobService js = new JobService(mc);
            js.InsertJob(new JobSample { Url = "google.com" }).Wait();
            js.InsertJob(new JobSample { Url = "yahoo.com" }).Wait();

            Parallel.For(0, 10, async (p) => {
                await js.InsertJob(new JobSample { Url = string.Format("example-{0}.com", p) });
            });


            Parallel.For(0, 10, async (p) =>
            {
                var jobs = await js.FetchPendingJobIds();
                foreach (var jobId in jobs)
                {
                    var job = await js.AcquireJob(jobId);
                    if (job != null)
                    {
                        job.Execute();
                    }
                }
            });

            Console.ReadKey();
        }
    }
}
