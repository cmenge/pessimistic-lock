namespace Shared
{
    public class JobSample : Job
    {
        public string Url { get; set; }

        public override void Execute() {
            // TBD: Download Url, open the page in a background browser, render it to an image and store that in the database.
            System.Console.WriteLine("Processing {0}", Url);
        }
    }
}
