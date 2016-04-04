using MongoDB.Bson.Serialization.Attributes;
using System;
using MongoDB.Bson;

namespace Shared
{
    /// <summary>
    /// Status of Jobs in the database
    /// </summary>
    public enum JobStatus
    {
        /// <summary>
        /// No status - must never end up in the database
        /// </summary>
        None = 0,

        /// <summary>
        /// Pending - has been added, but not processed yet
        /// </summary>
        Pending = 1,

        /// <summary>
        /// InProgess - LOCKED - A worker is working on this currently
        /// </summary>
        InProgress = 2,

        /// <summary>
        /// Failed - a worker encountered an error on this job
        /// </summary>
        Failed = 3,

        /// <summary>
        /// Succeded - a worker finished the task successfully
        /// </summary>
        Succeeded = 4,
    }

    /// <summary>
    /// Allowed return statuses for Job.Execute
    /// </summary>
    public enum JobResultStatus
    {
        Failure = 0,
        Success = 1,
        Indeterminate = 2
    }

    [BsonDiscriminator(Required = true)]
    public abstract class Job
    {
        public ObjectId Id { get; set; }
        public JobStatus Status { get; set; }

        public virtual void Execute() { throw new InvalidOperationException(); }
    }

}
