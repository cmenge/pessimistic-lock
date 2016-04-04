using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shared
{
    public sealed class JobService
    {
        private readonly MongoContext _db;
        public JobService(MongoContext db) 
        {
            _db = db;
        }

        /// <summary>
        /// Sets the job status to pending and adjusts its 'added time' to UtcNow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="job"></param>
        public async Task InsertJob<T>(T job) where T : Job
        {
            try
            {
                job.Status = JobStatus.Pending;
                await _db.Insert<Job>(job).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                // _log.Error(ex, "An error occurred trying to insert a job into the database!");
                throw;
            }
        }

        public async Task<IAsyncCursor<Job>> FetchFailedJobs()
        {
            try
            {
                var result = await _db.Cursor(Builders<Job>.Filter.Eq(p => p.Status, JobStatus.Failed)).ConfigureAwait(false);
                return result;
            }
            catch (Exception ex)
            {
                // _log.Error(ex, "An error occurred trying to fetch failed jobs from the database!");
                throw;
            }
        }

        public async Task<IAsyncCursor<Job>> FetchInterruptedJobs()
        {
            try
            {
                var result = await _db.Cursor(Builders<Job>.Filter.Eq(p => p.Status, JobStatus.InProgress)).ConfigureAwait(false);
                return result;
            }
            catch (Exception ex)
            {
                // _log.Error(ex, "An error occurred trying to fetch interrupted jobs from the database!");
                throw;
            }
        }

        /// <summary>
        /// Fetches Ids of all pending jobs WITHOUT locking them! Never process a job that hasn't been acquired first!
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<ObjectId>> FetchPendingJobIds()
        {
            try
            {
                var result = await _db.Find(Builders<Job>.Filter.Eq(p => p.Status, JobStatus.Pending)).Limit(100).Project(p => p.Id).ToListAsync().ConfigureAwait(false);
                return result;
            }
            catch (Exception ex)
            {
                // _log.Error(ex, "An error occurred trying to fetch pending jobs from the database!");
                throw;
            }
        }

        /// <summary>
        /// Acquires a pending job for processing and locks the database object. 
        /// NEVER PROCESS A JOB THAT IS NOT ACQUIRED!
        /// </summary>
        /// <param name="jobId"></param>
        public async Task<Job> AcquireJob(ObjectId jobId)
        {
            return await UpdateJobStatus(jobId, JobStatus.Pending, JobStatus.InProgress).ConfigureAwait(false);
        }

        /// <summary>
        /// Sets a *running* job's status to failed
        /// </summary>
        /// <param name="jobId"></param>
        public async Task<Job> MarkJobFailed(ObjectId jobId)
        {
            return await UpdateJobStatus(jobId, JobStatus.InProgress, JobStatus.Failed).ConfigureAwait(false);
        }

        /// <summary>
        /// Sets a *running* job's status to succeeded
        /// </summary>
        /// <param name="jobId"></param>
        public async Task<Job> MarkJobSuccessful(ObjectId jobId)
        {
            return await UpdateJobStatus(jobId, JobStatus.InProgress, JobStatus.Succeeded).ConfigureAwait(false);
        }

        /// <summary>
        /// Updates a job's status in an atomic manner and returns the original object (FindAndModify)
        /// </summary>
        /// <param name="jobId"></param>
        /// <param name="expectedStatus"></param>
        /// <param name="newStatus"></param>
        /// <returns></returns>
        private async Task<Job> UpdateJobStatus(ObjectId jobId, JobStatus expectedStatus, JobStatus newStatus)
        {
            var query = Builders<Job>.Filter.And(
                Builders<Job>.Filter.Eq(p => p.Id, jobId),
                Builders<Job>.Filter.Eq(p => p.Status, expectedStatus)
            );

            var update = Builders<Job>.Update.Set(p => p.Status, newStatus);

            Job acquiredJob = await _db.FindAndModify(query, update).ConfigureAwait(false);
            if (acquiredJob == null) 
            {
                // _log.Warn("Failed to update job id {0} status from {1} to {2}!", jobId, expectedStatus, newStatus);
            }
            return acquiredJob;
        }
    }
}
