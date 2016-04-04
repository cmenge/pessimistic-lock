
/*
 * The MIT License (MIT)

Copyright (c) 2015-2016 Christoph Menge

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
 * 
 */

using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

namespace Shared
{
    [Serializable]
    public class UniqueConstraintViolationException : Exception
    {
        public UniqueConstraintViolationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public UniqueConstraintViolationException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// TBD: Make this wrapper async (or rewrite it altogether)
    /// </summary>
    public class MongoContext
    {
        private static readonly object _syncRoot = new object();

        private IMongoDatabase _actualProvider = null;

        private IMongoDatabase _provider
        {
            get
            {
                if (_actualProvider == null)
                {
                    _actualProvider = CreateSession(DatabaseName);
                }
                return _actualProvider;
            }
        }

        private static readonly WriteConcern PARANOID_WRITE_CONCERN_RS = new WriteConcern("majority", TimeSpan.FromSeconds(5), false, true);
        private static readonly WriteConcern SAFE_WRITE_CONCERN_RS = PARANOID_WRITE_CONCERN_RS;

        private static readonly WriteConcern PARANOID_WRITE_CONCERN_PLAIN = new WriteConcern("1", TimeSpan.FromSeconds(5), false, true);
        private static readonly WriteConcern SAFE_WRITE_CONCERN_PLAIN = PARANOID_WRITE_CONCERN_PLAIN;
        private static readonly WriteConcern DEFAULT_WRITE_CONCERN = WriteConcern.Acknowledged;

        private static WriteConcern PARANOID_WRITE_CONCERN = PARANOID_WRITE_CONCERN_PLAIN;
        private static WriteConcern SAFE_WRITE_CONCERN = SAFE_WRITE_CONCERN_PLAIN;

        /// <summary>
        /// TBD: Config Mangement?!
        /// </summary>
        private static readonly string _host = "localhost";// ConfigurationManager.AppSettings["MongoHost"] ?? "localhost";

        public string DatabaseName { get; private set; }

        private IMongoClient _client;

        /// <summary>
        /// TBD: This has changed significantly on the MongoDB driver side, review!! 
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        private IMongoDatabase CreateSession(string databaseName)
        {
            if (_client == null)
            {
                //lock (_syncRoot)
                {
                    if (_client == null)
                    {
                        string connectionString = string.Format("mongodb://{1}/{0}", databaseName, _host);

                        _client = new MongoClient(connectionString);

                        // TBD: If connected to a replica set, use the appropriate write concerns
                        //if (_server.ReplicaSetName != null)
                        //{
                        //    PARANOID_WRITE_CONCERN = PARANOID_WRITE_CONCERN_RS;
                        //    SAFE_WRITE_CONCERN = SAFE_WRITE_CONCERN_RS;
                        //}
                    }
                }
            }

            var database = _client.GetDatabase(databaseName);
            return database;
        }

        public bool Exists<T>(ObjectId id)
            where T : IDBObject
        {
            // TBD: This used to be the fastest method by enforcing a covered index query on _id... Find out how to do that with the new driver
            // return GetCollection<T>().Find(Query<T>.EQ(p => p.Id, id)).SetFields(Fields<T>.Include(p => p.Id)).SetLimit(1).Size() == 1;
            return GetCollection<T>().CountAsync(Builders<T>.Filter.Eq(p => p.Id, id), new CountOptions { Limit = 1 }).Result == 1;
        }

        public void AssertIdExists<T>(ObjectId id)
            where T : IDBObject
        {
            var temp = Exists<T>(id);
            if (temp == false)
                throw new ApplicationException(string.Format("Foreign key violation: Couldn't find {0}:{1}", typeof(T).Name, id.ToString()));
        }

        public MongoContext(string databaseName)
        {
            Checker.Requires<ArgumentNullException>(databaseName != null);
            Checker.Requires<ArgumentException>(!string.IsNullOrWhiteSpace(databaseName));

            _actualProvider = CreateSession(databaseName);
            DatabaseName = databaseName;
        }

        public IMongoDatabase Provider
        {
            get
            {
                return _provider;
            }
        }

        public async Task<T> SingleByIdOrDefaultAsync<T>(ObjectId id) where T : IDBObject
        {
            return await GetCollection<T>().Find(p => p.Id == id).SingleOrDefaultAsync().ConfigureAwait(false);
        }

        public async Task<T> SingleByIdOrDefaultAsync<T>(object id)
        {
            return await GetCollection<T>().Find(Builders<T>.Filter.Eq("_id", id)).SingleOrDefaultAsync().ConfigureAwait(false);
        }

        public async Task<T> SingleById<T>(ObjectId id) where T : IDBObject
        {
            return await GetCollection<T>().Find(p => p.Id == id).SingleAsync().ConfigureAwait(false);
        }

        public async Task<T> SingleById<T>(object id)
        {
            return await GetCollection<T>().Find(Builders<T>.Filter.Eq("_id", BsonValue.Create(id))).SingleAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Inserts the given item of type T into a collection named after the type, more specifically,
        /// a collection with name typeof(T).Name.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="item"></param>
        public async Task Insert<T>(T item) where T : class
        {
            try
            {
                await GetCollection<T>().InsertOneAsync(item).ConfigureAwait(false);
            }
            catch (MongoException ex)
            {
                // TBD: These are no longer correct I presume...
                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
        }

        public async Task InsertBatch<T1>(IEnumerable<T1> batch)
        {
            try
            {
                await GetCollection<T1>().InsertManyAsync(batch).ConfigureAwait(false);
            }
            catch (MongoException ex)
            {
                // FIXME! This is no longer correct!
                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
        }

        public async Task<long> UpdateManyAsync<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            try
            {
                var result = await GetCollection<T>().UpdateManyAsync(query, update);
                return result.IsModifiedCountAvailable ? result.ModifiedCount : -1;
            }
            catch (MongoException ex)
            {
                // FIXME! This is no longer correct!
                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<long> Update<T>(T item) where T : IDBObject
        {
            return await UpdateInternal(item, item.Id);
        }

        private async Task<long> UpdateInternal<T>(T item, object id)
        {
            try
            {
                var result = await GetCollection<T>().ReplaceOneAsync(Builders<T>.Filter.Eq("_id", BsonValue.Create(id)), item).ConfigureAwait(false);
                //if (result.ModifiedCount != 1)
                //{
                //    throw new ApplicationException("Database insert failed");
                //}

                return result.ModifiedCount;
            }
            catch (MongoException ex)
            {
                // FIXME! This is no longer correct!
                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        /// <summary>
        /// Performs an update with a more strict write concern to ensure the update was persisted.
        /// Use this whenever data loss is irrecoverable, such as in webhooks. Do not use this method
        /// per default, because it can easily take 10 - 30ms to complete, rendering it an order of
        /// magnitude slower than normal updates
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="query"></param>
        /// <param name="newItemState"></param>
        /// <returns></returns>
        public async Task<long> UpdateSafe<T>(FilterDefinition<T> query, T newItemState)
        {
            return await UpdateReplaceInternal(query, newItemState, SAFE_WRITE_CONCERN);
        }

        public async Task<long> Update<T>(FilterDefinition<T> query, T newItemState)
        {
            return await UpdateReplaceInternal(query, newItemState, DEFAULT_WRITE_CONCERN);
        }

        private async Task<long> UpdateReplaceInternal<T>(FilterDefinition<T> query, T newItemState, WriteConcern writeConcern)
        {
            try
            {
                var result = await GetCollection<T>(writeConcern).ReplaceOneAsync(query, newItemState, new UpdateOptions { IsUpsert = false });
                if (result.ModifiedCount != 1)
                {
                    throw new ApplicationException("Database insert failed");
                }

                return result.ModifiedCount;
            }
            catch (MongoException ex)
            {
                // FIXME! This is no longer correct!
                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        public async Task<long> Update<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            var result = await GetCollection<T>(DEFAULT_WRITE_CONCERN).UpdateManyAsync(query, update).ConfigureAwait(false);
            return result.ModifiedCount;
        }

        public async Task<long> UpdateSafe<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            var result = await GetCollection<T>(SAFE_WRITE_CONCERN).UpdateManyAsync(query, update).ConfigureAwait(false);
            return result.ModifiedCount;
        }

        public async Task<long> UpsertOne<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            Checker.Requires<ArgumentNullException>(query != null);
            Checker.Requires<ArgumentNullException>(update != null);

            var result = await GetCollection<T>().UpdateOneAsync(query, update, new UpdateOptions { IsUpsert = true }).ConfigureAwait(false);
            return result.ModifiedCount;
        }

        public async Task<long> UpsertMany<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            Checker.Requires<ArgumentNullException>(query != null);
            Checker.Requires<ArgumentNullException>(update != null);

            var result = await GetCollection<T>().UpdateManyAsync(query, update, new UpdateOptions { IsUpsert = true }).ConfigureAwait(false);
            return result.ModifiedCount;
        }

        public async Task<T> FindModifyUpsert<T>(FilterDefinition<T> query, UpdateDefinition<T> update, WriteConcern writeConcern)
        {
            Checker.Requires<ArgumentNullException>(query != null);
            Checker.Requires<ArgumentNullException>(update != null);
            Checker.Requires<ArgumentNullException>(writeConcern != null);
            return await GetCollection<T>(writeConcern).FindOneAndUpdateAsync<T>(query, update, new FindOneAndUpdateOptions<T> { IsUpsert = true, ReturnDocument = ReturnDocument.After }).ConfigureAwait(false);
        }

        public async Task<T> FindModifyUpsert<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            Checker.Requires<ArgumentNullException>(query != null);
            Checker.Requires<ArgumentNullException>(update != null);
            return await FindModifyUpsert<T>(query, update, DEFAULT_WRITE_CONCERN).ConfigureAwait(false);
        }

        private async Task<T> FindAndModifyBase<T>(FilterDefinition<T> query, UpdateDefinition<T> update, bool returnNew)
        {
            try
            {
                return await GetCollection<T>().FindOneAndUpdateAsync<T>(query, update, new FindOneAndUpdateOptions<T> { IsUpsert = false, ReturnDocument = returnNew ? ReturnDocument.After : ReturnDocument.Before }).ConfigureAwait(false);
            }
            catch (MongoException ex)
            {
                // _log.Error(() => "Error in find and modify!", ex);

                if (ex.Message.Contains("duplicate key error"))
                    throw new UniqueConstraintViolationException("Duplicate Key!", ex);
                else
                    throw;
            }
            catch (Exception)
            {
                // _log.Error(() => "Error in find and modify!", ex2);
                throw;
            }
        }

        public async Task<T> ModifyAndFind<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
        {
            return await FindAndModifyBase<T>(query, update, true).ConfigureAwait(false);
        }

        public async Task<T> FindAndModify<T>(FilterDefinition<T> query, UpdateDefinition<T> update)
            where T : class
        {
            return await FindAndModifyBase<T>(query, update, false).ConfigureAwait(false);
        }

        public Task Delete<T>(T item) where T : IDBObject
        {
            Checker.Requires<ArgumentNullException>(item != null);
            return DeleteById<T>(item.Id);
        }

        public async Task DeleteById<T>(ObjectId id) where T : IDBObject
        {
            var result = await GetCollection<T>().DeleteOneAsync(Builders<T>.Filter.Eq(p => p.Id, id)).ConfigureAwait(false);
        }

        public async Task DeleteById<T>(object id)
        {
            var result = await GetCollection<T>().DeleteOneAsync(Builders<T>.Filter.Eq("_id", id)).ConfigureAwait(false);
        }

        public async Task<long> Delete<T>(FilterDefinition<T> query)
        {
            var result = await GetCollection<T>().DeleteOneAsync(query).ConfigureAwait(false);
            return result.DeletedCount;
        }

        public async Task<long> DeleteMany<T>(FilterDefinition<T> query)
            where T : class
        {
            var result = await GetCollection<T>().DeleteManyAsync(query).ConfigureAwait(false);
            return result.DeletedCount;
        }

        public async Task<bool> TryInsert<T>(T item) where T : class
        {
            try
            {
                await Insert(item).ConfigureAwait(false);
            }
            catch (Exception)
            {
                // _log.Warn(() => "Error in TryInsert!", ex);
                return false;
            }
            return true;
        }

        public IMongoCollection<T1> GetCollection<T1>(string collectionName)
        {
            Checker.Requires<ArgumentNullException>(collectionName != null);
            Checker.Requires<ArgumentException>(!string.IsNullOrEmpty(collectionName));
            var result = _provider.GetCollection<T1>(collectionName);
            return result;
        }

        public IMongoCollection<T1> GetCollection<T1>()
        {
            var result = _provider.GetCollection<T1>(typeof(T1).Name);
            return result;
        }

        public IMongoCollection<T1> GetCollection<T1>(WriteConcern writeConcern)
        {
            Checker.Requires<ArgumentNullException>(writeConcern != null);
            var result = _provider.GetCollection<T1>(typeof(T1).Name, new MongoCollectionSettings { WriteConcern = writeConcern });
            return result;
        }

        public async Task<T1> FindSingle<T1>(FilterDefinition<T1> query)
        {
            //TBD: Ensure uniqueness
            Checker.Requires<ArgumentNullException>(query != null);
            var result = await _provider.GetCollection<T1>(typeof(T1).Name).FindAsync<T1>(query);
            return result.Current.Single();
        }

        public IFindFluent<T1, T1> FindAll<T1>()
        {
            // TBD: no idea if this works, but there's no explicit 'empty criteria' object at the moment.
            var result = _provider.GetCollection<T1>(typeof(T1).Name).Find(p => true);
            return result;
        }

        public async Task<T1> FindFirstOrDefault<T1>()
        {
            var result = await _provider.GetCollection<T1>(typeof(T1).Name).Find(p => true).FirstOrDefaultAsync().ConfigureAwait(false);
            return result;
        }

        public async Task<IAsyncCursor<T1>> Cursor<T1>(FilterDefinition<T1> query)
        {
            var result = await _provider.GetCollection<T1>(typeof(T1).Name).FindAsync<T1>(query).ConfigureAwait(false);
            return result;
        }

        public async Task<IAsyncCursor<T1>> CursorAll<T1>()
        {
            var result = await _provider.GetCollection<T1>(typeof(T1).Name).FindAsync<T1>(p => true).ConfigureAwait(false);
            return result;
        }

        public IFindFluent<T1, T1> Find<T1>(FilterDefinition<T1> query)
        {
            var result = _provider.GetCollection<T1>(typeof(T1).Name).Find(query);
            return result;
        }
    }
}
