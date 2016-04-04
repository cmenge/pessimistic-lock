using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Shared
{
    public interface IDBObject
    {
        [BsonIgnoreIfDefault]
        ObjectId Id { get; }
    }

    public abstract class DBObject : IDBObject
    {
        /// <summary>
        /// The [BsonIgnoreIfDefault] attribute is important so an Update.Replace in an upsert operation behaves the way it should!
        /// </summary>
        [BsonIgnoreIfDefault]
        public ObjectId Id { get; protected set; }

        public void ForceId(ObjectId objectId)
        {
            Id = objectId;
        }

        public override string ToString()
        {
            return string.Format("{0} {1}", base.ToString(), Id);
        }
    }

}
