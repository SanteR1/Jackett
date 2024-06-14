using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Models.Config;
using Jackett.Common.Models.DTO;
using Jackett.Common.Services.Interfaces;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Newtonsoft.Json;
using NLog;
using ServerConfig = Jackett.Common.Models.Config.ServerConfig;

namespace Jackett.Common.Services
{
    public class MongoDBCacheService : ICacheService
    {
        private readonly Logger _logger;
        private readonly string _connectionString;
        private readonly IMongoDatabase _database;
        private readonly IMongoCollection<CacheEntry> _cacheEntries;
        private readonly ServerConfig _serverConfig;

        public MongoDBCacheService(Logger logger, string connectionString, ServerConfig serverConfig)
        {
            _logger = logger;
            _connectionString = connectionString;
            _serverConfig = serverConfig;
            try
            {
                var client = new MongoClient("mongodb://" + connectionString);
                _database = client.GetDatabase("CacheDatabase");
                _cacheEntries = _database.GetCollection<CacheEntry>("CacheEntries");
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed to initialize MongoDB connection");
                throw;
            }
        }

        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            if (query.IsTest)
                return;

            var cacheEntry = new CacheEntry
            {
                IndexerId = indexer.Id,
                QueryHash = GetQueryHash(query),
                Created = DateTime.Now,
                Results = JsonConvert.SerializeObject(releases),
                TrackerName = indexer.Name,
                TrackerType = indexer.Type
            };
            _cacheEntries.InsertOne(cacheEntry);
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            var filter = Builders<CacheEntry>.Filter.And(
                Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id),
                Builders<CacheEntry>.Filter.Eq(e => e.QueryHash, GetQueryHash(query))
            );
            var cacheEntry = _cacheEntries.Find(filter).FirstOrDefault();
            if (cacheEntry != null)
            {
                return JsonConvert.DeserializeObject<List<ReleaseInfo>>(cacheEntry.Results);
            }
            return null;
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            var results = new List<TrackerCacheResult>();
            var cacheEntries = _cacheEntries.Find(_ => true).ToList();
            foreach (var cacheEntry in cacheEntries)
            {
                var releases = JsonConvert.DeserializeObject<List<ReleaseInfo>>(cacheEntry.Results);
                foreach (var release in releases)
                {
                    results.Add(new TrackerCacheResult(release)
                    {
                        FirstSeen = cacheEntry.Created,
                        Tracker = cacheEntry.TrackerName,
                        TrackerId = cacheEntry.IndexerId,
                        TrackerType = cacheEntry.TrackerType
                    });
                }
            }
            return results;
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            var filter = Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id);
            _cacheEntries.DeleteMany(filter);
        }

        public void CleanCache()
        {
            _cacheEntries.DeleteMany(_ => true);
        }

        public TimeSpan CacheTTL => TimeSpan.FromSeconds(_serverConfig.CacheTtl);

        private string GetQueryHash(TorznabQuery query)
        {
            var json = JsonConvert.SerializeObject(query);
            json = json.Replace("\"SearchTerm\":null", "\"SearchTerm\":\"\"");
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                return BitConverter.ToString(sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(json))).Replace("-", "");
            }
        }

        public class CacheEntry
        {
            [BsonId]
            public ObjectId Id { get; set; }
            public string IndexerId { get; set; }
            public string QueryHash { get; set; }
            public DateTime Created { get; set; }
            public string Results { get; set; }
            public string TrackerName { get; set; }
            public string TrackerType { get; set; }
        }
    }
}
