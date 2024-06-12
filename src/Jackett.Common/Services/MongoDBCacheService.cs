using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Services.Interfaces;
using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using NLog;

namespace Jackett.Common.Services
{
    public class MongoDBCacheService : IDatabaseCacheService
    {
        private readonly Logger _logger;
        private readonly IMongoDatabase _database;
        private readonly IMongoCollection<CacheEntry> _collection;
        private readonly SHA256Managed _sha256 = new SHA256Managed();

        public MongoDBCacheService(Logger logger, string connectionString)
        {
            _logger = logger;
            var client = new MongoClient(connectionString);
            _database = client.GetDatabase("CacheDatabase");
            _collection = _database.GetCollection<CacheEntry>("CacheEntries");
            Initialize();
        }

        public void Initialize()
        {
            
        }

        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            var queryHash = GetQueryHash(query);

            var cacheEntry = new CacheEntry
            {
                IndexerId = indexer.Id,
                QueryHash = queryHash,
                Created = DateTime.Now,
                Results = releases,
                TrackerName = indexer.Name,
                TrackerType = indexer.Type
            };

            var filter = Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id) & Builders<CacheEntry>.Filter.Eq(e => e.QueryHash, queryHash);
            _collection.ReplaceOne(filter, cacheEntry, new ReplaceOptions { IsUpsert = true });

            _logger.Debug($"CACHE CacheResults / Indexer: {indexer.Id} / Added: {releases.Count} releases");
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            var queryHash = GetQueryHash(query);

            var filter = Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id) & Builders<CacheEntry>.Filter.Eq(e => e.QueryHash, queryHash);
            var cacheEntry = _collection.Find(filter).FirstOrDefault();

            if (cacheEntry != null)
            {
                _logger.Debug($"CACHE Search Hit / Indexer: {indexer.Id} / Found: {cacheEntry.Results.Count} releases");
                return cacheEntry.Results;
            }
            else
            {
                _logger.Debug($"CACHE Search Miss / Indexer: {indexer.Id}");
                return null;
            }
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            var results = new List<TrackerCacheResult>();

            var cacheEntries = _collection.Find(FilterDefinition<CacheEntry>.Empty).SortByDescending(e => e.Created).Limit(3000).ToList();

            foreach (var entry in cacheEntries)
            {
                foreach (var release in entry.Results)
                {
                    results.Add(new TrackerCacheResult(release)
                    {
                        FirstSeen = entry.Created,
                        Tracker = entry.TrackerName,
                        TrackerId = entry.IndexerId,
                        TrackerType = entry.TrackerType
                    });
                }
            }

            _logger.Debug($"CACHE GetCachedResults / Results: {results.Count}");
            return results;
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            var filter = Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id);
            _collection.DeleteMany(filter);

            _logger.Debug($"CACHE CleanIndexerCache / Indexer: {indexer.Id}");
        }

        public void CleanCache()
        {
            _collection.DeleteMany(FilterDefinition<CacheEntry>.Empty);

            _logger.Debug("CACHE CleanCache");
        }

        public TimeSpan CacheTTL => TimeSpan.FromDays(1); // example TTL

        private string GetQueryHash(TorznabQuery query)
        {
            var json = GetSerializedQuery(query);
            return BitConverter.ToString(_sha256.ComputeHash(Encoding.UTF8.GetBytes(json)));
        }

        private static string GetSerializedQuery(TorznabQuery query)
        {
            var json = JsonConvert.SerializeObject(query);
            json = json.Replace("\"SearchTerm\":null", "\"SearchTerm\":\"\"");
            return json;
        }
    }

    public class CacheEntry
    {
        public ObjectId Id { get; set; }
        public string IndexerId { get; set; }
        public string QueryHash { get; set; }
        public DateTime Created { get; set; }
        public List<ReleaseInfo> Results { get; set; }
        public string TrackerName { get; set; }
        public string TrackerType { get; set; }
    }
}
