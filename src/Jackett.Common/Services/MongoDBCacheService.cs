using System;
using System.Collections.Generic;
using System.Linq;
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
        private readonly ServerConfig _serverConfig;
        private readonly IMongoDatabase _database;
        private readonly SHA256Managed _sha256 = new SHA256Managed();
        private readonly object _dbLock = new object();

        public MongoDBCacheService(Logger logger, string connectionString, ServerConfig serverConfig)
        {
            _logger = logger;
            _connectionString = connectionString;
            _serverConfig = serverConfig;
            try
            {
                var client = new MongoClient("mongodb://" + _connectionString);
                _database = client.GetDatabase("CacheDatabase");
                Initialize();
            }
            catch (Exception ex)
            {
                _logger.Error(ex, "Failed to initialize MongoDB connection");
            }
        }

        public void Initialize()
        {
            var trackerCaches = _database.GetCollection<BsonDocument>("TrackerCaches");
            var trackerCacheQueries = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
            var releaseInfos = _database.GetCollection<BsonDocument>("ReleaseInfos");

            // Create indexes or any initialization if needed
        }

        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            if (query.IsTest)
                return;

            lock (_dbLock)
            {
                try
                {
                    var trackerCacheId = GetOrAddTrackerCache(indexer);
                    var trackerCacheQueryId = AddTrackerCacheQuery(trackerCacheId, query);

                    var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");

                    foreach (var release in releases)
                    {
                        var document = new BsonDocument
                    {
                        { "TrackerCacheQueryId", trackerCacheQueryId },
                        { "Title", release.Title },
                        { "Guid", release.Guid?.ToString() },
                        { "Link", release.Link?.ToString() },
                        { "Details", release.Details?.ToString() },
                        { "PublishDate", release.PublishDate },
                        { "Category", new BsonArray(release.Category) },
                        { "Size", release.Size },
                        { "Files", (BsonValue)release.Files ?? BsonNull.Value },
                        { "Grabs", (BsonValue)release.Grabs ?? BsonNull.Value },
                        { "Description", release.Description },
                        { "RageID", (BsonValue)release.RageID ?? BsonNull.Value },
                        { "TVDBId", (BsonValue)release.TVDBId ?? BsonNull.Value },
                        { "Imdb", (BsonValue)release.Imdb ?? BsonNull.Value },
                        { "TMDb", (BsonValue)release.TMDb ?? BsonNull.Value },
                        { "TVMazeId", (BsonValue)release.TVMazeId ?? BsonNull.Value },
                        { "TraktId", (BsonValue)release.TraktId ?? BsonNull.Value },
                        { "DoubanId", (BsonValue)release.DoubanId ?? BsonNull.Value },
                        { "Genres", new BsonArray(release.Genres ?? new List<string>()) },
                        { "Languages", new BsonArray(release.Languages ?? new List<string>()) },
                        { "Subs", new BsonArray(release.Subs ?? new List<string>()) },
                        { "Year", (BsonValue)release.Year ?? BsonNull.Value },
                        { "Author", (BsonValue)release.Author ?? BsonNull.Value },
                        { "BookTitle", (BsonValue)release.BookTitle ?? BsonNull.Value },
                        { "Publisher", (BsonValue)release.Publisher ?? BsonNull.Value },
                        { "Artist", (BsonValue)release.Artist ?? BsonNull.Value },
                        { "Album", (BsonValue)release.Album ?? BsonNull.Value },
                        { "Label", (BsonValue)release.Label ?? BsonNull.Value },
                        { "Track", (BsonValue)release.Track ?? BsonNull.Value },
                        { "Seeders", (BsonValue)release.Seeders ?? BsonNull.Value },
                        { "Peers", (BsonValue)release.Peers ?? BsonNull.Value },
                        { "Poster", (BsonValue)release.Poster?.ToString() ?? BsonNull.Value },
                        { "InfoHash", (BsonValue)release.InfoHash ?? BsonNull.Value },
                        { "MagnetUri", (BsonValue) release.MagnetUri ?.ToString() ?? BsonNull.Value },
                        { "MinimumRatio", (BsonValue)release.MinimumRatio ?? BsonNull.Value },
                        { "MinimumSeedTime", (BsonValue) release.MinimumSeedTime ?? BsonNull.Value },
                        { "DownloadVolumeFactor", (BsonValue) release.DownloadVolumeFactor ?? BsonNull.Value },
                        { "UploadVolumeFactor", (BsonValue) release.UploadVolumeFactor ?? BsonNull.Value }
                    };
                        releaseInfosCollection.InsertOne(document);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            }
        }

        private ObjectId GetOrAddTrackerCache(IIndexer indexer)
        {
            var trackerCachesCollection = _database.GetCollection<BsonDocument>("TrackerCaches");
            var filter = Builders<BsonDocument>.Filter.Eq("TrackerId", indexer.Id);
            var trackerCache = trackerCachesCollection.Find(filter).FirstOrDefault();

            if (trackerCache == null)
            {
                var document = new BsonDocument
                {
                    { "TrackerId", indexer.Id },
                    { "TrackerName", indexer.Name },
                    { "TrackerType", indexer.Type }
                };
                trackerCachesCollection.InsertOne(document);
                return document["_id"].AsObjectId;
            }

            return trackerCache["_id"].AsObjectId;
        }

        private ObjectId AddTrackerCacheQuery(ObjectId trackerCacheId, TorznabQuery query)
        {
            var trackerCacheQueriesCollection = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
            var document = new BsonDocument
            {
                { "TrackerCacheId", trackerCacheId },
                { "QueryHash", GetQueryHash(query) },
                { "Created", DateTime.Now }
            };
            trackerCacheQueriesCollection.InsertOne(document);
            return document["_id"].AsObjectId;
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return null;

            PruneCacheByTtl();

            var queryHash = GetQueryHash(query);
            var filter = Builders<BsonDocument>.Filter.And(
                Builders<BsonDocument>.Filter.Eq("TrackerId", indexer.Id),
                Builders<BsonDocument>.Filter.Eq("QueryHash", queryHash)
            );

            var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");
            var results = new List<ReleaseInfo>();

            using (var cursor = releaseInfosCollection.Find(filter).ToCursor())
            {
                while (cursor.MoveNext())
                {
                    foreach (var doc in cursor.Current)
                    {
                        results.Add(new ReleaseInfo
                        {
                            Title = doc["Title"].AsString,
                            Guid = new Uri(doc["Guid"].AsString),
                            Link = new Uri(doc["Link"].AsString),
                            Details = new Uri(doc["Details"].AsString),
                            PublishDate = doc["PublishDate"].ToUniversalTime(),
                            Category = doc["Category"].AsBsonArray.Select(v => v.AsInt32).ToList(),
                            Size = doc["Size"].AsNullableInt64,
                            Files = doc["Files"].AsNullableInt64,
                            Grabs = doc["Grabs"].AsNullableInt64,
                            Description = doc["Description"].AsString,
                            RageID = doc["RageID"].AsNullableInt64,
                            TVDBId = doc["TVDBId"].AsNullableInt64,
                            Imdb = doc["Imdb"].AsNullableInt64,
                            TMDb = doc["TMDb"].AsNullableInt64,
                            TVMazeId = doc["TVMazeId"].AsNullableInt64,
                            TraktId = doc["TraktId"].AsNullableInt64,
                            DoubanId = doc["DoubanId"].AsNullableInt64,
                            Genres = doc["Genres"].AsBsonArray.Select(v => v.AsString).ToList(),
                            Languages = doc["Languages"].AsBsonArray.Select(v => v.AsString).ToList(),
                            Subs = doc["Subs"].AsBsonArray.Select(v => v.AsString).ToList(),
                            Year = doc["Year"].AsNullableInt64,
                            Author = doc["Author"].AsString,
                            BookTitle = doc["BookTitle"].AsString,
                            Publisher = doc["Publisher"].AsString,
                            Artist = doc["Artist"].AsString,
                            Album = doc["Album"].AsString,
                            Label = doc["Label"].AsString,
                            Track = doc["Track"].AsString,
                            Seeders = doc["Seeders"].AsNullableInt64,
                            Peers = doc["Peers"].AsNullableInt64,
                            Poster = new Uri(doc["Poster"].AsString),
                            InfoHash = doc["InfoHash"].AsString,
                            MagnetUri = new Uri(doc["MagnetUri"].AsString),
                            MinimumRatio = doc["MinimumRatio"].AsNullableDouble,
                            MinimumSeedTime = doc["MinimumSeedTime"].AsNullableInt64,
                            DownloadVolumeFactor = doc["DownloadVolumeFactor"].AsNullableDouble,
                            UploadVolumeFactor = doc["UploadVolumeFactor"].AsNullableDouble
                        });
                    }
                }
            }

            if (results.Count > 0)
            {
                _logger.Debug($"CACHE Search Hit / Indexer: {indexer.Id} / Found: {results.Count} releases");
                return results;
            }

            return null;
        }


        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            lock (_dbLock)
            {
                if (_serverConfig.CacheType == CacheType.Disabled)
                    return Array.Empty<TrackerCacheResult>();

                var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");
                var releaseInfos = releaseInfosCollection.Find(FilterDefinition<BsonDocument>.Empty).ToList();
                var results = new List<TrackerCacheResult>();

                foreach (var doc in releaseInfos)
                {
                    var trackerCacheQueryId = doc["TrackerCacheQueryId"].AsObjectId;
                    var queryFilter = Builders<BsonDocument>.Filter.Eq("_id", trackerCacheQueryId);
                    var trackerCacheQueriesCollection = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
                    var queryDoc = trackerCacheQueriesCollection.Find(queryFilter).FirstOrDefault();

                    results.Add(new TrackerCacheResult(new ReleaseInfo
                    {
                        Title = doc["Title"].AsString,
                        Guid = new Uri(doc["Guid"].AsString),
                        Link = new Uri(doc["Link"].AsString),
                        Details = new Uri(doc["Details"].AsString),
                        PublishDate = doc["PublishDate"].ToUniversalTime(),
                        Category = doc["Category"].AsBsonArray.Select(v => v.AsInt32).ToList(),
                        Size = doc["Size"].AsNullableInt64,
                        Files = doc["Files"].AsNullableInt64,
                        Grabs = doc["Grabs"].AsNullableInt64,
                        Description = doc["Description"].AsString,
                        RageID = doc["RageID"].AsNullableInt64,
                        TVDBId = doc["TVDBId"].AsNullableInt64,
                        Imdb = doc["Imdb"].AsNullableInt64,
                        TMDb = doc["TMDb"].AsNullableInt64,
                        TVMazeId = doc["TVMazeId"].AsNullableInt64,
                        TraktId = doc["TraktId"].AsNullableInt64,
                        DoubanId = doc["DoubanId"].AsNullableInt64,
                        Genres = doc["Genres"].AsBsonArray.Select(v => v.AsString).ToList(),
                        Languages = doc["Languages"].AsBsonArray.Select(v => v.AsString).ToList(),
                        Subs = doc["Subs"].AsBsonArray.Select(v => v.AsString).ToList(),
                        Year = doc["Year"].AsNullableInt64,
                        Author = doc["Author"].AsString,
                        BookTitle = doc["BookTitle"].AsString,
                        Publisher = doc["Publisher"].AsString,
                        Artist = doc["Artist"].AsString,
                        Album = doc["Album"].AsString,
                        Label = doc["Label"].AsString,
                        Track = doc["Track"].AsString,
                        Seeders = doc["Seeders"].AsNullableInt64,
                        Peers = doc["Peers"].AsNullableInt64,
                        Poster = new Uri(doc["Poster"].AsString),
                        InfoHash = doc["InfoHash"].AsString,
                        MagnetUri = new Uri(doc["MagnetUri"].AsString),
                        MinimumRatio = doc["MinimumRatio"].AsNullableDouble,
                        MinimumSeedTime = doc["MinimumSeedTime"].AsNullableInt64,
                        DownloadVolumeFactor = doc["DownloadVolumeFactor"].AsNullableDouble,
                        UploadVolumeFactor = doc["UploadVolumeFactor"].AsNullableDouble
                    })
                    {
                        TrackerId = queryDoc["TrackerId"].AsString,
                        Tracker = queryDoc["Tracker"].AsString,
                        TrackerType = queryDoc["TrackerType"].AsString
                        //Id = queryDoc["_id"].AsObjectId,
                        //TrackerCacheId = queryDoc["TrackerCacheId"].AsObjectId,
                        //QueryHash = queryDoc["QueryHash"].AsString,
                        //Created = queryDoc["Created"].ToUniversalTime()

                    });
                }

                return results;
            }
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            //var filter = Builders<CacheEntry>.Filter.Eq(e => e.IndexerId, indexer.Id);
            //_cacheEntries.DeleteMany(filter);
        }

        public void CleanCache()
        {
            lock (_dbLock)
            {
                _database.DropCollection("ReleaseInfos");
                _database.DropCollection("TrackerCaches");
                _database.DropCollection("TrackerCacheQueries");
            }

            //_cacheEntries.DeleteMany(_ => true);
        }

        public TimeSpan CacheTTL => TimeSpan.FromSeconds(_serverConfig.CacheTtl);

        private string GetQueryHash(TorznabQuery query)
        {
            var inputBytes = System.Text.Encoding.UTF8.GetBytes(query.ToString());
            var hashBytes = _sha256.ComputeHash(inputBytes);
            return Convert.ToBase64String(hashBytes);
        }

        public void PruneCacheByTtl()
        {
            var expirationDate = DateTime.Now.AddSeconds(-_serverConfig.CacheTtl);
            var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");
            var filter = Builders<BsonDocument>.Filter.Lt("Created", expirationDate);
            releaseInfosCollection.DeleteMany(filter);
        }
    }
}
