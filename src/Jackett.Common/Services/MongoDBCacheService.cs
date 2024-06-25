using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Amazon.Runtime.Internal.Util;
using AngleSharp.Dom;
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
                            { "MagnetUri", (BsonValue)release.MagnetUri?.ToString() ?? BsonNull.Value },
                            { "MinimumRatio", (BsonValue)release.MinimumRatio ?? BsonNull.Value },
                            { "MinimumSeedTime", (BsonValue)release.MinimumSeedTime ?? BsonNull.Value },
                            { "DownloadVolumeFactor", (BsonValue)release.DownloadVolumeFactor ?? BsonNull.Value },
                            { "UploadVolumeFactor", (BsonValue)release.UploadVolumeFactor ?? BsonNull.Value }
                        };
                        releaseInfosCollection.InsertOne(document);
                    }
                    _logger.Debug($"CACHE CacheResults / Indexer: {trackerCache.TrackerId} / Added: {releases.Count} releases");


                    PruneCacheByMaxResultsPerIndexer(trackerCache); // remove old results if we exceed the maximum limit
                }
                catch (Exception e)
                {
                    _logger.Error(e, $"Failed CacheResults in indexer {indexer}");
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
                    { "TrackerId", indexer.Id }, { "TrackerName", indexer.Name }, { "TrackerType", indexer.Type }
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
                { "TrackerCacheId", trackerCacheId }, { "QueryHash", GetQueryHash(query) }, { "Created", DateTime.Now }
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
            var releaseInfos = _database.GetCollection<BsonDocument>("ReleaseInfos");

            var results = releaseInfos.Aggregate()
                                      .Lookup("TrackerCacheQueries", "TrackerCacheQueryId", "_id", "TrackerCacheQuery")
                                      .Unwind("TrackerCacheQuery")
                                      .Lookup("TrackerCaches", "TrackerCacheQuery.TrackerCacheId", "_id", "TrackerCache")
                                      .Unwind("TrackerCache").Match(
                                          Builders<BsonDocument>.Filter.And(
                                              Builders<BsonDocument>.Filter.Eq("TrackerCache.TrackerId", indexer.Id),
                                              Builders<BsonDocument>.Filter.Eq("TrackerCacheQuery.QueryHash", queryHash)))
                                      .ToList();

            if (results.Count > 0)
            {
                _logger.Debug($"CACHE Search Hit / Indexer: {indexer.Id} / Found: {results.Count} releases");
                return results.Select(ConvertBsonToReleaseInfo).ToList();
            }

            return null;
        }

        private ReleaseInfo ConvertBsonToReleaseInfo(BsonDocument doc)
        {
            return new ReleaseInfo
            {
                Title = doc["Title"].AsString,
                Guid = new Uri(doc["Guid"].AsString),
                Link = new Uri(doc["Link"].AsString),
                Details = new Uri(doc["Details"].AsString),
                PublishDate = doc["PublishDate"].ToLocalTime(),
                Category = doc["Category"].AsBsonArray.Select(c => c.AsInt32).ToList(),
                Size = doc["Size"].IsInt64 ? doc["Size"].AsInt64 : (long?)null,
                Files = doc["Files"].IsInt64 ? doc["Files"].AsInt64 : (long?)null,
                Grabs = doc["Grabs"].IsInt64 ? doc["Grabs"].AsInt64 : (long?)null,
                Description = doc["Description"].AsString,
                RageID = doc["RageID"].IsInt64 ? doc["RageID"].AsInt64 : (long?)null,
                TVDBId = doc["TVDBId"].IsInt64 ? doc["TVDBId"].AsInt64 : (long?)null,
                Imdb = doc["Imdb"].IsInt64 ? doc["Imdb"].AsInt64 : (long?)null,
                TMDb = doc["TMDb"].IsInt64 ? doc["TMDb"].AsInt64 : (long?)null,
                TVMazeId = doc["TVMazeId"].IsInt64 ? doc["TVMazeId"].AsInt64 : (long?)null,
                TraktId = doc["TraktId"].IsInt64 ? doc["TraktId"].AsInt64 : (long?)null,
                DoubanId = doc["DoubanId"].IsInt64 ? doc["DoubanId"].AsInt64 : (long?)null,
                Genres = doc["Genres"].AsBsonArray.Select(g => g.AsString).ToList(),
                Languages = doc["Languages"].AsBsonArray.Select(l => l.AsString).ToList(),
                Subs = doc["Subs"].AsBsonArray.Select(s => s.AsString).ToList(),
                Year = doc["Year"].IsInt64 ? doc["Year"].AsInt64 : (long?)null,
                Author = doc["Author"].IsBsonNull ? null : doc["Author"].AsString,
                BookTitle = doc["BookTitle"].IsBsonNull ? null : doc["BookTitle"].AsString,
                Publisher = doc["Publisher"].IsBsonNull ? null : doc["Publisher"].AsString,
                Artist = doc["Artist"].IsBsonNull ? null : doc["Artist"].AsString,
                Album = doc["Album"].IsBsonNull ? null : doc["Album"].AsString,
                Label = doc["Label"].IsBsonNull ? null : doc["Label"].AsString,
                Track = doc["Track"].IsBsonNull ? null : doc["Track"].AsString,
                Seeders = doc["Seeders"].IsInt64 ? doc["Seeders"].AsInt64 : (long?)null,
                Peers = doc["Peers"].IsInt64 ? doc["Peers"].AsInt64 : (long?)null,
                Poster = doc["Poster"].IsBsonNull ? null : new Uri(doc["Poster"].AsString),
                InfoHash = doc["InfoHash"].IsBsonNull ? null : doc["InfoHash"].AsString,
                MagnetUri = doc["MagnetUri"].IsBsonNull ? null : new Uri(doc["MagnetUri"].AsString),
                MinimumRatio = doc["MinimumRatio"].IsDouble ? doc["MinimumRatio"].AsDouble : (double?)null,
                MinimumSeedTime = doc["MinimumSeedTime"].IsInt64 ? doc["MinimumSeedTime"].AsInt64 : (long?)null,
                DownloadVolumeFactor =
                    doc["DownloadVolumeFactor"].IsDouble ? doc["DownloadVolumeFactor"].AsDouble : (double?)null,
                UploadVolumeFactor = doc["UploadVolumeFactor"].IsDouble
                    ? doc["UploadVolumeFactor"].AsDouble
                    : (double?)null
            };
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return Array.Empty<TrackerCacheResult>();

            PruneCacheByTtl(); // remove expired results

            var releaseInfos = _database.GetCollection<BsonDocument>("ReleaseInfos");

            var results = releaseInfos.Aggregate()
                                      .Lookup("TrackerCacheQueries", "TrackerCacheQueryId", "_id", "TrackerCacheQuery")
                                      .Unwind("TrackerCacheQuery")
                                      .Lookup("TrackerCaches", "TrackerCacheQuery.TrackerCacheId", "_id", "TrackerCache")
                                      .Unwind("TrackerCache").SortByDescending(doc => doc["PublishDate"]).Limit(3000)
                                      .ToList();

            return results.Select(doc =>
            {
                var releaseInfo = ConvertBsonToReleaseInfo(doc);

                return new TrackerCacheResult(
                    new ReleaseInfo()
                    {
                        // Initialize the properties of the base class (ReleaseInfo) manually
                        Title = releaseInfo.Title,
                        Guid = releaseInfo.Guid,
                        Link = releaseInfo.Link,
                        Details = releaseInfo.Details,
                        PublishDate = releaseInfo.PublishDate,
                        Category = releaseInfo.Category,
                        Size = releaseInfo.Size,
                        Files = releaseInfo.Files,
                        Grabs = releaseInfo.Grabs,
                        Description = releaseInfo.Description,
                        RageID = releaseInfo.RageID,
                        TVDBId = releaseInfo.TVDBId,
                        Imdb = releaseInfo.Imdb,
                        TMDb = releaseInfo.TMDb,
                        TVMazeId = releaseInfo.TVMazeId,
                        TraktId = releaseInfo.TraktId,
                        DoubanId = releaseInfo.DoubanId,
                        Genres = releaseInfo.Genres,
                        Languages = releaseInfo.Languages,
                        Subs = releaseInfo.Subs,
                        Year = releaseInfo.Year,
                        Author = releaseInfo.Author,
                        BookTitle = releaseInfo.BookTitle,
                        Publisher = releaseInfo.Publisher,
                        Artist = releaseInfo.Artist,
                        Album = releaseInfo.Album,
                        Label = releaseInfo.Label,
                        Track = releaseInfo.Track,
                        Seeders = releaseInfo.Seeders,
                        Peers = releaseInfo.Peers,
                        Poster = releaseInfo.Poster,
                        InfoHash = releaseInfo.InfoHash,
                        MagnetUri = releaseInfo.MagnetUri,
                        MinimumRatio = releaseInfo.MinimumRatio,
                        MinimumSeedTime = releaseInfo.MinimumSeedTime,
                        DownloadVolumeFactor = releaseInfo.DownloadVolumeFactor,
                        UploadVolumeFactor = releaseInfo.UploadVolumeFactor,
                        Origin = releaseInfo.Origin
                    })
                {
                    Tracker = doc["TrackerCache"]["TrackerName"].AsString,
                    TrackerId = doc["TrackerCache"]["TrackerId"].AsString,
                    TrackerType = doc["TrackerCache"]["TrackerType"].AsString,
                    FirstSeen = doc["TrackerCacheQuery"]["Created"].ToLocalTime()
                };
            }).ToList();
        }
        
        public void CleanIndexerCache(IIndexer indexer)
        {
            if (indexer == null)
            {
                _logger.Debug("Indexer is null, skipping cache cleaning.");
                return;
            }

            var trackerCachesCollection = _database.GetCollection<BsonDocument>("TrackerCaches");
            var trackerCacheQueriesCollection = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
            var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");

            // Step 1: Find all TrackerCaches documents associated with the given indexer
            var trackerCachesFilter = Builders<BsonDocument>.Filter.Eq("TrackerId", indexer.Id);
            var trackerCachesDocs = trackerCachesCollection.Find(trackerCachesFilter).ToList();

            if (!trackerCachesDocs.Any())
            {
                _logger.Debug($"No TrackerCaches documents found for indexer {indexer.Id}, skipping cache cleaning.");
                return;
            }

            // Step 2: Collect _id values of TrackerCaches documents
            var trackerCachesIds = trackerCachesDocs.Select(doc => doc["_id"].AsObjectId).ToList();

            // Step 3: Find and delete corresponding TrackerCacheQueries documents
            var trackerCacheQueriesFilter = Builders<BsonDocument>.Filter.In("TrackerCacheId", trackerCachesIds);
            var trackerCacheQueriesDocs = trackerCacheQueriesCollection.Find(trackerCacheQueriesFilter).ToList();

            if (trackerCacheQueriesDocs.Any())
            {
                // Step 4: Collect _id values of TrackerCacheQueries documents
                var trackerCacheQueryIds = trackerCacheQueriesDocs.Select(doc => doc["_id"].AsObjectId).ToList();

                // Step 5: Find and delete corresponding ReleaseInfos documents
                var releaseInfosFilter = Builders<BsonDocument>.Filter.In("TrackerCacheQueryId", trackerCacheQueryIds);
                var deleteReleaseInfosResult = releaseInfosCollection.DeleteMany(releaseInfosFilter);
                _logger.Debug($"Deleted {deleteReleaseInfosResult.DeletedCount} documents from ReleaseInfos for indexer {indexer.Id}");

                // Delete TrackerCacheQueries documents
                var deleteTrackerCacheQueriesResult = trackerCacheQueriesCollection.DeleteMany(trackerCacheQueriesFilter);
                _logger.Debug($"Deleted {deleteTrackerCacheQueriesResult.DeletedCount} documents from TrackerCacheQueries for indexer {indexer.Id}");
            }
            else
            {
                _logger.Debug($"No TrackerCacheQueries documents found for TrackerCaches of indexer {indexer.Id}");
            }

            // Step 6: Delete TrackerCaches documents
            var deleteTrackerCachesResult = trackerCachesCollection.DeleteMany(trackerCachesFilter);
            _logger.Debug($"Deleted {deleteTrackerCachesResult.DeletedCount} documents from TrackerCaches for indexer {indexer.Id}");
        }

        public void CleanCache()
        {
            lock (_dbLock)
            {
                _database.DropCollection("ReleaseInfos");
                _database.DropCollection("TrackerCaches");
                _database.DropCollection("TrackerCacheQueries");
            }
        }

        public TimeSpan CacheTTL => TimeSpan.FromSeconds(_serverConfig.CacheTtl);

        private string GetQueryHash(TorznabQuery query)
        {
            var json = GetSerializedQuery(query);
            // Compute the hash
            return BitConverter.ToString(_sha256.ComputeHash(Encoding.UTF8.GetBytes(json)));
        }
        private static string GetSerializedQuery(TorznabQuery query)
        {
            var json = JsonConvert.SerializeObject(query);

            // Changes in the query to improve cache hits
            // Both request must return the same results, if not we are breaking Jackett search
            json = json.Replace("\"SearchTerm\":null", "\"SearchTerm\":\"\"");

            return json;
        }

        public void PruneCacheByTtl()
        {
            if (_serverConfig.CacheTtl <= 0)
            {
                _logger.Debug("Cache TTL is disabled or set to a non-positive value, skipping pruning.");
                return;
            }

            lock (_dbLock)
            {
                var expirationDate = DateTime.Now.AddSeconds(-_serverConfig.CacheTtl);

                var trackerCacheQueriesCollection = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
                var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");
                var trackerCachesCollection = _database.GetCollection<BsonDocument>("TrackerCaches");

                // Step 1: Find expired documents in the TrackerCacheQueries collection
                var trackerCacheQueryFilter = Builders<BsonDocument>.Filter.Lt("Created", expirationDate);
                var expiredTrackerCacheQueryDocs = trackerCacheQueriesCollection.Find(trackerCacheQueryFilter).ToList();

                if (!expiredTrackerCacheQueryDocs.Any())
                {
                    _logger.Debug("No expired documents found in TrackerCacheQueries for pruning.");
                    return;
                }

                // Step 2: Collect _id values of expired TrackerCacheQuery documents
                var expiredTrackerCacheQueryIds = expiredTrackerCacheQueryDocs.Select(doc => doc["_id"].AsObjectId).ToList();

                // Step 3: Delete corresponding entries in the ReleaseInfos collection
                var releaseInfoFilter = Builders<BsonDocument>.Filter.In("TrackerCacheQueryId", expiredTrackerCacheQueryIds);
                var deleteResult1 = releaseInfosCollection.DeleteMany(releaseInfoFilter);
                _logger.Debug($"Pruned {deleteResult1.DeletedCount} documents from ReleaseInfos");

                // Step 4: Collect TrackerCacheId values from the expired TrackerCacheQuery documents
                var expiredTrackerCacheIds =
                    expiredTrackerCacheQueryDocs.Select(doc => doc["TrackerCacheId"].AsObjectId).ToList();

                // Step 5: Delete corresponding entries in the TrackerCaches collection
                var trackerCachesFilter = Builders<BsonDocument>.Filter.In("_id", expiredTrackerCacheIds);
                var deleteResult2 = trackerCachesCollection.DeleteMany(trackerCachesFilter);
                _logger.Debug($"Pruned {deleteResult2.DeletedCount} documents from TrackerCaches");

                // Step 6: Delete expired documents from the TrackerCacheQueries collection
                var deleteResult3 = trackerCacheQueriesCollection.DeleteMany(trackerCacheQueryFilter);
                _logger.Debug($"Pruned {deleteResult3.DeletedCount} documents from TrackerCacheQueries");
            }
        }

        public void PruneCacheByMaxResultsPerIndexer(ObjectId trackerCache)
        {
            var trackerCacheQueriesCollection = _database.GetCollection<BsonDocument>("TrackerCacheQueries");
            var releaseInfosCollection = _database.GetCollection<BsonDocument>("ReleaseInfos");

            // Step 1: Find all TrackerCacheQueries documents for the given TrackerId
            var filter = Builders<BsonDocument>.Filter.Eq("TrackerId", trackerCache.TrackerId);
            var queries = trackerCacheQueriesCollection.Find(filter).ToList();

            if (!queries.Any())
            {
                _logger.Debug($"No TrackerCacheQueries documents found for tracker {trackerCache.TrackerId}");
                return;
            }

            // Step 2: Order queries by Created date descending and calculate the total results count
            var orderedQueries = queries.OrderByDescending(q => q["Created"].ToUniversalTime()).ToList();
            var resultsPerQuery = orderedQueries.Select(q => new { Id = q["_id"].AsObjectId, ResultsCount = q["Results"].AsBsonArray.Count }).ToList();

            int totalResultsCount = resultsPerQuery.Sum(q => q.ResultsCount);

            // Step 3: Prune old queries if total results exceed the limit
            int prunedCounter = 0;
            while (totalResultsCount > _serverConfig.CacheMaxResultsPerIndexer)
            {
                var oldestQuery = resultsPerQuery.Last();
                totalResultsCount -= oldestQuery.ResultsCount;

                // Delete the old query document
                var deleteQueryFilter = Builders<BsonDocument>.Filter.Eq("_id", oldestQuery.Id);
                trackerCacheQueriesCollection.DeleteOne(deleteQueryFilter);

                // Delete related ReleaseInfos documents
                var deleteReleaseInfosFilter = Builders<BsonDocument>.Filter.Eq("TrackerCacheQueryId", oldestQuery.Id);
                releaseInfosCollection.DeleteMany(deleteReleaseInfosFilter);

                resultsPerQuery.Remove(oldestQuery);
                prunedCounter++;
            }

            if (_logger.IsDebugEnabled)
            {
                _logger.Debug($"CACHE PruneCacheByMaxResultsPerIndexer / Indexer: {trackerCache.TrackerId} / Pruned queries: {prunedCounter}");
                //todo PrintCacheStatus();
            }


        }
    }
}
