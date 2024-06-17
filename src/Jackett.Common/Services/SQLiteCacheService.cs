using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Amazon.Auth.AccessControlPolicy;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Models.Config;
using Jackett.Common.Models.DTO;
using Jackett.Common.Services.Interfaces;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using NLog;
using ServerConfig = Jackett.Common.Models.Config.ServerConfig;

namespace Jackett.Common.Services
{
    public class SQLiteCacheService : ICacheService
    {
        private readonly Logger _logger;
        private readonly string _connectionString;
        private readonly ServerConfig _serverConfig;
        private readonly SHA256Managed _sha256 = new SHA256Managed();

        public SQLiteCacheService(Logger logger, string connectionString, ServerConfig serverConfig)
        {
            _logger = logger;
            _connectionString = connectionString;
            _serverConfig = serverConfig;
            Initialize();
        }

        public void Initialize()
        {
            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                @"
                CREATE TABLE IF NOT EXISTS CacheEntries (
                    Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    IndexerId TEXT,
                    QueryHash TEXT,
                    Created TEXT,
                    Results TEXT,
                    TrackerName TEXT,
                    TrackerType TEXT
                )
            ";
                command.ExecuteNonQuery();
            }
        }

        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            if (query.IsTest)
                return;

            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                @"
                INSERT INTO CacheEntries (IndexerId, QueryHash, Created, Results, TrackerName, TrackerType)
                VALUES ($indexerId, $queryHash, $created, $results, $trackerName, $trackerType)
            ";
                command.Parameters.AddWithValue("$indexerId", indexer.Id);
                command.Parameters.AddWithValue("$queryHash", GetQueryHash(query));
                command.Parameters.AddWithValue("$created", DateTime.Now.ToString("o"));
                command.Parameters.AddWithValue("$results", JsonConvert.SerializeObject(releases));
                command.Parameters.AddWithValue("$trackerName", indexer.Name);
                command.Parameters.AddWithValue("$trackerType", indexer.Type);
                command.ExecuteNonQuery();
            }
            // remove old results if we exceed the maximum limit
            var trackerCache = new TrackerCache { TrackerId = indexer.Id };
            PruneCacheByMaxResultsPerIndexer(trackerCache);
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return null;

            PruneCacheByTtl();

            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                @"
                SELECT Results FROM CacheEntries
                WHERE IndexerId = $indexerId AND QueryHash = $queryHash
            ";
                command.Parameters.AddWithValue("$indexerId", indexer.Id);
                command.Parameters.AddWithValue("$queryHash", GetQueryHash(query));
                using (var reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        var resultsJson = reader.GetString(0);
                        return JsonConvert.DeserializeObject<List<ReleaseInfo>>(resultsJson);
                    }
                }
            }
            return null;
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return Array.Empty<TrackerCacheResult>();

            PruneCacheByTtl(); // remove expired results

            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "SELECT * FROM CacheEntries";
                using (var reader = command.ExecuteReader())
                {
                    var results = new List<TrackerCacheResult>();
                    while (reader.Read())
                    {
                        var resultsJson = reader.GetString(3);
                        var releases = JsonConvert.DeserializeObject<List<ReleaseInfo>>(resultsJson);
                        foreach (var release in releases)
                        {
                            results.Add(new TrackerCacheResult(release)
                            {
                                FirstSeen = DateTime.Parse(reader.GetString(2)),
                                Tracker = reader.GetString(4),
                                TrackerId = reader.GetString(1),
                                TrackerType = reader.GetString(5)
                            });
                        }
                    }

                    results = results.GroupBy(r => r.Guid)
                                     .Select(g => g.First())
                                     .OrderByDescending(i => i.PublishDate)
                                     .Take(3000)
                                     .ToList();

                    _logger.Debug($"CACHE GetCachedResults / Results: {results.Count} (cache may contain more results)");
                    PrintCacheStatus();

                    return results;
                }
            }


        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return;

            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM CacheEntries WHERE IndexerId = $indexerId";
                command.Parameters.AddWithValue("$indexerId", indexer.Id);
                command.ExecuteNonQuery();
            }
            _logger.Debug($"CACHE CleanIndexerCache / Indexer: {indexer.Id}");

            PruneCacheByTtl(); // remove expired results
        }

        public void CleanCache()
        {
            if (_serverConfig.CacheType == CacheType.Disabled)
                return;

            using (var connection = new SqliteConnection("Data Source =" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM CacheEntries";
                command.ExecuteNonQuery();
            }

            _logger.Debug("CACHE CleanCache");
        }

        public void PruneCacheByTtl()
        {
            using (var connection = new SqliteConnection("Data Source=" + _connectionString))
            {
                connection.Open();
                var expirationDate = DateTime.Now.AddSeconds(-_serverConfig.CacheTtl);//.ToString("o");
                var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM CacheEntries WHERE Created < $expirationDate";
                command.Parameters.AddWithValue("$expirationDate", expirationDate);
                var prunedCounter = command.ExecuteNonQuery();

                _logger.Debug($"CACHE PruneCacheByTtl / Pruned queries: {prunedCounter}");
                PrintCacheStatus();

            }
        }

        public void PruneCacheByMaxResultsPerIndexer(TrackerCache trackerCache)
        {
            using (var connection = new SqliteConnection("Data Source=" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText =
                    @"
            SELECT QueryHash, Created, LENGTH(Results) AS ResultsLength
            FROM CacheEntries
            WHERE IndexerId = $indexerId
            ORDER BY Created DESC
            ";
                command.Parameters.AddWithValue("$indexerId", trackerCache.TrackerId);
                using (var reader = command.ExecuteReader())
                {
                    var resultsPerQuery = new List<Tuple<string, int>>();
                    while (reader.Read())
                    {
                        resultsPerQuery.Add(new Tuple<string, int>(reader.GetString(0), reader.GetInt32(2)));
                    }

                    var prunedCounter = 0;
                    while (true)
                    {
                        var total = resultsPerQuery.Select(q => q.Item2).Sum();
                        if (total <= _serverConfig.CacheMaxResultsPerIndexer)
                            break;
                        var olderQuery = resultsPerQuery.Last();
                        var deleteCommand = connection.CreateCommand();
                        deleteCommand.CommandText = "DELETE FROM CacheEntries WHERE QueryHash = $queryHash";
                        deleteCommand.Parameters.AddWithValue("$queryHash", olderQuery.Item1);
                        deleteCommand.ExecuteNonQuery();
                        resultsPerQuery.Remove(olderQuery);
                        prunedCounter++;
                    }

                    if (_logger.IsDebugEnabled)
                    {
                        _logger.Debug($"CACHE PruneCacheByMaxResultsPerIndexer / Indexer: {trackerCache.TrackerId} / Pruned queries: {prunedCounter}");
                        PrintCacheStatus();
                    }
                }
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

        public void PrintCacheStatus()
        {
            using (var connection = new SqliteConnection("Data Source=" + _connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM CacheEntries";
                var totalCount = Convert.ToInt32(command.ExecuteScalar());
                _logger.Debug($"CACHE STATUS / Total cache entries: {totalCount}");
            }
        }
    }
}
