using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Services.Interfaces;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using NLog;

namespace Jackett.Common.Services
{
    public class SQLiteCacheService : IDatabaseCacheService
    {
        private readonly Logger _logger;
        private readonly string _connectionString;

        public SQLiteCacheService(Logger logger, string connectionString)
        {
            _logger = logger;
            _connectionString = connectionString;
            Initialize();
        }

        public void Initialize()
        {
            using (var connection = new SqliteConnection(_connectionString))
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

            using (var connection = new SqliteConnection(_connectionString))
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
            
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            using (var connection = new SqliteConnection(_connectionString))
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
            using (var connection = new SqliteConnection(_connectionString))
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
                    return results;
                }
            }
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM CacheEntries WHERE IndexerId = $indexerId";
                command.Parameters.AddWithValue("$indexerId", indexer.Id);
                command.ExecuteNonQuery();
            }
        }

        public void CleanCache()
        {
            using (var connection = new SqliteConnection(_connectionString))
            {
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = "DELETE FROM CacheEntries";
                command.ExecuteNonQuery();
            }
        }

        public TimeSpan CacheTTL => TimeSpan.FromSeconds(3600); // Example TTL

        private string GetQueryHash(TorznabQuery query)
        {
            var json = JsonConvert.SerializeObject(query);
            json = json.Replace("\"SearchTerm\":null", "\"SearchTerm\":\"\"");
            using (var sha256 = System.Security.Cryptography.SHA256.Create())
            {
                return BitConverter.ToString(sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes(json))).Replace("-", "");
            }
        }
    }
}
