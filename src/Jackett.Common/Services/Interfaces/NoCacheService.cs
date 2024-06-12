using System;
using System.Collections.Generic;
using System.Text;
using Jackett.Common.Indexers;
using Jackett.Common.Models;

namespace Jackett.Common.Services.Interfaces
{
    public class NoCacheService : ICacheService
    {
        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            // No operation
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            // No operation
            return null;
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            // No operation
            return Array.Empty<TrackerCacheResult>();
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            // No operation
        }

        public void CleanCache()
        {
            // No operation
        }

        public TimeSpan CacheTTL => TimeSpan.Zero; // No cache expiration
    }
}
