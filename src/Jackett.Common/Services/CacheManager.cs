using System;
using System.Collections.Generic;
using System.Text;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Services.Interfaces;

namespace Jackett.Common.Services
{
    public class CacheManager
    {
        private readonly ICacheService _cacheService;

        public CacheManager(ICacheService cacheService)
        {
            _cacheService = cacheService;
        }

        public void CacheResults(IIndexer indexer, TorznabQuery query, List<ReleaseInfo> releases)
        {
            _cacheService.CacheResults(indexer, query, releases);
        }

        public List<ReleaseInfo> Search(IIndexer indexer, TorznabQuery query)
        {
            return _cacheService.Search(indexer, query);
        }

        public IReadOnlyList<TrackerCacheResult> GetCachedResults()
        {
            return _cacheService.GetCachedResults();
        }

        public void CleanIndexerCache(IIndexer indexer)
        {
            _cacheService.CleanIndexerCache(indexer);
        }

        public void CleanCache()
        {
            _cacheService.CleanCache();
        }
    }
}
