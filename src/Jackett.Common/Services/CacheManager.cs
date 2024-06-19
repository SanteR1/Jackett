using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using Jackett.Common.Indexers;
using Jackett.Common.Models;
using Jackett.Common.Models.Config;
using Jackett.Common.Services.Interfaces;
using Microsoft.Extensions.Options;

namespace Jackett.Common.Services
{
    public class CacheManager
    {
        private readonly CacheServiceFactory _factory;
        private ICacheService _cacheService;

        public CacheManager(CacheServiceFactory factory, ServerConfig serverConfig)
        {
            _factory = factory;
            _cacheService = factory.CreateCacheService(serverConfig.CacheType, serverConfig.ConnectionString);
        }

        public ICacheService CurrentCacheService => _cacheService;

        public void UpdateCacheService(ICacheService cacheService)
        {
            _cacheService = cacheService;
        }

        public void ChangeCacheType(CacheType newCacheType, string str)
        {
            if (CurrentCacheService is CacheService && newCacheType != CacheType.Memory)
            {
                CurrentCacheService.CleanCache();
            }

            _cacheService = _factory.CreateCacheService(newCacheType, str);
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