using System;
using System.Collections.Generic;
using System.Text;
using Autofac;
using Jackett.Common.Models.Config;
using Jackett.Common.Services.Interfaces;
using NLog;

namespace Jackett.Common.Services
{
    public class CacheServiceFactory
    {
        private readonly IComponentContext _context;

        public CacheServiceFactory(IComponentContext context)
        {
            _context = context;
        }

        public ICacheService CreateCacheService(CacheType cacheType)
        {
            switch (cacheType)
            {
                case CacheType.Memory:
                    return _context.Resolve<CacheService>();
                case CacheType.SqLite:
                    return _context.Resolve<SQLiteCacheService>();
                case CacheType.MongoDb:
                    return _context.Resolve<MongoDBCacheService>();
                case CacheType.Disabled:
                    return _context.Resolve<NoCacheService>();
                default:
                    throw new ArgumentException($"Unknown cache type: {cacheType}");
            }
        }
    }
}
