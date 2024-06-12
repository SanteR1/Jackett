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

        public ICacheService CreateCacheService(string cacheType)
        {
            switch (cacheType)
            {
                case "Memory":
                    return _context.Resolve<CacheService>();
                case "SQLite":
                    return _context.Resolve<SQLiteCacheService>();
                case "MongoDB":
                    return _context.Resolve<MongoDBCacheService>();
                case "None":
                    return _context.Resolve<NoCacheService>();
                default:
                    throw new ArgumentException($"Unknown cache type: {cacheType}");
            }
        }
    }
}
