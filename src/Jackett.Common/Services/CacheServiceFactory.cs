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

        public ICacheService CreateCacheService(CacheType cacheType, string str)
        {
            return cacheType switch
            {
                CacheType.Memory => _context.Resolve<CacheService>(),
                CacheType.SqLite => _context.Resolve<SQLiteCacheService>(
                    new TypedParameter(typeof(string), str)),
                CacheType.MongoDb => _context.Resolve<MongoDBCacheService>(),
                CacheType.Disabled => _context.Resolve<NoCacheService>(),
                _ => throw new ArgumentOutOfRangeException(nameof(cacheType), cacheType, null)
            };
        }
    }
}
