using System;
using Autofac;
using Jackett.Common.Models.Config;
using Jackett.Common.Services.Interfaces;

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
            switch (cacheType)
            {
                case CacheType.Memory:
                    var memoryCacheService = _context.Resolve<CacheService>();
                    memoryCacheService.UpdateConnectionString(str);
                    return memoryCacheService;
                case CacheType.SqLite:
                    var sqliteCacheService = _context.Resolve<SQLiteCacheService>();
                    sqliteCacheService.UpdateConnectionString(str);
                    return sqliteCacheService;
                case CacheType.MongoDb:
                    var mongoDbService = _context.Resolve<MongoDBCacheService>();
                    var mongoDbConfigurable = mongoDbService as ICacheService;
                    mongoDbConfigurable?.UpdateConnectionString(str);
                    return mongoDbService;
                case CacheType.Disabled:
                    var noCacheService = _context.Resolve<NoCacheService>();
                    noCacheService.UpdateConnectionString(str);
                    return noCacheService;
                default:
                    throw new ArgumentOutOfRangeException(nameof(cacheType), cacheType, null);
            }
        }
    }
}
