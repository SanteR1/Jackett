using System;
using System.Collections.Generic;
using System.Text;
using Jackett.Common.Models.Config;
using Jackett.Common.Services.Interfaces;
using NLog;

namespace Jackett.Common.Services
{
    public static class CacheServiceFactory
    {
        public static ICacheService CreateCacheService(string cacheType, Logger logger, ServerConfig serverConfig, string connectionString)
        {
            return cacheType.ToLower() switch
            {
                "memory" => new CacheService(logger, serverConfig),
                "sqlite" => new SQLiteCacheService(logger, connectionString),
                "mongodb" => new MongoDBCacheService(logger, connectionString),
                _ => throw new ArgumentException("Invalid cache type specified"),
            };
        }
    }
}
