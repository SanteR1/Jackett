using System;
using System.Collections.Generic;
using System.Text;

namespace Jackett.Common.Services
{
    public static class CacheManagerProvider
    {
        private static CacheManager _cacheManager;

        public static CacheManager CacheManager
        {
            get
            {
                if (_cacheManager == null)
                {
                    _cacheManager = ServiceLocator.GetService<CacheManager>();
                }
                return _cacheManager;
            }
        }
    }
}
