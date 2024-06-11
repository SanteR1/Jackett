using System;
using System.Collections.Generic;
using System.Text;

namespace Jackett.Common.Models.Config
{
    public enum CacheType
    {
        Disabled = -1,
        Memory,
        SqLite,
        MongoDb
    }
}
