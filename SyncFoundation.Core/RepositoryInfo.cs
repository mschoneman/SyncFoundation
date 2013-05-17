using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core
{
    public class RepositoryInfo : IRepositoryInfo
    {
        public string RepositoryID { get; set; }
        public long RepositoryTickCount { get; set; }
    }
}
