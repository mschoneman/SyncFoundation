using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core
{
    public class ReplicaInfo : IReplicaInfo
    {
        public string ReplicaId { get; set; }
        public long ReplicaTickCount { get; set; }
    }
}
