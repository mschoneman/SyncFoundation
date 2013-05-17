using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface IReplicaInfo
    {
        string ReplicaId { get; }
        long ReplicaTickCount { get; }
    }
}
