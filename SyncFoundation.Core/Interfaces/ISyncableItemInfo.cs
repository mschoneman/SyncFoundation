using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncableItemInfo
    {
        string ItemType { get; }
        IReplicaInfo Created { get; }
        IReplicaInfo Modified { get; }
        bool Deleted { get; }
    }
}
