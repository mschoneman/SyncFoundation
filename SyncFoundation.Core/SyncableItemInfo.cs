using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using SyncFoundation.Core.Interfaces;

namespace SyncFoundation.Core
{
    public class SyncableItemInfo : ISyncableItemInfo
    {
        public string ItemType { get; set; }
        public IReplicaInfo Created { get; set; }
        public IReplicaInfo Modified { get; set; }
        public bool Deleted { get; set; }
    }
}
