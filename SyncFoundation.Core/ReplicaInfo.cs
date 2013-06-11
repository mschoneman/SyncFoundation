using SyncFoundation.Core.Interfaces;

namespace SyncFoundation.Core
{
    public class ReplicaInfo : IReplicaInfo
    {
        public string ReplicaId { get; set; }
        public long ReplicaTickCount { get; set; }
    }
}
