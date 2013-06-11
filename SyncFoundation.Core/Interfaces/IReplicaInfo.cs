namespace SyncFoundation.Core.Interfaces
{
    public interface IReplicaInfo
    {
        string ReplicaId { get; }
        long ReplicaTickCount { get; }
    }
}
