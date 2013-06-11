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
