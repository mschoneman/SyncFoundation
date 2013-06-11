using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;

namespace SyncFoundation.Client
{
    public class SyncConflict
    {
        private readonly ISyncableItemInfo _remoteItemInfo;
        private readonly ISyncableItemInfo _localItemInfo;
        private SyncStatus _syncStatus;
        private long _itemId;
        internal SyncConflict(long itemId, SyncStatus syncStatus, ISyncableItemInfo localItemInfo, ISyncableItemInfo remoteItemInfo)
        {
            _itemId = itemId;
            _syncStatus = syncStatus;
            _remoteItemInfo = remoteItemInfo;
            _localItemInfo = localItemInfo;
        }

        internal ISyncableItemInfo RemoteItemInfo
        {
            get
            {
                return _remoteItemInfo;
            }
        }
        internal ISyncableItemInfo LocalItemInfo
        {
            get
            {
                return _localItemInfo;
            }
        }
    }
}
