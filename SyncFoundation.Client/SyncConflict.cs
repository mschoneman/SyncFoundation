using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Client
{
    public class SyncConflict
    {
        private ISyncableItemInfo _remoteItemInfo;
        private ISyncableItemInfo _localItemInfo;
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
