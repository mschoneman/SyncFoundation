using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Sync
{
    interface ISyncableTypeHandler
    {
        string TypeName { get; }
        int TypeId { get; }
        string DbTable { get; }

        long GetRowIdFromItemInfo(ISyncableItemInfo itemInfo);

        void BuildItemData(ISyncableItemInfo itemInfo, JObject builder);
        void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData);
        void DeleteItem(ISyncableItemInfo itemInfo);

        void UpdateInRepos(ISyncableItemInfo itemInfo);
        void RemoveFromRepos(ISyncableItemInfo itemInfo);

        DuplicateStatus GetDuplicateStatus(JObject localItemData, JObject remoteItemData);
    }
}
