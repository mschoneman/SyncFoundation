﻿using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncableStore : IDisposable
    {
        IEnumerable<string> GetItemTypes();

        long IncrementLocalRepositoryTickCount();

        IEnumerable<IRepositoryInfo> GenerateLocalKnowledge();
        IEnumerable<ISyncableItemInfo> LocateChangedItems(IEnumerable<IRepositoryInfo> remoteKnowledge);

        ISyncableItemInfo LocateCurrentItemInfo(ISyncableItemInfo source);

        void BuildItemData(ISyncableItemInfo itemInfo, JObject itemData);

        void BeginChanges();

        void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData);
        void DeleteItem(ISyncableItemInfo itemInfo);
        void UpdateLocalKnowledge(IEnumerable<IRepositoryInfo> remoteKnowledge);

        void AcceptChanges();
        void RejectChanges();

        DuplicateStatus GetDuplicateStatus(string itemType, JObject localItemData, JObject remoteItemData);
    }
}
