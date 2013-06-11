using Newtonsoft.Json.Linq;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace SyncFoundation.Core
{
    public static class SyncUtil
    {
        public static JArray KnowledgeToJson(IEnumerable<IReplicaInfo> knowledge)
        {
            var knowledgeJson = new JArray();
            foreach (IReplicaInfo reposInfo in knowledge)
            {
                var item = new JObject
                    {
                        {"replicaID", reposInfo.ReplicaId},
                        {"replicaTickCount", reposInfo.ReplicaTickCount}
                    };
                knowledgeJson.Add(item);
            }
            return knowledgeJson;
        }

        public static IEnumerable<ReplicaInfo> KnowledgeFromJson(JToken knowledgeJson)
        {
            return (from item in knowledgeJson
                    let id = (string) item["replicaID"]
                    let tick = (long) item["replicaTickCount"]
                    select new ReplicaInfo {ReplicaId = id, ReplicaTickCount = tick}).ToList();
        }

        public static bool KnowledgeContains(IEnumerable<IReplicaInfo> knowledge, IReplicaInfo replicaInfo)
        {
            return (from reposInfo in knowledge
                    where reposInfo.ReplicaId == replicaInfo.ReplicaId
                    select reposInfo.ReplicaTickCount >= replicaInfo.ReplicaTickCount).FirstOrDefault();
        }

        public static ISyncableItemInfo SyncableItemInfoFromJson(JToken jToken)
        {
            var itemType = (string) jToken["itemType"];

            var globalCreationId = (string) jToken["creationReplicaID"];
            var creationTickCount = (long) jToken["creationReplicaTickCount"];
            var created = new ReplicaInfo {ReplicaId = globalCreationId, ReplicaTickCount = creationTickCount};

            var globalModificationId = (string) jToken["modificationReplicaID"];
            var modificationTickCount = (long) jToken["modificationReplicaTickCount"];
            var modified = new ReplicaInfo {ReplicaId = globalModificationId, ReplicaTickCount = modificationTickCount};

            var deleted = (jToken["deleted"] != null) && (bool) jToken["deleted"];

            return new SyncableItemInfo {ItemType = itemType, Created = created, Modified = modified, Deleted = deleted};
        }

        public static JObject SyncableItemInfoToJson(ISyncableItemInfo syncItemInfo)
        {
            var item = new JObject
                {
                    {"itemType", syncItemInfo.ItemType},
                    {"creationReplicaID", syncItemInfo.Created.ReplicaId},
                    {"creationReplicaTickCount", syncItemInfo.Created.ReplicaTickCount},
                    {"modificationReplicaID", syncItemInfo.Modified.ReplicaId},
                    {"modificationReplicaTickCount", syncItemInfo.Modified.ReplicaTickCount},
                    {"deleted", syncItemInfo.Deleted}
                };
            return item;
        }

        public static SyncStatus CalculateSyncStatus(ISyncableItemInfo remoteSyncableItemInfo,
                                                     ISyncableItemInfo localSyncableItemInfo,
                                                     IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            if (localSyncableItemInfo == null) // We have no knowledge of the remote item
            {
                if (remoteSyncableItemInfo.Deleted)
                    return SyncStatus.DeleteNonExisting;
                return SyncStatus.Insert;
            }

            if (localSyncableItemInfo.Deleted && remoteSyncableItemInfo.Deleted)
                return SyncStatus.DeleteNonExisting;

            if (KnowledgeContains(remoteKnowledge, localSyncableItemInfo.Modified))
            {
                if (remoteSyncableItemInfo.Deleted)
                    return SyncStatus.Delete;
                if (localSyncableItemInfo.Deleted)
                    return SyncStatus.Insert; // Should never happen
                return SyncStatus.Update;
            }

            if (localSyncableItemInfo.Deleted)
                return SyncStatus.DeleteConflict;
            if (remoteSyncableItemInfo.Deleted)
                return SyncStatus.DeleteConflict;
            return SyncStatus.UpdateConflict;
        }

        public static JObject JsonItemFromSyncableItemInfo(ISyncableItemInfo syncItemInfo)
        {
            var item = SyncableItemInfoToJson(syncItemInfo);
            item.Add("itemRefs", new JArray());
            return item;
        }

        public static JArray JsonItemArrayFromSyncableItemInfoEnumberable(
            IEnumerable<ISyncableItemInfo> syncItemInfoEnumerable)
        {
            var items = new JArray();
            foreach (ISyncableItemInfo syncItemInfo in syncItemInfoEnumerable)
            {
                items.Add(SyncableItemInfoToJson(syncItemInfo));
            }
            return items;
        }


        private static JObject JsonItemRefFromSyncableItemInfo(ISyncableItemInfo syncItemInfo)
        {
            var item = new JObject
                {
                    {"itemType", syncItemInfo.ItemType},
                    {"creationReplicaID", syncItemInfo.Created.ReplicaId},
                    {"creationReplicaTickCount", syncItemInfo.Created.ReplicaTickCount},
                    {"modificationReplicaID", syncItemInfo.Modified.ReplicaId},
                    {"modificationReplicaTickCount", syncItemInfo.Modified.ReplicaTickCount}
                };
            return item;
        }

        public static int AddJsonItemRef(JObject json, ISyncableItemInfo item)
        {
            var itemRefs = (JArray) json["itemRefs"];
            for (int i = 0; i < itemRefs.Count; i++)
            {
                var test = SyncableItemInfoFromJson(itemRefs[i]);
                if (test.ItemType == item.ItemType &&
                    test.Created.ReplicaId == item.Created.ReplicaId &&
                    test.Created.ReplicaTickCount == item.Created.ReplicaTickCount &&
                    test.Created.ReplicaId == item.Modified.ReplicaId &&
                    test.Created.ReplicaTickCount == item.Modified.ReplicaTickCount)
                {
                    return i;
                }
            }
            itemRefs.Add(JsonItemRefFromSyncableItemInfo(item));
            return itemRefs.Count - 1;
        }

        public static void ApplyChangesAndUpdateKnowledge(IDbConnection connection, ISyncableStore store,
                                                          IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            if (ConflictsExist(connection) || MissingData(connection))
                throw new InvalidOperationException();

            store.BeginChanges();
            try
            {
                UpdateItems(connection, store);
                DeleteItems(connection, store);

                store.UpdateLocalKnowledge(remoteKnowledge);
                store.AcceptChanges();
            }
            catch (Exception)
            {
                store.RejectChanges();
                throw;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "No user input possible")]
        private static void DeleteItems(IDbConnection connection, ISyncableStore store)
        {
            // loop backwards through types and apply deletions
            foreach (var itemType in store.GetItemTypes().Reverse())
            {
                IDbCommand getDeletedItemsCommand = connection.CreateCommand();
                getDeletedItemsCommand.CommandText =
                    String.Format("SELECT * FROM SyncItems WHERE SyncStatus IN ({0},{1}) AND ItemType=@ItemType",
                                  (int) SyncStatus.Delete, (int) SyncStatus.DeleteNonExisting);
                getDeletedItemsCommand.AddParameter("@ItemType", itemType);

                using (var reader = getDeletedItemsCommand.ExecuteReader())
                {
                    while (reader != null && reader.Read())
                    {
                        var createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        var modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        var itemInfo = new SyncableItemInfo
                            {
                                ItemType = itemType,
                                Created = createdReplicaInfo,
                                Modified = modifiedReplicaInfo,
                                Deleted = true
                            };
                        store.DeleteItem(itemInfo);
                    }
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "No user input possible")]
        private static void UpdateItems(IDbConnection connection, ISyncableStore store)
        {
            // loop through types and apply updates or inserts
            foreach (var itemType in store.GetItemTypes())
            {
                System.Diagnostics.Debug.WriteLine("Updating itemType = " + itemType);

                IDbCommand getUpdatedItemsCommand = connection.CreateCommand();
                getUpdatedItemsCommand.CommandText =
                    String.Format(
                        "SELECT SyncStatus, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus IN ({0},{1}) AND ItemType=@ItemType",
                        (int) SyncStatus.Insert, (int) SyncStatus.Update);
                getUpdatedItemsCommand.AddParameter("@ItemType", itemType);

                using (var reader = getUpdatedItemsCommand.ExecuteReader())
                {
                    while (reader != null && reader.Read())
                    {
                        var createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        var modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        var itemData = JObject.Parse((string) reader["ItemData"]);
                        var itemInfo = new SyncableItemInfo
                            {
                                ItemType = itemType,
                                Created = createdReplicaInfo,
                                Modified = modifiedReplicaInfo,
                                Deleted = false
                            };
                        UpdateItem(connection, store, itemInfo, itemData);
                    }
                }
            }
        }

        private static void UpdateItem(IDbConnection connection, ISyncableStore store, ISyncableItemInfo itemInfo,
                                       JObject itemData)
        {
            // Make sure all the item refs for this item have already been handled
            foreach (var itemRefJson in itemData["item"]["itemRefs"])
            {
                var referencedItemInfo = SyncableItemInfoFromJson(itemRefJson);
                var localReferencedItemInfo = store.LocateCurrentItemInfo(referencedItemInfo);
                if (localReferencedItemInfo == null || localReferencedItemInfo.Deleted)
                {
                    var getReferencedItemCommand = connection.CreateCommand();
                    getReferencedItemCommand.CommandText =
                        String.Format(
                            "SELECT SyncStatus, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE  GlobalCreatedReplica=@GlobalCreatedReplica AND CreatedTickCount=@CreatedTickCount AND ItemType=@ItemType");
                    getReferencedItemCommand.AddParameter("@GlobalCreatedReplica", referencedItemInfo.Created.ReplicaId);
                    getReferencedItemCommand.AddParameter("@CreatedTickCount",
                                                          referencedItemInfo.Created.ReplicaTickCount);
                    getReferencedItemCommand.AddParameter("@ItemType", referencedItemInfo.ItemType);

                    using (var reader = getReferencedItemCommand.ExecuteReader())
                    {
                        while (reader != null && reader.Read())
                        {
                            var createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                            var modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                            var refItemData = JObject.Parse((string) reader["ItemData"]);
                            var possiblyUpdatedRefItemInfo = new SyncableItemInfo
                                {
                                    ItemType = referencedItemInfo.ItemType,
                                    Created = createdReplicaInfo,
                                    Modified = modifiedReplicaInfo,
                                    Deleted = false
                                };
                            UpdateItem(connection, store, possiblyUpdatedRefItemInfo, refItemData);
                        }
                    }
                }
            }
            store.SaveItemData(itemInfo, itemData);
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security",
            "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "No user input possible")]
        private static bool ConflictsExist(IDbConnection connection)
        {
            var sql = String.Format("SELECT COUNT(*) FROM SyncItems WHERE SyncStatus IN ({0},{1},{2})",
                                    (int) SyncStatus.DeleteConflict, (int) SyncStatus.InsertConflict,
                                    (int) SyncStatus.UpdateConflict);
            var checkConflictsCommand = connection.CreateCommand();
            checkConflictsCommand.CommandText = sql;
            var conflicts = Convert.ToInt64(checkConflictsCommand.ExecuteScalar());
            return conflicts != 0;
        }

        private static bool MissingData(IDbConnection connection)
        {
            var sql = String.Format("SELECT COUNT(*) FROM SyncItems WHERE ItemData IS NULL");
            var command = connection.CreateCommand();
            command.CommandText = sql;
            var missing = Convert.ToInt64(command.ExecuteScalar());
            return missing != 0;
        }

        public static JToken GenerateItemRefAndIndex(JObject builder, ISyncableItemInfo syncableItemInfo)
        {
            var itemRef = new JObject {{"itemRefIndex", AddJsonItemRef(builder, syncableItemInfo)}};
            return itemRef;
        }

        public static ISyncableItemInfo SyncableItemInfoFromJsonItemRef(JToken parentItem, JToken itemRef)
        {
            var itemRefs = (JArray) parentItem["itemRefs"];
            var i = (int) itemRef["itemRefIndex"];
            return SyncableItemInfoFromJson(itemRefs[i]);
        }
    }
}