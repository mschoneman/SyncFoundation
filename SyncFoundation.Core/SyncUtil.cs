using Newtonsoft.Json.Linq;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core
{
    static public class SyncUtil
    {
        public static JArray KnowledgeToJson(IEnumerable<IReplicaInfo> knowledge)
        {
            var knowledgeJson = new JArray();
            foreach (IReplicaInfo reposInfo in knowledge)
            {
                var item = new JObject();
                item.Add("replicaID", reposInfo.ReplicaId);
                item.Add("replicaTickCount", reposInfo.ReplicaTickCount);
                knowledgeJson.Add(item);
            }
            return knowledgeJson;
        }

        public static IEnumerable<ReplicaInfo> KnowledgeFromJson(JToken knowledgeJson)
        {
            List<ReplicaInfo> remoteKnowledge = new List<ReplicaInfo>();
            foreach (var item in knowledgeJson)
            {
                string id = (string)item["replicaID"];
                long tick = (long)item["replicaTickCount"];
                remoteKnowledge.Add(new ReplicaInfo { ReplicaId = id, ReplicaTickCount = tick });
            }
            return remoteKnowledge;
        }

        public static bool KnowledgeContains(IEnumerable<IReplicaInfo> knowledge, IReplicaInfo replicaInfo)
        {
            foreach (IReplicaInfo reposInfo in knowledge)
            {
                if (reposInfo.ReplicaId == replicaInfo.ReplicaId)
                    return reposInfo.ReplicaTickCount >= replicaInfo.ReplicaTickCount;
            }
            return false;
        }

        public static ISyncableItemInfo SyncableItemInfoFromJson(JToken jToken)
        {
            string itemType = (string)jToken["itemType"];

            string globalCreationId = (string)jToken["creationReplicaID"];
            long creationTickCount = (long)jToken["creationReplicaTickCount"];
            ReplicaInfo created = new ReplicaInfo { ReplicaId = globalCreationId, ReplicaTickCount = creationTickCount };

            string globalModificationId = (string)jToken["modificationReplicaID"];
            long modificationTickCount = (long)jToken["modificationReplicaTickCount"];
            ReplicaInfo modified = new ReplicaInfo { ReplicaId = globalModificationId, ReplicaTickCount = modificationTickCount };

            bool deleted = (jToken["deleted"] == null) ? false : (bool)jToken["deleted"];

            return new SyncableItemInfo { ItemType = itemType, Created = created, Modified = modified, Deleted = deleted };
        }

        public static JObject SyncableItemInfoToJson(ISyncableItemInfo syncItemInfo)
        {
            var item = new JObject();
            item.Add("itemType", syncItemInfo.ItemType);
            item.Add("creationReplicaID", syncItemInfo.Created.ReplicaId);
            item.Add("creationReplicaTickCount", syncItemInfo.Created.ReplicaTickCount);
            item.Add("modificationReplicaID", syncItemInfo.Modified.ReplicaId);
            item.Add("modificationReplicaTickCount", syncItemInfo.Modified.ReplicaTickCount);
            item.Add("deleted", syncItemInfo.Deleted);
            return item;
        }

        public static SyncStatus CalculateSyncStatus(ISyncableItemInfo remoteSyncableItemInfo, ISyncableItemInfo localSyncableItemInfo, IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            if (localSyncableItemInfo == null) // We have no knowledge of the remote item
            {
                if (remoteSyncableItemInfo.Deleted)
                    return SyncStatus.DeleteNonExisting;
                return SyncStatus.Insert;
            }

            if (localSyncableItemInfo.Deleted && remoteSyncableItemInfo.Deleted)
                return SyncStatus.DeleteNonExisting;

            if (SyncUtil.KnowledgeContains(remoteKnowledge, localSyncableItemInfo.Modified))
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
            var item = new JObject();
            item.Add("itemType", syncItemInfo.ItemType);
            item.Add("creationReplicaID", syncItemInfo.Created.ReplicaId);
            item.Add("creationReplicaTickCount", syncItemInfo.Created.ReplicaTickCount);
            item.Add("modificationReplicaID", syncItemInfo.Modified.ReplicaId);
            item.Add("modificationReplicaTickCount", syncItemInfo.Modified.ReplicaTickCount);
            item.Add("itemRefs", new JArray());
            return item;
        }

        public static JArray JsonItemArrayFromSyncableItemInfoEnumberable(IEnumerable<ISyncableItemInfo> syncItemInfoEnumerable)
        {
            var items = new JArray();
            foreach (ISyncableItemInfo syncItemInfo in syncItemInfoEnumerable)
            {
                items.Add(SyncUtil.SyncableItemInfoToJson(syncItemInfo));
            }
            return items;
        }


        private static JObject JsonItemRefFromSyncableItemInfo(ISyncableItemInfo syncItemInfo)
        {
            var item = new JObject();
            item.Add("itemType", syncItemInfo.ItemType);
            item.Add("creationReplicaID", syncItemInfo.Created.ReplicaId);
            item.Add("creationReplicaTickCount", syncItemInfo.Created.ReplicaTickCount);
            item.Add("modificationReplicaID", syncItemInfo.Modified.ReplicaId);
            item.Add("modificationReplicaTickCount", syncItemInfo.Modified.ReplicaTickCount);
            return item;
        }

        public static int AddJsonItemRef(JObject json, ISyncableItemInfo item)
        {
            JArray itemRefs = (JArray)json["itemRefs"];
            for (int i = 0; i < itemRefs.Count; i++)
            {
                ISyncableItemInfo test = SyncUtil.SyncableItemInfoFromJson(itemRefs[i]);
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

        public static void ApplyChangesAndUpdateKnowledge(IDbConnection connection, ISyncableStore store, IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            if (conflictsExist(connection) || missingData(connection))
                throw new InvalidOperationException();

            store.BeginChanges();
            try
            {
                updateItems(connection, store);
                deleteItems(connection, store);

                store.UpdateLocalKnowledge(remoteKnowledge);
                store.AcceptChanges();
            }
            catch (Exception)
            {
                store.RejectChanges();
                throw;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "No user input possible")]
        private static void deleteItems(IDbConnection connection, ISyncableStore store)
        {
            // loop backwards through types and apply deletions
            foreach (string itemType in store.GetItemTypes().Reverse())
            {
                IDbCommand getDeletedItemsCommand = connection.CreateCommand();
                getDeletedItemsCommand.CommandText = String.Format("SELECT * FROM SyncItems WHERE SyncStatus IN ({0},{1}) AND ItemType=@ItemType", (int)SyncStatus.Delete, (int)SyncStatus.DeleteNonExisting);
                getDeletedItemsCommand.AddParameter("@ItemType", itemType);

                using (IDataReader reader = getDeletedItemsCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        ISyncableItemInfo itemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = true };
                        store.DeleteItem(itemInfo);
                    }
                }

            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities",
            Justification = "No user input possible")]
        private static void updateItems(IDbConnection connection, ISyncableStore store)
        {
            // loop through types and apply updates or inserts
            foreach (string itemType in store.GetItemTypes())
            {
                IDbCommand getUpdatedItemsCommand = connection.CreateCommand();
                getUpdatedItemsCommand.CommandText = String.Format("SELECT SyncStatus, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus IN ({0},{1}) AND ItemType=@ItemType", (int)SyncStatus.Insert, (int)SyncStatus.Update);
                getUpdatedItemsCommand.AddParameter("@ItemType", itemType);

                using (IDataReader reader = getUpdatedItemsCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        JObject itemData = JObject.Parse((string)reader["ItemData"]);
                        ISyncableItemInfo itemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false };
                        updateItem(connection, store, itemInfo, itemData);
                    }
                }

            }
        }

        private static void updateItem(IDbConnection connection, ISyncableStore store, ISyncableItemInfo itemInfo, JObject itemData)
        {
            // Make sure all the item refs for this item have already been handled
            foreach (var itemRefJson in itemData["item"]["itemRefs"])
            {
                ISyncableItemInfo referencedItemInfo = SyncUtil.SyncableItemInfoFromJson(itemRefJson);
                ISyncableItemInfo localReferencedItemInfo = store.LocateCurrentItemInfo(referencedItemInfo);
                if (localReferencedItemInfo == null || localReferencedItemInfo.Deleted)
                {
                    IDbCommand getReferencedItemCommand = connection.CreateCommand();
                    getReferencedItemCommand.CommandText = String.Format("SELECT SyncStatus, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE  GlobalCreatedReplica=@GlobalCreatedReplica AND CreatedTickCount=@CreatedTickCount AND ItemType=@ItemType");
                    getReferencedItemCommand.AddParameter("@GlobalCreatedReplica", referencedItemInfo.Created.ReplicaId);
                    getReferencedItemCommand.AddParameter("@CreatedTickCount", referencedItemInfo.Created.ReplicaTickCount);
                    getReferencedItemCommand.AddParameter("@ItemType", referencedItemInfo.ItemType);

                    using (IDataReader reader = getReferencedItemCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                            IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                            JObject refItemData = JObject.Parse((string)reader["ItemData"]);
                            ISyncableItemInfo possiblyUpdatedRefItemInfo = new SyncableItemInfo { ItemType = referencedItemInfo.ItemType, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false };
                            updateItem(connection, store, possiblyUpdatedRefItemInfo, refItemData);
                        }
                    }
                }
            }
            store.SaveItemData(itemInfo, itemData);
        }


        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities", 
            Justification="No user input possible")]
        private static bool conflictsExist(IDbConnection connection)
        {
            string sql = String.Format("SELECT COUNT(*) FROM SyncItems WHERE SyncStatus IN ({0},{1},{2})", (int)SyncStatus.DeleteConflict, (int)SyncStatus.InsertConflict, (int)SyncStatus.UpdateConflict);
            IDbCommand checkConflictsCommand = connection.CreateCommand();
            checkConflictsCommand.CommandText = sql;
            long conflicts = Convert.ToInt64(checkConflictsCommand.ExecuteScalar());
            return conflicts != 0;
        }

        private static bool missingData(IDbConnection connection)
        {
            string sql = String.Format("SELECT COUNT(*) FROM SyncItems WHERE ItemData IS NULL");
            IDbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            long missing = Convert.ToInt64(command.ExecuteScalar());
            return missing != 0;
        }

        public static JToken GenerateItemRefAndIndex(JObject builder, ISyncableItemInfo syncableItemInfo)
        {
            JObject itemRef = new JObject();
            itemRef.Add("itemRefIndex", SyncUtil.AddJsonItemRef(builder, syncableItemInfo));
            return itemRef;
        }

        public static ISyncableItemInfo SyncableItemInfoFromJsonItemRef(JToken parentItem, JToken itemRef)
        {
            JArray itemRefs = (JArray)parentItem["itemRefs"];
            int i = (int)itemRef["itemRefIndex"];
            return SyncUtil.SyncableItemInfoFromJson(itemRefs[i]);
        }

    }
}
