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
        public static JArray KnowledgeToJson(IEnumerable<IRepositoryInfo> knowledge)
        {
            var knowledgeJson = new JArray();
            foreach (IRepositoryInfo reposInfo in knowledge)
            {
                var item = new JObject();
                item.Add("repositoryID", reposInfo.RepositoryID);
                item.Add("repositoryTickCount", reposInfo.RepositoryTickCount);
                knowledgeJson.Add(item);
            }
            return knowledgeJson;
        }

        public static IEnumerable<RepositoryInfo> KnowledgeFromJson(JToken knowledgeJson)
        {
            List<RepositoryInfo> remoteKnowledge = new List<RepositoryInfo>();
            foreach (var item in knowledgeJson)
            {
                string id = (string)item["repositoryID"];
                long tick = (long)item["repositoryTickCount"];
                remoteKnowledge.Add(new RepositoryInfo { RepositoryID = id, RepositoryTickCount = tick });
            }
            return remoteKnowledge;
        }

        public static bool KnowledgeContains(IEnumerable<IRepositoryInfo> knowledge, IRepositoryInfo repositoryInfo)
        {
            foreach (IRepositoryInfo reposInfo in knowledge)
            {
                if (reposInfo.RepositoryID == repositoryInfo.RepositoryID)
                    return reposInfo.RepositoryTickCount >= repositoryInfo.RepositoryTickCount;
            }
            return false;
        }

        public static ISyncableItemInfo SyncableItemInfoFromJson(JToken jToken)
        {
            string itemType = (string)jToken["itemType"];

            string globalCreationId = (string)jToken["creationRepositoryID"];
            long creationTickCount = (long)jToken["creationRepositoryTickCount"];
            RepositoryInfo created = new RepositoryInfo { RepositoryID = globalCreationId, RepositoryTickCount = creationTickCount };

            string globalModificationId = (string)jToken["modificationRepositoryID"];
            long modificationTickCount = (long)jToken["modificationRepositoryTickCount"];
            RepositoryInfo modified = new RepositoryInfo { RepositoryID = globalModificationId, RepositoryTickCount = modificationTickCount };

            bool deleted = (jToken["deleted"] == null) ? false : (bool)jToken["deleted"];

            return new SyncableItemInfo { ItemType = itemType, Created = created, Modified = modified, Deleted = deleted };
        }

        public static JObject SyncableItemInfoToJson(ISyncableItemInfo syncItemInfo)
        {
            var item = new JObject();
            item.Add("itemType", syncItemInfo.ItemType);
            item.Add("creationRepositoryID", syncItemInfo.Created.RepositoryID);
            item.Add("creationRepositoryTickCount", syncItemInfo.Created.RepositoryTickCount);
            item.Add("modificationRepositoryID", syncItemInfo.Modified.RepositoryID);
            item.Add("modificationRepositoryTickCount", syncItemInfo.Modified.RepositoryTickCount);
            item.Add("deleted", syncItemInfo.Deleted);
            return item;
        }

        public static SyncStatus CalculateSyncStatus(ISyncableItemInfo remoteSyncableItemInfo, ISyncableItemInfo localSyncableItemInfo, IEnumerable<IRepositoryInfo> remoteKnowledge)
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
            item.Add("creationRepositoryID", syncItemInfo.Created.RepositoryID);
            item.Add("creationRepositoryTickCount", syncItemInfo.Created.RepositoryTickCount);
            item.Add("modificationRepositoryID", syncItemInfo.Modified.RepositoryID);
            item.Add("modificationRepositoryTickCount", syncItemInfo.Modified.RepositoryTickCount);
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
            item.Add("creationRepositoryID", syncItemInfo.Created.RepositoryID);
            item.Add("creationRepositoryTickCount", syncItemInfo.Created.RepositoryTickCount);
            item.Add("modificationRepositoryID", syncItemInfo.Modified.RepositoryID);
            item.Add("modificationRepositoryTickCount", syncItemInfo.Modified.RepositoryTickCount);
            return item;
        }

        public static int AddJsonItemRef(JObject json, ISyncableItemInfo item)
        {
            JArray itemRefs = (JArray)json["itemRefs"];
            for (int i = 0; i < itemRefs.Count; i++)
            {
                ISyncableItemInfo test = SyncUtil.SyncableItemInfoFromJson(itemRefs[i]);
                if (test.ItemType == item.ItemType &&
                    test.Created.RepositoryID == item.Created.RepositoryID &&
                    test.Created.RepositoryTickCount == item.Created.RepositoryTickCount &&
                    test.Created.RepositoryID == item.Modified.RepositoryID &&
                    test.Created.RepositoryTickCount == item.Modified.RepositoryTickCount)
                {
                    return i;
                }
            }
            itemRefs.Add(JsonItemRefFromSyncableItemInfo(item));
            return itemRefs.Count - 1;
        }

        public static void ApplyChangesAndUpdateKnowledge(IDbConnection connection, ISyncableStore store, IEnumerable<IRepositoryInfo> remoteKnowledge)
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
                        IRepositoryInfo createdRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Created");
                        IRepositoryInfo modifiedRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Modified");
                        ISyncableItemInfo itemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = true };
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
                getUpdatedItemsCommand.CommandText = String.Format("SELECT SyncStatus, GlobalCreatedRepos, CreatedTickCount, GlobalModifiedRepos, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus IN ({0},{1}) AND ItemType=@ItemType", (int)SyncStatus.Insert, (int)SyncStatus.Update);
                getUpdatedItemsCommand.AddParameter("@ItemType", itemType);

                using (IDataReader reader = getUpdatedItemsCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IRepositoryInfo createdRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Created");
                        IRepositoryInfo modifiedRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Modified");
                        JObject itemData = JObject.Parse((string)reader["ItemData"]);
                        ISyncableItemInfo itemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = false };
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
                    getReferencedItemCommand.CommandText = String.Format("SELECT SyncStatus, GlobalCreatedRepos, CreatedTickCount, GlobalModifiedRepos, ModifiedTickCount, ItemData  FROM SyncItems WHERE  GlobalCreatedRepos=@GlobalCreatedRepos AND CreatedTickCount=@CreatedTickCount AND ItemType=@ItemType");
                    getReferencedItemCommand.AddParameter("@GlobalCreatedRepos", referencedItemInfo.Created.RepositoryID);
                    getReferencedItemCommand.AddParameter("@CreatedTickCount", referencedItemInfo.Created.RepositoryTickCount);
                    getReferencedItemCommand.AddParameter("@ItemType", referencedItemInfo.ItemType);

                    using (IDataReader reader = getReferencedItemCommand.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            IRepositoryInfo createdRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Created");
                            IRepositoryInfo modifiedRepositoryInfo = SessionDbHelper.RepositoryInfoFromDataReader(reader, "Modified");
                            JObject refItemData = JObject.Parse((string)reader["ItemData"]);
                            ISyncableItemInfo possiblyUpdatedRefItemInfo = new SyncableItemInfo { ItemType = referencedItemInfo.ItemType, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = false };
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
