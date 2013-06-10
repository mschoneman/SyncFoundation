using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core
{
    public class SyncServerSession
    {
        private readonly ISyncableStore store;
        private readonly IDbConnection connection;

        public SyncServerSession(ISyncableStore store, IDbConnection connection)
        {
            this.store = store;
            this.connection = connection;
        }

        public JObject BeginSession(JObject request)
        {
            List<string> remoteItemTypes = new List<string>();
            foreach (var itemType in request["itemTypes"])
                remoteItemTypes.Add((string)itemType);

            if (!remoteItemTypes.SequenceEqual(store.GetItemTypes()))
                throw new Exception("Mismatched item types");

            SessionDbHelper.CreateSessionDbTables(connection);

            var json = new JObject();
            json.Add("maxBatchCount", 50);
            json.Add("maxBatchSize", 1024 * 1024);
            json.Add("useGzip", true);

            return json;
        }


        public JObject EndSession(JObject request)
        {
            return new JObject();
        }

        public JObject ApplyChanges(JObject request)
        {
            SyncUtil.ApplyChangesAndUpdateKnowledge(connection, store, SessionDbHelper.LoadRemoteKnowledge(connection));

            return new JObject();
        }

        public JObject GetChanges(JObject request)
        {
            IEnumerable<ReplicaInfo> remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);

            var json = new JObject();

            json.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(store.GenerateLocalKnowledge())));

            var changedItems = store.LocateChangedItems(remoteKnowledge);
            SessionDbHelper.SaveChangedItems(connection, changedItems);
            json.Add(new JProperty("totalChanges", changedItems.Count()));

            return json;
        }

        public JObject GetItemData(JObject request)
        {
            ISyncableItemInfo requestedItemInfo = SyncUtil.SyncableItemInfoFromJson(request["item"]);

            var json = new JObject();

            JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(requestedItemInfo);
            store.BuildItemData(requestedItemInfo, builder);
            json.Add(new JProperty("item", builder));

            return json;
        }

        public JObject GetItemDataBatch(JObject request)
        {
            int startItem = (int)request["startItem"];
            int maxBatchCount = (int)request["maxBatchCount"];
            int maxBatchSize = (int)request["maxBatchSize"]; ;

            JArray batchArray = new JArray();

            IDbCommand getChangedItemsCommand = connection.CreateCommand();
            getChangedItemsCommand.CommandText = String.Format("SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount FROM SyncItems WHERE ItemID >= {0} ORDER BY ItemID", startItem);
            using (IDataReader reader = getChangedItemsCommand.ExecuteReader())
            {
                int batchSize = 0;
                while (reader.Read())
                {
                    ISyncableItemInfo requestedItemInfo = SessionDbHelper.SyncableItemInfoFromDataReader(reader);

                    var singleItem = new JObject();
                    JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(requestedItemInfo);
                    if (!requestedItemInfo.Deleted)
                        store.BuildItemData(requestedItemInfo, builder);
                    singleItem.Add(new JProperty("item", builder));

                    batchSize += singleItem.ToString().Length;
                    batchArray.Add(singleItem);

                    if (batchArray.Count >= maxBatchCount || batchSize >= maxBatchSize)
                        break;
                }
            }

            var json = new JObject();
            json.Add("batch", batchArray);
            return json;
        }

        public JObject PutChanges(JObject request)
        {
            SessionDbHelper.ClearSyncItems(connection);

            IEnumerable<ReplicaInfo> remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);
            SessionDbHelper.SaveRemoteKnowledge(connection, remoteKnowledge);

            int changeCount = (int)request["changeCount"];

            var json = new JObject();
            return json;
        }

        public JObject PutItemDataBatch(JObject request)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "BEGIN";
            command.ExecuteNonQuery();
            try
            {
                JArray batch = (JArray)request["batch"];
                for (int i = 0; i < batch.Count; i++)
                {
                    ISyncableItemInfo remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(batch[i]["item"]);
                    JObject itemData = new JObject();
                    itemData.Add("item", batch[i]["item"]);
                    int changeNumber = (int)batch[i]["changeNumber"];

                    var remoteKnowledge = SessionDbHelper.LoadRemoteKnowledge(connection);
                    ISyncableItemInfo localSyncableItemInfo = store.LocateCurrentItemInfo(remoteSyncableItemInfo);
                    SyncStatus status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo, remoteKnowledge);
                    SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, itemData);
                }
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();
            }
            catch (Exception)
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
            }

            var json = new JObject();
            return json;
        }
    }
}
