using Newtonsoft.Json.Linq;
using SyncFoundation.Core.Interfaces;
using System;
using System.Data;
using System.Linq;

namespace SyncFoundation.Core
{
    public class SyncServerSession
    {
        private readonly ISyncableStore _store;
        private readonly IDbConnection _connection;

        public SyncServerSession(ISyncableStore store, IDbConnection connection)
        {
            _store = store;
            _connection = connection;
        }

        public JObject BeginSession(JObject request)
        {
            var remoteItemTypes = request["itemTypes"].Select(itemType => (string) itemType).ToList();

            if (!remoteItemTypes.SequenceEqual(_store.GetItemTypes()))
                throw new Exception("Mismatched item types");

            SessionDbHelper.CreateSessionDbTables(_connection);

            var json = new JObject {{"maxBatchCount", 50}, {"maxBatchSize", 1024*1024}, {"useGzip", true}};

            return json;
        }


        public JObject EndSession(JObject request)
        {
            return new JObject();
        }

        public JObject ApplyChanges(JObject request)
        {
            SyncUtil.ApplyChangesAndUpdateKnowledge(_connection, _store, SessionDbHelper.LoadRemoteKnowledge(_connection));

            return new JObject();
        }

        public JObject GetChanges(JObject request)
        {
            var remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);
            var changedItems = _store.LocateChangedItems(remoteKnowledge).ToList();

            SessionDbHelper.SaveChangedItems(_connection, changedItems);
            var json = new JObject
                {
                    {"knowledge", SyncUtil.KnowledgeToJson(_store.GenerateLocalKnowledge())},
                    {"totalChanges", changedItems.Count()}
                };

            return json;
        }

        public JObject GetItemData(JObject request)
        {
            ISyncableItemInfo requestedItemInfo = SyncUtil.SyncableItemInfoFromJson(request["item"]);

            var json = new JObject();

            JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(requestedItemInfo);
            _store.BuildItemData(requestedItemInfo, builder);
            json.Add(new JProperty("item", builder));

            return json;
        }

        public JObject GetItemDataBatch(JObject request)
        {
            var startItem = (int)request["startItem"];
            var maxBatchCount = (int)request["maxBatchCount"];
            var maxBatchSize = (int)request["maxBatchSize"];

            var batchArray = new JArray();

            var getChangedItemsCommand = _connection.CreateCommand();
            getChangedItemsCommand.CommandText = String.Format("SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount FROM SyncItems WHERE ItemID >= {0} ORDER BY ItemID", startItem);
            using (var reader = getChangedItemsCommand.ExecuteReader())
            {
                var batchSize = 0;
                while (reader != null && reader.Read())
                {
                    ISyncableItemInfo requestedItemInfo = SessionDbHelper.SyncableItemInfoFromDataReader(reader);

                    var singleItem = new JObject();
                    JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(requestedItemInfo);
                    if (!requestedItemInfo.Deleted)
                        _store.BuildItemData(requestedItemInfo, builder);
                    singleItem.Add(new JProperty("item", builder));

                    batchSize += singleItem.ToString().Length;
                    batchArray.Add(singleItem);

                    if (batchArray.Count >= maxBatchCount || batchSize >= maxBatchSize)
                        break;
                }
            }

            var json = new JObject {{"batch", batchArray}};
            return json;
        }

        public JObject PutChanges(JObject request)
        {
            SessionDbHelper.ClearSyncItems(_connection);

            var remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);
            SessionDbHelper.SaveRemoteKnowledge(_connection, remoteKnowledge);

            var json = new JObject();
            return json;
        }

        public JObject PutItemDataBatch(JObject request)
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "BEGIN";
            command.ExecuteNonQuery();
            try
            {
                var batch = (JArray)request["batch"];
                foreach (JToken item in batch)
                {
                    var remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(item["item"]);
                    var itemData = new JObject {{"item", item["item"]}};
                    var remoteKnowledge = SessionDbHelper.LoadRemoteKnowledge(_connection);

                    var localSyncableItemInfo = _store.LocateCurrentItemInfo(remoteSyncableItemInfo);

                    var status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo, remoteKnowledge);

                    SessionDbHelper.SaveItemData(_connection, remoteSyncableItemInfo, status, itemData);
                }
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();
            }
            catch (Exception)
            {
                command.CommandText = "ROLLBACK";
                command.ExecuteNonQuery();
                throw;
            }

            var json = new JObject();
            return json;
        }
    }
}
