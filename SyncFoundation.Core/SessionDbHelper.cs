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
    public static class SessionDbHelper
    {
        public static void CreateSessionDbTables(IDbConnection connection)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "CREATE TABLE IF NOT EXISTS SyncItems(ItemID INTEGER PRIMARY KEY AUTOINCREMENT, SyncStatus INTEGER, ItemType INTEGER, GlobalCreatedReplica TEXT, CreatedTickCount INTEGER, GlobalModifiedReplica TEXT, ModifiedTickCount INTEGER, ItemData BLOB)";
            command.ExecuteNonQuery();

            command.CommandText = "DELETE FROM SyncItems";
            command.ExecuteNonQuery();

            command.CommandText = "CREATE TABLE IF NOT EXISTS RemoteKnowledge(RowID INTEGER PRIMARY KEY AUTOINCREMENT, GlobalReplicaID TEXT, ReplicaTickCount INTEGER)";
            command.ExecuteNonQuery();

            command.CommandText = "DELETE FROM RemoteKnowledge";
            command.ExecuteNonQuery();
        }

        public static void ResolveItemNoData(IDbConnection connection, ISyncableItemInfo itemInfo, SyncStatus resolvedStatus, IReplicaInfo modifiedReplica)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET SyncStatus=@SyncStatus, GlobalModifiedReplica=@ModifiedReplica, ModifiedTickCount=@ModifiedTick WHERE GlobalCreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTick AND ItemType=@ItemType";
            command.AddParameter("@SyncStatus", resolvedStatus);
            command.AddParameter("@ItemType", itemInfo.ItemType);
            command.AddParameter("@CreatedReplica", itemInfo.Created.ReplicaId);
            command.AddParameter("@CreatedTick", itemInfo.Created.ReplicaTickCount);
            command.AddParameter("@ModifiedReplica", modifiedReplica.ReplicaId);
            command.AddParameter("@ModifiedTick", modifiedReplica.ReplicaTickCount);
            command.ExecuteNonQuery();
        }

        public static void ResolveItemWithData(IDbConnection connection, ISyncableItemInfo itemInfo, SyncStatus resolvedStatus, IReplicaInfo modifiedReplica, JObject itemData)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET SyncStatus=@SyncStatus, GlobalModifiedReplica=@ModifiedReplica, ModifiedTickCount=@ModifiedTick, ItemData=@ItemData WHERE GlobalCreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTick AND ItemType=@ItemType";
            command.AddParameter("@SyncStatus", resolvedStatus);
            command.AddParameter("@ItemType", itemInfo.ItemType);
            command.AddParameter("@CreatedReplica", itemInfo.Created.ReplicaId);
            command.AddParameter("@CreatedTick", itemInfo.Created.ReplicaTickCount);
            command.AddParameter("@ModifiedReplica", modifiedReplica.ReplicaId);
            command.AddParameter("@ModifiedTick", modifiedReplica.ReplicaTickCount);
            command.AddParameter("@ItemData", itemData.ToString());
            command.ExecuteNonQuery();
        }

        public static void SaveItemData(IDbConnection connection, ISyncableItemInfo remoteSyncableItemInfo, SyncStatus status, JObject data)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "INSERT INTO SyncItems(SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData) VALUES(@SyncStatus,@ItemType,@CreatedReplica,@CreatedTick,@ModifiedReplica,@ModifiedTick,@ItemData)";
            command.AddParameter("@SyncStatus", status);
            command.AddParameter("@ItemType", remoteSyncableItemInfo.ItemType);
            command.AddParameter("@CreatedReplica", remoteSyncableItemInfo.Created.ReplicaId);
            command.AddParameter("@CreatedTick", remoteSyncableItemInfo.Created.ReplicaTickCount);
            command.AddParameter("@ModifiedReplica", remoteSyncableItemInfo.Modified.ReplicaId);
            command.AddParameter("@ModifiedTick", remoteSyncableItemInfo.Modified.ReplicaTickCount);
            command.AddParameter("@ItemData", data.ToString());
            command.ExecuteNonQuery();
        }

        public static void ClearSyncItems(IDbConnection connection)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "DELETE FROM SyncItems";
            command.ExecuteNonQuery();
        }

        public static void SaveItemPlaceHolders(IDbConnection connection, int changeCount)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "BEGIN";
            command.ExecuteNonQuery();
            for (int itemNumber = 1; itemNumber <= changeCount; itemNumber++)
            {
                command.CommandText = "INSERT INTO SyncItems(RowID) VALUES(@RowID)";
                command.AddParameter("@RowID", itemNumber);
                command.ExecuteNonQuery();
            }
            command.CommandText = "COMMIT";
            command.ExecuteNonQuery();
        }

        public static void UpdateItemPlaceholderData(IDbConnection connection, int itemNumber, SyncStatus status, ISyncableItemInfo remoteSyncableItemInfo, JObject itemData)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET SyncStatus=@SyncStatus, ItemType=@ItemType, GlobalCreatedReplica=@CreatedReplica, CreatedTickCount=@CreatedTick, GlobalModifiedReplica=@ModifiedReplica, ModifiedTickCount=@ModifiedTick, ItemData=@ItemData WHERE RowID=@RowID";
            command.AddParameter("@RowID", itemNumber);
            command.AddParameter("@SyncStatus", status);
            command.AddParameter("@ItemType", remoteSyncableItemInfo.ItemType);
            command.AddParameter("@CreatedReplica", remoteSyncableItemInfo.Created.ReplicaId);
            command.AddParameter("@CreatedTick", remoteSyncableItemInfo.Created.ReplicaTickCount);
            command.AddParameter("@ModifiedReplica", remoteSyncableItemInfo.Modified.ReplicaId);
            command.AddParameter("@ModifiedTick", remoteSyncableItemInfo.Modified.ReplicaTickCount);
            if (status == SyncStatus.Delete || status == SyncStatus.DeleteNonExisting)
                command.AddParameter("@ItemData", "{item:{itemRefs:[]}}");
            else
                command.AddParameter("@ItemData", itemData.ToString());
            command.ExecuteNonQuery();
        }


        public static void SaveRemoteKnowledge(IDbConnection connection, IEnumerable<ReplicaInfo> remoteKnowledge)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "DELETE FROM RemoteKnowledge";
            command.ExecuteNonQuery();

            foreach (var reposInfo in remoteKnowledge)
            {
                IDbCommand insertCommand = connection.CreateCommand();
                insertCommand.CommandText = "INSERT INTO RemoteKnowledge(GlobalReplicaID, ReplicaTickCount) VALUES (@GlobalReplicaID, @ReplicaTickCount)";
                insertCommand.AddParameter("@GlobalReplicaID", reposInfo.ReplicaId);
                insertCommand.AddParameter("@ReplicaTickCount", reposInfo.ReplicaTickCount);
                insertCommand.ExecuteNonQuery();
            }
        }

        public static IEnumerable<IReplicaInfo> LoadRemoteKnowledge(IDbConnection connection)
        {
            List<IReplicaInfo> remoteKnowledge = new List<IReplicaInfo>();
            IDbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT GlobalReplicaID, ReplicaTickCount FROM RemoteKnowledge";
            using (IDataReader reader = selectCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    string reposId = (string)reader["GlobalReplicaID"];
                    long tick = Convert.ToInt64(reader["ReplicaTickCount"]);
                    remoteKnowledge.Add(new ReplicaInfo { ReplicaId = reposId, ReplicaTickCount = tick });
                }
            }

            return remoteKnowledge;
        }

        public static IReplicaInfo ReplicaInfoFromDataReader(IDataReader reader, string fieldPrefix)
        {
            string reposId = (string)reader["Global" + fieldPrefix + "Replica"];
            long reposTick = Convert.ToInt64(reader[fieldPrefix + "TickCount"]);
            return new ReplicaInfo { ReplicaId = reposId, ReplicaTickCount = reposTick };
        }



        public static void ReplaceAllItemRefs(IDbConnection connection, ISyncableStore store, ISyncableItemInfo remoteItemInfo, ISyncableItemInfo changedItemInfo)
        {
            // update *all* the itemData.item refs in all rows from remoteItemInfo to changedItemInfo
            IDbCommand selectItemDataCommand = connection.CreateCommand();
            selectItemDataCommand.CommandText = String.Format("SELECT ItemID, ItemData FROM SyncItems");

            using (IDataReader reader = selectItemDataCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    JObject itemData = JObject.Parse((string)reader["ItemData"]);
                    bool needToUpdate = false;
                    foreach (var itemRefJson in itemData["item"]["itemRefs"])
                    {
                        ISyncableItemInfo referencedItemInfo = SyncUtil.SyncableItemInfoFromJson(itemRefJson);
                        if (referencedItemInfo.ItemType == remoteItemInfo.ItemType &&
                            referencedItemInfo.Created.ReplicaId == remoteItemInfo.Created.ReplicaId &&
                            referencedItemInfo.Created.ReplicaTickCount == remoteItemInfo.Created.ReplicaTickCount &&
                            referencedItemInfo.Modified.ReplicaId == remoteItemInfo.Modified.ReplicaId &&
                            referencedItemInfo.Modified.ReplicaTickCount == remoteItemInfo.Modified.ReplicaTickCount
                            )
                        {
                            itemRefJson["creationReplicaID"] = changedItemInfo.Created.ReplicaId;
                            itemRefJson["creationReplicaTickCount"] = changedItemInfo.Created.ReplicaTickCount;
                            itemRefJson["modificationReplicaID"] = changedItemInfo.Modified.ReplicaId;
                            itemRefJson["modificationReplicaTickCount"] = changedItemInfo.Modified.ReplicaTickCount;
                            needToUpdate = true;
                        }
                    }

                    if (needToUpdate)
                    {
                        IDbCommand updateCommand = connection.CreateCommand();
                        updateCommand.CommandText = "UPDATE SyncItems SET ItemData=@ItemData, GlobalModifiedReplica=@ModifiedReplica, ModifiedTickCount=@TickCount WHERE ItemID=@ItemID";
                        updateCommand.AddParameter("@ItemID", reader["ItemID"]);
                        updateCommand.AddParameter("@ItemData", itemData.ToString());
                        updateCommand.AddParameter("@TickCount", store.IncrementLocalRepilcaTickCount());
                        updateCommand.AddParameter("@ModifiedReplica", store.GetLocalReplicaId());
                        updateCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
