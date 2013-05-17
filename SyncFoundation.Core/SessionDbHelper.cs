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
            command.CommandText = "CREATE TABLE IF NOT EXISTS SyncItems(ItemID INTEGER PRIMARY KEY AUTOINCREMENT, SyncStatus INTEGER, ItemType INTEGER, GlobalCreatedRepos TEXT, CreatedTickCount INTEGER, GlobalModifiedRepos TEXT, ModifiedTickCount INTEGER, ItemData BLOB)";
            command.ExecuteNonQuery();

            command.CommandText = "DELETE FROM SyncItems";
            command.ExecuteNonQuery();

            command.CommandText = "CREATE TABLE IF NOT EXISTS RemoteKnowledge(RowID INTEGER PRIMARY KEY AUTOINCREMENT, GlobalReposID TEXT, ReposTickCount INTEGER)";
            command.ExecuteNonQuery();

            command.CommandText = "DELETE FROM RemoteKnowledge";
            command.ExecuteNonQuery();
        }

        public static void ResolveItemNoData(IDbConnection connection, ISyncableItemInfo itemInfo, SyncStatus resolvedStatus, IRepositoryInfo modifiedRepos)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET SyncStatus=@SyncStatus, GlobalModifiedRepos=@ModifiedRepos, ModifiedTickCount=@ModifiedTick WHERE GlobalCreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTick AND ItemType=@ItemType";
            command.AddParameter("@SyncStatus", resolvedStatus);
            command.AddParameter("@ItemType", itemInfo.ItemType);
            command.AddParameter("@CreatedRepos", itemInfo.Created.RepositoryID);
            command.AddParameter("@CreatedTick", itemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", modifiedRepos.RepositoryID);
            command.AddParameter("@ModifiedTick", modifiedRepos.RepositoryTickCount);
            command.ExecuteNonQuery();
        }

        public static void ResolveItemWithData(IDbConnection connection, ISyncableItemInfo itemInfo, SyncStatus resolvedStatus, IRepositoryInfo modifiedRepos, JObject itemData)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET SyncStatus=@SyncStatus, GlobalModifiedRepos=@ModifiedRepos, ModifiedTickCount=@ModifiedTick, ItemData=@ItemData WHERE GlobalCreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTick AND ItemType=@ItemType";
            command.AddParameter("@SyncStatus", resolvedStatus);
            command.AddParameter("@ItemType", itemInfo.ItemType);
            command.AddParameter("@CreatedRepos", itemInfo.Created.RepositoryID);
            command.AddParameter("@CreatedTick", itemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", modifiedRepos.RepositoryID);
            command.AddParameter("@ModifiedTick", modifiedRepos.RepositoryTickCount);
            command.AddParameter("@ItemData", itemData.ToString());
            command.ExecuteNonQuery();
        }

        public static void SaveItemData(IDbConnection connection, ISyncableItemInfo remoteSyncableItemInfo, SyncStatus status, JObject data)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "INSERT INTO SyncItems(SyncStatus, ItemType, GlobalCreatedRepos, CreatedTickCount, GlobalModifiedRepos, ModifiedTickCount, ItemData) VALUES(@SyncStatus,@ItemType,@CreatedRepos,@CreatedTick,@ModifiedRepos,@ModifiedTick,@ItemData)";
            command.AddParameter("@SyncStatus", status);
            command.AddParameter("@ItemType", remoteSyncableItemInfo.ItemType);
            command.AddParameter("@CreatedRepos", remoteSyncableItemInfo.Created.RepositoryID);
            command.AddParameter("@CreatedTick", remoteSyncableItemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", remoteSyncableItemInfo.Modified.RepositoryID);
            command.AddParameter("@ModifiedTick", remoteSyncableItemInfo.Modified.RepositoryTickCount);
            command.AddParameter("@ItemData", data.ToString());
            command.ExecuteNonQuery();
        }

        public static void SaveItemPlaceHolder(IDbConnection connection, SyncStatus status, ISyncableItemInfo remoteSyncableItemInfo)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "INSERT INTO SyncItems(SyncStatus, ItemType, GlobalCreatedRepos, CreatedTickCount, GlobalModifiedRepos, ModifiedTickCount, ItemData) VALUES(@SyncStatus,@ItemType,@CreatedRepos,@CreatedTick,@ModifiedRepos,@ModifiedTick,@ItemData)";
            command.AddParameter("@SyncStatus", status);
            command.AddParameter("@ItemType", remoteSyncableItemInfo.ItemType);
            command.AddParameter("@CreatedRepos", remoteSyncableItemInfo.Created.RepositoryID);
            command.AddParameter("@CreatedTick", remoteSyncableItemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", remoteSyncableItemInfo.Modified.RepositoryID);
            command.AddParameter("@ModifiedTick", remoteSyncableItemInfo.Modified.RepositoryTickCount);
            if (status == SyncStatus.Delete || status == SyncStatus.DeleteNonExisting)
                command.AddParameter("@ItemData", "{item:{itemRefs:[]}}");
            else
                command.AddParameter("@ItemData", DBNull.Value);
            command.ExecuteNonQuery();
        }

        public static void UpdateItemPlaceholderData(IDbConnection connection, ISyncableItemInfo remoteSyncableItemInfo, JObject itemData)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "UPDATE SyncItems SET ItemData=@ItemData WHERE ItemType=@ItemType AND GlobalCreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTick AND GlobalModifiedRepos=@ModifiedRepos AND ModifiedTickCount=@ModifiedTick";
            command.AddParameter("@ItemType", remoteSyncableItemInfo.ItemType);
            command.AddParameter("@CreatedRepos", remoteSyncableItemInfo.Created.RepositoryID);
            command.AddParameter("@CreatedTick", remoteSyncableItemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", remoteSyncableItemInfo.Modified.RepositoryID);
            command.AddParameter("@ModifiedTick", remoteSyncableItemInfo.Modified.RepositoryTickCount);
            command.AddParameter("@ItemData", itemData.ToString());
            command.ExecuteNonQuery();
        }


        public static void SaveRemoteKnowledge(IDbConnection connection, IEnumerable<RepositoryInfo> remoteKnowledge)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = "DELETE FROM RemoteKnowledge";
            command.ExecuteNonQuery();

            foreach (var reposInfo in remoteKnowledge)
            {
                IDbCommand insertCommand = connection.CreateCommand();
                insertCommand.CommandText = "INSERT INTO RemoteKnowledge(GlobalReposID, ReposTickCount) VALUES (@GlobalReposID, @ReposTickCount)";
                insertCommand.AddParameter("@GlobalReposID", reposInfo.RepositoryID);
                insertCommand.AddParameter("@ReposTickCount", reposInfo.RepositoryTickCount);
                insertCommand.ExecuteNonQuery();
            }
        }

        public static IEnumerable<IRepositoryInfo> LoadRemoteKnowledge(IDbConnection connection)
        {
            List<IRepositoryInfo> remoteKnowledge = new List<IRepositoryInfo>();
            IDbCommand selectCommand = connection.CreateCommand();
            selectCommand.CommandText = "SELECT GlobalReposID, ReposTickCount FROM RemoteKnowledge";
            using (IDataReader reader = selectCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    string reposId = (string)reader["GlobalReposID"];
                    long tick = Convert.ToInt64(reader["ReposTickCount"]);
                    remoteKnowledge.Add(new RepositoryInfo { RepositoryID = reposId, RepositoryTickCount = tick });
                }
            }

            return remoteKnowledge;
        }

        public static IRepositoryInfo RepositoryInfoFromDataReader(IDataReader reader, string fieldPrefix)
        {
            string reposId = (string)reader["Global" + fieldPrefix + "Repos"];
            long reposTick = Convert.ToInt64(reader[fieldPrefix + "TickCount"]);
            return new RepositoryInfo { RepositoryID = reposId, RepositoryTickCount = reposTick };
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
                            referencedItemInfo.Created.RepositoryID == remoteItemInfo.Created.RepositoryID &&
                            referencedItemInfo.Created.RepositoryTickCount == remoteItemInfo.Created.RepositoryTickCount &&
                            referencedItemInfo.Modified.RepositoryID == remoteItemInfo.Modified.RepositoryID &&
                            referencedItemInfo.Modified.RepositoryTickCount == remoteItemInfo.Modified.RepositoryTickCount
                            )
                        {
                            itemRefJson["creationRepositoryID"] = changedItemInfo.Created.RepositoryID;
                            itemRefJson["creationRepositoryTickCount"] = changedItemInfo.Created.RepositoryTickCount;
                            itemRefJson["modificationRepositoryID"] = changedItemInfo.Modified.RepositoryID;
                            itemRefJson["modificationRepositoryTickCount"] = changedItemInfo.Modified.RepositoryTickCount;
                            needToUpdate = true;
                        }
                    }

                    if (needToUpdate)
                    {
                        IDbCommand updateCommand = connection.CreateCommand();
                        updateCommand.CommandText = "UPDATE SyncItems SET ItemData=@ItemData, GlobalModifiedRepos=@ModifiedRepos, ModifiedTickCount=@TickCount WHERE ItemID=@ItemID";
                        updateCommand.AddParameter("@ItemID", reader["ItemID"]);
                        updateCommand.AddParameter("@ItemData", itemData.ToString());
                        updateCommand.AddParameter("@TickCount", store.IncrementLocalRepositoryTickCount());
                        updateCommand.AddParameter("@ModifiedRepos", store.GenerateLocalKnowledge().First().RepositoryID);
                        updateCommand.ExecuteNonQuery();
                    }
                }
            }
        }
    }
}
