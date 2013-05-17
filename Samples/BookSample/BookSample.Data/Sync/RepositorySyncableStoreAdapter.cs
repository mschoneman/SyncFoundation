using Newtonsoft.Json.Linq;
using BookSample.Data.Interfaces;
using BookSample.Data.Model;
using BookSample.Data.Sync.TypeHandlers;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace BookSample.Data.Sync
{
    public class RepositorySyncableStoreAdapter : ISyncableStore, IDisposable
    {
        private BookRepository _repos;
        private IDbConnection _connection;
        private IList<ISyncableTypeHandler> _syncableTypes = new List<ISyncableTypeHandler>();

        private SynchronizationContext synchronizationContext;
        private IList<ISyncableItemInfo> _pendingUpdates = new List<ISyncableItemInfo>();
        private IList<ISyncableItemInfo> _pendingDeletes = new List<ISyncableItemInfo>();


        public RepositorySyncableStoreAdapter(BookRepository repos)
        {
            _repos = repos;
            _connection = _repos.createConnection();
            _syncableTypes.Add(new PersonSyncableTypeHandler(this));
            _syncableTypes.Add(new BookSyncableTypeHandler(this));
            this.synchronizationContext = SynchronizationContext.Current;
        }

        public void Close()
        {
            _connection.Close();
        }

        public BookRepository Repos
        {
            get
            {
                return _repos;
            }
        }

        internal IDbConnection Connection
        {
            get
            {
                return _connection;
            }
        }

        public IEnumerable<string> GetItemTypes()
        {
            List<string> types = new List<string>();
            foreach (var handler in _syncableTypes)
                types.Add(handler.TypeName);
            return types;
        }

        public IEnumerable<IRepositoryInfo> GenerateLocalKnowledge()
        {
            List<IRepositoryInfo> knowledge = new List<IRepositoryInfo>();
            IDbCommand select = _connection.CreateCommand();
            select.CommandText = String.Format("SELECT GlobalReposID,ReposTickCount FROM SyncRepositories");
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    string globalId = Convert.ToString(reader["GlobalReposID"]);
                    long tick = Convert.ToInt64(reader["ReposTickCount"]);
                    knowledge.Add(new RepositoryInfo { RepositoryID = globalId, RepositoryTickCount = tick });
                }
            }
            return knowledge;
        }

        public IEnumerable<ISyncableItemInfo> LocateChangedItems(IEnumerable<IRepositoryInfo> remoteKnowledge)
        {
            List<ISyncableItemInfo> changes = new List<ISyncableItemInfo>();

            foreach (ISyncableTypeHandler typeHandler in _syncableTypes)
            {
                IEnumerable<ISyncableItemInfo> changedOrDeletedItems = LocateChangedOrDeletedItems(typeHandler, remoteKnowledge);
                changes.AddRange(changedOrDeletedItems);
            }

            return changes;
        }

        private IEnumerable<ISyncableItemInfo> LocateChangedOrDeletedItems(ISyncableTypeHandler typeHandler, IEnumerable<IRepositoryInfo> remoteKnowledge)
        {
            List<ISyncableItemInfo> changes = new List<ISyncableItemInfo>();

            IDbCommand select = _connection.CreateCommand();
            select.CommandText = GenerateChangeDetectionSQL(remoteKnowledge, typeHandler.DbTable);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    IRepositoryInfo createdRepositoryInfo = RepositoryInfoFromDataReader(reader, "Created");
                    IRepositoryInfo modifiedRepositoryInfo = RepositoryInfoFromDataReader(reader, "Modified");

                    changes.Add(new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = false });
                }
            }

            select.CommandText = GenerateChangeDetectionSQL(remoteKnowledge, "Tombstones");
            select.CommandText += String.Format(" AND ItemType={0}", typeHandler.TypeId);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    IRepositoryInfo createdRepositoryInfo = RepositoryInfoFromDataReader(reader, "Created");
                    IRepositoryInfo modifiedRepositoryInfo = RepositoryInfoFromDataReader(reader, "Modified");

                    changes.Add(new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = true });
                }
            }

            return changes;
        }

        public ISyncableItemInfo LocateCurrentItemInfo(ISyncableItemInfo source)
        {
            var typeHandler = HandlerForItemType(source.ItemType);
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount FROM {0} WHERE CreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTick", typeHandler.DbTable);
            command.AddParameter("@CreatedRepos", GetLocalRepositoryIdForGlobalRepositoryId(source.Created.RepositoryID));
            command.AddParameter("@CreatedTick", source.Created.RepositoryTickCount);
            using (IDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    IRepositoryInfo createdRepositoryInfo = RepositoryInfoFromDataReader(reader, "Created");
                    IRepositoryInfo modifiedRepositoryInfo = RepositoryInfoFromDataReader(reader, "Modified");

                  return new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = false };
                }
            }

            command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount FROM {0} WHERE CreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTick AND ItemType={1}", "Tombstones", typeHandler.TypeId);
            command.AddParameter("@CreatedRepos", GetLocalRepositoryIdForGlobalRepositoryId(source.Created.RepositoryID));
            command.AddParameter("@CreatedTick", source.Created.RepositoryTickCount);
            using (IDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    IRepositoryInfo createdRepositoryInfo = RepositoryInfoFromDataReader(reader, "Created");
                    IRepositoryInfo modifiedRepositoryInfo = RepositoryInfoFromDataReader(reader, "Modified");

                    return new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = true };
                }
            }

            return null;
        }



        internal ISyncableTypeHandler HandlerForItemType(string itemType)
        {
            foreach (ISyncableTypeHandler typeHandler in _syncableTypes)
            {
                if (typeHandler.TypeName == itemType)
                    return typeHandler;
            }
            throw new ArgumentException();
        }

        private IRepositoryInfo RepositoryInfoFromDataReader(IDataReader reader, string fieldPrefix)
        {
            string reposId = GetGlobalRepositoryIdForLocalRepositoryId(Convert.ToInt64(reader[fieldPrefix + "Repos"]));
            long reposTick = Convert.ToInt64(reader[fieldPrefix + "TickCount"]);
            return new RepositoryInfo { RepositoryID = reposId, RepositoryTickCount = reposTick };
        }

        private string GenerateChangeDetectionSQL(IEnumerable<IRepositoryInfo> remoteKnowledge, string tableName)
        {
            StringBuilder sql = new StringBuilder();
            StringBuilder sqlNotIn = new StringBuilder();
            sql.AppendFormat("SELECT CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount FROM {0} WHERE (", tableName);
	        sqlNotIn.Append(" OR (ModifiedRepos NOT IN (");

            bool foundMatchingRepos = false;

	        foreach(IRepositoryInfo reposInfo in remoteKnowledge)
	        {
		        long remoteRepositoryTickCount = reposInfo.RepositoryTickCount;
		        long localReposID = GetLocalRepositoryIdForGlobalRepositoryId(reposInfo.RepositoryID);


		        /* Check to see if we have a matching repos here*/
		        if(localReposID != -1)
		        {
			        if(foundMatchingRepos)
			        {
                        sql.Append(" OR ");
                        sqlNotIn.Append(", ");
			        }

                    sql.AppendFormat("(ModifiedRepos = {0} AND ModifiedTickCount > {1})", localReposID, remoteRepositoryTickCount);

                    sqlNotIn.AppendFormat("{0}", localReposID);
			        foundMatchingRepos = true;
		        }
	        }

            if (!foundMatchingRepos)
                return String.Format("SELECT CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount FROM {0}", tableName);

            sqlNotIn.Append(")))");
	        sql.Append(sqlNotIn);
            return sql.ToString();
        }

        internal string GetGlobalRepositoryIdForLocalRepositoryId(long localId)
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT GlobalReposID FROM SyncRepositories WHERE LocalReposID={0}", localId);
            return command.ExecuteScalar().ToString();
        }

        internal long GetLocalRepositoryIdForGlobalRepositoryId(string globalId)
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT LocalReposID FROM SyncRepositories WHERE GlobalReposID='{0}'", escapeSQL(globalId));
            object o = command.ExecuteScalar();
            if (o == null)
            {
                command.CommandText = String.Format("INSERT INTO SyncRepositories(GlobalReposID,ReposTickCount) VALUES('{0}', 0)", escapeSQL(globalId));
                command.ExecuteNonQuery();
                return GetLocalRepositoryIdForGlobalRepositoryId(globalId);
            }
            return Convert.ToInt64(o);
        }

        private static string escapeSQL(string source)
        {
            return Community.CsharpSqlite.Sqlite3.sqlite3_mprintf("%q", source);
        }

        public void BuildItemData(ISyncableItemInfo itemInfo, JObject builder)
        {
            HandlerForItemType(itemInfo.ItemType).BuildItemData(itemInfo, builder);
        }

        public void BeginChanges()
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "BEGIN";
            command.ExecuteNonQuery();
            _pendingUpdates.Clear();
            _pendingDeletes.Clear();
        }

        public void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData)
        {
            RemoveTombstone(itemInfo);

            HandlerForItemType(itemInfo.ItemType).SaveItemData(itemInfo, itemData);

            _pendingUpdates.Add(itemInfo);
        }

        private void RemoveTombstone(ISyncableItemInfo itemInfo)
        {
            var handler = HandlerForItemType(itemInfo.ItemType);
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "DELETE FROM Tombstones WHERE ItemType=@ItemType AND CreatedRepos=@CreatedRepos AND CreatedTickCount=@CreatedTickCount";
            command.AddParameter("@ItemType", handler.TypeId);
            command.AddParameter("@CreatedRepos", GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID));
            command.AddParameter("@CreatedTickCount", itemInfo.Created.RepositoryTickCount);
            command.ExecuteNonQuery();
        }

        public void DeleteItem(ISyncableItemInfo itemInfo)
        {
            InsertTombstone(itemInfo);

            HandlerForItemType(itemInfo.ItemType).DeleteItem(itemInfo);

            _pendingDeletes.Add(itemInfo);
        }

        private void InsertTombstone(ISyncableItemInfo itemInfo)
        {
            var handler = HandlerForItemType(itemInfo.ItemType);
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "INSERT INTO Tombstones(ItemType, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount) VALUES (@ItemType,@CreatedRepos,@CreatedTick,@ModifiedRepos,@ModifiedTickCount)";
            command.AddParameter("@ItemType", handler.TypeId);
            command.AddParameter("@CreatedRepos", GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID));
            command.AddParameter("@CreatedTick", itemInfo.Created.RepositoryTickCount);
            command.AddParameter("@ModifiedRepos", GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Modified.RepositoryID));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.RepositoryTickCount);
            command.ExecuteNonQuery();
        }

        public void UpdateLocalKnowledge(IEnumerable<IRepositoryInfo> remoteKnowledge)
        {
            foreach (var reposInfo in remoteKnowledge)
            {
                IDbCommand command = _connection.CreateCommand();
                command.CommandText = "UPDATE SyncRepositories SET ReposTickCount=@TickCount WHERE GlobalReposID=@ReposID AND ReposTickCount < @TickCount";
                command.AddParameter("@ReposID", reposInfo.RepositoryID);
                command.AddParameter("@TickCount", reposInfo.RepositoryTickCount);
                command.ExecuteNonQuery();
            }
        }

        public void AcceptChanges()
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "COMMIT";
            command.ExecuteNonQuery();
            if (synchronizationContext != null && synchronizationContext != SynchronizationContext.Current)
            {
                synchronizationContext.Post((state) =>
                {
                    updateRepository();
                }, null);
            }
            else
            {
                updateRepository();
            }
        }

        private void updateRepository()
        {
            foreach (var itemInfo in _pendingUpdates)
            {
                var handler = HandlerForItemType(itemInfo.ItemType);
                handler.UpdateInRepos(itemInfo);
            }
            foreach (var itemInfo in _pendingDeletes)
            {
                var handler = HandlerForItemType(itemInfo.ItemType);
                handler.RemoveFromRepos(itemInfo);
            }
        }

        public void RejectChanges()
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = "ROLLBACK";
            command.ExecuteNonQuery();
        }

        public void Dispose()
        {
            _connection.Close();
        }

        public long IncrementLocalRepositoryTickCount()
        {
            return _repos.incrementTickCount(_connection, 0);
        }


        public DuplicateStatus GetDuplicateStatus(string itemType, JObject localItemData, JObject remoteItemData)
        {
            return HandlerForItemType(itemType).GetDuplicateStatus(localItemData, remoteItemData);
        }

        public string GetDbState()
        {
            JObject state = new JObject();
            var knowledge = GenerateLocalKnowledge().OrderBy((ri) => { return ri.RepositoryID + ri.RepositoryTickCount.ToString(); });
            state.Add("knowledge",SyncUtil.KnowledgeToJson(knowledge));
            foreach(var itemType in GetItemTypes())
            {
                var handler = HandlerForItemType(itemType);
                JArray items = new JArray();
                IDbCommand selectCommand = _connection.CreateCommand();
                selectCommand.CommandText = String.Format("SELECT CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount FROM {0}", handler.DbTable);
                IList<ISyncableItemInfo> itemInfos = new List<ISyncableItemInfo>();
                using (IDataReader reader = selectCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IRepositoryInfo createdRepositoryInfo = RepositoryInfoFromDataReader(reader, "Created");
                        IRepositoryInfo modifiedRepositoryInfo = RepositoryInfoFromDataReader(reader, "Modified");

                        itemInfos.Add(new SyncableItemInfo { ItemType = handler.TypeName, Created = createdRepositoryInfo, Modified = modifiedRepositoryInfo, Deleted = false });
                    }
                }


                var sortedItemInfos = itemInfos.OrderBy((ii) => { return ii.Created.RepositoryID + ii.Created.RepositoryTickCount.ToString(); });
                foreach (var syncItemInfo in sortedItemInfos)
                {
                    JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(syncItemInfo);
                    BuildItemData(syncItemInfo, builder);
                    items.Add(builder);
                }
                state.Add(itemType,items);
            }
            return state.ToString();
        }
    }
}
