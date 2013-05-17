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
    public class BookRepositorySyncableStoreAdapter : ISyncableStore, IDisposable
    {
        private BookRepository _repos;
        private IDbConnection _connection;
        private IList<ISyncableTypeHandler> _syncableTypes = new List<ISyncableTypeHandler>();

        private SynchronizationContext synchronizationContext;
        private IList<ISyncableItemInfo> _pendingUpdates = new List<ISyncableItemInfo>();
        private IList<ISyncableItemInfo> _pendingDeletes = new List<ISyncableItemInfo>();


        public BookRepositorySyncableStoreAdapter(BookRepository repos)
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

        public BookRepository BookRepository
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

        public IEnumerable<IReplicaInfo> GenerateLocalKnowledge()
        {
            List<IReplicaInfo> knowledge = new List<IReplicaInfo>();
            IDbCommand select = _connection.CreateCommand();
            select.CommandText = String.Format("SELECT GlobalReplicaID,ReplicaTickCount FROM SyncReplicaitories");
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    string globalId = Convert.ToString(reader["GlobalReplicaID"]);
                    long tick = Convert.ToInt64(reader["ReplicaTickCount"]);
                    knowledge.Add(new ReplicaInfo { ReplicaId = globalId, ReplicaTickCount = tick });
                }
            }
            return knowledge;
        }

        public IEnumerable<ISyncableItemInfo> LocateChangedItems(IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            List<ISyncableItemInfo> changes = new List<ISyncableItemInfo>();

            foreach (ISyncableTypeHandler typeHandler in _syncableTypes)
            {
                IEnumerable<ISyncableItemInfo> changedOrDeletedItems = LocateChangedOrDeletedItems(typeHandler, remoteKnowledge);
                changes.AddRange(changedOrDeletedItems);
            }

            return changes;
        }

        private IEnumerable<ISyncableItemInfo> LocateChangedOrDeletedItems(ISyncableTypeHandler typeHandler, IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            List<ISyncableItemInfo> changes = new List<ISyncableItemInfo>();

            IDbCommand select = _connection.CreateCommand();
            select.CommandText = GenerateChangeDetectionSQL(remoteKnowledge, typeHandler.DbTable);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    IReplicaInfo createdReplicaInfo = ReplicaInfoFromDataReader(reader, "Created");
                    IReplicaInfo modifiedReplicaInfo = ReplicaInfoFromDataReader(reader, "Modified");

                    changes.Add(new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false });
                }
            }

            select.CommandText = GenerateChangeDetectionSQL(remoteKnowledge, "Tombstones");
            select.CommandText += String.Format(" AND ItemType={0}", typeHandler.TypeId);
            using (IDataReader reader = select.ExecuteReader())
            {
                while (reader.Read())
                {
                    IReplicaInfo createdReplicaInfo = ReplicaInfoFromDataReader(reader, "Created");
                    IReplicaInfo modifiedReplicaInfo = ReplicaInfoFromDataReader(reader, "Modified");

                    changes.Add(new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = true });
                }
            }

            return changes;
        }

        public ISyncableItemInfo LocateCurrentItemInfo(ISyncableItemInfo source)
        {
            var typeHandler = HandlerForItemType(source.ItemType);
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount FROM {0} WHERE CreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTick", typeHandler.DbTable);
            command.AddParameter("@CreatedReplica", GetLocalReplicaIdForGlobalReplicaId(source.Created.ReplicaId));
            command.AddParameter("@CreatedTick", source.Created.ReplicaTickCount);
            using (IDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    IReplicaInfo createdReplicaInfo = ReplicaInfoFromDataReader(reader, "Created");
                    IReplicaInfo modifiedReplicaInfo = ReplicaInfoFromDataReader(reader, "Modified");

                  return new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false };
                }
            }

            command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount FROM {0} WHERE CreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTick AND ItemType={1}", "Tombstones", typeHandler.TypeId);
            command.AddParameter("@CreatedReplica", GetLocalReplicaIdForGlobalReplicaId(source.Created.ReplicaId));
            command.AddParameter("@CreatedTick", source.Created.ReplicaTickCount);
            using (IDataReader reader = command.ExecuteReader())
            {
                if (reader.Read())
                {
                    IReplicaInfo createdReplicaInfo = ReplicaInfoFromDataReader(reader, "Created");
                    IReplicaInfo modifiedReplicaInfo = ReplicaInfoFromDataReader(reader, "Modified");

                    return new SyncableItemInfo { ItemType = typeHandler.TypeName, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = true };
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

        private IReplicaInfo ReplicaInfoFromDataReader(IDataReader reader, string fieldPrefix)
        {
            string reposId = GetGlobalReplicaIdForLocalReplicaId(Convert.ToInt64(reader[fieldPrefix + "Replica"]));
            long reposTick = Convert.ToInt64(reader[fieldPrefix + "TickCount"]);
            return new ReplicaInfo { ReplicaId = reposId, ReplicaTickCount = reposTick };
        }

        private string GenerateChangeDetectionSQL(IEnumerable<IReplicaInfo> remoteKnowledge, string tableName)
        {
            StringBuilder sql = new StringBuilder();
            StringBuilder sqlNotIn = new StringBuilder();
            sql.AppendFormat("SELECT CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount FROM {0} WHERE (", tableName);
	        sqlNotIn.Append(" OR (ModifiedReplica NOT IN (");

            bool foundMatchingReplica = false;

	        foreach(IReplicaInfo reposInfo in remoteKnowledge)
	        {
		        long remoteReplicaTickCount = reposInfo.ReplicaTickCount;
		        long localReplicaID = GetLocalReplicaIdForGlobalReplicaId(reposInfo.ReplicaId);


		        /* Check to see if we have a matching repos here*/
		        if(localReplicaID != -1)
		        {
			        if(foundMatchingReplica)
			        {
                        sql.Append(" OR ");
                        sqlNotIn.Append(", ");
			        }

                    sql.AppendFormat("(ModifiedReplica = {0} AND ModifiedTickCount > {1})", localReplicaID, remoteReplicaTickCount);

                    sqlNotIn.AppendFormat("{0}", localReplicaID);
			        foundMatchingReplica = true;
		        }
	        }

            if (!foundMatchingReplica)
                return String.Format("SELECT CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount FROM {0}", tableName);

            sqlNotIn.Append(")))");
	        sql.Append(sqlNotIn);
            return sql.ToString();
        }

        internal string GetGlobalReplicaIdForLocalReplicaId(long localId)
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT GlobalReplicaID FROM SyncReplicaitories WHERE LocalReplicaID={0}", localId);
            return command.ExecuteScalar().ToString();
        }

        internal long GetLocalReplicaIdForGlobalReplicaId(string globalId)
        {
            IDbCommand command = _connection.CreateCommand();
            command.CommandText = String.Format("SELECT LocalReplicaID FROM SyncReplicaitories WHERE GlobalReplicaID='{0}'", escapeSQL(globalId));
            object o = command.ExecuteScalar();
            if (o == null)
            {
                command.CommandText = String.Format("INSERT INTO SyncReplicaitories(GlobalReplicaID,ReplicaTickCount) VALUES('{0}', 0)", escapeSQL(globalId));
                command.ExecuteNonQuery();
                return GetLocalReplicaIdForGlobalReplicaId(globalId);
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
            command.CommandText = "DELETE FROM Tombstones WHERE ItemType=@ItemType AND CreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTickCount";
            command.AddParameter("@ItemType", handler.TypeId);
            command.AddParameter("@CreatedReplica", GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId));
            command.AddParameter("@CreatedTickCount", itemInfo.Created.ReplicaTickCount);
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
            command.CommandText = "INSERT INTO Tombstones(ItemType, CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount) VALUES (@ItemType,@CreatedReplica,@CreatedTick,@ModifiedReplica,@ModifiedTickCount)";
            command.AddParameter("@ItemType", handler.TypeId);
            command.AddParameter("@CreatedReplica", GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId));
            command.AddParameter("@CreatedTick", itemInfo.Created.ReplicaTickCount);
            command.AddParameter("@ModifiedReplica", GetLocalReplicaIdForGlobalReplicaId(itemInfo.Modified.ReplicaId));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.ReplicaTickCount);
            command.ExecuteNonQuery();
        }

        public void UpdateLocalKnowledge(IEnumerable<IReplicaInfo> remoteKnowledge)
        {
            foreach (var reposInfo in remoteKnowledge)
            {
                IDbCommand command = _connection.CreateCommand();
                command.CommandText = "UPDATE SyncReplicaitories SET ReplicaTickCount=@TickCount WHERE GlobalReplicaID=@ReplicaID AND ReplicaTickCount < @TickCount";
                command.AddParameter("@ReplicaID", reposInfo.ReplicaId);
                command.AddParameter("@TickCount", reposInfo.ReplicaTickCount);
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
                    updateReplica();
                }, null);
            }
            else
            {
                updateReplica();
            }
        }

        private void updateReplica()
        {
            foreach (var itemInfo in _pendingUpdates)
            {
                var handler = HandlerForItemType(itemInfo.ItemType);
                handler.UpdateInReplica(itemInfo);
            }
            foreach (var itemInfo in _pendingDeletes)
            {
                var handler = HandlerForItemType(itemInfo.ItemType);
                handler.RemoveFromReplica(itemInfo);
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

        public long IncrementLocalRepilcaTickCount()
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
            var knowledge = GenerateLocalKnowledge().OrderBy((ri) => { return ri.ReplicaId + ri.ReplicaTickCount.ToString(); });
            state.Add("knowledge",SyncUtil.KnowledgeToJson(knowledge));
            foreach(var itemType in GetItemTypes())
            {
                var handler = HandlerForItemType(itemType);
                JArray items = new JArray();
                IDbCommand selectCommand = _connection.CreateCommand();
                selectCommand.CommandText = String.Format("SELECT CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount FROM {0}", handler.DbTable);
                IList<ISyncableItemInfo> itemInfos = new List<ISyncableItemInfo>();
                using (IDataReader reader = selectCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        IReplicaInfo createdReplicaInfo = ReplicaInfoFromDataReader(reader, "Created");
                        IReplicaInfo modifiedReplicaInfo = ReplicaInfoFromDataReader(reader, "Modified");

                        itemInfos.Add(new SyncableItemInfo { ItemType = handler.TypeName, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false });
                    }
                }


                var sortedItemInfos = itemInfos.OrderBy((ii) => { return ii.Created.ReplicaId + ii.Created.ReplicaTickCount.ToString(); });
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
