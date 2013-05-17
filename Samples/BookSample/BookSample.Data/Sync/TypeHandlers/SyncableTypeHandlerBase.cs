using BookSample.Data.Model;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.Data.Sync.TypeHandlers
{
    abstract class SyncableTypeHandlerBase : ISyncableTypeHandler
    {
        private BookRepositorySyncableStoreAdapter adapter;
        private ReplicaItemType reposItemType;
        private string dbTable;
        public SyncableTypeHandlerBase(BookRepositorySyncableStoreAdapter adapter, ReplicaItemType reposItemType, string dbTable)
        {
            this.adapter = adapter;
            this.reposItemType = reposItemType;
            this.dbTable = dbTable;
        }


        protected BookRepositorySyncableStoreAdapter Adapter
        {
            get
            {
                return adapter;
            }
        }

        public string TypeName
        {
            get
            {
                return reposItemType.ToString();
            }
        }

        public int TypeId
        {
            get
            {
                return (int)reposItemType;
            }
        }

        public string DbTable
        {
            get
            {
                return dbTable;
            }
        }

        abstract public void BuildItemData(SyncFoundation.Core.Interfaces.ISyncableItemInfo itemInfo, Newtonsoft.Json.Linq.JObject builder);
        abstract public void SaveItemData(SyncFoundation.Core.Interfaces.ISyncableItemInfo itemInfo, Newtonsoft.Json.Linq.JObject itemData);

        public virtual void DeleteItem(SyncFoundation.Core.Interfaces.ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            if (rowId != -1)
            {
                IDbCommand command = adapter.Connection.CreateCommand();
                command.CommandText = String.Format("DELETE FROM {0} WHERE RowID=@RowID", DbTable);
                command.AddParameter("@RowID", rowId);
                command.ExecuteNonQuery();
            }
        }

        public long GetRowIdFromItemInfo(ISyncableItemInfo itemInfo)
        {
            IDbCommand command = Adapter.Connection.CreateCommand();
            command.CommandText = String.Format("SELECT RowID FROM {0} WHERE CreatedReplica=@CreatedReplica AND CreatedTickCount=@CreatedTick", DbTable);
            command.AddParameter("@CreatedReplica", adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId));
            command.AddParameter("@CreatedTick", itemInfo.Created.ReplicaTickCount);

            object o = command.ExecuteScalar();
            if (o == null)
                return -1;
            return Convert.ToInt64(o);
        }

        abstract public void UpdateInReplica(SyncFoundation.Core.Interfaces.ISyncableItemInfo itemInfo);

        protected ISyncableItemInfo getSyncableItemInfoFrom(ReplicaItemId reposItemId)
        {
            IReplicaInfo created = new ReplicaInfo { ReplicaId = Adapter.GetGlobalReplicaIdForLocalReplicaId(reposItemId.CreationReplicaLocalId), ReplicaTickCount = reposItemId.CreationTickCount };
            IReplicaInfo modified = new ReplicaInfo { ReplicaId = Adapter.GetGlobalReplicaIdForLocalReplicaId(reposItemId.ModificationReplicaLocalId), ReplicaTickCount = reposItemId.ModificationTickCount };
            return new SyncableItemInfo { ItemType = reposItemId.ItemType.ToString(), Created = created, Modified = modified, Deleted = false };
        }

        abstract public void RemoveFromReplica(ISyncableItemInfo itemInfo);

        abstract public DuplicateStatus GetDuplicateStatus(Newtonsoft.Json.Linq.JObject localItemData, Newtonsoft.Json.Linq.JObject remoteItemData);
    }
}
