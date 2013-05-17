using Newtonsoft.Json.Linq;
using BookSample.Data.Interfaces;
using BookSample.Data.Model;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SyncFoundation.Core;

namespace BookSample.Data.Sync.TypeHandlers
{
    class PersonSyncableTypeHandler : SyncableTypeHandlerBase
    {
        public PersonSyncableTypeHandler(BookRepositorySyncableStoreAdapter adapter) : base(adapter, ReplicaItemType.Person, "People")
        {
        }

        public override void BuildItemData(ISyncableItemInfo itemInfo, JObject builder)
        {
            IPerson person = Adapter.BookRepository.getPersonByRowID(GetRowIdFromItemInfo(itemInfo));
            builder.Add("name", person.Name);
        }

        public override void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            IDbCommand command = Adapter.Connection.CreateCommand();
            if (rowId == -1)
            {
                command.CommandText = "INSERT INTO People(PersonName, CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount ) VALUES(@PersonName, @CreatedReplica, @CreatedTickCount, @ModifiedReplica, @ModifiedTickCount)";
                command.AddParameter("@CreatedReplica", Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId));
                command.AddParameter("@CreatedTickCount", itemInfo.Created.ReplicaTickCount);
            }
            else
            {
                command.CommandText = "UPDATE People SET PersonName=@PersonName, ModifiedReplica=@ModifiedReplica, ModifiedTickCount=@ModifiedTickCount WHERE PersonID=@PersonID";
                command.AddParameter("@PersonID", rowId);
            }
            command.AddParameter("@PersonName", (string)itemData["item"]["name"]);
            command.AddParameter("@ModifiedReplica", Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Modified.ReplicaId));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.ReplicaTickCount);
            command.ExecuteNonQuery();

        }

        public override void UpdateInReplica(ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            Adapter.BookRepository.updatePerson(Adapter.Connection, rowId);
        }

        public override void RemoveFromReplica(ISyncableItemInfo itemInfo)
        {
            Adapter.BookRepository.removePerson(Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId), itemInfo.Created.ReplicaTickCount);
        }

        public override DuplicateStatus GetDuplicateStatus(JObject localItemData, JObject remoteItemData)
        {
            string localName = (string)localItemData["item"]["name"];
            string remoteName = (string)remoteItemData["item"]["name"];

            if (localName == remoteName)
                return DuplicateStatus.Exact;

            return DuplicateStatus.None;
        }
    }
}
