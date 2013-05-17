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
        public PersonSyncableTypeHandler(RepositorySyncableStoreAdapter adapter) : base(adapter, ReposItemType.Person, "People")
        {
        }

        public override void BuildItemData(ISyncableItemInfo itemInfo, JObject builder)
        {
            IPerson person = Adapter.Repos.getPersonByRowID(GetRowIdFromItemInfo(itemInfo));
            builder.Add("name", person.Name);
        }

        public override void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            IDbCommand command = Adapter.Connection.CreateCommand();
            if (rowId == -1)
            {
                command.CommandText = "INSERT INTO People(PersonName, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount ) VALUES(@PersonName, @CreatedRepos, @CreatedTickCount, @ModifiedRepos, @ModifiedTickCount)";
                command.AddParameter("@CreatedRepos", Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID));
                command.AddParameter("@CreatedTickCount", itemInfo.Created.RepositoryTickCount);
            }
            else
            {
                command.CommandText = "UPDATE People SET PersonName=@PersonName, ModifiedRepos=@ModifiedRepos, ModifiedTickCount=@ModifiedTickCount WHERE PersonID=@PersonID";
                command.AddParameter("@PersonID", rowId);
            }
            command.AddParameter("@PersonName", (string)itemData["item"]["name"]);
            command.AddParameter("@ModifiedRepos", Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Modified.RepositoryID));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.RepositoryTickCount);
            command.ExecuteNonQuery();

        }

        public override void UpdateInRepos(ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            Adapter.Repos.updatePerson(Adapter.Connection, rowId);
        }

        public override void RemoveFromRepos(ISyncableItemInfo itemInfo)
        {
            Adapter.Repos.removePerson(Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID), itemInfo.Created.RepositoryTickCount);
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
