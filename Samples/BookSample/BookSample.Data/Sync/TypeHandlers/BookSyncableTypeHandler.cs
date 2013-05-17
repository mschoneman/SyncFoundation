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
    class BookSyncableTypeHandler : SyncableTypeHandlerBase
    {
        public BookSyncableTypeHandler(RepositorySyncableStoreAdapter adapter)
            : base(adapter, ReposItemType.Book, "Books")
        {
        }

        public override void BuildItemData(ISyncableItemInfo itemInfo, JObject builder)
        {
            IBook book = Adapter.Repos.getBookByRowID(GetRowIdFromItemInfo(itemInfo));
            builder.Add("title", book.Title);
            JArray authors = new JArray();
            foreach(IPerson author in book.Authors)
            {
                ReposItemId id = ((Person)author).Id;
                authors.Add(SyncUtil.GenerateItemRefAndIndex(builder, getSyncableItemInfoFrom(id)));
            }
            builder.Add("authors", authors);
        }

        public override void SaveItemData(ISyncableItemInfo itemInfo, JObject itemData)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            IDbCommand command = Adapter.Connection.CreateCommand();
            if (rowId == -1)
            {
                command.CommandText = "INSERT INTO Books(BookTitle, CreatedRepos, CreatedTickCount, ModifiedRepos, ModifiedTickCount ) VALUES(@BookTitle, @CreatedRepos, @CreatedTickCount, @ModifiedRepos, @ModifiedTickCount)";
                command.AddParameter("@CreatedRepos", Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID));
                command.AddParameter("@CreatedTickCount", itemInfo.Created.RepositoryTickCount);
            }
            else
            {
                command.CommandText = "UPDATE Books SET BookTitle=@BookTitle,ModifiedRepos=@ModifiedRepos, ModifiedTickCount=@ModifiedTickCount WHERE BookID=@BookID";
                command.AddParameter("@BookID", rowId);
            }
            command.AddParameter("@BookTitle", (string)itemData["item"]["title"]);
            command.AddParameter("@ModifiedRepos", Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Modified.RepositoryID));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.RepositoryTickCount);
            command.ExecuteNonQuery();
            command.Parameters.Clear();

            if (rowId == -1)
                rowId = GetRowIdFromItemInfo(itemInfo);

            command.CommandText = "DELETE FROM BookAuthors WHERE BookID = @ID;";
            command.AddParameter("@ID", rowId);
            command.ExecuteNonQuery();
            command.Parameters.Clear();


            int authorPriority = 0;
            foreach (var authors in itemData["item"]["authors"])
            {
                ISyncableItemInfo authorItemInfo = SyncUtil.SyncableItemInfoFromJsonItemRef(itemData["item"], authors);
                long authorRowId = Adapter.HandlerForItemType(authorItemInfo.ItemType).GetRowIdFromItemInfo(authorItemInfo);
                authorPriority++;
                command.CommandText = String.Format("INSERT INTO BookAuthors(BookID, PersonID, AuthorPriority) VALUES ({0},{1},{2})", rowId, authorRowId, authorPriority);
                command.ExecuteNonQuery();
            }
        }

        public override void DeleteItem(ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            if (rowId != -1)
            {
                IDbCommand command = Adapter.Connection.CreateCommand();
                command.CommandText = "DELETE FROM BookAuthors WHERE BookID = @ID;";
                command.AddParameter("@ID", rowId);
                command.ExecuteNonQuery();
            }
            base.DeleteItem(itemInfo);
        }

        public override void UpdateInRepos(ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            Adapter.Repos.updateBook(Adapter.Connection, rowId);
        }

        public override void RemoveFromRepos(ISyncableItemInfo itemInfo)
        {
            Adapter.Repos.removeBook(Adapter.GetLocalRepositoryIdForGlobalRepositoryId(itemInfo.Created.RepositoryID), itemInfo.Created.RepositoryTickCount);
        }

        public override DuplicateStatus GetDuplicateStatus(JObject localItemData, JObject remoteItemData)
        {
            string localName = (string)localItemData["item"]["title"];
            string remoteName = (string)remoteItemData["item"]["title"];

            if (localName != remoteName)
                return DuplicateStatus.None;

            JArray localAuthors = (JArray)localItemData["item"]["authors"];
            JArray remoteAuthors = (JArray)remoteItemData["item"]["authors"];

            if(localAuthors.Count != remoteAuthors.Count)
                return DuplicateStatus.None;

            for (int i = 0; i < localAuthors.Count; i++)
            {
                ISyncableItemInfo localAuthorItemInfo = SyncUtil.SyncableItemInfoFromJsonItemRef(localItemData["item"], localAuthors[i]);
                ISyncableItemInfo remoteAuthorItemInfo = SyncUtil.SyncableItemInfoFromJsonItemRef(remoteItemData["item"], remoteAuthors[i]);

                if(localAuthorItemInfo.Created.RepositoryID != remoteAuthorItemInfo.Created.RepositoryID)
                    return DuplicateStatus.None;
                if (localAuthorItemInfo.Created.RepositoryTickCount != remoteAuthorItemInfo.Created.RepositoryTickCount)
                    return DuplicateStatus.None;
            }


            return DuplicateStatus.Exact;
        }
    }
}
