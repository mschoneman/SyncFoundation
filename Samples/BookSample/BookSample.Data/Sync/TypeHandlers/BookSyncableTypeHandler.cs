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
        public BookSyncableTypeHandler(BookRepositorySyncableStoreAdapter adapter)
            : base(adapter, ReplicaItemType.Book, "Books")
        {
        }

        public override void BuildItemData(ISyncableItemInfo itemInfo, JObject builder)
        {
            IBook book = Adapter.BookRepository.getBookByRowID(GetRowIdFromItemInfo(itemInfo));
            builder.Add("title", book.Title);
            JArray authors = new JArray();
            foreach(IPerson author in book.Authors)
            {
                ReplicaItemId id = ((Person)author).Id;
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
                command.CommandText = "INSERT INTO Books(BookTitle, CreatedReplica, CreatedTickCount, ModifiedReplica, ModifiedTickCount ) VALUES(@BookTitle, @CreatedReplica, @CreatedTickCount, @ModifiedReplica, @ModifiedTickCount)";
                command.AddParameter("@CreatedReplica", Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId));
                command.AddParameter("@CreatedTickCount", itemInfo.Created.ReplicaTickCount);
            }
            else
            {
                command.CommandText = "UPDATE Books SET BookTitle=@BookTitle,ModifiedReplica=@ModifiedReplica, ModifiedTickCount=@ModifiedTickCount WHERE BookID=@BookID";
                command.AddParameter("@BookID", rowId);
            }
            command.AddParameter("@BookTitle", (string)itemData["item"]["title"]);
            command.AddParameter("@ModifiedReplica", Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Modified.ReplicaId));
            command.AddParameter("@ModifiedTickCount", itemInfo.Modified.ReplicaTickCount);
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

        public override void UpdateInReplica(ISyncableItemInfo itemInfo)
        {
            long rowId = GetRowIdFromItemInfo(itemInfo);
            Adapter.BookRepository.updateBook(Adapter.Connection, rowId);
        }

        public override void RemoveFromReplica(ISyncableItemInfo itemInfo)
        {
            Adapter.BookRepository.removeBook(Adapter.GetLocalReplicaIdForGlobalReplicaId(itemInfo.Created.ReplicaId), itemInfo.Created.ReplicaTickCount);
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

                if(localAuthorItemInfo.Created.ReplicaId != remoteAuthorItemInfo.Created.ReplicaId)
                    return DuplicateStatus.None;
                if (localAuthorItemInfo.Created.ReplicaTickCount != remoteAuthorItemInfo.Created.ReplicaTickCount)
                    return DuplicateStatus.None;
            }


            return DuplicateStatus.Exact;
        }
    }
}
