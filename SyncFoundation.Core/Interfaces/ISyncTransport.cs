using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncTransport
    {
        Task<JObject> TransportAsync(SyncEndpoint endpoint, JObject contents);
    }

    public enum SyncEndpoint
    {
        BeginSession,
        EndSession,
        GetChanges,
        GetItemData,
        GetItemDataBatch,
        PutChanges,
        PutItemDataBatch,
        ApplyChanges,
    }
}
