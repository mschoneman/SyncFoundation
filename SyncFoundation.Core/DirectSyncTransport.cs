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
    public class DirectSyncTransport : ISyncTransport
    {
        private readonly SyncServerSession _serverSession;
        public DirectSyncTransport(ISyncableStore store, IDbConnection connection)
        {
            _serverSession = new SyncServerSession(store, connection);
        }
        public Task<JObject> TransportAsync(SyncEndpoint endpoint, JObject contents)
        {
            switch (endpoint)
            {
                case SyncEndpoint.BeginSession:
                    return Task.FromResult(_serverSession.BeginSession(contents));
                case SyncEndpoint.EndSession:
                    return Task.FromResult(_serverSession.EndSession(contents));
                case SyncEndpoint.GetChanges:
                    return Task.FromResult(_serverSession.GetChanges(contents));
                case SyncEndpoint.GetItemData:
                    return Task.FromResult(_serverSession.GetItemData(contents));
                case SyncEndpoint.GetItemDataBatch:
                    return Task.FromResult(_serverSession.GetItemDataBatch(contents));
                case SyncEndpoint.PutChanges:
                    return Task.FromResult(_serverSession.PutChanges(contents));
                case SyncEndpoint.PutItemDataBatch:
                    return Task.FromResult(_serverSession.PutItemDataBatch(contents));
                case SyncEndpoint.ApplyChanges:
                    return Task.FromResult(_serverSession.ApplyChanges(contents));
                default:
                    throw new Exception("Unknown endpoint");
            }
        }
    }
}
