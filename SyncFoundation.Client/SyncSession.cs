using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
using System.Net.Http;
#if NETFX_CORE
using Windows.Security.Cryptography;
using Windows.Security.Cryptography.Core;
#else
using System.Security.Cryptography;
#endif
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;

namespace SyncFoundation.Client
{
    public class SyncSession
    {
        private readonly ISyncableStore _store;
        private readonly Uri _remoteAddress;
        private readonly string _username;
        private readonly string _password;
        private readonly ISyncSessionDbConnectionProvider _syncSessionDbConnectionProvider;
        private readonly string _localSessionId;

        private IProgress<SyncProgress> _progressWatcher = null;
        private string _remoteSessionId = null;
        private IEnumerable<IReplicaInfo> _remoteKnowledge = null;
        private Task<IEnumerable<SyncConflict>> _task;
        private CancellationToken _cancellationToken = CancellationToken.None;
        private bool _closed = false;

        public int PushMaxBatchCount { get; set; }
        public int PushMaxBatchSize { get; set; }

        public int PullMaxBatchCount { get; set; }
        public int PullMaxBatchSize { get; set; } 

        public SyncSession(ISyncableStore store, ISyncSessionDbConnectionProvider syncSessionDbConnectionProvider, Uri remoteAddress, string username, string password)
        {
            PushMaxBatchCount = 500;
            PushMaxBatchSize = 1024 * 1024;

            PullMaxBatchCount = 5000;
            PullMaxBatchSize = 1024 * 1024 * 10;

            _store = store;
            _syncSessionDbConnectionProvider = syncSessionDbConnectionProvider;
            _remoteAddress = remoteAddress;
            _username = username;
            _password = password;
            _localSessionId = Guid.NewGuid().ToString();
            _syncSessionDbConnectionProvider.SessionStart(_localSessionId);
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.CreateSessionDbTables(connection);
            }
        }

        private void reportProgressAndCheckCacellation(SyncProgress progress)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            if(_progressWatcher != null)
                _progressWatcher.Report(progress);
        }

        private HttpClient _client;
        private HttpClient client
        {
            get
            {
                if (_client == null)
                {
                    _client = new HttpClient(new HttpClientHandler() { AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip });
                    _client.Timeout = TimeSpan.FromMinutes(5);
                }
                return _client;
            }
        }

        private async Task<JObject> sendRequestAsync(Uri endpoint, JObject request)
        {
            //System.Diagnostics.Debug.WriteLine("\n\nREQUEST to: " + endpoint.ToString());
            //System.Diagnostics.Debug.WriteLine(request.ToString());

            var content = new StringContent(request.ToString(),Encoding.UTF8,"application/json");
            var compressedContent = new CompressedContent(content, "gzip");

            HttpResponseMessage responseMessage = await client.PostAsync(endpoint, compressedContent);
            string responseString = await responseMessage.Content.ReadAsStringAsync();

            if (!responseMessage.IsSuccessStatusCode)
                throw new Exception(String.Format("Remote call failed (HTTP Status Code {0}): {1}", responseMessage.StatusCode, responseString));

            //System.Diagnostics.Debug.WriteLine("RESPONSE:\n" + responseString);
            JObject response = JObject.Parse(responseString);

            if (response["errorCode"] != null)
            {
                throw new Exception(String.Format("Remote call failed with error code {0} - {1}",response["errorCode"], response["errorMessage"]));
            }

            return response;
        }

        private byte[] generateNonce()
        {
#if NETFX_CORE
            byte[] nonce;
            CryptographicBuffer.CopyToByteArray(CryptographicBuffer.GenerateRandom(16), out nonce);
            return nonce;
#else
            byte[] nonce = new byte[16];
            RNGCryptoServiceProvider rng = new RNGCryptoServiceProvider();
            rng.GetBytes(nonce);
            return nonce;
#endif
        }

        private byte[] computeHash(byte[] source)
        {
#if NETFX_CORE
            var bufSource = CryptographicBuffer.CreateFromByteArray(source);
            // Create a HashAlgorithmProvider object.
            var hashProvider = HashAlgorithmProvider.OpenAlgorithm(HashAlgorithmNames.Sha1);

            // Hash the message.
            var buffHash = hashProvider.HashData(bufSource);
            byte[] digest;
            CryptographicBuffer.CopyToByteArray(buffHash, out digest);
            return digest;
#else
            return new SHA1Managed().ComputeHash(source);
#endif
        }

        private void addCredentials(JObject request)
        {
            string now = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
            byte[] nonce = generateNonce();
            byte[] created = Encoding.UTF8.GetBytes(now);
            byte[] password = Encoding.UTF8.GetBytes(_password);
            byte[] digestSource = new byte[nonce.Length + created.Length + password.Length];

            for (int i = 0; i < nonce.Length; i++)
                digestSource[i] = nonce[i];
            for (int i = 0; i < created.Length; i++)
                digestSource[nonce.Length + i] = created[i];
            for (int i = 0; i < password.Length; i++)
                digestSource[created.Length + nonce.Length + i] = password[i];


            byte[] digestBytes = computeHash(digestSource);
            string digest = Convert.ToBase64String(digestBytes);

            request.Add("username", _username);
            request.Add("nonce", Convert.ToBase64String(nonce));
            request.Add("created", now);
            request.Add("digest", digest);
            if(_remoteSessionId != null)
                request.Add("sessionID", _remoteSessionId);
        }

        private async Task openSession()
        {
            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Opening Session" });
            JObject request = new JObject();
            addCredentials(request);

            JArray types = new JArray();
            foreach (var type in _store.GetItemTypes())
                types.Add(type);
            request.Add("itemTypes", types);

            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "beginSession"), request);
            _remoteSessionId = (string)response["sessionID"];
        }

        private async Task closeSession()
        {
            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Closing Session" });
            JObject request = new JObject();
            addCredentials(request);
            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "endSession"), request);
            _remoteSessionId = null;
            _syncSessionDbConnectionProvider.SessionEnd(_remoteSessionId);
        }

        private async Task<IEnumerable<SyncConflict>> pullChanges()
        {
            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Pulling Changes" });

            JObject request = new JObject();
            addCredentials(request);

            var localKnowledge = _store.GenerateLocalKnowledge();
            request.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(localKnowledge)));

            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "getChanges"), request);
            _remoteKnowledge = SyncUtil.KnowledgeFromJson(response["knowledge"]);

            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = "BEGIN";
                command.ExecuteNonQuery();
                SessionDbHelper.ClearSyncItems(connection);

                long startTick = Environment.TickCount;
                int previousPercentComplete = -1;
                int totalChanges = (int)response["totalChanges"];
                for (int i = 1; i <= totalChanges; )
                {
                    i += await saveChangesBatch(connection, localKnowledge, i);

                    int percentComplete = ((i * 100) / totalChanges);
                    if (percentComplete != previousPercentComplete)
                    {
                        reportProgressAndCheckCacellation(new SyncProgress() { Message = String.Format("Pulling Item Changes {0}% complete ({1})", percentComplete, String.Format("Averaging {0}ms/item over {1} items", (Environment.TickCount - startTick) / i, i)) });
                    }
                    previousPercentComplete = percentComplete;

                }
                command.CommandText = "COMMIT";
                command.ExecuteNonQuery();

                List<SyncConflict> conflicts = new List<SyncConflict>();
                conflicts.AddRange(checkForDuplicates(connection));
                conflicts.AddRange(loadConflicts(connection));
                return conflicts;
            }
        }

        private async Task<int> saveChangesBatch(IDbConnection connection, IEnumerable<IReplicaInfo> localKnowledge, int startItem)
        {
            JObject request = new JObject();
            addCredentials(request);
            request.Add("startItem", startItem);
            request.Add("maxBatchCount", PullMaxBatchCount);
            request.Add("maxBatchSize", PullMaxBatchSize);

            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "GetItemDataBatch"), request);
            JArray batch = (JArray)response["batch"];

            for (int i = 0; i < batch.Count; i++)
            {
                ISyncableItemInfo remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(batch[i]["item"]);
                JObject itemData = new JObject();
                itemData.Add("item", batch[i]["item"]);

                SyncStatus status = SyncStatus.MayBeNeeded;
                if (!SyncUtil.KnowledgeContains(localKnowledge, remoteSyncableItemInfo.Modified))
                {
                    ISyncableItemInfo localSyncableItemInfo = _store.LocateCurrentItemInfo(remoteSyncableItemInfo);
                    status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo, _remoteKnowledge);
                }
                SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, itemData);
                JArray itemRefs = (JArray)itemData["item"]["itemRefs"];
                foreach (var item in itemRefs)
                {
                    ISyncableItemInfo itemRefInfo = SyncUtil.SyncableItemInfoFromJson(item);
                    ISyncableItemInfo localItemRefInfo = _store.LocateCurrentItemInfo(itemRefInfo);
                    if (localItemRefInfo != null && localItemRefInfo.Deleted)
                        await saveSyncData(connection, itemRefInfo, SyncStatus.MayBeNeeded);
                }
            }
            return batch.Count;
        }

        private IEnumerable<SyncConflict> loadConflicts(IDbConnection connection)
        {
            List<SyncConflict> conflicts = new List<SyncConflict>();
            IDbCommand getInsertedItemsCommand = connection.CreateCommand();
            getInsertedItemsCommand.CommandText = String.Format("SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus NOT IN ({0},{1},{2},{3},{4},{5})", 
                (int)SyncStatus.Insert,
                (int)SyncStatus.Update,
                (int)SyncStatus.Delete,
                (int)SyncStatus.DeleteNonExisting,
                (int)SyncStatus.MayBeNeeded,
                (int)SyncStatus.InsertConflict
                );
            using (IDataReader reader = getInsertedItemsCommand.ExecuteReader())
            {
                while (reader.Read())
                {
                    IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                    IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                    string itemType = (string)reader["ItemType"];
                    ISyncableItemInfo remoteItemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false };
                    long itemId = Convert.ToInt64(reader["ItemID"]);
                    SyncStatus status = (SyncStatus)reader["SyncStatus"];

                    ISyncableItemInfo localItemInfo = _store.LocateCurrentItemInfo(remoteItemInfo);
                    
                    if (status == SyncStatus.UpdateConflict)
                    {
                        // Check to see if the "conflict" is actually an exact same update
                        JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(localItemInfo);
                        _store.BuildItemData(localItemInfo, builder);

                        JObject localItemData = new JObject();
                        localItemData.Add("item", builder);

                        JObject remoteItemData = JObject.Parse((string)reader["ItemData"]);
                        DuplicateStatus dupStatus = _store.GetDuplicateStatus(remoteItemInfo.ItemType, localItemData, remoteItemData);
                        if (dupStatus == DuplicateStatus.Exact)
                        {
                            long tickCount = _store.IncrementLocalRepilcaTickCount();
                            IReplicaInfo modifiedReplica = new ReplicaInfo { ReplicaId = _store.GetLocalReplicaId(), ReplicaTickCount = tickCount };
                            SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo, SyncStatus.Update, modifiedReplica); // TODO: Really should have an update status that just updates the modified repos without doing everything else, but this should work
                            continue;
                        }
                    }
                     
                    conflicts.Add(new SyncConflict(itemId, status, localItemInfo, remoteItemInfo));
                }
            }

            return conflicts;
        }

        private IEnumerable<SyncConflict> checkForDuplicates(IDbConnection connection)
        {
            List<SyncConflict> conflicts = new List<SyncConflict>();

            var changedItemInfos = _store.LocateChangedItems(_remoteKnowledge);

            foreach (string itemType in _store.GetItemTypes())
            {
                IDbCommand getInsertedItemsCommand = connection.CreateCommand();
                getInsertedItemsCommand.CommandText = String.Format("SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus={0} AND ItemType='{1}'", (int)SyncStatus.Insert, itemType);
                using (IDataReader reader = getInsertedItemsCommand.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        long itemId = Convert.ToInt64(reader["ItemID"]);
                        IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        ISyncableItemInfo remoteItemInfo = new SyncableItemInfo { ItemType = itemType, Created = createdReplicaInfo, Modified = modifiedReplicaInfo, Deleted = false };
                        JObject remoteItemData = JObject.Parse((string)reader["ItemData"]);

                        foreach (var changedItemInfo in changedItemInfos)
                        {
                            if (changedItemInfo.ItemType != remoteItemInfo.ItemType)
                                continue;
                            if (SyncUtil.KnowledgeContains(_remoteKnowledge, changedItemInfo.Created))
                                continue;

                            // Inserted here without remote knowledge, could be a dup
                            JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(changedItemInfo);
                            _store.BuildItemData(changedItemInfo, builder);

                            JObject localItemData = new JObject();
                            localItemData.Add("item", builder);

                            DuplicateStatus dupStatus = _store.GetDuplicateStatus(remoteItemInfo.ItemType, localItemData, remoteItemData);
                            if (dupStatus == DuplicateStatus.Exact)
                            {
                                SessionDbHelper.ReplaceAllItemRefs(connection, _store, remoteItemInfo, changedItemInfo);
                                long tickCount = _store.IncrementLocalRepilcaTickCount();
                                IReplicaInfo modifiedReplica = new ReplicaInfo { ReplicaId = _store.GetLocalReplicaId(), ReplicaTickCount = tickCount };
                                SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo, SyncStatus.DeleteNonExisting, modifiedReplica);
                                break;
                            }
                            if (dupStatus == DuplicateStatus.Possible)
                            {
                                // TODO: clean this up, this call does more than we need
                                SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo, SyncStatus.InsertConflict, remoteItemInfo.Modified);
                                conflicts.Add(new SyncConflict(itemId, SyncStatus.InsertConflict, changedItemInfo, remoteItemInfo));
                                break;
                            }
                        }
                    }
                }
            }
            return conflicts;
        }

        private async Task saveSyncData(IDbConnection connection, ISyncableItemInfo remoteSyncableItemInfo, SyncStatus status)
        {
            JObject data = JObject.Parse("{item:{itemRefs:[]}}");
            if (!remoteSyncableItemInfo.Deleted)
            {
                JObject request = new JObject();
                addCredentials(request);
                request.Add(new JProperty("item", SyncUtil.SyncableItemInfoToJson(remoteSyncableItemInfo)));
                JObject response = await sendRequestAsync(new Uri(_remoteAddress, "getItemData"), request);
                data = response;
            }

            SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, data);

            JArray itemRefs = (JArray)data["item"]["itemRefs"];
            foreach (var item in itemRefs)
            {
                ISyncableItemInfo itemRefInfo = SyncUtil.SyncableItemInfoFromJson(item);
                ISyncableItemInfo  localItemRefInfo = _store.LocateCurrentItemInfo(itemRefInfo);
                if(localItemRefInfo != null && localItemRefInfo.Deleted)
                    await saveSyncData(connection, itemRefInfo, SyncStatus.MayBeNeeded);
            }
        }

        private void commitChanges()
        {
            if (_remoteKnowledge == null)
                return;

            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Applying Changes Locally" });
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SyncUtil.ApplyChangesAndUpdateKnowledge(connection, _store, _remoteKnowledge);
            }
        }

        private async Task pushChanges()
        {
            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Pushing Changes" });

            JObject request = new JObject();
            addCredentials(request);

            var localKnowledge = _store.GenerateLocalKnowledge();
            var changedItemInfos = _store.LocateChangedItems(_remoteKnowledge);

            int totalChanges = changedItemInfos.Count();
            request.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(localKnowledge)));
            request.Add(new JProperty("changeCount", totalChanges));

            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "putChanges"), request);

            int maxBatchCount = PushMaxBatchCount;
            int maxBatchSize = PushMaxBatchSize;

            long startTick = Environment.TickCount;
            int previousPercentComplete = -1;
            int i = 0;
            JArray batchArray = new JArray();
            int batchSize = 0;
            foreach (ISyncableItemInfo syncItemInfo in changedItemInfos)
            {
                i++;
                int percentComplete = ((i * 100) / totalChanges);
                if (percentComplete != previousPercentComplete)
                {
                    reportProgressAndCheckCacellation(new SyncProgress() { Message = String.Format("Pushing Item Changes {0}% complete ({1})", percentComplete, String.Format("Averaging {0}ms/item over {1} items", (Environment.TickCount - startTick) / i, i)) });
                }
                previousPercentComplete = percentComplete;

                JObject singleItemRequest = new JObject();
                singleItemRequest.Add("changeNumber", i);
                JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(syncItemInfo);
                if (!syncItemInfo.Deleted)
                    _store.BuildItemData(syncItemInfo, builder);
                singleItemRequest.Add(new JProperty("item", builder));
                batchSize += singleItemRequest.ToString().Length;
                batchArray.Add(singleItemRequest);

                if (i == totalChanges || (i % maxBatchCount) == 0 || batchSize >= maxBatchSize)
                {
                    JObject batchRequest = new JObject();
                    addCredentials(batchRequest);
                    batchRequest.Add(new JProperty("batch", batchArray));
                    JObject batchResponse = await sendRequestAsync(new Uri(_remoteAddress, "putItemDataBatch"), batchRequest);
                    batchArray = new JArray();
                    batchSize = 0;
                }
            }

            await applyChanges(totalChanges);
        }

        private async Task applyChanges(int totalChanges)
        {
            reportProgressAndCheckCacellation(new SyncProgress() { Message = "Applying Changes Remotely" });

            JObject request = new JObject();
            addCredentials(request);

            var localKnowledge = _store.GenerateLocalKnowledge();
            var changedItemInfos = _store.LocateChangedItems(_remoteKnowledge);

            request.Add(new JProperty("changeCount", totalChanges));

            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "applyChanges"), request);
        }

        private async Task pushItemData(int changeNumber, ISyncableItemInfo syncItemInfo)
        {
            JObject request = new JObject();
            addCredentials(request);
            request.Add("changeNumber", changeNumber);
            JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(syncItemInfo);
            if(!syncItemInfo.Deleted)
                _store.BuildItemData(syncItemInfo, builder);
            request.Add(new JProperty("item", builder));
            JObject response = await sendRequestAsync(new Uri(_remoteAddress, "putItemData"), request);
        }

        private async Task<IEnumerable<SyncConflict>> sync()
        {
            commitChanges();
            await openSession();
            IEnumerable<SyncConflict> conflicts = await pullChanges();
            if (conflicts.Any())
            {
                await closeSession();
                return conflicts;
            }
            commitChanges();
            await pushChanges();
            await closeSession();
            return conflicts;
        }

        public void Close()
        {
            if (_closed)
                return;

            _syncSessionDbConnectionProvider.SessionEnd(_localSessionId);
        }

        public Task<IEnumerable<SyncConflict>> SyncWithRemoteAsync(IProgress<SyncProgress> progress, CancellationToken cancellationToken)
        {
            if (_closed)
                throw new InvalidOperationException();
            if (_task != null && !_task.IsCompleted)
                throw new InvalidOperationException();
            _progressWatcher = progress;
            _cancellationToken = cancellationToken;
            _task = Task.Run(() => sync(), cancellationToken);
            return _task;
        }

        public Task<IEnumerable<SyncConflict>> SyncWithRemoteAsync()
        {
            return SyncWithRemoteAsync(null, CancellationToken.None);
        }

        public void ResolveConflictRemoteWins(SyncConflict conflict)
        {
            if (_closed)
                throw new InvalidOperationException();

            long tickCount = _store.IncrementLocalRepilcaTickCount();
            IReplicaInfo modifiedReplica = new ReplicaInfo { ReplicaId = _store.GetLocalReplicaId(), ReplicaTickCount = tickCount };
            SyncStatus resolvedStatus = conflict.RemoteItemInfo.Deleted ? SyncStatus.Delete : SyncStatus.Update;
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemNoData(connection, conflict.RemoteItemInfo, resolvedStatus, modifiedReplica);
            }
        }

        public void ResolveConflictLocalWins(SyncConflict conflict)
        {
            if (_closed)
                throw new InvalidOperationException();

            long tickCount = _store.IncrementLocalRepilcaTickCount();
            IReplicaInfo modifiedReplica = new ReplicaInfo { ReplicaId = _store.GetLocalReplicaId(), ReplicaTickCount = tickCount };
            SyncStatus resolvedStatus = conflict.LocalItemInfo.Deleted ? SyncStatus.Delete : SyncStatus.Update;
            JObject data = JObject.Parse("{item:{itemRefs:[]}}");

            if (resolvedStatus != SyncStatus.Delete)
            {
                JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(conflict.LocalItemInfo);
                _store.BuildItemData(conflict.LocalItemInfo, builder);
                data = new JObject();
                data.Add("item", builder);
            }

            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemWithData(connection, conflict.RemoteItemInfo, resolvedStatus, modifiedReplica, data);
            }
        }

        public void ResolveConflictMerge(SyncConflict conflict, JObject itemData)
        {
            if (_closed)
                throw new InvalidOperationException();

            long tickCount = _store.IncrementLocalRepilcaTickCount();
            IReplicaInfo modifiedReplica = new ReplicaInfo { ReplicaId = _store.GetLocalReplicaId(), ReplicaTickCount = tickCount };
            SyncStatus resolvedStatus = SyncStatus.Update;
            ISyncableItemInfo itemInfo = conflict.RemoteItemInfo;

            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemWithData(connection, itemInfo, resolvedStatus, modifiedReplica, itemData);
            }
        }

    }

    internal class CompressedContent : HttpContent
    {
        private HttpContent originalContent;
        private string encodingType;

        public CompressedContent(HttpContent content, string encodingType)
        {
            if (content == null)
            {
                throw new ArgumentNullException("content");
            }

            if (encodingType == null)
            {
                throw new ArgumentNullException("encodingType");
            }

            originalContent = content;
            this.encodingType = encodingType.ToLowerInvariant();

            if (this.encodingType != "gzip" && this.encodingType != "deflate")
            {
                throw new InvalidOperationException(string.Format("Encoding '{0}' is not supported. Only supports gzip or deflate encoding.", this.encodingType));
            }

            // copy the headers from the original content
            foreach (KeyValuePair<string, IEnumerable<string>> header in originalContent.Headers)
            {
                this.Headers.TryAddWithoutValidation(header.Key, header.Value);
            }

            this.Headers.ContentEncoding.Add(encodingType);
        }

        protected override bool TryComputeLength(out long length)
        {
            length = -1;

            return false;
        }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext context)
        {
            Stream compressedStream = null;

            if (encodingType == "gzip")
            {
                compressedStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true);
            }
            else if (encodingType == "deflate")
            {
                compressedStream = new DeflateStream(stream, CompressionMode.Compress, leaveOpen: true);
            }

            return originalContent.CopyToAsync(compressedStream).ContinueWith(tsk =>
            {
                if (compressedStream != null)
                {
                    compressedStream.Dispose();
                }
            });
        }
    }

}
