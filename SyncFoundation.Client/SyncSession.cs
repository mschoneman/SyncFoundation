using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SyncFoundation.Client
{
    public class SyncSession
    {
        private readonly ISyncableStore _store;
        private readonly ISyncTransport _transport;
        private readonly ISyncSessionDbConnectionProvider _syncSessionDbConnectionProvider;
        private readonly string _localSessionId;

        private IProgress<SyncProgress> _progressWatcher;
        private string _remoteSessionId;
        private IEnumerable<IReplicaInfo> _remoteKnowledge;
        private Task<IEnumerable<SyncConflict>> _task;
        private CancellationToken _cancellationToken = CancellationToken.None;
        private bool _closed;

        public int PushMaxBatchCount { get; set; }
        public int PushMaxBatchSize { get; set; }

        public int PullMaxBatchCount { get; set; }
        public int PullMaxBatchSize { get; set; }

        public SyncSession(ISyncableStore store, ISyncSessionDbConnectionProvider syncSessionDbConnectionProvider,
                           ISyncTransport transport)
        {
            if (store == null)
                throw new ArgumentNullException("store");
            if (syncSessionDbConnectionProvider == null)
                throw new ArgumentNullException("syncSessionDbConnectionProvider");
            if (transport == null)
                throw new ArgumentNullException("transport");

            PushMaxBatchCount = 500;
            PushMaxBatchSize = 1024*1024;

            PullMaxBatchCount = 5000;
            PullMaxBatchSize = 1024*1024*10;

            _store = store;
            _syncSessionDbConnectionProvider = syncSessionDbConnectionProvider;
            _transport = transport;
            _localSessionId = Guid.NewGuid().ToString();
            _syncSessionDbConnectionProvider.SessionStart(_localSessionId);
            using (var connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.CreateSessionDbTables(connection);
            }
        }

        private void ReportProgressAndCheckCacellation(SyncProgress progress)
        {
            _cancellationToken.ThrowIfCancellationRequested();
            if (_progressWatcher != null)
                _progressWatcher.Report(progress);
        }


        private async Task OpenSession()
        {
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.Connecting,
                    PercentComplete = 0,
                    Message = "Opening Session"
                });

            var request = new JObject();
            var types = new JArray();
            foreach (var type in _store.GetItemTypes())
                types.Add(type);
            request.Add("itemTypes", types);

            var response = await _transport.TransportAsync(SyncEndpoint.BeginSession, request);
            _remoteSessionId = (string) response["sessionID"];

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.Connecting,
                    PercentComplete = 100,
                    Message = "Session Established"
                });
        }

        private async Task CloseSession()
        {
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.Disconnecting,
                    PercentComplete = 0,
                    Message = "Closing Session"
                });
            var request = new JObject {{"sessionID", _remoteSessionId}};

            await _transport.TransportAsync(SyncEndpoint.EndSession, request);

            _remoteSessionId = null;
            _syncSessionDbConnectionProvider.SessionEnd(_remoteSessionId);
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.Disconnecting,
                    PercentComplete = 100,
                    Message = "Session Closed"
                });
        }

        private async Task<IEnumerable<SyncConflict>> PullChanges()
        {
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.FindingRemoteChanges,
                    PercentComplete = 0,
                    Message = "Looking for remote changes"
                });

            var request = new JObject {{"sessionID", _remoteSessionId}};

            var localKnowledge = _store.GenerateLocalKnowledge().ToList();
            request.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(localKnowledge)));

            JObject response = await _transport.TransportAsync(SyncEndpoint.GetChanges, request);
            _remoteKnowledge = SyncUtil.KnowledgeFromJson(response["knowledge"]);
            var totalChanges = (int) response["totalChanges"];

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.FindingRemoteChanges,
                    PercentComplete = 100,
                    Message = String.Format("Found {0} remote changes", totalChanges)
                });

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.DownloadingRemoteChanges,
                    PercentComplete = 0,
                    Message = String.Format("Downloading {0} remote changes", totalChanges)
                });
            using (var connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                connection.ExecuteNonQuery("BEGIN");
                SessionDbHelper.ClearSyncItems(connection);

                long startTick = Environment.TickCount;
                int previousPercentComplete = -1;
                for (int i = 1; i <= totalChanges;)
                {
                    i += await SaveChangesBatch(connection, localKnowledge, i);

                    int percentComplete = ((i*100)/totalChanges);
                    if (percentComplete != previousPercentComplete)
                    {
                        ReportProgressAndCheckCacellation(new SyncProgress
                            {
                                Stage = SyncStage.DownloadingRemoteChanges,
                                PercentComplete = percentComplete,
                                Message =
                                    String.Format("Downloading remote changes, {0}% complete ({1})", percentComplete,
                                                  String.Format("Averaging {0}ms/item over {1} items",
                                                                (Environment.TickCount - startTick)/i, i))
                            });
                    }
                    previousPercentComplete = percentComplete;
                }
                connection.ExecuteNonQuery("COMMIT");
                ReportProgressAndCheckCacellation(new SyncProgress
                    {
                        Stage = SyncStage.DownloadingRemoteChanges,
                        PercentComplete = 100,
                        Message = String.Format("Downloaded all {0} remote changes", totalChanges)
                    });

                ReportProgressAndCheckCacellation(new SyncProgress
                    {
                        Stage = SyncStage.CheckingForConflicts,
                        PercentComplete = 0,
                        Message = "Looking for conflicts"
                    });
                var conflicts = new List<SyncConflict>();
                conflicts.AddRange(CheckForDuplicates(connection));
                conflicts.AddRange(LoadConflicts(connection));
                ReportProgressAndCheckCacellation(new SyncProgress
                    {
                        Stage = SyncStage.CheckingForConflicts,
                        PercentComplete = 100,
                        Message = String.Format("Found {0} conflicts", conflicts.Count)
                    });
                return conflicts;
            }
        }

        private async Task<int> SaveChangesBatch(IDbConnection connection, IList<IReplicaInfo> localKnowledge,
                                                 int startItem)
        {
            var request = new JObject
                {
                    {"sessionID", _remoteSessionId},
                    {"startItem", startItem},
                    {"maxBatchCount", PullMaxBatchCount},
                    {"maxBatchSize", PullMaxBatchSize}
                };

            JObject response = await _transport.TransportAsync(SyncEndpoint.GetItemDataBatch, request);
            var batch = (JArray) response["batch"];

            foreach (var item in batch)
            {
                ISyncableItemInfo remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(item["item"]);
                var itemData = new JObject {{"item", item["item"]}};

                var status = SyncStatus.MayBeNeeded;
                if (!SyncUtil.KnowledgeContains(localKnowledge, remoteSyncableItemInfo.Modified))
                {
                    ISyncableItemInfo localSyncableItemInfo = _store.LocateCurrentItemInfo(remoteSyncableItemInfo);
                    status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo,
                                                          _remoteKnowledge);
                }
                SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, itemData);
                var itemRefs = (JArray) itemData["item"]["itemRefs"];
                foreach (var itemRef in itemRefs)
                {
                    ISyncableItemInfo itemRefInfo = SyncUtil.SyncableItemInfoFromJson(itemRef);
                    ISyncableItemInfo localItemRefInfo = _store.LocateCurrentItemInfo(itemRefInfo);
                    if (localItemRefInfo != null && localItemRefInfo.Deleted)
                        await SaveSyncData(connection, itemRefInfo, SyncStatus.MayBeNeeded);
                }
            }
            return batch.Count;
        }

        private IEnumerable<SyncConflict> LoadConflicts(IDbConnection connection)
        {
            var conflicts = new List<SyncConflict>();
            IDbCommand getInsertedItemsCommand = connection.CreateCommand();
            getInsertedItemsCommand.CommandText =
                String.Format(
                    "SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus NOT IN ({0},{1},{2},{3},{4},{5})",
                    (int) SyncStatus.Insert,
                    (int) SyncStatus.Update,
                    (int) SyncStatus.Delete,
                    (int) SyncStatus.DeleteNonExisting,
                    (int) SyncStatus.MayBeNeeded,
                    (int) SyncStatus.InsertConflict
                    );
            using (var reader = getInsertedItemsCommand.ExecuteReader())
            {
                while (reader != null && reader.Read())
                {
                    var createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                    var modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                    var itemType = (string) reader["ItemType"];
                    var remoteItemInfo = new SyncableItemInfo
                        {
                            ItemType = itemType,
                            Created = createdReplicaInfo,
                            Modified = modifiedReplicaInfo,
                            Deleted = false
                        };
                    var itemId = Convert.ToInt64(reader["ItemID"]);
                    var status = (SyncStatus) reader["SyncStatus"];

                    ISyncableItemInfo localItemInfo = _store.LocateCurrentItemInfo(remoteItemInfo);

                    if (status == SyncStatus.UpdateConflict)
                    {
                        // Check to see if the "conflict" is actually an exact same update
                        var builder = SyncUtil.JsonItemFromSyncableItemInfo(localItemInfo);
                        _store.BuildItemData(localItemInfo, builder);

                        var localItemData = new JObject {{"item", builder}};

                        var remoteItemData = JObject.Parse((string) reader["ItemData"]);
                        var dupStatus = _store.GetDuplicateStatus(remoteItemInfo.ItemType, localItemData,
                                                                  remoteItemData);
                        if (dupStatus == DuplicateStatus.Exact)
                        {
                            var tickCount = _store.IncrementLocalRepilcaTickCount();
                            var modifiedReplica = new ReplicaInfo
                                {
                                    ReplicaId = _store.GetLocalReplicaId(),
                                    ReplicaTickCount = tickCount
                                };
                            SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo, SyncStatus.Update,
                                                              modifiedReplica);
                            // TODO: Really should have an update status that just updates the modified repos without doing everything else, but this should work
                            continue;
                        }
                    }

                    conflicts.Add(new SyncConflict(itemId, status, localItemInfo, remoteItemInfo));
                }
            }

            return conflicts;
        }

        private IEnumerable<SyncConflict> CheckForDuplicates(IDbConnection connection)
        {
            var conflicts = new List<SyncConflict>();

            var changedItemInfos = _store.LocateChangedItems(_remoteKnowledge).ToList();

            foreach (string itemType in _store.GetItemTypes())
            {
                IDbCommand getInsertedItemsCommand = connection.CreateCommand();
                getInsertedItemsCommand.CommandText =
                    String.Format(
                        "SELECT ItemID, SyncStatus, ItemType, GlobalCreatedReplica, CreatedTickCount, GlobalModifiedReplica, ModifiedTickCount, ItemData  FROM SyncItems WHERE SyncStatus={0} AND ItemType='{1}'",
                        (int) SyncStatus.Insert, itemType);
                using (IDataReader reader = getInsertedItemsCommand.ExecuteReader())
                {
                    while (reader != null && reader.Read())
                    {
                        long itemId = Convert.ToInt64(reader["ItemID"]);
                        IReplicaInfo createdReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Created");
                        IReplicaInfo modifiedReplicaInfo = SessionDbHelper.ReplicaInfoFromDataReader(reader, "Modified");
                        ISyncableItemInfo remoteItemInfo = new SyncableItemInfo
                            {
                                ItemType = itemType,
                                Created = createdReplicaInfo,
                                Modified = modifiedReplicaInfo,
                                Deleted = false
                            };
                        var remoteItemData = JObject.Parse((string) reader["ItemData"]);

                        foreach (var changedItemInfo in changedItemInfos)
                        {
                            if (changedItemInfo.ItemType != remoteItemInfo.ItemType)
                                continue;
                            if (SyncUtil.KnowledgeContains(_remoteKnowledge, changedItemInfo.Created))
                                continue;

                            // Inserted here without remote knowledge, could be a dup
                            var builder = SyncUtil.JsonItemFromSyncableItemInfo(changedItemInfo);
                            _store.BuildItemData(changedItemInfo, builder);

                            var localItemData = new JObject {{"item", builder}};

                            DuplicateStatus dupStatus = _store.GetDuplicateStatus(remoteItemInfo.ItemType, localItemData,
                                                                                  remoteItemData);
                            if (dupStatus == DuplicateStatus.Exact)
                            {
                                SessionDbHelper.ReplaceAllItemRefs(connection, _store, remoteItemInfo, changedItemInfo);
                                long tickCount = _store.IncrementLocalRepilcaTickCount();
                                IReplicaInfo modifiedReplica = new ReplicaInfo
                                    {
                                        ReplicaId = _store.GetLocalReplicaId(),
                                        ReplicaTickCount = tickCount
                                    };
                                SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo,
                                                                  SyncStatus.DeleteNonExisting, modifiedReplica);
                                break;
                            }
                            if (dupStatus == DuplicateStatus.Possible)
                            {
                                // TODO: clean this up, this call does more than we need
                                SessionDbHelper.ResolveItemNoData(connection, remoteItemInfo, SyncStatus.InsertConflict,
                                                                  remoteItemInfo.Modified);
                                conflicts.Add(new SyncConflict(itemId, SyncStatus.InsertConflict, changedItemInfo,
                                                               remoteItemInfo));
                                break;
                            }
                        }
                    }
                }
            }
            return conflicts;
        }

        private async Task SaveSyncData(IDbConnection connection, ISyncableItemInfo remoteSyncableItemInfo,
                                        SyncStatus status)
        {
            var data = JObject.Parse("{item:{itemRefs:[]}}");
            if (!remoteSyncableItemInfo.Deleted)
            {
                var request = new JObject
                    {
                        {"sessionID", _remoteSessionId},
                        {"item", SyncUtil.SyncableItemInfoToJson(remoteSyncableItemInfo)}
                    };
                var response = await _transport.TransportAsync(SyncEndpoint.GetItemData, request);
                data = response;
            }

            SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, data);

            var itemRefs = (JArray) data["item"]["itemRefs"];
            foreach (var item in itemRefs)
            {
                ISyncableItemInfo itemRefInfo = SyncUtil.SyncableItemInfoFromJson(item);
                ISyncableItemInfo localItemRefInfo = _store.LocateCurrentItemInfo(itemRefInfo);
                if (localItemRefInfo != null && localItemRefInfo.Deleted)
                    await SaveSyncData(connection, itemRefInfo, SyncStatus.MayBeNeeded);
            }
        }

        private void CommitChanges()
        {
            if (_remoteKnowledge == null)
                return;

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.ApplyingChangesLocally,
                    PercentComplete = 0,
                    Message = "Applying changes locally"
                });
            using (
                IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SyncUtil.ApplyChangesAndUpdateKnowledge(connection, _store, _remoteKnowledge);
            }
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.ApplyingChangesLocally,
                    PercentComplete = 100,
                    Message = "Applied all changes locally"
                });
        }

        private async Task PushChanges()
        {
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.FindingLocalChanges,
                    PercentComplete = 0,
                    Message = "Finding local changes"
                });

            var request = new JObject {{"sessionID", _remoteSessionId}};

            var localKnowledge = _store.GenerateLocalKnowledge();
            var changedItemInfos = _store.LocateChangedItems(_remoteKnowledge).ToList();

            int totalChanges = changedItemInfos.Count();
            request.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(localKnowledge)));
            request.Add(new JProperty("changeCount", totalChanges));

            await _transport.TransportAsync(SyncEndpoint.PutChanges, request);

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.FindingLocalChanges,
                    PercentComplete = 100,
                    Message = String.Format("Found {0} local changes", totalChanges)
                });

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.UploadingLocalChanges,
                    PercentComplete = 0,
                    Message = String.Format("Uploading {0} local changes", totalChanges)
                });

            int maxBatchCount = PushMaxBatchCount;
            int maxBatchSize = PushMaxBatchSize;
            long startTick = Environment.TickCount;
            int previousPercentComplete = -1;
            int i = 0;
            var batchArray = new JArray();
            int batchSize = 0;
            foreach (ISyncableItemInfo syncItemInfo in changedItemInfos)
            {
                i++;
                int percentComplete = ((i*100)/totalChanges);
                if (percentComplete != previousPercentComplete)
                {
                    ReportProgressAndCheckCacellation(new SyncProgress
                        {
                            Stage = SyncStage.UploadingLocalChanges,
                            PercentComplete = 100,
                            Message =
                                String.Format("Uploading local changes, {0}% complete ({1})", percentComplete,
                                              String.Format("Averaging {0}ms/item over {1} items",
                                                            (Environment.TickCount - startTick)/i, i))
                        });
                }
                previousPercentComplete = percentComplete;

                var builder = SyncUtil.JsonItemFromSyncableItemInfo(syncItemInfo);
                if (!syncItemInfo.Deleted)
                    _store.BuildItemData(syncItemInfo, builder);

                var singleItemRequest = new JObject {{"changeNumber", i}, {"item", builder}};

                batchSize += singleItemRequest.ToString().Length;
                batchArray.Add(singleItemRequest);

                if (i == totalChanges || (i%maxBatchCount) == 0 || batchSize >= maxBatchSize)
                {
                    var batchRequest = new JObject {{"sessionID", _remoteSessionId}, {"batch", batchArray}};
                    await _transport.TransportAsync(SyncEndpoint.PutItemDataBatch, batchRequest);
                    batchArray = new JArray();
                    batchSize = 0;
                }
            }
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.UploadingLocalChanges,
                    PercentComplete = 100,
                    Message = String.Format("Uploaded {0} local changes", totalChanges)
                });

            await ApplyChanges(totalChanges);
        }

        private async Task ApplyChanges(int totalChanges)
        {
            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.ApplyingChangesRemotely,
                    PercentComplete = 0,
                    Message = "Applying changes remotely"
                });

            var request = new JObject {{"sessionID", _remoteSessionId}, {"changeCount", totalChanges}};

            await _transport.TransportAsync(SyncEndpoint.ApplyChanges, request);

            ReportProgressAndCheckCacellation(new SyncProgress
                {
                    Stage = SyncStage.ApplyingChangesRemotely,
                    PercentComplete = 100,
                    Message = "Applied changes remotely"
                });
        }

        private async Task<IEnumerable<SyncConflict>> Sync()
        {
            CommitChanges();
            await OpenSession();
            var conflicts = await PullChanges();
            if (conflicts.Any())
            {
                await CloseSession();
                return conflicts;
            }
            CommitChanges();
            await PushChanges();
            await CloseSession();
            return conflicts;
        }

        public void Close()
        {
            if (_closed)
                return;

            _syncSessionDbConnectionProvider.SessionEnd(_localSessionId);
            _closed = true;
        }

        public Task<IEnumerable<SyncConflict>> SyncWithRemoteAsync(IProgress<SyncProgress> progress,
                                                                   CancellationToken cancellationToken)
        {
            if (_closed)
                throw new InvalidOperationException();
            if (_task != null && !_task.IsCompleted)
                throw new InvalidOperationException();
            _progressWatcher = progress;
            _cancellationToken = cancellationToken;
            _task = Task.Run(() => Sync(), cancellationToken);
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

            var tickCount = _store.IncrementLocalRepilcaTickCount();
            var modifiedReplica = new ReplicaInfo
                {
                    ReplicaId = _store.GetLocalReplicaId(),
                    ReplicaTickCount = tickCount
                };
            var resolvedStatus = conflict.RemoteItemInfo.Deleted ? SyncStatus.Delete : SyncStatus.Update;
            using (var connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemNoData(connection, conflict.RemoteItemInfo, resolvedStatus, modifiedReplica);
            }
        }

        public void ResolveConflictLocalWins(SyncConflict conflict)
        {
            if (_closed)
                throw new InvalidOperationException();

            var tickCount = _store.IncrementLocalRepilcaTickCount();
            var modifiedReplica = new ReplicaInfo
                {
                    ReplicaId = _store.GetLocalReplicaId(),
                    ReplicaTickCount = tickCount
                };
            var resolvedStatus = conflict.LocalItemInfo.Deleted ? SyncStatus.Delete : SyncStatus.Update;
            var data = JObject.Parse("{item:{itemRefs:[]}}");

            if (resolvedStatus != SyncStatus.Delete)
            {
                var builder = SyncUtil.JsonItemFromSyncableItemInfo(conflict.LocalItemInfo);
                _store.BuildItemData(conflict.LocalItemInfo, builder);
                data = new JObject {{"item", builder}};
            }

            using (var connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemWithData(connection, conflict.RemoteItemInfo, resolvedStatus, modifiedReplica,
                                                    data);
            }
        }

        public void ResolveConflictMerge(SyncConflict conflict, JObject itemData)
        {
            if (_closed)
                throw new InvalidOperationException();

            long tickCount = _store.IncrementLocalRepilcaTickCount();
            IReplicaInfo modifiedReplica = new ReplicaInfo
                {
                    ReplicaId = _store.GetLocalReplicaId(),
                    ReplicaTickCount = tickCount
                };
            ISyncableItemInfo itemInfo = conflict.RemoteItemInfo;

            using (
                IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_localSessionId))
            {
                SessionDbHelper.ResolveItemWithData(connection, itemInfo, SyncStatus.Update, modifiedReplica, itemData);
            }
        }
    }
}