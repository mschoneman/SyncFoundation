using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Web.Http;

namespace SyncFoundation.Server
{
    public class SyncController : ApiController
    {
        private IUserService _userService;
        private ISyncSessionDbConnectionProvider _syncSessionDbConnectionProvider;
        private string _username;

        public SyncController(IUserService userService, ISyncSessionDbConnectionProvider syncSessionDbConnectionProvider)
        {
            _userService = userService;
            _syncSessionDbConnectionProvider = syncSessionDbConnectionProvider;
        }

        [HttpGet]
        public HttpResponseMessage About()
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);
            resp.Content = new StringContent("SyncFoundation Sync API v1.0",
                                                Encoding.UTF8, "text/plain");
            return resp;
        }

        private void ThrowSafeException(string message,
                    int code = 1,
                    HttpStatusCode statusCode = HttpStatusCode.InternalServerError)
        {
            var errResponse = Request.CreateResponse<ApiError>(statusCode,
                                        new ApiError() { errorMessage = message, errorCode = code });
            throw new HttpResponseException(errResponse);
        }

        private void validateRequest(JObject request, bool checkSession = true)
        {
            DateTime min = DateTime.UtcNow - TimeSpan.FromHours(1);
            DateTime max = DateTime.UtcNow + TimeSpan.FromHours(1);

            string username = (string)request["username"];

            string passwordEquivalent = _userService.GetPasswordEquivalent(username);

            if (passwordEquivalent == null)
                ThrowSafeException("Invalid Username", 1, HttpStatusCode.BadRequest);

            DateTime createdDateTime = (DateTime)request["created"];
            if (createdDateTime < min || createdDateTime > max)
                ThrowSafeException("Invalid Created Date/Time", 1, HttpStatusCode.BadRequest);


            string nonceEncoded = (string)request["nonce"];

            if (nonceEncoded == _userService.GetLastNonce(username))
                ThrowSafeException("Invalid Nonce", 1, HttpStatusCode.BadRequest);


            byte[] nonce = Convert.FromBase64String(nonceEncoded);
            byte[] created = Encoding.UTF8.GetBytes(createdDateTime.ToString("yyyy-MM-ddTHH:mm:ssZ"));


            byte[] password = Encoding.UTF8.GetBytes(passwordEquivalent);
            byte[] digestSource = new byte[nonce.Length + created.Length + password.Length];

            for (int i = 0; i < nonce.Length; i++)
                digestSource[i] = nonce[i];
            for (int i = 0; i < created.Length; i++)
                digestSource[nonce.Length + i] = created[i];
            for (int i = 0; i < password.Length; i++)
                digestSource[created.Length + nonce.Length + i] = password[i];

            byte[] digestBytes = new SHA1Managed().ComputeHash(digestSource);
            string computedDigest = Convert.ToBase64String(digestBytes);
            string digestEncoded = (string)request["digest"];

            if (computedDigest != digestEncoded)
                ThrowSafeException("Invalid Username", 1, HttpStatusCode.BadRequest);


            _username = username;
            _userService.SetLastNonce(_username, nonceEncoded);

            if (checkSession)
            {
                string sessionId = (string)request["sessionID"];
                if (sessionId != _userService.GetSessionId(_username))
                    ThrowSafeException("Invalid SessionID", 1, HttpStatusCode.BadRequest);

                _userService.SetSessionId(_username, sessionId);
            }
        }


        [HttpPost]
        public HttpResponseMessage BeginSession(JObject request)
        {
            validateRequest(request, false);
            if (_userService.GetSessionId((string)request["username"]) != null)
                ThrowSafeException("Session In Progress", 1, HttpStatusCode.ServiceUnavailable);

            List<string> remoteItemTypes = new List<string>();
            foreach (var itemType in request["itemTypes"])
                remoteItemTypes.Add((string)itemType);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            {
                if (!remoteItemTypes.SequenceEqual(store.GetItemTypes()))
                    ThrowSafeException("Mismatched item types", 1);
            }

            string sessionId = Guid.NewGuid().ToString();
            _userService.SetSessionId(_username, sessionId);

            _syncSessionDbConnectionProvider.SessionStart(sessionId);
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(sessionId))
            {
                SessionDbHelper.CreateSessionDbTables(connection);
            }

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);
            var json = new JObject();
            json.Add(new JProperty("sessionID", sessionId));
            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

        [HttpPost]
        public HttpResponseMessage EndSession(JObject request)
        {
            validateRequest(request);

            try
            {
                using (ISyncableStore store = _userService.GetSyncableStore(_username))
                using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
                {
                    SyncUtil.ApplyChangesAndUpdateKnowledge(connection, store, SessionDbHelper.LoadRemoteKnowledge(connection));
                }
            }
            finally
            {
                _syncSessionDbConnectionProvider.SessionEnd(_userService.GetSessionId(_username));
                _userService.SetSessionId(_username, null);
            }
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);
            var json = new JObject();
            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

        [HttpPost]
        public HttpResponseMessage GetChanges(JObject request)
        {
            validateRequest(request);

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            IEnumerable<ReplicaInfo> remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);

            var json = new JObject();
            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            {
                json.Add(new JProperty("knowledge", SyncUtil.KnowledgeToJson(store.GenerateLocalKnowledge())));
                json.Add(new JProperty("changes", SyncUtil.JsonItemArrayFromSyncableItemInfoEnumberable(store.LocateChangedItems(remoteKnowledge))));
            }

            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

        [HttpPost]
        public HttpResponseMessage GetItemData(JObject request)
        {
            validateRequest(request);

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ISyncableItemInfo requestedItemInfo = SyncUtil.SyncableItemInfoFromJson(request["item"]);

            var json = new JObject();
            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            {
                JObject builder = SyncUtil.JsonItemFromSyncableItemInfo(requestedItemInfo);
                store.BuildItemData(requestedItemInfo, builder);
                json.Add(new JProperty("item", builder));
            }


            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }


        [HttpPost]
        public HttpResponseMessage PutChanges(JObject request)
        {
            validateRequest(request);

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            IEnumerable<ReplicaInfo> remoteKnowledge = SyncUtil.KnowledgeFromJson(request["knowledge"]);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                SessionDbHelper.ClearSyncItems(connection);

                SessionDbHelper.SaveRemoteKnowledge(connection, remoteKnowledge);

                int changeCount = (int)request["changeCount"];
                //SessionDbHelper.SaveItemPlaceHolders(connection, changeCount);
            }

            var json = new JObject();
            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

        [HttpPost]
        public HttpResponseMessage PutItemData(JObject request)
        {
            validateRequest(request);

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ISyncableItemInfo remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(request["item"]);
            JObject itemData = new JObject();
            itemData.Add("item", request["item"]);
            int changeNumber = (int)request["changeNumber"];

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                var remoteKnowledge = SessionDbHelper.LoadRemoteKnowledge(connection);
                ISyncableItemInfo localSyncableItemInfo = store.LocateCurrentItemInfo(remoteSyncableItemInfo);
                SyncStatus status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo, remoteKnowledge);
                SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, itemData);
            }

            var json = new JObject();
            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

        [HttpPost]
        public HttpResponseMessage PutItemDataBatch(JObject request)
        {
            validateRequest(request);

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);
            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                IDbCommand command = connection.CreateCommand();
                command.CommandText = "BEGIN";
                command.ExecuteNonQuery();
                try
                {
                    JArray batch = (JArray)request["batch"];
                    for (int i = 0; i < batch.Count; i++)
                    {
                        ISyncableItemInfo remoteSyncableItemInfo = SyncUtil.SyncableItemInfoFromJson(batch[i]["item"]);
                        JObject itemData = new JObject();
                        itemData.Add("item", batch[i]["item"]);
                        int changeNumber = (int)batch[i]["changeNumber"];

                        var remoteKnowledge = SessionDbHelper.LoadRemoteKnowledge(connection);
                        ISyncableItemInfo localSyncableItemInfo = store.LocateCurrentItemInfo(remoteSyncableItemInfo);
                        SyncStatus status = SyncUtil.CalculateSyncStatus(remoteSyncableItemInfo, localSyncableItemInfo, remoteKnowledge);
                        SessionDbHelper.SaveItemData(connection, remoteSyncableItemInfo, status, itemData);
                    }
                    command.CommandText = "COMMIT";
                    command.ExecuteNonQuery();
                }
                catch (Exception)
                {
                    command.CommandText = "ROLLBACK";
                    command.ExecuteNonQuery();
                }
            }
            var json = new JObject();
            resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            return resp;
        }

    }
}