using Newtonsoft.Json.Linq;
using SyncFoundation.Core;
using SyncFoundation.Core.Interfaces;
using System;
using System.Data;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Text;
using System.Web.Http;

namespace SyncFoundation.Server
{
    public class SyncController : ApiController
    {
        private readonly IUserService _userService;
        private readonly ISyncSessionDbConnectionProvider _syncSessionDbConnectionProvider;
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
            var errResponse = Request.CreateResponse(statusCode,
                                        new ApiError { ErrorMessage = message, ErrorCode = code });
            throw new HttpResponseException(errResponse);
        }

        private void ValidateRequest(JObject request, bool checkSession = true)
        {
            DateTime min = DateTime.UtcNow - TimeSpan.FromHours(1);
            DateTime max = DateTime.UtcNow + TimeSpan.FromHours(1);

            var username = (string)request["username"];

            string passwordEquivalent = _userService.GetPasswordEquivalent(username);

            if (passwordEquivalent == null)
                throw new HttpResponseException(Request.CreateResponse(HttpStatusCode.BadRequest,
                                                                       new ApiError
                                                                           {
                                                                               ErrorMessage = "Invalid Username",
                                                                               ErrorCode = 1
                                                                           }));

            var createdDateTime = (DateTime)request["created"];
            if (createdDateTime < min || createdDateTime > max)
                throw new HttpResponseException(Request.CreateResponse(HttpStatusCode.BadRequest,
                                                                       new ApiError
                                                                       {
                                                                           ErrorMessage = "Invalid Created Date/Time",
                                                                           ErrorCode = 1
                                                                       }));

            var nonceEncoded = (string)request["nonce"];

            if (nonceEncoded == _userService.GetLastNonce(username))
                throw new HttpResponseException(Request.CreateResponse(HttpStatusCode.BadRequest,
                                                                       new ApiError
                                                                       {
                                                                           ErrorMessage = "Invalid Nonce",
                                                                           ErrorCode = 1
                                                                       }));


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
            var computedDigest = Convert.ToBase64String(digestBytes);
            var digestEncoded = (string)request["digest"];

            if (computedDigest != digestEncoded)
                ThrowSafeException("Invalid Username", 1, HttpStatusCode.BadRequest);


            _username = username;
            _userService.SetLastNonce(_username, nonceEncoded);

            if (checkSession)
            {
                var sessionId = (string)request["sessionID"];
                if (sessionId != _userService.GetSessionId(_username))
                    ThrowSafeException("Invalid SessionID", 1, HttpStatusCode.BadRequest);

                _userService.SetSessionId(_username, sessionId);
            }
        }


        [HttpPost]
        public HttpResponseMessage BeginSession(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request, false);

            if (_userService.GetSessionId((string)request["username"]) != null)
                ThrowSafeException("Session In Progress", 1, HttpStatusCode.ServiceUnavailable);

            string sessionId = Guid.NewGuid().ToString();
            _syncSessionDbConnectionProvider.SessionStart(sessionId);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(sessionId))
            {
                var json = new SyncServerSession(store, connection).BeginSession(request);
                json.Add(new JProperty("sessionID", sessionId));

                _userService.SetSessionId(_username, sessionId);

                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }

        [HttpPost]
        public HttpResponseMessage EndSession(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                var json = new SyncServerSession(store, connection).EndSession(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            _syncSessionDbConnectionProvider.SessionEnd(_userService.GetSessionId(_username));
            _userService.SetSessionId(_username, null);

            return resp;
        }

        [HttpPost]
        public HttpResponseMessage ApplyChanges(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                var json = new SyncServerSession(store, connection).ApplyChanges(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }


        [HttpPost]
        public HttpResponseMessage GetChanges(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                JObject json = new SyncServerSession(store, connection).GetChanges(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }

        [HttpPost]
        public HttpResponseMessage GetItemData(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                JObject json = new SyncServerSession(store, connection).GetItemData(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }


        [HttpPost]
        public HttpResponseMessage GetItemDataBatch(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                JObject json = new SyncServerSession(store, connection).GetItemDataBatch(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }

        [HttpPost]
        public HttpResponseMessage PutChanges(JObject request)
        {
            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                JObject json = new SyncServerSession(store, connection).PutChanges(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }

        [HttpPost]
        public HttpResponseMessage PutItemDataBatch(JObject request)
        {

            var resp = Request.CreateResponse(HttpStatusCode.OK, string.Empty);

            ValidateRequest(request);

            using (ISyncableStore store = _userService.GetSyncableStore(_username))
            using (IDbConnection connection = _syncSessionDbConnectionProvider.GetSyncSessionDbConnection(_userService.GetSessionId(_username)))
            {
                JObject json = new SyncServerSession(store, connection).PutItemDataBatch(request);
                resp.Content = new StringContent(json.ToString(), Encoding.UTF8, "application/json");
            }

            return resp;
        }

    }
}