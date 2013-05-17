using BookSample.Data;
using BookSample.Data.Sync;
using Community.CsharpSqlite.SQLiteClient;
using SyncFoundation.Core.Interfaces;
using SyncFoundation.Server;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.WebService
{
    class SingleUserService : IUserService
    {
        private string _lastNonce;
        private string _sessionId;
        private DateTime _sessionExpires = DateTime.MinValue;

        public string GetPasswordEquivalent(string username)
        {
            if (username != "test@example.com")
                return null;
            return "monkey";
        }

        public string GetLastNonce(string username)
        {
            return _lastNonce;
        }

        public void SetLastNonce(string username, string nonce)
        {
            _lastNonce = nonce;
        }

        public string GetSessionId(string username)
        {
            if (DateTime.Now > _sessionExpires)
            {
                //DeleteSessionDbFile(_sessionId);
                _sessionId = null;
                return null;
            }
            return _sessionId;
        }

        public void SetSessionId(string username, string sessionId)
        {
            if (sessionId == null)
            {
                //DeleteSessionDbFile(_sessionId);                
            }
            _sessionId = sessionId;
            _sessionExpires = DateTime.Now + TimeSpan.FromMinutes(5);
        }


        public ISyncableStore GetSyncableStore(string username)
        {
            String path = username + ".User.sqlite";
            if (Path.GetDirectoryName(path) == "")
                path = Path.Combine(AppDomain.CurrentDomain.GetData("DataDirectory").ToString(), path);

            BookRepository repos = new BookRepository(path);
            return new BookRepositorySyncableStoreAdapter(repos);
        }
    }
}
