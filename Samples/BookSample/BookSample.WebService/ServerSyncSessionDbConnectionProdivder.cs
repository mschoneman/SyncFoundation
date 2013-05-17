using Community.CsharpSqlite.SQLiteClient;
using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BookSample.WebService
{
    class ServerSyncSessionDbConnectionProdivder : ISyncSessionDbConnectionProvider
    {
        public void SessionStart(string sessionId)
        {
            // Nothing to do here
        }

        public IDbConnection GetSyncSessionDbConnection(string sessionId)
        {
            string cs = string.Format("Version=3,busy_timeout=500,uri=file:{0}", GenerateSessionDbFileFromSessionId(sessionId));
            IDbConnection connection = new SqliteConnection();
            connection.ConnectionString = cs;
            connection.Open();
            return connection;
        }

        public void SessionEnd(string sessionId)
        {
            DeleteSessionDbFile(sessionId);
        }

        private string GenerateSessionDbFileFromSessionId(string sessionId)
        {
            return Path.Combine(DataPath, sessionId + ".ServerSyncSession.sqlite");
        }

        private string DataPath
        {
            get
            {
                return AppDomain.CurrentDomain.GetData("DataDirectory").ToString();
            }
        }

        private void DeleteSessionDbFile(string sessionId)
        {
            if (sessionId == null)
                return;
            File.Delete(GenerateSessionDbFileFromSessionId(sessionId));
        }

    }
}
