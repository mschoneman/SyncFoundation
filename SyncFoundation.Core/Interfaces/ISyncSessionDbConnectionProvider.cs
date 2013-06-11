using System.Data;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncSessionDbConnectionProvider
    {
        void SessionStart(string sessionId);
        IDbConnection GetSyncSessionDbConnection(string sessionId);
        void SessionEnd(string sessionId);
    }
}
