using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncSessionDbConnectionProvider
    {
        void SessionStart(string sessionId);
        IDbConnection GetSyncSessionDbConnection(string sessionId);
        void SessionEnd(string sessionId);
    }
}
