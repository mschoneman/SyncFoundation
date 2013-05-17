using SyncFoundation.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Server
{
    public interface IUserService
    {
        string GetPasswordEquivalent(string username);

        string GetLastNonce(string username);
        void SetLastNonce(string username, string nonce);

        string GetSessionId(string username);
        void SetSessionId(string username, string sessionId);

        ISyncableStore GetSyncableStore(string username);
    }
}
