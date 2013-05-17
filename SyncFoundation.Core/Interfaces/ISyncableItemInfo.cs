using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Core.Interfaces
{
    public interface ISyncableItemInfo
    {
        string ItemType { get; }
        IRepositoryInfo Created { get; }
        IRepositoryInfo Modified { get; }
        bool Deleted { get; }
    }
}
