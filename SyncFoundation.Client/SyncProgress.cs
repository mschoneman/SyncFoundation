using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Client
{
    public class SyncProgress
    {
        public SyncStage Stage { get; set; }
        public int PercentComplete { get; set; }
        public string Message { get; set; }
    }

    public enum SyncStage
    {
        Connecting,
        FindingRemoteChanges,
        DownloadingRemoteChanges,
        CheckingForConflicts,
        ApplyingChangesLocally,
        FindingLocalChanges,
        UploadingLocalChanges,
        ApplyingChangesRemotely,
        Disconnecting,
    }

}
