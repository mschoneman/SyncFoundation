namespace SyncFoundation.Core
{
    public enum SyncStatus
    {
        Insert = 30,
        InsertConflict = 31,
        Update = 33,
        UpdateConflict = 34,
        Delete = 35,
        DeleteNonExisting = 36,
        DeleteConflict = 37,
        MayBeNeeded = 50,
    }
}
