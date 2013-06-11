namespace SyncFoundation.Server
{
    class ApiError
    {
        public ApiError()
        {
            ErrorCode = 1;
        }
        public int ErrorCode { get; set; }
        public string ErrorMessage { get; set; }
    }
}
