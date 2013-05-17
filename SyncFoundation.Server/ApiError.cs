using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SyncFoundation.Server
{
    class ApiError
    {
        public ApiError()
        {
            errorCode = 1;
        }
        public int errorCode { get; set; }
        public string errorMessage { get; set; }
    }
}
