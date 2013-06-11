using System.Data;

namespace SyncFoundation.Server
{
    public static class DbCommandExtensions
    {
        public static void AddParameter(this IDbCommand command, string name, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            command.Parameters.Add(param);
        }
    }
}
