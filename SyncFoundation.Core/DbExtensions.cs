using System.Data;

namespace SyncFoundation.Core
{
    public static class DbExtensions
    {
        public static void AddParameter(this IDbCommand command, string name, object value)
        {
            IDbDataParameter param = command.CreateParameter();
            param.ParameterName = name;
            param.Value = value;
            command.Parameters.Add(param);
        }

        public static int ExecuteNonQuery(this IDbConnection connection, string commandText)
        {
            IDbCommand command = connection.CreateCommand();
            command.CommandText = commandText;
            return command.ExecuteNonQuery();
        }
    }
}
