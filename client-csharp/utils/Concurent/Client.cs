using Thrift.Transport;


namespace iotdb_client_csharp.client.utils
{
    public class Client
    {
        public TSIService.Client client;
        public long sessionId, statementId;
        public TFramedTransport transport;
    }
}