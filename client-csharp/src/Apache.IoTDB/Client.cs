using Thrift.Transport;

namespace Apache.IoTDB
{
    public class Client
    {
        public TSIService.Client ServiceClient { get; }
        public long SessionId { get; }
        public long StatementId { get; }
        public TFramedTransport Transport { get; }

        public Client(TSIService.Client client, long sessionId, long statementId, TFramedTransport transport)
        {
            ServiceClient = client;
            SessionId = sessionId;
            StatementId = statementId;
            Transport = transport;
        }
    }
}