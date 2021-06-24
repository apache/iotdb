using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Apache.IoTDB
{
    public class ConcurrentClientQueue
    {
        public ConcurrentQueue<Client> ClientQueue { get; }

        public ConcurrentClientQueue(List<Client> clients)
        {
            ClientQueue = new ConcurrentQueue<Client>(clients);
        }

        public ConcurrentClientQueue()
        {
            ClientQueue = new ConcurrentQueue<Client>();
        }

        public void Add(Client client)
        {
            Monitor.Enter(ClientQueue);
            ClientQueue.Enqueue(client);
            Monitor.Pulse(ClientQueue);
            Monitor.Exit(ClientQueue);
        }

        public Client Take()
        {
            Monitor.Enter(ClientQueue);
            
            if (ClientQueue.IsEmpty)
            {
                Monitor.Wait(ClientQueue);
            }

            ClientQueue.TryDequeue(out var client);
            Monitor.Exit(ClientQueue);
            return client;
        }
    }
}