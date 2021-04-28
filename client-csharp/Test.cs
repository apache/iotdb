using iotdb_client_csharp.client.test;
using System;
namespace iotdb_client_csharp.client
{
    public class Test
    {
        static void Main(){
            // Session Async Test
            SessionPoolTest session_pool_test = new SessionPoolTest();
            session_pool_test.Test();
           
        
            Console.WriteLine("TEST PASSED");

        }
        
    }
}