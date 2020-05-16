/**
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
**/
﻿using System;
using System.Net;
using System.Threading.Tasks;
using IoTDBService;
using Thrift;
using Thrift.Protocol;
using Thrift.Server;

using Thrift.Transport;
using Thrift.Transport.Client;

namespace IoTDBClient
{

    public enum TSDataType : ushort
    {
        BOOLEAN = 0,
        INT32 = 1,
        INT64 = 2,
        FLOAT = 3,
        DOUBLE =4,
        TEXT = 5
    }

    public enum TSEncoding : ushort
    {
        PLAIN,
        PLAIN_DICTIONARY,
        RLE,
        DIFF,
        TS_2DIFF,
        BITMAP,
        GORILLA,
        REGULAR
    }
    public enum Compressor: ushort { UNCOMPRESSED, SNAPPY, GZIP, LZO, SDT, PAA, PLA}

    class Program
    {


        static async Task Main(string[] args)
        {
            var port = 6667;
            var username = "root";
            var password = "root";
            
            IoTDBService.TSIService.Client client = null;
            try
            {

                var rawTransport = new TSocketTransport(IPAddress.Loopback, port);
                var transport = new TBufferedTransport(rawTransport);
                var protocol = new TBinaryProtocol(transport);
                client = new IoTDBService.TSIService.Client(protocol);
                await transport.OpenAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Cannot connect " +ex.Message);
                return;
            }

            var sessionArgs = new TSOpenSessionReq {Username = username, Password = password};
            Console.WriteLine("Client version: "+ sessionArgs.Client_protocol);

            var session = await client.openSessionAsync(sessionArgs);
            if (session.ServerProtocolVersion > TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2)
            {
               Console.WriteLine("Inconsistent protocol, server version: %d, client version: %d", session.ServerProtocolVersion, TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2);
               return;
            }
            var stmtId = await client.requestStatementIdAsync(session.SessionId);

            var status = await client.setStorageGroupAsync(session.SessionId, "root.group1");
            Console.WriteLine("Status: %d", status.Code);
            var timeserieRequest = new TSCreateTimeseriesReq(session.SessionId,
                "root.group1.s1", Convert.ToInt32(TSDataType.INT64), 
                Convert.ToInt32(TSEncoding.PLAIN), Convert.ToInt32(Compressor.UNCOMPRESSED));

            status = await client.createTimeseriesAsync(timeserieRequest);
            Console.WriteLine("Status %d %s", status.Code, status.Message);
	    
        }
    }
}
