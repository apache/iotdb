/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Client;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class DataBlockServiceClientFactory {
  public DataBlockService.Client getDataBlockServiceClient(String hostname, int port)
      throws TTransportException {
    TTransport transport = RpcTransportFactory.INSTANCE.getTransportWithNoTimeout(hostname, port);
    transport.open();
    TProtocol protocol =
        IoTDBDescriptor.getInstance().getConfig().isRpcThriftCompressionEnable()
            ? new TCompactProtocol(transport)
            : new TBinaryProtocol(transport);
    return new Client(protocol);
  }
}
