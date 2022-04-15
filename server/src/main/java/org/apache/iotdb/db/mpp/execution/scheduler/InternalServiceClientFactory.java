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

package org.apache.iotdb.db.mpp.execution.scheduler;

import org.apache.iotdb.mpp.rpc.thrift.InternalService;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class InternalServiceClientFactory {
  private static final int TIMEOUT_MS = 10000;

  // TODO: (xingtanzjr) consider the best practice to maintain the clients
  public static InternalService.Client getInternalServiceClient(String endpoint, int port)
      throws TTransportException {
    TTransport transport =
        RpcTransportFactory.INSTANCE.getTransport(
            // as there is a try-catch already, we do not need to use TSocket.wrap
            endpoint, port, TIMEOUT_MS);
    transport.open();
    TProtocol protocol = new TBinaryProtocol(transport);
    return new InternalService.Client(protocol);
  }
}
