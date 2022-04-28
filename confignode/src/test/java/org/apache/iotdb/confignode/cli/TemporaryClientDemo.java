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
package org.apache.iotdb.confignode.cli;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.confignode.rpc.thrift.TStorageGroupSchema;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class TemporaryClientDemo {

  private static final int timeOutInMS = 10000;

  private final Random random = new Random();
  private Map<Integer, ConfigIService.Client> clients;
  private ConfigIService.Client defaultClient;

  public void setStorageGroupsDemo() throws TException {
    createClients();
    defaultClient = clients.get(22277);

    for (int i = 0; i < 5; i++) {
      TSetStorageGroupReq setReq =
          new TSetStorageGroupReq(new TStorageGroupSchema().setName("root.sg" + i));
      while (true) {
        TSStatus status = defaultClient.setStorageGroup(setReq);
        System.out.println(status.toString());
        if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          break;
        } else if (status.getCode() == TSStatusCode.NEED_REDIRECTION.getStatusCode()) {
          int port = random.nextInt(3) * 2 + 22277;
          if (status.getRedirectNode() != null) {
            port = status.getRedirectNode().getPort();
          }
          defaultClient = clients.get(port);
        }
      }
    }
  }

  private void createClients() throws TTransportException {
    clients = new HashMap<>();
    for (int i = 22277; i <= 22281; i += 2) {
      TTransport transport = RpcTransportFactory.INSTANCE.getTransport("0.0.0.0", i, timeOutInMS);
      transport.open();
      clients.put(i, new ConfigIService.Client(new TBinaryProtocol(transport)));
    }
  }
}
