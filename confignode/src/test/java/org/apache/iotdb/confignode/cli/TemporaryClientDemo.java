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

import org.apache.iotdb.common.rpc.thrift.EndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.rpc.thrift.ConfigIService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TSetStorageGroupReq;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;

import java.util.concurrent.TimeUnit;

public class TemporaryClientDemo {

  private static final int timeOutInMS = 10000;

  public void setStorageGroupAndCreateRegionsDemo() throws TException, InterruptedException {
    TTransport transport = RpcTransportFactory.INSTANCE.getTransport("0.0.0.0", 22277, timeOutInMS);
    transport.open();
    ConfigIService.Client client = new ConfigIService.Client(new TBinaryProtocol(transport));

    TDataNodeRegisterReq registerReq = new TDataNodeRegisterReq(new EndPoint("0.0.0.0", 6667));
    TDataNodeRegisterResp registerResp = client.registerDataNode(registerReq);
    System.out.println(registerResp);
    TimeUnit.MILLISECONDS.sleep(200);

    for (int i = 0; i < 5; i++) {
      TSetStorageGroupReq setReq = new TSetStorageGroupReq("root.sg" + i);
      TSStatus status = client.setStorageGroup(setReq);
      System.out.println(status.toString());
    }
  }
}
