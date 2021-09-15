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

package org.apache.iotdb.cluster.common;

import org.apache.iotdb.cluster.rpc.thrift.RaftService.AsyncClient;

import org.apache.thrift.async.TAsyncClientManager;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TNonblockingTransport;

public class TestAsyncClient extends AsyncClient {

  private int serialNum;

  public TestAsyncClient() {
    super(null, null, null);
  }

  public TestAsyncClient(int serialNum) {
    this();
    this.serialNum = serialNum;
  }

  TestAsyncClient(
      TProtocolFactory protocolFactory,
      TAsyncClientManager clientManager,
      TNonblockingTransport transport,
      int serialNum) {
    super(protocolFactory, clientManager, transport);
    this.serialNum = serialNum;
  }

  public int getSerialNum() {
    return serialNum;
  }
}
