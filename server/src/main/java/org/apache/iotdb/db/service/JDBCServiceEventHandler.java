/**
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
package org.apache.iotdb.db.service;

import java.util.concurrent.CountDownLatch;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

public class JDBCServiceEventHandler implements TServerEventHandler {

  private TSServiceImpl serviceImpl;
  private CountDownLatch startLatch;

  JDBCServiceEventHandler(TSServiceImpl serviceImpl, CountDownLatch startLatch) {
    this.serviceImpl = serviceImpl;
    this.startLatch = startLatch;
  }

  @Override
  public ServerContext createContext(TProtocol arg0, TProtocol arg1) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void deleteContext(ServerContext arg0, TProtocol arg1, TProtocol arg2) {
    serviceImpl.handleClientExit();
  }

  @Override
  public void preServe() {
    this.startLatch.countDown();
  }

  @Override
  public void processContext(ServerContext arg0, TTransport arg1, TTransport arg2) {
    // TODO Auto-generated method stub

  }

}
