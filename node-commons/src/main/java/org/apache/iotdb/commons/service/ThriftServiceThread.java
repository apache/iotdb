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

package org.apache.iotdb.commons.service;

import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.TBaseAsyncProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransportFactory;

public class ThriftServiceThread extends AbstractThriftServiceThread {

  /** for asynced ThriftService. */
  @SuppressWarnings("squid:S107")
  public ThriftServiceThread(
      TBaseAsyncProcessor<?> processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int selectorThreads,
      int minWorkerThreads,
      int maxWorkerThreads,
      int timeoutSecond,
      TServerEventHandler serverEventHandler,
      boolean compress,
      int connectionTimeoutInMS,
      int maxReadBufferBytes,
      ServerType serverType) {
    super(
        processor,
        serviceName,
        threadsName,
        bindAddress,
        port,
        selectorThreads,
        minWorkerThreads,
        maxWorkerThreads,
        timeoutSecond,
        serverEventHandler,
        compress,
        connectionTimeoutInMS,
        maxReadBufferBytes,
        serverType);
  }

  /** for synced ThriftServiceThread */
  @SuppressWarnings("squid:S107")
  public ThriftServiceThread(
      TProcessor processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int maxWorkerThreads,
      int timeoutSecond,
      TServerEventHandler serverEventHandler,
      boolean compress) {
    super(
        processor,
        serviceName,
        threadsName,
        bindAddress,
        port,
        maxWorkerThreads,
        timeoutSecond,
        serverEventHandler,
        compress);
  }

  @Override
  public TTransportFactory getTTransportFactory() {
    return RpcTransportFactory.INSTANCE;
  }
}
