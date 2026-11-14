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

package org.apache.iotdb.consensus.raft.service;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.consensus.raft.config.RaftConfig;
import org.apache.iotdb.consensus.raft.thrift.RaftConsensusIService;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;

import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * The Raft/TRaft Thrift server on the data-region consensus port. It holds a {@link
 * ThriftServiceThread} directly instead of extending {@code ThriftService}, so no new {@code
 * ServiceType} enum constant (node-commons) is needed; the only cost is the missing service-level
 * MBean.
 */
public final class RaftRPCService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RaftRPCService.class);
  public static final String SERVICE_NAME = "RaftConsensusRPCService";
  public static final String PROCESSOR_THREADS = "RaftConsensusRPC-Processor";

  private final TEndPoint bind;
  private final RaftConfig.Rpc rpc;
  private final RaftRPCServiceProcessor processor;
  private ThriftServiceThread thread;
  private CountDownLatch stopLatch;

  public RaftRPCService(TEndPoint bind, RaftConfig.Rpc rpc, RaftRPCServiceProcessor processor) {
    this.bind = bind;
    this.rpc = rpc;
    this.processor = processor;
  }

  public void start() throws StartupException {
    TProcessor p = new RaftConsensusIService.Processor<>(processor);
    try {
      thread =
          rpc.enableSsl
              ? new ThriftServiceThread(
                  p,
                  SERVICE_NAME,
                  PROCESSOR_THREADS,
                  bind.getIp(),
                  bind.getPort(),
                  rpc.rpcMaxConcurrentClientNum,
                  rpc.thriftServerAwaitTimeForStopService,
                  new RaftRPCServiceHandler(processor),
                  rpc.rpcThriftCompressionEnabled,
                  rpc.sslKeyStorePath,
                  rpc.sslKeyStorePassword,
                  rpc.sslTrustStorePath,
                  rpc.sslTrustStorePassword,
                  ZeroCopyRpcTransportFactory.INSTANCE)
              : new ThriftServiceThread(
                  p,
                  SERVICE_NAME,
                  PROCESSOR_THREADS,
                  bind.getIp(),
                  bind.getPort(),
                  rpc.rpcMaxConcurrentClientNum,
                  rpc.thriftServerAwaitTimeForStopService,
                  new RaftRPCServiceHandler(processor),
                  rpc.rpcThriftCompressionEnabled,
                  ZeroCopyRpcTransportFactory.INSTANCE);
    } catch (RuntimeException e) {
      throw new StartupException(SERVICE_NAME, "cannot bind " + bind + ": " + e.getMessage());
    }
    thread.setName(SERVICE_NAME);
    stopLatch = new CountDownLatch(1);
    thread.setThreadStopLatch(stopLatch);
    thread.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!thread.isServing()) {
      if (stopLatch.getCount() == 0 || !thread.isAlive()) {
        throw new StartupException(SERVICE_NAME, "server thread exited before serving on " + bind);
      }
      if (System.nanoTime() > deadline) {
        throw new StartupException(SERVICE_NAME, "server did not start serving on " + bind);
      }
      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new StartupException(SERVICE_NAME, "interrupted");
      }
    }
    LOGGER.info("{} serving on {}", SERVICE_NAME, bind);
  }

  public void stop() {
    if (thread == null) {
      return;
    }
    thread.close();
    try {
      stopLatch.await(rpc.thriftServerAwaitTimeForStopService, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    thread = null;
  }

  public boolean isServing() {
    return thread != null && thread.isServing();
  }
}
