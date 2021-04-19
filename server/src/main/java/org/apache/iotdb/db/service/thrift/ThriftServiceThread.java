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

package org.apache.iotdb.db.service.thrift;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.runtime.RPCServiceException;
import org.apache.iotdb.db.utils.CommonUtils;
import org.apache.iotdb.rpc.RpcTransportFactory;

import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;

public class ThriftServiceThread extends Thread {

  private static final Logger logger = LoggerFactory.getLogger(ThriftServiceThread.class);
  private TServerTransport serverTransport;
  private TServer poolServer;
  private CountDownLatch threadStopLatch;

  private String serviceName;

  private TProtocolFactory protocolFactory;
  private TThreadPoolServer.Args poolArgs;

  @SuppressWarnings("squid:S107")
  public ThriftServiceThread(
      TProcessor processor,
      String serviceName,
      String threadsName,
      String bindAddress,
      int port,
      int maxWorkerThreads,
      int timeoutMs,
      TServerEventHandler serverEventHandler,
      boolean compress) {
    if (compress) {
      protocolFactory = new TCompactProtocol.Factory();
    } else {
      protocolFactory = new TBinaryProtocol.Factory();
    }
    this.serviceName = serviceName;

    try {
      serverTransport = openTransport(bindAddress, port);
      poolArgs =
          new TThreadPoolServer.Args(serverTransport)
              .maxWorkerThreads(maxWorkerThreads)
              .minWorkerThreads(CommonUtils.getCpuCores())
              .stopTimeoutVal(timeoutMs);
      poolArgs.executorService =
          IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs, threadsName);
      poolArgs.processor(processor);
      poolArgs.protocolFactory(protocolFactory);
      poolArgs.transportFactory(RpcTransportFactory.INSTANCE);
      poolServer = new TThreadPoolServer(poolArgs);
      poolServer.setServerEventHandler(serverEventHandler);
    } catch (TTransportException e) {
      close();
      if (threadStopLatch == null) {
        logger.debug("Stop Count Down latch is null");
      } else {
        logger.debug("Stop Count Down latch is {}", threadStopLatch.getCount());
      }
      if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
        threadStopLatch.countDown();
      }
      logger.debug(
          "{}: close TThreadPoolServer and TServerSocket for {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          serviceName);
      throw new RPCServiceException(
          String.format(
              "%s: failed to start %s, because ", IoTDBConstant.GLOBAL_DB_NAME, serviceName),
          e);
    }
  }

  @SuppressWarnings("java:S2259")
  public TServerTransport openTransport(String bindAddress, int port) throws TTransportException {
    int maxRetry = 5;
    long retryIntervalMS = 5000;
    TTransportException lastExp = null;
    for (int i = 0; i < maxRetry; i++) {
      try {
        return new TServerSocket(new InetSocketAddress(bindAddress, port));
      } catch (TTransportException e) {
        lastExp = e;
        try {
          Thread.sleep(retryIntervalMS);
        } catch (InterruptedException interruptedException) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    throw lastExp;
  }

  public void setThreadStopLatch(CountDownLatch threadStopLatch) {
    this.threadStopLatch = threadStopLatch;
  }

  @SuppressWarnings("squid:S2093") // socket will be used later
  @Override
  public void run() {
    logger.info("The {} service thread begin to run...", serviceName);
    try {
      poolServer.serve();
    } catch (Exception e) {
      throw new RPCServiceException(
          String.format("%s: %s exit, because ", IoTDBConstant.GLOBAL_DB_NAME, serviceName), e);
    } finally {
      close();
      if (threadStopLatch == null) {
        logger.debug("Stop Count Down latch is null");
      } else {
        logger.debug("Stop Count Down latch is {}", threadStopLatch.getCount());
      }

      if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
        threadStopLatch.countDown();
      }
      logger.debug(
          "{}: close TThreadPoolServer and TServerSocket for {}",
          IoTDBConstant.GLOBAL_DB_NAME,
          serviceName);
    }
  }

  public synchronized void close() {
    if (poolServer != null) {
      poolServer.setShouldStop(true);
      poolServer.stop();

      poolServer = null;
    }
    if (serverTransport != null) {
      serverTransport.close();
      serverTransport = null;
    }
  }

  public boolean isServing() {
    if (poolServer != null) {
      return poolServer.isServing();
    }
    return false;
  }
}
