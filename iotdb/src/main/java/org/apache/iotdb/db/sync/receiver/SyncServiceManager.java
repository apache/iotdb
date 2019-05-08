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
package org.apache.iotdb.db.sync.receiver;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.service.sync.thrift.SyncService;
import org.apache.iotdb.service.sync.thrift.SyncService.Processor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TBinaryProtocol.Factory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sync receiver server.
 */
public class SyncServiceManager implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(SyncServiceManager.class);
  private Thread syncServerThread;
  private IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();
  private CountDownLatch startLatch;
  private CountDownLatch stopLatch;

  private SyncServiceManager() {
  }

  public static final SyncServiceManager getInstance() {
    return ServerManagerHolder.INSTANCE;
  }

  /**
   * Start sync receiver's server.
   */
  @Override
  public void start() throws StartupException {
    if (!conf.isSyncEnable()) {
      return;
    }
    if (conf.getIpWhiteList() == null) {
      LOGGER.error(
          "Sync server failed to start because IP white list is null, please set IP white list.");
      return;
    }
    conf.setIpWhiteList(conf.getIpWhiteList().replaceAll(" ", ""));
    resetLatch();
    try {
      syncServerThread = new SyncServiceThread(startLatch, stopLatch);
      syncServerThread.setName(ThreadName.SYNC_SERVER.getName());
      syncServerThread.start();
      startLatch.await();
    } catch (InterruptedException e) {
      String errorMessage = String
          .format("Failed to start %s because of %s", this.getID().getName(),
              e.getMessage());
      LOGGER.error(errorMessage);
      throw new StartupException(errorMessage);
    }
    LOGGER
        .info("{}: start {} successfully, listening on ip {} port {}", IoTDBConstant.GLOBAL_DB_NAME,
            this.getID().getName(), conf.getRpcAddress(), conf.getSyncServerPort());
  }

  private void resetLatch(){
    startLatch = new CountDownLatch(1);
    stopLatch = new CountDownLatch(1);
  }

  /**
   * Close sync receiver's server.
   */
  @Override
  public void stop() {
    if (!conf.isSyncEnable()) {
      return;
    }
    ((SyncServiceThread) syncServerThread).close();
    LOGGER.info("{}: closing {}...", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    if (syncServerThread != null) {
      ((SyncServiceThread) syncServerThread).close();
    }
    try {
      stopLatch.await();
      resetLatch();
      LOGGER.info("{}: close {} successfully", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName());
    } catch (InterruptedException e) {
      LOGGER.error("{}: close {} failed because {}", IoTDBConstant.GLOBAL_DB_NAME, this.getID().getName(), e);
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SYNC_SERVICE;
  }

  private static class ServerManagerHolder {

    private static final SyncServiceManager INSTANCE = new SyncServiceManager();
  }

  private class SyncServiceThread extends Thread {

    private TServerSocket serverTransport;
    private TServer poolServer;
    private Factory protocolFactory;
    private Processor<SyncService.Iface> processor;
    private TThreadPoolServer.Args poolArgs;
    private CountDownLatch threadStartLatch;
    private CountDownLatch threadStopLatch;

    public SyncServiceThread(CountDownLatch threadStartLatch, CountDownLatch threadStopLatch) {
      this.processor = new SyncService.Processor<>(new SyncServiceImpl());
      this.threadStartLatch = threadStartLatch;
      this.threadStopLatch = threadStopLatch;
    }

    @Override
    public void run() {
      try {
        serverTransport = new TServerSocket(
            new InetSocketAddress(conf.getRpcAddress(), conf.getSyncServerPort()));
        protocolFactory = new TBinaryProtocol.Factory();
        processor = new SyncService.Processor<>(new SyncServiceImpl());
        poolArgs = new TThreadPoolServer.Args(serverTransport);
        poolArgs.executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs,
            ThreadName.SYNC_CLIENT.getName());
        poolArgs.protocolFactory(protocolFactory);
        poolArgs.processor(processor);
        poolServer = new TThreadPoolServer(poolArgs);
        poolServer.setServerEventHandler(new SyncServiceEventHandler(threadStartLatch));
        poolServer.serve();
      } catch (TTransportException e) {
        LOGGER.error("{}: failed to start {}, because ", IoTDBConstant.GLOBAL_DB_NAME,
            getID().getName(), e);
      } catch (Exception e) {
        LOGGER.error("{}: {} exit, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      } finally {
        close();
        if(threadStopLatch == null) {
          LOGGER.info("Sync Service Stop Count Down latch is null");
        } else {
          LOGGER.info("Sync Service Stop Count Down latch is {}", threadStopLatch.getCount());
        }
        if (threadStopLatch != null && threadStopLatch.getCount() == 1) {
          threadStopLatch.countDown();
        }
        LOGGER.info("{}: close TThreadPoolServer and TServerSocket for {}",
            IoTDBConstant.GLOBAL_DB_NAME, getID().getName());
      }
    }

    private synchronized void close() {
      if (poolServer != null) {
        poolServer.stop();
        poolServer = null;
      }
      if (serverTransport != null) {
        serverTransport.close();
        serverTransport = null;
      }
    }
  }
}