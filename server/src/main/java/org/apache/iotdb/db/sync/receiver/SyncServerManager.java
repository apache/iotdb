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
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * sync receiver server.
 */
public class SyncServerManager implements IService {

  private static final Logger logger = LoggerFactory.getLogger(SyncServerManager.class);
  private Thread syncServerThread;
  private IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private SyncServerManager() {
  }

  public static final SyncServerManager getInstance() {
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
      logger.error(
          "Sync server failed to start because IP white list is null, please set IP white list.");
      return;
    }
    conf.setIpWhiteList(conf.getIpWhiteList().replaceAll(" ", ""));
    syncServerThread = new SyncServiceThread();
    syncServerThread.setName(ThreadName.SYNC_SERVER.getName());
    syncServerThread.start();
    logger.info("Sync server has started.");
  }

  /**
   * Close sync receiver's server.
   */
  @Override
  public void stop() {
    if (conf.isSyncEnable()) {
      ((SyncServiceThread) syncServerThread).close();
    }
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SYNC_SERVICE;
  }

  private static class ServerManagerHolder {

    private static final SyncServerManager INSTANCE = new SyncServerManager();
  }

  private class SyncServiceThread extends Thread {

    private TServerSocket serverTransport;
    private TServer poolServer;
    private TProtocolFactory protocolFactory;
    private Processor<SyncService.Iface> processor;
    private TThreadPoolServer.Args poolArgs;

    public SyncServiceThread() {
      processor = new SyncService.Processor<>(new SyncServiceImpl());
    }

    @Override
    public void run() {
      try {
        serverTransport = new TServerSocket(
            new InetSocketAddress(conf.getRpcAddress(), conf.getSyncServerPort()));
        if(conf.isRpcThriftCompressionEnable()) {
          protocolFactory = new TCompactProtocol.Factory();
        }
        else {
          protocolFactory = new TBinaryProtocol.Factory();
        }
        processor = new SyncService.Processor<>(new SyncServiceImpl());
        poolArgs = new TThreadPoolServer.Args(serverTransport);
        poolArgs.executorService = IoTDBThreadPoolFactory.createThriftRpcClientThreadPool(poolArgs,
            ThreadName.SYNC_CLIENT.getName());
        poolArgs.protocolFactory(protocolFactory);
        poolArgs.processor(processor);
        poolServer = new TThreadPoolServer(poolArgs);
        poolServer.serve();
      } catch (TTransportException e) {
        logger.error("{}: failed to start {}, because ", IoTDBConstant.GLOBAL_DB_NAME,
            getID().getName(), e);
      } catch (Exception e) {
        logger.error("{}: {} exit, because ", IoTDBConstant.GLOBAL_DB_NAME, getID().getName(), e);
      } finally {
        close();
        logger.info("{}: close TThreadPoolServer and TServerSocket for {}",
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