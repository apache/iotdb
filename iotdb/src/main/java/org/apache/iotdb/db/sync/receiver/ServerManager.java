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

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.thrift.TProcessor;
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
 *
 * @author Tianan Li
 */
public class ServerManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);
  private TServerSocket serverTransport;
  private TServer poolServer;
  private IoTDBConfig conf = IoTDBDescriptor.getInstance().getConfig();

  private ServerManager() {
  }

  public static final ServerManager getInstance() {
    return ServerManagerHolder.INSTANCE;
  }

  /**
   * start sync receiver's server.
   */
  public void startServer() throws StartupException {
    Factory protocolFactory;
    TProcessor processor;
    TThreadPoolServer.Args poolArgs;
    if (!conf.isPostbackEnable()) {
      return;
    }
    try {
      if (conf.getIpWhiteList() == null) {
        LOGGER.error(
            "Sync server failed to start because IP white list is null, please set IP white list.");
        return;
      }
      conf.setIpWhiteList(conf.getIpWhiteList().replaceAll(" ", ""));
      serverTransport = new TServerSocket(conf.getPostbackServerPort());
      protocolFactory = new TBinaryProtocol.Factory();
      processor = new ServerService.Processor<>(new ServerServiceImpl());
      poolArgs = new TThreadPoolServer.Args(serverTransport);
      poolArgs.processor(processor);
      poolArgs.protocolFactory(protocolFactory);
      poolServer = new TThreadPoolServer(poolArgs);
      LOGGER.info("Sync server has started.");
      Runnable syncServerRunnable = () -> poolServer.serve();
      Thread syncServerThread = new Thread(syncServerRunnable, ThreadName.SYNC_SERVER.getName());
      syncServerThread.start();
    } catch (TTransportException e) {
      throw new StartupException("cannot start sync server.", e);
    }
  }

  /**
   * close sync receiver's server.
   */
  public void closeServer() {
    if (conf.isPostbackEnable() && poolServer != null) {
      poolServer.stop();
      serverTransport.close();
      LOGGER.info("stop sync server.");
    }
  }

  private static class ServerManagerHolder {

    private static final ServerManager INSTANCE = new ServerManager();
  }
}