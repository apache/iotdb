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
 *
 */
package org.apache.iotdb.db.newsync.transport.server;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.service.transport.thrift.TransportService;

import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportServerManager extends ThriftService
    implements Runnable, TransportServerManagerMBean {

  private static final Logger logger = LoggerFactory.getLogger(TransportServerManager.class);
  private TransportServiceImpl serviceImpl;

  @Override
  public void run() {
    TransportServerManager serverManager = new TransportServerManager();
    try {
      serverManager.start();
    } catch (StartupException e) {
      e.printStackTrace();
    }
  }

  private static class ServiceManagerHolder {
    private static final TransportServerManager INSTANCE = new TransportServerManager();
  }

  public static TransportServerManager getInstance() {
    return TransportServerManager.ServiceManagerHolder.INSTANCE;
  }

  @Override
  public ServiceType getID() {
    return ServiceType.SYNC_SERVICE;
  }

  @Override
  public ThriftService getImplementation() {
    return getInstance();
  }

  @Override
  public void initTProcessor() {
    initSyncedServiceImpl(null);
    serviceImpl = new TransportServiceImpl();
    processor = new TransportService.Processor<>(serviceImpl);
  }

  @Override
  public void initThriftServiceThread() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    thriftServiceThread =
        new ThriftServiceThread(
            processor,
            getID().getName(),
            ThreadName.SYNC_CLIENT.getName(),
            config.getRpcAddress(),
            config.getPipeServerPort(),
            Integer.MAX_VALUE,
            config.getThriftServerAwaitTimeForStopService(),
            new TransportServerThriftHandler(serviceImpl),
            config.isRpcThriftCompressionEnable());
    thriftServiceThread.setName(ThreadName.SYNC_SERVER.getName());
  }

  @Override
  public String getBindIP() {
    // TODO: Whether to change this config here
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    // TODO: Whether to change this config here
    return IoTDBDescriptor.getInstance().getConfig().getPipeServerPort();
  }

  //  @Override
  public int getRPCPort() {
    return getBindPort();
  }

  @Override
  public void startService() throws StartupException {
    super.startService();
  }

  @Override
  public void stopService() {
    super.stopService();
  }

  @TestOnly
  public static void main(String[] args) throws TTransportException, StartupException {
    logger.info("Transport server for testing only.");
    TransportServerManager serverManager = new TransportServerManager();
    serverManager.start();
  }
}
