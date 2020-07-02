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
package org.apache.iotdb.db.sync.receiver;

import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.service.thrift.ThriftService;
import org.apache.iotdb.db.service.thrift.ThriftServiceThread;
import org.apache.iotdb.db.sync.receiver.transfer.SyncServiceImpl;
import org.apache.iotdb.service.sync.thrift.SyncService;

/**
 * sync receiver server.
 */
public class SyncServerManager  extends ThriftService implements SyncServerManagerMBean {

  private SyncServiceImpl serviceImpl;

  private static class ServerManagerHolder {

    private static final SyncServerManager INSTANCE = new SyncServerManager();
  }

  public static SyncServerManager getInstance() {
    return SyncServerManager.ServerManagerHolder.INSTANCE;
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
    serviceImpl = new SyncServiceImpl();
    processor = new SyncService.Processor<>(serviceImpl);
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    thriftServiceThread = new ThriftServiceThread(processor,
        getID().getName(), ThreadName.SYNC_CLIENT.getName(),
        config.getRpcAddress(), config.getSyncServerPort(),
        Integer.MAX_VALUE, config.getThriftServerAwaitTimeForStopService(),
        new SyncServerThriftHandler(serviceImpl),
        config.isRpcThriftCompressionEnable()
        );
    thriftServiceThread.setName(ThreadName.SYNC_SERVER.getName());
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getSyncServerPort();
  }
}