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

package org.apache.iotdb.db.mpp.buffer;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.DataBlockService.Processor;

import org.apache.commons.lang3.Validate;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DataBlockService extends ThriftService {

  private LocalMemoryManager localMemoryManager;
  private TsBlockSerdeFactory tsBlockSerdeFactory;
  private DataBlockManager dataBlockManager;
  private ExecutorService executorService;
  private DataBlockServiceClientFactory clientFactory;

  private DataBlockService() {}

  @Override
  public ThriftService getImplementation() {
    return DataBlockManagerServiceHolder.INSTANCE;
  }

  @Override
  public void initTProcessor()
      throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    executorService =
        IoTDBThreadPoolFactory.newThreadPool(
            config.getDataBlockManagerCorePoolSize(),
            config.getDataBlockManagerMaxPoolSize(),
            config.getDataBlockManagerKeepAliveTimeInMs(),
            TimeUnit.MILLISECONDS,
            // TODO: Use a priority queue.
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory("data-block-manager-task-executors"),
            "data-block-manager-task-executors");
    clientFactory = new DataBlockServiceClientFactory();
    this.dataBlockManager =
        new DataBlockManager(
            localMemoryManager, tsBlockSerdeFactory, executorService, clientFactory);
    processor = new Processor<>(dataBlockManager.getOrCreateDataBlockServiceImpl());
  }

  public void setLocalMemoryManager(LocalMemoryManager localMemoryManager) {
    this.localMemoryManager = Validate.notNull(localMemoryManager);
  }

  public void setTsBlockSerdeFactory(TsBlockSerdeFactory tsBlockSerdeFactory) {
    this.tsBlockSerdeFactory = Validate.notNull(tsBlockSerdeFactory);
  }

  public DataBlockManager getDataBlockManager() {
    return dataBlockManager;
  }

  @Override
  public void initThriftServiceThread()
      throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    try {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.DATA_BLOCK_MANAGER_CLIENT.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new DataBlockServiceThriftHandler(),
              // TODO: hard coded compress strategy
              true);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.DATA_BLOCK_MANAGER_SERVICE.getName());
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getRpcAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getDataBlockManagerPort();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.DATA_BLOCK_MANAGER_SERVICE;
  }

  @Override
  public void stop() {
    super.stop();
    executorService.shutdown();
  }

  private static class DataBlockManagerServiceHolder {
    private static final DataBlockService INSTANCE = new DataBlockService();

    private DataBlockManagerServiceHolder() {}
  }
}
