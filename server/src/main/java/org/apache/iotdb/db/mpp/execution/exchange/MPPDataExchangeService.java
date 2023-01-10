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

package org.apache.iotdb.db.mpp.execution.exchange;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.ClientPoolFactory;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.IoTThreadFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.mpp.execution.memory.LocalMemoryManager;
import org.apache.iotdb.mpp.rpc.thrift.MPPDataExchangeService.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MPPDataExchangeService extends ThriftService implements MPPDataExchangeServiceMBean {

  private static final Logger LOGGER = LoggerFactory.getLogger(MPPDataExchangeService.class);

  private final MPPDataExchangeManager mppDataExchangeManager;
  private final ExecutorService executorService;

  private MPPDataExchangeService() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    executorService =
        IoTDBThreadPoolFactory.newThreadPool(
            config.getMppDataExchangeCorePoolSize(),
            config.getMppDataExchangeMaxPoolSize(),
            config.getMppDataExchangeKeepAliveTimeInMs(),
            TimeUnit.MILLISECONDS,
            // TODO: Use a priority queue.
            new LinkedBlockingQueue<>(),
            new IoTThreadFactory("mpp-data-exchange-task-executors"),
            "mpp-data-exchange-task-executors");
    this.mppDataExchangeManager =
        new MPPDataExchangeManager(
            new LocalMemoryManager(),
            new TsBlockSerdeFactory(),
            executorService,
            new IClientManager.Factory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>()
                .createClientManager(
                    new ClientPoolFactory.SyncDataNodeMPPDataExchangeServiceClientPoolFactory()));
    LOGGER.info("MPPDataExchangeManager init successfully");
  }

  @Override
  public void initTProcessor() {
    initSyncedServiceImpl(null);
    processor = new Processor<>(mppDataExchangeManager.getOrCreateMPPDataExchangeServiceImpl());
  }

  public MPPDataExchangeManager getMPPDataExchangeManager() {
    return mppDataExchangeManager;
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    try {
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      thriftServiceThread =
          new ThriftServiceThread(
              processor,
              getID().getName(),
              ThreadName.MPP_DATA_EXCHANGE_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new MPPDataExchangeServiceThriftHandler(),
              // TODO: hard coded compress strategy
              false);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.MPP_DATA_EXCHANGE_RPC_SERVICE.getName());
    MetricService.getInstance()
        .addMetricSet(new MPPDataExchangeServiceMetrics(thriftServiceThread));
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getMppDataExchangePort();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.MPP_DATA_EXCHANGE_SERVICE;
  }

  @Override
  public void stop() {
    super.stop();
    executorService.shutdown();
  }

  public static MPPDataExchangeService getInstance() {
    return MPPDataExchangeServiceHolder.INSTANCE;
  }

  @Override
  public int getRPCPort() {
    return getBindPort();
  }

  private static class MPPDataExchangeServiceHolder {
    private static final MPPDataExchangeService INSTANCE = new MPPDataExchangeService();

    private MPPDataExchangeServiceHolder() {}
  }
}
