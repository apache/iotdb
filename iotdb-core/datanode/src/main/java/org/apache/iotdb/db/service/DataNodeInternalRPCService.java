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

package org.apache.iotdb.db.service;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.handler.InternalServiceThriftHandler;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.service.metrics.DataNodeInternalRPCServiceMetrics;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService.Processor;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DataNodeInternalRPCService extends ThriftService
    implements DataNodeInternalRPCServiceMBean {

  private static final ConsensusReadiness NOT_READY =
      new ConsensusReadiness() {
        @Override
        public boolean isAllConsensusStarted() {
          return false;
        }

        @Override
        public boolean awaitAllConsensusStarted(long timeout, TimeUnit unit) {
          return false;
        }
      };

  private final AtomicReference<DataNodeInternalRPCServiceImpl> impl = new AtomicReference<>();
  private final AtomicReference<ConsensusReadiness> consensusReadiness =
      new AtomicReference<>(NOT_READY);

  private final ConsensusReadiness delegatingConsensusReadiness =
      new ConsensusReadiness() {
        @Override
        public boolean isAllConsensusStarted() {
          return consensusReadiness.get().isAllConsensusStarted();
        }

        @Override
        public boolean awaitAllConsensusStarted(long timeout, TimeUnit unit)
            throws InterruptedException {
          return consensusReadiness.get().awaitAllConsensusStarted(timeout, unit);
        }
      };

  private DataNodeInternalRPCService() {}

  @Override
  public ServiceType getID() {
    return ServiceType.INTERNAL_SERVICE;
  }

  @Override
  public void initTProcessor() {
    DataNodeInternalRPCServiceImpl service = getImpl();
    initSyncedServiceImpl(null);
    processor = new Processor<>(service);
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
              ThreadName.DATANODE_INTERNAL_RPC_PROCESSOR.getName(),
              getBindIP(),
              getBindPort(),
              config.getRpcMaxConcurrentClientNum(),
              config.getThriftServerAwaitTimeForStopService(),
              new InternalServiceThriftHandler(),
              config.isRpcThriftCompressionEnable(),
              DeepCopyRpcTransportFactory.INSTANCE);
    } catch (RPCServiceException e) {
      throw new IllegalAccessException(e.getMessage());
    }
    thriftServiceThread.setName(ThreadName.DATANODE_INTERNAL_RPC_SERVICE.getName());
    MetricService.getInstance()
        .addMetricSet(new DataNodeInternalRPCServiceMetrics(thriftServiceThread));
  }

  @Override
  public String getBindIP() {
    return IoTDBDescriptor.getInstance().getConfig().getInternalAddress();
  }

  @Override
  public int getBindPort() {
    return IoTDBDescriptor.getInstance().getConfig().getInternalPort();
  }

  public DataNodeInternalRPCServiceImpl getImpl() {
    impl.compareAndSet(null, new DataNodeInternalRPCServiceImpl(delegatingConsensusReadiness));
    return impl.get();
  }

  void setConsensusReadiness(ConsensusReadiness consensusReadiness) {
    this.consensusReadiness.set(Objects.requireNonNull(consensusReadiness));
  }

  ConsensusReadiness getConsensusReadiness() {
    return delegatingConsensusReadiness;
  }

  private static class DataNodeInternalRPCServiceHolder {
    private static final DataNodeInternalRPCService INSTANCE = new DataNodeInternalRPCService();

    private DataNodeInternalRPCServiceHolder() {}
  }

  public static DataNodeInternalRPCService getInstance() {
    return DataNodeInternalRPCServiceHolder.INSTANCE;
  }
}
