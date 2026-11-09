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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.runtime.RPCServiceException;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.ThriftService;
import org.apache.iotdb.commons.service.ThriftServiceThread;
import org.apache.iotdb.commons.service.TrustedChannelAuditServerEventHandler;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.db.audit.DNAuditLogger;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.InternalClientRPCServiceProcessor;
import org.apache.iotdb.db.protocol.thrift.handler.InternalServiceThriftHandler;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.protocol.thrift.impl.IClientRPCServiceWithHandler;
import org.apache.iotdb.db.protocol.thrift.impl.InternalClientRPCServiceImpl;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.db.service.metrics.DataNodeInternalRPCServiceMetrics;
import org.apache.iotdb.mpp.rpc.thrift.IDataNodeRPCService.Processor;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcServiceConstants;

import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.TProcessor;

import java.util.concurrent.atomic.AtomicReference;

public class DataNodeInternalRPCService extends ThriftService
    implements DataNodeInternalRPCServiceMBean {

  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();

  protected final AtomicReference<DataNodeInternalRPCServiceImpl> impl = new AtomicReference<>();
  protected final AtomicReference<IClientRPCServiceWithHandler> clientImpl =
      new AtomicReference<>();
  protected DataNodeContext dataNodeContext;

  protected DataNodeInternalRPCService() {}

  @Override
  public ServiceType getID() {
    return ServiceType.INTERNAL_SERVICE;
  }

  @Override
  public void initTProcessor() {
    DataNodeInternalRPCServiceImpl service = getImpl();
    IClientRPCServiceWithHandler clientService = getClientImpl();
    initSyncedServiceImpl(null);
    processor =
        createMultiplexedProcessor(
            new Processor<>(service), new InternalClientRPCServiceProcessor(clientService));
  }

  @Override
  public void initThriftServiceThread() throws IllegalAccessException {
    // Internal client sessions intentionally share this server with cluster RPC traffic. Their
    // connection capacity is bounded together by rpcMaxConcurrentClientNum and must be sized for
    // both workloads.
    try {
      thriftServiceThread =
          commonConfig.isEnableInternalSSL()
              ? new ThriftServiceThread(
                  processor,
                  getID().getName(),
                  ThreadName.DATANODE_INTERNAL_RPC_PROCESSOR.getName(),
                  getBindIP(),
                  getBindPort(),
                  config.getRpcMaxConcurrentClientNum(),
                  config.getThriftServerAwaitTimeForStopService(),
                  new TrustedChannelAuditServerEventHandler(
                      new InternalServiceThriftHandler(getClientImpl()),
                      new TEndPoint(getBindIP(), getBindPort()),
                      DNAuditLogger.getInstance()::recordTrustedChannelFailureAuditLogIfNecessary),
                  config.isRpcThriftCompressionEnable(),
                  commonConfig.getKeyStorePath(),
                  commonConfig.getKeyStorePwd(),
                  commonConfig.getTrustStorePath(),
                  commonConfig.getTrustStorePwd(),
                  DeepCopyRpcTransportFactory.INSTANCE)
              : new ThriftServiceThread(
                  processor,
                  getID().getName(),
                  ThreadName.DATANODE_INTERNAL_RPC_PROCESSOR.getName(),
                  getBindIP(),
                  getBindPort(),
                  config.getRpcMaxConcurrentClientNum(),
                  config.getThriftServerAwaitTimeForStopService(),
                  new InternalServiceThriftHandler(getClientImpl()),
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
    return config.getInternalAddress();
  }

  @Override
  public int getBindPort() {
    return config.getInternalPort();
  }

  public DataNodeInternalRPCServiceImpl getImpl() {
    impl.compareAndSet(null, new DataNodeInternalRPCServiceImpl(dataNodeContext));
    return impl.get();
  }

  static TProcessor createMultiplexedProcessor(
      final TProcessor controlProcessor, final TProcessor clientProcessor) {
    final TMultiplexedProcessor multiplexedProcessor = new TMultiplexedProcessor();
    multiplexedProcessor.registerProcessor(
        RpcServiceConstants.INTERNAL_CLIENT_RPC_SERVICE, clientProcessor);
    multiplexedProcessor.registerDefault(controlProcessor);
    return multiplexedProcessor;
  }

  protected IClientRPCServiceWithHandler getClientImpl() {
    clientImpl.compareAndSet(null, new InternalClientRPCServiceImpl());
    return clientImpl.get();
  }

  private static class DataNodeInternalRPCServiceHolder {
    private static final DataNodeInternalRPCService INSTANCE = new DataNodeInternalRPCService();

    private DataNodeInternalRPCServiceHolder() {}
  }

  public static DataNodeInternalRPCService getInstance() {
    return DataNodeInternalRPCServiceHolder.INSTANCE;
  }

  public void setDataNodeContext(DataNodeContext dataNodeContext) {
    this.dataNodeContext = dataNodeContext;
  }
}
