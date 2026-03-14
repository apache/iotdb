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

package org.apache.iotdb.commons.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.async.AsyncAINodeInternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncConfigNodeInternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeExternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.async.AsyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.client.async.AsyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.commons.client.async.AsyncPipeDataTransferServiceClient;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.client.property.IoTConsensusV2ClientProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.property.ThriftClientProperty.DefaultProperty;
import org.apache.iotdb.commons.client.sync.SyncAINodeClient;
import org.apache.iotdb.commons.client.sync.SyncConfigNodeIServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.sync.SyncDataNodeMPPDataExchangeServiceClient;
import org.apache.iotdb.commons.client.sync.SyncIoTConsensusV2ServiceClient;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;

import org.apache.commons.pool2.impl.GenericKeyedObjectPool;

public class ClientPoolFactory {

  private static final CommonConfig conf = CommonDescriptor.getInstance().getConfig();

  private ClientPoolFactory() {}

  public static class SyncConfigNodeIServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncConfigNodeIServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncConfigNodeIServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncConfigNodeIServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncConfigNodeIServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncConfigNodeIServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .build()),
              new ClientPoolProperty.Builder<SyncConfigNodeIServiceClient>().build().getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncConfigNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncConfigNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncConfigNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncConfigNodeInternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncConfigNodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncConfigNodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .build(),
                  ThreadName.ASYNC_CONFIGNODE_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncConfigNodeInternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class SyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncDataNodeInternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncDataNodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncDataNodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .build()),
              new ClientPoolProperty.Builder<SyncDataNodeInternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncDataNodeInternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncDataNodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(false)
                      .build(),
                  ThreadName.ASYNC_DATANODE_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncDataNodeExternalServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeExternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncDataNodeExternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeExternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncDataNodeExternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncDataNodeExternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .build(),
                  ThreadName.ASYNC_DATANODE_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncDataNodeExternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncConfigNodeHeartbeatServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncConfigNodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncConfigNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncConfigNodeInternalServiceClient> manager) {

      GenericKeyedObjectPool<TEndPoint, AsyncConfigNodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncConfigNodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(false)
                      .build(),
                  ThreadName.ASYNC_CONFIGNODE_HEARTBEAT_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncConfigNodeInternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncDataNodeHeartbeatServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeInternalServiceClient> {
    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncDataNodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncDataNodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(false)
                      .build(),
                  ThreadName.ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncDataNodeInternalServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class SyncDataNodeMPPDataExchangeServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient>
        createClientPool(
            ClientManager<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncDataNodeMPPDataExchangeServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncDataNodeMPPDataExchangeServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .build()),
              new ClientPoolProperty.Builder<SyncDataNodeMPPDataExchangeServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncDataNodeMPPDataExchangeServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient>
        createClientPool(
            ClientManager<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncDataNodeMPPDataExchangeServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncDataNodeMPPDataExchangeServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .build(),
                  ThreadName.ASYNC_DATANODE_MPP_DATA_EXCHANGE_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncDataNodeMPPDataExchangeServiceClient>()
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncPipeDataTransferServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncPipeDataTransferServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncPipeDataTransferServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> manager) {
      final GenericKeyedObjectPool<TEndPoint, AsyncPipeDataTransferServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncPipeDataTransferServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getPipeSinkTransferTimeoutMs())
                      .setRpcThriftCompressionEnabled(conf.isPipeSinkRPCThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getPipeAsyncSinkSelectorNumber())
                      .build(),
                  ThreadName.PIPE_ASYNC_CONNECTOR_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncPipeDataTransferServiceClient>()
                  .setMaxClientNumForEachNode(conf.getPipeAsyncSinkMaxClientNumber())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncPipeTsFileDataTransferServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncPipeDataTransferServiceClient> {
    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncPipeDataTransferServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncPipeDataTransferServiceClient> manager) {
      final GenericKeyedObjectPool<TEndPoint, AsyncPipeDataTransferServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncPipeDataTransferServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getPipeSinkTransferTimeoutMs())
                      .setRpcThriftCompressionEnabled(conf.isPipeSinkRPCThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getPipeAsyncSinkSelectorNumber())
                      .setPrintLogWhenEncounterException(conf.isPrintLogWhenEncounterException())
                      .build(),
                  ThreadName.PIPE_ASYNC_CONNECTOR_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncPipeDataTransferServiceClient>()
                  .setMaxClientNumForEachNode(conf.getPipeAsyncSinkMaxTsFileClientNumber())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class SyncIoTConsensusV2ServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncIoTConsensusV2ServiceClient> {

    private final IoTConsensusV2ClientProperty config;

    public SyncIoTConsensusV2ServiceClientPoolFactory(IoTConsensusV2ClientProperty config) {
      this.config = config;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncIoTConsensusV2ServiceClient> createClientPool(
        ClientManager<TEndPoint, SyncIoTConsensusV2ServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncIoTConsensusV2ServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncIoTConsensusV2ServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // TODO: consider timeout and evict strategy.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(config.isRpcThriftCompressionEnabled())
                      .setPrintLogWhenEncounterException(
                          config.isPrintLogWhenThriftClientEncounterException())
                      .build()),
              new ClientPoolProperty.Builder<SyncIoTConsensusV2ServiceClient>()
                  .setMaxClientNumForEachNode(config.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncIoTConsensusV2ServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncIoTConsensusV2ServiceClient> {

    private final IoTConsensusV2ClientProperty config;

    public AsyncIoTConsensusV2ServiceClientPoolFactory(IoTConsensusV2ClientProperty config) {
      this.config = config;
    }

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncIoTConsensusV2ServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncIoTConsensusV2ServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncIoTConsensusV2ServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncIoTConsensusV2ServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      // TODO: consider timeout and evict strategy.
                      .setConnectionTimeoutMs(DefaultProperty.CONNECTION_NEVER_TIMEOUT_MS)
                      .setRpcThriftCompressionEnabled(config.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(config.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(
                          config.isPrintLogWhenThriftClientEncounterException())
                      .build(),
                  ThreadName.ASYNC_DATANODE_IOT_CONSENSUS_V2_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncIoTConsensusV2ServiceClient>()
                  .setMaxClientNumForEachNode(config.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class SyncAINodeClientPoolFactory
      implements IClientPoolFactory<TEndPoint, SyncAINodeClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, SyncAINodeClient> createClientPool(
        ClientManager<TEndPoint, SyncAINodeClient> manager) {
      GenericKeyedObjectPool<TEndPoint, SyncAINodeClient> clientPool =
          new GenericKeyedObjectPool<>(
              new SyncAINodeClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getDnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .build()),
              new ClientPoolProperty.Builder<SyncAINodeClient>().build().getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }

  public static class AsyncAINodeHeartbeatServiceClientPoolFactory
      implements IClientPoolFactory<TEndPoint, AsyncAINodeInternalServiceClient> {

    @Override
    public GenericKeyedObjectPool<TEndPoint, AsyncAINodeInternalServiceClient> createClientPool(
        ClientManager<TEndPoint, AsyncAINodeInternalServiceClient> manager) {
      GenericKeyedObjectPool<TEndPoint, AsyncAINodeInternalServiceClient> clientPool =
          new GenericKeyedObjectPool<>(
              new AsyncAINodeInternalServiceClient.Factory(
                  manager,
                  new ThriftClientProperty.Builder()
                      .setConnectionTimeoutMs(conf.getCnConnectionTimeoutInMS())
                      .setRpcThriftCompressionEnabled(conf.isRpcThriftCompressionEnabled())
                      .setSelectorNumOfAsyncClientManager(conf.getSelectorNumOfClientManager())
                      .setPrintLogWhenEncounterException(false)
                      .build(),
                  ThreadName.ASYNC_DATANODE_HEARTBEAT_CLIENT_POOL.getName()),
              new ClientPoolProperty.Builder<AsyncAINodeInternalServiceClient>()
                  .setMaxClientNumForEachNode(conf.getMaxClientNumForEachNode())
                  .build()
                  .getConfig());
      ClientManagerMetrics.getInstance()
          .registerClientManager(this.getClass().getSimpleName(), clientPool);
      return clientPool;
    }
  }
}
