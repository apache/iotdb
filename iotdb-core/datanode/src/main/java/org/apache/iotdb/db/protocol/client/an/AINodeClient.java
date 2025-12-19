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

package org.apache.iotdb.db.protocol.client.an;

import org.apache.iotdb.ainode.rpc.thrift.IAINodeRPCService;
import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatReq;
import org.apache.iotdb.ainode.rpc.thrift.TAIHeartbeatResp;
import org.apache.iotdb.ainode.rpc.thrift.TDeleteModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.ainode.rpc.thrift.TInferenceReq;
import org.apache.iotdb.ainode.rpc.thrift.TInferenceResp;
import org.apache.iotdb.ainode.rpc.thrift.TLoadModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TRegisterModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TRegisterModelResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowAIDevicesResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowLoadedModelsReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowLoadedModelsResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsResp;
import org.apache.iotdb.ainode.rpc.thrift.TTuningReq;
import org.apache.iotdb.ainode.rpc.thrift.TUnloadModelReq;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.client.sync.SyncThriftClientWithErrorHandler;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.confignode.rpc.thrift.TGetAINodeLocationResp;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AINodeClient implements IAINodeRPCService.Iface, AutoCloseable, ThriftClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(AINodeClient.class);

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();
  private static final IoTDBConfig IOTDB_CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private TTransport transport;

  private final ThriftClientProperty property;
  private IAINodeRPCService.Client client;

  private static final int MAX_RETRY = 5;
  private static final int RETRY_INTERVAL_MS = 100;
  public static final String MSG_ALL_RETRY_FAILED =
      String.format(
          "Failed to connect to AINode after %d retries, please check the status of AINode",
          MAX_RETRY);
  public static final String MSG_AINODE_CONNECTION_FAIL =
      "Fail to connect to AINode from DataNode %s when executing %s.";
  private static final String UNSUPPORTED_INVOCATION =
      "This method is not supported for invocation by DataNode";

  @Override
  public TSStatus stopAINode() throws TException {
    return executeRemoteCallWithRetry(() -> client.stopAINode());
  }

  @Override
  public TShowModelsResp showModels(TShowModelsReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.showModels(req));
  }

  @Override
  public TShowLoadedModelsResp showLoadedModels(TShowLoadedModelsReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.showLoadedModels(req));
  }

  @Override
  public TShowAIDevicesResp showAIDevices() throws TException {
    return executeRemoteCallWithRetry(() -> client.showAIDevices());
  }

  @Override
  public TSStatus deleteModel(TDeleteModelReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.deleteModel(req));
  }

  @Override
  public TRegisterModelResp registerModel(TRegisterModelReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.registerModel(req));
  }

  @Override
  public TAIHeartbeatResp getAIHeartbeat(TAIHeartbeatReq req) {
    throw new UnsupportedOperationException(UNSUPPORTED_INVOCATION);
  }

  @Override
  public TSStatus createTuningTask(TTuningReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.createTuningTask(req));
  }

  @Override
  public TSStatus loadModel(TLoadModelReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.loadModel(req));
  }

  @Override
  public TSStatus unloadModel(TUnloadModelReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.unloadModel(req));
  }

  @Override
  public TInferenceResp inference(TInferenceReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.inference(req));
  }

  @Override
  public TForecastResp forecast(TForecastReq req) throws TException {
    return executeRemoteCallWithRetry(() -> client.forecast(req));
  }

  @FunctionalInterface
  private interface RemoteCall<R> {
    R apply() throws TException;
  }

  ClientManager<Integer, AINodeClient> clientManager;

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  private static final AtomicReference<TAINodeLocation> CURRENT_LOCATION = new AtomicReference<>();

  private <R> R executeRemoteCallWithRetry(RemoteCall<R> call) throws TException {
    for (int attempt = 0; attempt < MAX_RETRY; attempt++) {
      try {
        return call.apply();
      } catch (TException e) {
        final String message =
            String.format(
                MSG_AINODE_CONNECTION_FAIL,
                IOTDB_CONFIG.getAddressAndPort(),
                Thread.currentThread().getStackTrace()[2].getMethodName());
        LOGGER.warn(message, e);
        CURRENT_LOCATION.set(null);
        if (e.getCause() != null && e.getCause() instanceof SSLHandshakeException) {
          throw e;
        }
      }
      try {
        TimeUnit.MILLISECONDS.sleep(RETRY_INTERVAL_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(
            "Unexpected interruption when waiting to try to connect to AINode, may because current node has been down. Will break current execution process to avoid meaningless wait.");
        break;
      }
      tryToConnect(property.getConnectionTimeoutMs());
    }
    throw new TException(MSG_ALL_RETRY_FAILED);
  }

  private void tryToConnect(int timeoutMs) {
    TEndPoint endpoint = getCurrentEndpoint();
    if (endpoint != null) {
      try {
        connect(endpoint, timeoutMs);
        return;
      } catch (TException e) {
        LOGGER.warn("The current AINode may have been down {}, because", endpoint, e);
        CURRENT_LOCATION.set(null);
      }
    } else {
      LOGGER.warn("Cannot connect to any AINode due to there are no available ones.");
    }
    if (transport != null) {
      transport.close();
    }
  }

  public void connect(TEndPoint endpoint, int timeoutMs) throws TException {
    transport =
        COMMON_CONFIG.isEnableInternalSSL()
            ? DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                endpoint.getIp(),
                endpoint.getPort(),
                timeoutMs,
                COMMON_CONFIG.getTrustStorePath(),
                COMMON_CONFIG.getTrustStorePwd(),
                COMMON_CONFIG.getKeyStorePath(),
                COMMON_CONFIG.getKeyStorePwd())
            : DeepCopyRpcTransportFactory.INSTANCE.getTransport(
                // As there is a try-catch already, we do not need to use TSocket.wrap
                endpoint.getIp(), endpoint.getPort(), timeoutMs);
    if (!transport.isOpen()) {
      transport.open();
    }
    client = new IAINodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  public TEndPoint getCurrentEndpoint() {
    TAINodeLocation loc = CURRENT_LOCATION.get();
    if (loc == null) {
      loc = refreshFromConfigNode();
    }
    return (loc == null) ? null : loc.getInternalEndPoint();
  }

  private TAINodeLocation refreshFromConfigNode() {
    try (final ConfigNodeClient cn =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      final TGetAINodeLocationResp resp = cn.getAINodeLocation();
      if (resp.isSetAiNodeLocation()) {
        final TAINodeLocation loc = resp.getAiNodeLocation();
        CURRENT_LOCATION.set(loc);
        return loc;
      }
    } catch (Exception e) {
      LoggerFactory.getLogger(AINodeClient.class)
          .debug("[AINodeClient] refreshFromConfigNode failed: {}", e.toString());
    }
    return null;
  }

  public AINodeClient(
      ThriftClientProperty property, ClientManager<Integer, AINodeClient> clientManager) {
    this.property = property;
    this.clientManager = clientManager;
    tryToConnect(property.getConnectionTimeoutMs());
  }

  public TTransport getTransport() {
    return transport;
  }

  @Override
  public void close() {
    clientManager.returnClient(AINodeClientManager.AINODE_ID_PLACEHOLDER, this);
  }

  @Override
  public void invalidate() {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(AINodeClientManager.AINODE_ID_PLACEHOLDER);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  public static class Factory extends ThriftClientFactory<Integer, AINodeClient> {

    public Factory(
        ClientManager<Integer, AINodeClient> clientClientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientClientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(Integer aiNodeId, PooledObject<AINodeClient> pooledObject) {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<AINodeClient> makeObject(Integer Integer) throws Exception {
      return new DefaultPooledObject<>(
          SyncThriftClientWithErrorHandler.newErrorHandler(
              AINodeClient.class,
              AINodeClient.class.getConstructor(
                  thriftClientProperty.getClass(), clientManager.getClass()),
              thriftClientProperty,
              clientManager));
    }

    @Override
    public boolean validateObject(Integer Integer, PooledObject<AINodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
