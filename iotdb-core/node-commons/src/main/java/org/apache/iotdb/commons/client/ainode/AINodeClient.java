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

package org.apache.iotdb.commons.client.ainode;

import org.apache.iotdb.ainode.rpc.thrift.IAINodeRPCService;
import org.apache.iotdb.ainode.rpc.thrift.TConfigs;
import org.apache.iotdb.ainode.rpc.thrift.TDeleteModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastReq;
import org.apache.iotdb.ainode.rpc.thrift.TForecastResp;
import org.apache.iotdb.ainode.rpc.thrift.TInferenceReq;
import org.apache.iotdb.ainode.rpc.thrift.TInferenceResp;
import org.apache.iotdb.ainode.rpc.thrift.TRegisterModelReq;
import org.apache.iotdb.ainode.rpc.thrift.TRegisterModelResp;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsReq;
import org.apache.iotdb.ainode.rpc.thrift.TShowModelsResp;
import org.apache.iotdb.ainode.rpc.thrift.TTrainingReq;
import org.apache.iotdb.ainode.rpc.thrift.TWindowParams;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.exception.ainode.LoadModelException;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.column.TsBlockSerde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

import static org.apache.iotdb.rpc.TSStatusCode.CAN_NOT_CONNECT_AINODE;
import static org.apache.iotdb.rpc.TSStatusCode.INTERNAL_SERVER_ERROR;

public class AINodeClient implements AutoCloseable, ThriftClient {

  private static final Logger logger = LoggerFactory.getLogger(AINodeClient.class);

  private final TEndPoint endPoint;

  private TTransport transport;

  private final ThriftClientProperty property;
  private IAINodeRPCService.Client client;

  public static final String MSG_CONNECTION_FAIL =
      "Fail to connect to AINode. Please check status of AINode";

  private final TsBlockSerde tsBlockSerde = new TsBlockSerde();

  ClientManager<TEndPoint, AINodeClient> clientManager;

  public AINodeClient(
      ThriftClientProperty property,
      TEndPoint endPoint,
      ClientManager<TEndPoint, AINodeClient> clientManager)
      throws TException {
    this.property = property;
    this.clientManager = clientManager;
    this.endPoint = endPoint;
    init();
  }

  private void init() throws TException {
    try {
      transport =
          new TFramedTransport.Factory()
              .getTransport(
                  new TSocket(
                      TConfigurationConst.defaultTConfiguration,
                      endPoint.getIp(),
                      endPoint.getPort(),
                      property.getConnectionTimeoutMs()));
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new TException(MSG_CONNECTION_FAIL);
    }
    client = new IAINodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  public TTransport getTransport() {
    return transport;
  }

  public TSStatus stopAINode() throws TException {
    try {
      TSStatus status = client.stopAINode();
      if (status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new TException(status.message);
      }
      return status;
    } catch (TException e) {
      logger.warn(
          "Failed to connect to AINode from ConfigNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public ModelInformation registerModel(String modelName, String uri) throws LoadModelException {
    try {
      TRegisterModelReq req = new TRegisterModelReq(uri, modelName);
      TRegisterModelResp resp = client.registerModel(req);
      if (resp.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new LoadModelException(resp.status.message, resp.status.getCode());
      }
      return parseModelInformation(modelName, resp.getAttributes(), resp.getConfigs());
    } catch (TException e) {
      throw new LoadModelException(
          e.getMessage(), TSStatusCode.AI_NODE_INTERNAL_ERROR.getStatusCode());
    }
  }

  private ModelInformation parseModelInformation(
      String modelName, String attributes, TConfigs configs) {
    int[] inputShape = configs.getInput_shape().stream().mapToInt(Integer::intValue).toArray();
    int[] outputShape = configs.getOutput_shape().stream().mapToInt(Integer::intValue).toArray();

    TSDataType[] inputType = new TSDataType[inputShape[1]];
    TSDataType[] outputType = new TSDataType[outputShape[1]];
    for (int i = 0; i < inputShape[1]; i++) {
      inputType[i] = TSDataType.values()[configs.getInput_type().get(i)];
    }
    for (int i = 0; i < outputShape[1]; i++) {
      outputType[i] = TSDataType.values()[configs.getOutput_type().get(i)];
    }

    return new ModelInformation(
        modelName, inputShape, outputShape, inputType, outputType, attributes);
  }

  public TSStatus deleteModel(String modelId) throws TException {
    try {
      return client.deleteModel(new TDeleteModelReq(modelId));
    } catch (TException e) {
      logger.warn(
          "Failed to connect to AINode from ConfigNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TShowModelsResp showModels(TShowModelsReq req) throws TException {
    try {
      return client.showModels(req);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to AINode from ConfigNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TInferenceResp inference(
      String modelId,
      TsBlock inputTsBlock,
      Map<String, String> inferenceAttributes,
      TWindowParams windowParams)
      throws TException {
    try {
      TInferenceReq inferenceReq = new TInferenceReq(modelId, tsBlockSerde.serialize(inputTsBlock));
      if (windowParams != null) {
        inferenceReq.setWindowParams(windowParams);
      }
      if (inferenceAttributes != null) {
        inferenceReq.setInferenceAttributes(inferenceAttributes);
      }
      return client.inference(inferenceReq);
    } catch (IOException e) {
      throw new TException("An exception occurred while serializing input data", e);
    } catch (TException e) {
      logger.warn(
          "Error happens in AINode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TForecastResp forecast(
      String modelId, TsBlock inputTsBlock, int outputLength, Map<String, String> options) {
    try {
      TForecastReq forecastReq =
          new TForecastReq(modelId, tsBlockSerde.serialize(inputTsBlock), outputLength);
      forecastReq.setOptions(options);
      return client.forecast(forecastReq);
    } catch (IOException e) {
      TSStatus tsStatus = new TSStatus(INTERNAL_SERVER_ERROR.getStatusCode());
      tsStatus.setMessage(String.format("Failed to serialize input tsblock %s", e.getMessage()));
      return new TForecastResp(tsStatus);
    } catch (TException e) {
      TSStatus tsStatus = new TSStatus(CAN_NOT_CONNECT_AINODE.getStatusCode());
      tsStatus.setMessage(
          String.format(
              "Failed to connect to AINode when executing %s: %s",
              Thread.currentThread().getStackTrace()[1].getMethodName(), e.getMessage()));
      return new TForecastResp(tsStatus);
    }
  }

  public TSStatus createTrainingTask(TTrainingReq req) throws TException {
    try {
      return client.createTrainingTask(req);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to AINode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  @Override
  public void close() throws Exception {
    clientManager.returnClient(endPoint, this);
  }

  @Override
  public void invalidate() {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
  }

  @Override
  public void invalidateAll() {
    clientManager.clear(endPoint);
  }

  @Override
  public boolean printLogWhenEncounterException() {
    return property.isPrintLogWhenEncounterException();
  }

  public static class Factory extends ThriftClientFactory<TEndPoint, AINodeClient> {

    public Factory(
        ClientManager<TEndPoint, AINodeClient> clientClientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientClientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(TEndPoint tEndPoint, PooledObject<AINodeClient> pooledObject)
        throws Exception {
      pooledObject.getObject().invalidate();
    }

    @Override
    public PooledObject<AINodeClient> makeObject(TEndPoint endPoint) throws Exception {
      return new DefaultPooledObject<>(
          new AINodeClient(thriftClientProperty, endPoint, clientManager));
    }

    @Override
    public boolean validateObject(TEndPoint tEndPoint, PooledObject<AINodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
