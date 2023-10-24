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

package org.apache.iotdb.commons.client.mlnode;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.ClientManager;
import org.apache.iotdb.commons.client.ThriftClient;
import org.apache.iotdb.commons.client.factory.ThriftClientFactory;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.mlnode.rpc.thrift.IMLNodeRPCService;
import org.apache.iotdb.mlnode.rpc.thrift.TCreateTrainingTaskReq;
import org.apache.iotdb.mlnode.rpc.thrift.TDeleteModelReq;
import org.apache.iotdb.mlnode.rpc.thrift.TForecastReq;
import org.apache.iotdb.mlnode.rpc.thrift.TForecastResp;
import org.apache.iotdb.rpc.TConfigurationConst;
import org.apache.iotdb.tsfile.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MLNodeClient implements AutoCloseable, ThriftClient {

  private static final Logger logger = LoggerFactory.getLogger(MLNodeClient.class);

  private static final TEndPoint endPoint = MLNodeInfo.endPoint;

  private TTransport transport;

  private final ThriftClientProperty property;
  private IMLNodeRPCService.Client client;

  public static final String MSG_CONNECTION_FAIL =
      "Fail to connect to MLNode. Please check status of MLNode";

  private final TsBlockSerde tsBlockSerde = new TsBlockSerde();

  ClientManager<TEndPoint, MLNodeClient> clientManager;

  public MLNodeClient(
      ThriftClientProperty property, ClientManager<TEndPoint, MLNodeClient> clientManager)
      throws TException {
    this.property = property;
    this.clientManager = clientManager;
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
    client = new IMLNodeRPCService.Client(property.getProtocolFactory().getProtocol(transport));
  }

  public TTransport getTransport() {
    return transport;
  }

  public TSStatus createTrainingTask(
      ModelInformation modelInformation, Map<String, String> hyperparameters) throws TException {
    try {
      TCreateTrainingTaskReq req =
          new TCreateTrainingTaskReq(
              modelInformation.getModelId(),
              modelInformation.getOptions(),
              hyperparameters,
              modelInformation.getDatasetFetchSql());
      return client.createTrainingTask(req);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from ConfigNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TSStatus deleteModel(String modelId) throws TException {
    try {
      return client.deleteModel(new TDeleteModelReq(modelId));
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from ConfigNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TForecastResp forecast(
      String modelPath,
      TsBlock inputTsBlock,
      List<TSDataType> inputTypeList,
      List<String> inputColumnNameList,
      int predictLength)
      throws TException {
    try {
      List<String> reqInputTypeList = new ArrayList<>();
      for (TSDataType dataType : inputTypeList) {
        reqInputTypeList.add(dataType.toString());
      }
      TForecastReq forecastReq =
          new TForecastReq(
              modelPath,
              tsBlockSerde.serialize(inputTsBlock),
              reqInputTypeList,
              inputColumnNameList,
              predictLength);
      return client.forecast(forecastReq);
    } catch (IOException e) {
      throw new TException("An exception occurred while serializing input tsblock", e);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from DataNode when executing {}: {}",
          Thread.currentThread().getStackTrace()[1].getMethodName(),
          e.getMessage());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  @Override
  public void close() throws Exception {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
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

  public static class Factory extends ThriftClientFactory<TEndPoint, MLNodeClient> {

    public Factory(
        ClientManager<TEndPoint, MLNodeClient> clientClientManager,
        ThriftClientProperty thriftClientProperty) {
      super(clientClientManager, thriftClientProperty);
    }

    @Override
    public void destroyObject(TEndPoint tEndPoint, PooledObject<MLNodeClient> pooledObject)
        throws Exception {
      pooledObject.getObject().close();
    }

    @Override
    public PooledObject<MLNodeClient> makeObject(TEndPoint tEndPoint) throws Exception {
      return new DefaultPooledObject<>(new MLNodeClient(thriftClientProperty, clientManager));
    }

    @Override
    public boolean validateObject(TEndPoint tEndPoint, PooledObject<MLNodeClient> pooledObject) {
      return Optional.ofNullable(pooledObject.getObject().getTransport())
          .map(TTransport::isOpen)
          .orElse(false);
    }
  }
}
