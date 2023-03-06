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

package org.apache.iotdb.db.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ClientPoolProperty;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.model.ModelInformation;
import org.apache.iotdb.mlnode.rpc.thrift.IMLNodeRPCService;
import org.apache.iotdb.mlnode.rpc.thrift.TCreateTrainingTaskReq;
import org.apache.iotdb.mlnode.rpc.thrift.TDeleteModelReq;
import org.apache.iotdb.mlnode.rpc.thrift.TForecastReq;
import org.apache.iotdb.mlnode.rpc.thrift.TForecastResp;
import org.apache.iotdb.rpc.RpcTransportFactory;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.TsBlockSerde;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public class MLNodeClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(MLNodeClient.class);

  private final TTransport transport;
  private final IMLNodeRPCService.Client client;

  public static final String MSG_CONNECTION_FAIL =
      "Fail to connect to MLNode. Please check status of MLNode";

  private final TsBlockSerde tsBlockSerde = new TsBlockSerde();

  public MLNodeClient() throws TException {
    TEndPoint endpoint = CommonDescriptor.getInstance().getConfig().getTargetMLNodeEndPoint();
    try {
      long connectionTimeout = ClientPoolProperty.DefaultProperty.WAIT_CLIENT_TIMEOUT_MS;
      transport =
          RpcTransportFactory.INSTANCE.getTransport(
              // As there is a try-catch already, we do not need to use TSocket.wrap
              endpoint.getIp(), endpoint.getPort(), (int) connectionTimeout);
      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new TException(MSG_CONNECTION_FAIL);
    }

    TProtocolFactory protocolFactory = new TBinaryProtocol.Factory();
    client = new IMLNodeRPCService.Client(protocolFactory.getProtocol(transport));
  }

  public TSStatus createTrainingTask(
      ModelInformation modelInformation, Map<String, String> modelConfigs) throws TException {
    try {
      TCreateTrainingTaskReq req =
          new TCreateTrainingTaskReq(
              modelInformation.getModelId(),
              modelInformation.isAuto(),
              modelConfigs,
              modelInformation.getQueryExpressions());
      if (modelInformation.getQueryFilter() != null) {
        req.setQueryFilter(modelInformation.getQueryFilter());
      }
      return client.createTrainingTask(req);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from ConfigNode when executing {}",
          Thread.currentThread().getStackTrace()[1].getMethodName());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TSStatus deleteModel(String modelId) throws TException {
    try {
      return client.deleteModel(new TDeleteModelReq(modelId));
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from ConfigNode when executing {}",
          Thread.currentThread().getStackTrace()[1].getMethodName());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  public TsBlock forecast(String modelPath, TsBlock inputTsBlock) throws TException {
    try {
      TForecastReq forecastReq = new TForecastReq(modelPath, tsBlockSerde.serialize(inputTsBlock));
      TForecastResp resp = client.forecast(forecastReq);
      if (resp.status.code != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        throw new TException("Failed to execute forecast task, because: " + resp.status.message);
      }
      return tsBlockSerde.deserialize(resp.forecastResult);
    } catch (IOException e) {
      throw new TException("An exception occurred while serializing input tsblock", e);
    } catch (TException e) {
      logger.warn(
          "Failed to connect to MLNode from DataNode when executing {}",
          Thread.currentThread().getStackTrace()[1].getMethodName());
      throw new TException(MSG_CONNECTION_FAIL);
    }
  }

  @Override
  public void close() throws Exception {
    Optional.ofNullable(transport).ifPresent(TTransport::close);
  }
}
