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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.BaseRpcTransportFactory;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.ZeroCopyRpcTransportFactory;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeCriticalException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionRuntimeNonCriticalException;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeRequestType;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeCloseResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeCommitResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHeartbeatResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeSubscribeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeUnsubscribeResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeReq;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SubscriptionSessionConnection extends SessionConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionSessionConnection.class);

  private static final String SHOW_DATA_NODES_COMMAND = "SHOW DATANODES";
  private static final String NODE_ID_COLUMN_NAME = "NodeID";
  private static final String STATUS_COLUMN_NAME = "Status";
  private static final String IP_COLUMN_NAME = "RpcAddress";
  private static final String PORT_COLUMN_NAME = "RpcPort";
  private static final String REMOVING_STATUS = "Removing";

  public SubscriptionSessionConnection(
      Session session,
      TEndPoint endPoint,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs,
      String sqlDialect,
      String database)
      throws IoTDBConnectionException {
    super(
        session,
        endPoint,
        zoneId,
        availableNodes,
        maxRetryCount,
        retryIntervalInMs,
        sqlDialect,
        database);
  }

  @Override
  protected void initTransport(
      TEndPoint endPoint,
      boolean useSSL,
      String trustStore,
      String trustStorePwd,
      final BaseRpcTransportFactory rpcTransportFactory)
      throws IoTDBConnectionException {
    this.initTransportInternal(
        endPoint, useSSL, trustStore, trustStorePwd, ZeroCopyRpcTransportFactory.INSTANCE);
  }

  // from org.apache.iotdb.session.NodesSupplier.updateDataNodeList
  public Map<Integer, TEndPoint> fetchAllEndPoints()
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = session.executeQueryStatement(SHOW_DATA_NODES_COMMAND);
    SessionDataSet.DataIterator iterator = dataSet.iterator();
    Map<Integer, TEndPoint> endPoints = new HashMap<>();
    while (iterator.next()) {
      // ignore removing DN
      if (REMOVING_STATUS.equals(iterator.getString(STATUS_COLUMN_NAME))) {
        continue;
      }
      String ip = iterator.getString(IP_COLUMN_NAME);
      String port = iterator.getString(PORT_COLUMN_NAME);
      if (ip != null && port != null) {
        endPoints.put(
            iterator.getInt(NODE_ID_COLUMN_NAME), new TEndPoint(ip, Integer.parseInt(port)));
      }
    }
    return endPoints;
  }

  public synchronized TPipeSubscribeResp pipeSubscribe(final TPipeSubscribeReq req)
      throws TException, SubscriptionException {
    final TPipeSubscribeResp resp = client.pipeSubscribe(req);
    verifyPipeSubscribeSuccess(resp.status);

    if (PipeSubscribeRequestType.isValidatedRequestType(req.type)) {
      switch (PipeSubscribeRequestType.valueOf(req.type)) {
        case HANDSHAKE:
          return PipeSubscribeHandshakeResp.fromTPipeSubscribeResp(resp);
        case HEARTBEAT:
          return PipeSubscribeHeartbeatResp.fromTPipeSubscribeResp(resp);
        case SUBSCRIBE:
          return PipeSubscribeSubscribeResp.fromTPipeSubscribeResp(resp);
        case UNSUBSCRIBE:
          return PipeSubscribeUnsubscribeResp.fromTPipeSubscribeResp(resp);
        case POLL:
          return PipeSubscribePollResp.fromTPipeSubscribeResp(resp);
        case COMMIT:
          return PipeSubscribeCommitResp.fromTPipeSubscribeResp(resp);
        case CLOSE:
          return PipeSubscribeCloseResp.fromTPipeSubscribeResp(resp);
        default:
          break;
      }
    }

    verifyPipeSubscribeSuccess(
        RpcUtils.getStatus(
            TSStatusCode.SUBSCRIPTION_TYPE_ERROR,
            String.format("Unknown PipeSubscribeRequestType %s.", req.type))); // throw exception
    return null;
  }

  private static void verifyPipeSubscribeSuccess(final TSStatus status)
      throws SubscriptionException {
    switch (status.code) {
      case 200: // SUCCESS_STATUS
        return;
      case 1902: // SUBSCRIPTION_HANDSHAKE_ERROR
      case 1903: // SUBSCRIPTION_HEARTBEAT_ERROR
      case 1904: // SUBSCRIPTION_POLL_ERROR
      case 1905: // SUBSCRIPTION_COMMIT_ERROR
      case 1906: // SUBSCRIPTION_CLOSE_ERROR
      case 1907: // SUBSCRIPTION_SUBSCRIBE_ERROR
      case 1908: // SUBSCRIPTION_UNSUBSCRIBE_ERROR
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionRuntimeNonCriticalException(status.message);
      case 1900: // SUBSCRIPTION_VERSION_ERROR
      case 1901: // SUBSCRIPTION_TYPE_ERROR
      case 1909: // SUBSCRIPTION_MISSING_CUSTOMER
      default:
        LOGGER.warn(
            "Internal error occurred, status code {}, status message {}",
            status.code,
            status.message);
        throw new SubscriptionRuntimeCriticalException(status.message);
    }
  }
}
