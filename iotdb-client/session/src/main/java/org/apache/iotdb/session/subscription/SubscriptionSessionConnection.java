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
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.EnrichedTablets;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCloseReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeCommitReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribePollReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeSubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeUnsubscribeReq;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribeHandshakeResp;
import org.apache.iotdb.rpc.subscription.payload.response.PipeSubscribePollResp;
import org.apache.iotdb.service.rpc.thrift.TPipeSubscribeResp;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;

import org.apache.thrift.TException;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

public class SubscriptionSessionConnection extends SessionConnection {

  public SubscriptionSessionConnection(
      Session session,
      TEndPoint endPoint,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs)
      throws IoTDBConnectionException {
    super(session, endPoint, zoneId, availableNodes, maxRetryCount, retryIntervalInMs);
  }

  public SubscriptionSessionConnection(
      Session session,
      ZoneId zoneId,
      Supplier<List<TEndPoint>> availableNodes,
      int maxRetryCount,
      long retryIntervalInMs)
      throws IoTDBConnectionException {
    super(session, zoneId, availableNodes, maxRetryCount, retryIntervalInMs);
  }

  public Map<Integer, TEndPoint> handshake(ConsumerConfig consumerConfig)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(PipeSubscribeHandshakeReq.toTPipeSubscribeReq(consumerConfig));
    RpcUtils.verifySuccess(resp.status);
    PipeSubscribeHandshakeResp handshakeResp =
        PipeSubscribeHandshakeResp.fromTPipeSubscribeResp(resp);
    return handshakeResp.getEndPoints();
  }

  public void closeConsumer() throws TException, StatementExecutionException {
    TPipeSubscribeResp resp = client.pipeSubscribe(PipeSubscribeCloseReq.toTPipeSubscribeReq());
    RpcUtils.verifySuccess(resp.status);
  }

  public void subscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(PipeSubscribeSubscribeReq.toTPipeSubscribeReq(topicNames));
    RpcUtils.verifySuccess(resp.status);
  }

  public void unsubscribe(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(PipeSubscribeUnsubscribeReq.toTPipeSubscribeReq(topicNames));
    RpcUtils.verifySuccess(resp.status);
  }

  public List<EnrichedTablets> poll(Set<String> topicNames)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(PipeSubscribePollReq.toTPipeSubscribeReq(topicNames, 0));
    RpcUtils.verifySuccess(resp.status);
    PipeSubscribePollResp pollResp = PipeSubscribePollResp.fromTPipeSubscribeResp(resp);
    return pollResp.getEnrichedTabletsList();
  }

  public List<EnrichedTablets> poll(Set<String> topicNames, long timeoutMs)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(PipeSubscribePollReq.toTPipeSubscribeReq(topicNames, timeoutMs));
    RpcUtils.verifySuccess(resp.status);
    PipeSubscribePollResp pollResp = PipeSubscribePollResp.fromTPipeSubscribeResp(resp);
    return pollResp.getEnrichedTabletsList();
  }

  public void commitSync(Map<String, List<String>> topicNameToSubscriptionCommitIds)
      throws TException, IOException, StatementExecutionException {
    TPipeSubscribeResp resp =
        client.pipeSubscribe(
            PipeSubscribeCommitReq.toTPipeSubscribeReq(topicNameToSubscriptionCommitIds));
    RpcUtils.verifySuccess(resp.status);
  }
}
