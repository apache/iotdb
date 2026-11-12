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

package org.apache.iotdb.db.queryengine.execution.fragment;

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.common.SessionInfo;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeInternalRPCServiceImpl;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.planner.plan.FragmentInstance;
import org.apache.iotdb.db.queryengine.plan.planner.plan.PlanFragment;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.source.SeriesScanNode;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.mpp.rpc.thrift.TCancelQueryReq;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstance;
import org.apache.iotdb.mpp.rpc.thrift.TQueryAdmissionToken;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceReq;
import org.apache.iotdb.mpp.rpc.thrift.TSendFragmentInstanceResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.BeforeClass;
import org.junit.Test;

import java.time.ZoneId;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class QueryAttemptRpcTest {
  private final DataNodeInternalRPCServiceImpl service =
      new DataNodeInternalRPCServiceImpl(mock(DataNodeContext.class));

  @BeforeClass
  public static void configureNodeId() {
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
  }

  @Test
  public void serializedDispatchHonorsAnAttemptClosedByRpc() throws Exception {
    QueryId queryId = new QueryId("rpc_closed_attempt");
    TQueryAdmissionToken token = service.fetchQueryAdmissionToken(30000);
    assertTrue(
        service
            .cancelQuery(
                new TCancelQueryReq(queryId.getId(), Collections.emptyList(), true)
                    .setQueryAttemptId(0)
                    .setAdmissionToken(token))
            .isCancelled());
    TSendFragmentInstanceResp response =
        service.sendFragmentInstance(request(instance(queryId, 0), 0, token));
    assertFalse(response.isAccepted());
    assertEquals(TSStatusCode.QUERY_WAS_KILLED.getStatusCode(), response.getStatus().getCode());
  }

  @Test
  public void serializedExpiredAdmissionReturnsRetryableDispatchError() throws Exception {
    TQueryAdmissionToken expired =
        service
            .fetchQueryAdmissionToken(30000)
            .setDeadlineNanos(System.nanoTime() - TimeUnit.SECONDS.toNanos(1));
    TSendFragmentInstanceResp response =
        service.sendFragmentInstance(
            request(instance(new QueryId("rpc_expired_attempt"), 0), 0, expired));
    assertFalse(response.isAccepted());
    assertEquals(TSStatusCode.DISPATCH_ERROR.getStatusCode(), response.getStatus().getCode());
  }

  @Test
  public void oldCancellationRpcDoesNotCloseRetry() throws Exception {
    QueryId queryId = new QueryId("rpc_retry_attempt");
    TQueryAdmissionToken token = service.fetchQueryAdmissionToken(30000);
    FragmentInstance retry = instance(queryId, 1);
    retry.setQueryAttempt(1, token);
    FragmentInstanceStateMachine state =
        new FragmentInstanceStateMachine(retry.getId(), Runnable::run);
    QueryAttemptRegistry registry = FragmentInstanceManager.getInstance().getQueryAttempts();
    try (QueryAttemptRegistry.Registration ignored = registry.register(retry, state, false)) {
      service.cancelQuery(
          new TCancelQueryReq(queryId.getId(), Collections.emptyList(), true)
              .setQueryAttemptId(0)
              .setAdmissionToken(token));
      assertEquals(FragmentInstanceState.RUNNING, state.getState());
      service.cancelQuery(
          new TCancelQueryReq(queryId.getId(), Collections.emptyList(), true)
              .setQueryAttemptId(1)
              .setAdmissionToken(token));
      assertEquals(FragmentInstanceState.CANCELLED, state.getState());
    }
  }

  private static FragmentInstance instance(QueryId queryId, int index) throws Exception {
    PlanFragmentId fragmentId = new PlanFragmentId(queryId, index);
    FragmentInstance instance =
        new FragmentInstance(
            new PlanFragment(
                fragmentId,
                new SeriesScanNode(
                    new PlanNodeId("scan"), new MeasurementPath("root.rpc.device.value"))),
            fragmentId.genFragmentInstanceId(),
            null,
            QueryType.READ,
            30000,
            new SessionInfo(0, "root", ZoneId.systemDefault()),
            false,
            false);
    instance.setDataNodeFINum(3);
    return instance;
  }

  private static TSendFragmentInstanceReq request(
      FragmentInstance instance, int attempt, TQueryAdmissionToken token) {
    return new TSendFragmentInstanceReq(new TFragmentInstance(instance.serializeToByteBuffer()))
        .setQueryAttemptId(attempt)
        .setAdmissionToken(token);
  }
}
