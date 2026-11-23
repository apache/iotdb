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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class AbstractConfigNodeClientTest {

  private static final TEndPoint FIRST = new TEndPoint("127.0.0.1", 10710);
  private static final TEndPoint SECOND = new TEndPoint("127.0.0.1", 10720);
  private static final TEndPoint THIRD = new TEndPoint("127.0.0.1", 10730);

  @Test
  public void testRefreshStaleListAfterRedirectWithoutLeader() throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(SECOND, FIRST, THIRD));

    // The latest list has a different order. Continue after the node that just replied, rather
    // than restarting at a fixed position that would select FIRST again.
    executeResponses(
        client, Arrays.asList(FIRST, THIRD), Arrays.asList(redirectWithoutLeader(), success()));

    assertEquals(Arrays.asList(SECOND, FIRST, THIRD), client.configNodes);
    assertNull(client.configLeader);
  }

  @Test
  public void testRepeatedRedirectsContinueThroughLatestList() throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND, THIRD));

    // Refreshing the same list on every response must not reset the cursor and pin retries to
    // SECOND. Both non-leaders respond successfully at the transport layer.
    executeResponses(
        client,
        Arrays.asList(FIRST, SECOND, THIRD),
        Arrays.asList(redirectWithoutLeader(), redirectWithoutLeader(), success()));
  }

  @Test
  public void testResponseCandidateListCanReorderNodesAfterLeaderUpdate() throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    when(nodeInfo.getLatestConfigNodes()).thenReturn(Arrays.asList(FIRST, SECOND, THIRD));
    TestConfigNodeClient client = newClient(Collections.singletonList(FIRST), nodeInfo);
    List<TEndPoint> expectedNodes = Arrays.asList(FIRST, THIRD);
    List<TEndPoint> requestedNodes = new ArrayList<>();

    TSStatus result =
        client.executeRemoteCallWithRetry(
            () -> {
              int requestIndex = requestedNodes.size();
              assertTrue(requestIndex < expectedNodes.size());
              assertEquals(expectedNodes.get(requestIndex), client.configNode);
              requestedNodes.add(client.configNode);
              return requestIndex == 0 ? redirectWithoutLeader() : success();
            },
            status -> {
              if (!client.updateConfigNodeLeader(status)) {
                return true;
              }
              // Registration updates both lists, but the shared set and response list may have
              // different iteration orders. Continue after FIRST in the refreshed shared list.
              when(nodeInfo.getLatestConfigNodes()).thenReturn(Arrays.asList(SECOND, FIRST, THIRD));
              client.configNodes = Arrays.asList(FIRST, THIRD, SECOND);
              return false;
            });

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getCode());
    assertEquals(expectedNodes, requestedNodes);
  }

  @Test
  public void testExplicitRedirectTakesPriorityOverCandidateList() throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND));
    TSStatus redirect = redirectWithoutLeader().setRedirectNode(THIRD);

    executeResponses(client, Arrays.asList(FIRST, THIRD), Arrays.asList(redirect, success()));

    assertEquals(THIRD, client.configLeader);
  }

  @Test
  public void testSingleCandidateCanBeRetried() throws TException {
    TestConfigNodeClient client = newClient(Collections.singletonList(FIRST));

    executeResponses(
        client, Arrays.asList(FIRST, FIRST), Arrays.asList(redirectWithoutLeader(), success()));
  }

  @Test
  public void testRemovedCurrentNodeStartsAtFirstLatestCandidate() throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(SECOND, THIRD));

    executeResponses(
        client, Arrays.asList(FIRST, SECOND), Arrays.asList(redirectWithoutLeader(), success()));
  }

  @Test
  public void testEmptyLatestListPreservesKnownCandidates() throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    when(nodeInfo.getLatestConfigNodes()).thenReturn(Collections.emptyList());
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND), nodeInfo);

    executeResponses(
        client, Arrays.asList(FIRST, SECOND), Arrays.asList(redirectWithoutLeader(), success()));

    assertEquals(Arrays.asList(FIRST, SECOND), client.configNodes);
  }

  @Test
  public void testRpcFailureRefreshesCandidatesEvenWhenOldNodeAcceptsConnections()
      throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND, THIRD));
    List<TEndPoint> requestedNodes = new ArrayList<>();

    TSStatus result =
        client.executeRemoteCallWithRetry(
            () -> {
              requestedNodes.add(client.configNode);
              if (requestedNodes.size() == 1) {
                throw new TException("RPC failed while the old endpoint still accepts connections");
              }
              assertEquals(SECOND, client.configNode);
              return success();
            },
            status -> !client.updateConfigNodeLeader(status));

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getCode());
    assertEquals(Arrays.asList(FIRST, SECOND), requestedNodes);
  }

  @Test
  public void testCachedLeaderConnectionFailureRefreshesBeforeTryingOldCandidates()
      throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    when(nodeInfo.getLatestConfigNodes()).thenReturn(Arrays.asList(SECOND, THIRD));
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, THIRD), nodeInfo);
    doThrow(new TException("Cached leader is unreachable"))
        .when(client)
        .connect(eq(FIRST), anyInt());

    client.connectAndSync();

    assertEquals(SECOND, client.configNode);
    assertNull(client.configLeader);
    assertEquals(Arrays.asList(SECOND, THIRD), client.configNodes);
    verify(client, never()).connect(eq(THIRD), anyInt());
  }

  @Test
  public void testUnavailableCandidatesHaveBoundedConnectionAttempts() throws TException {
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND, THIRD));
    client.configLeader = null;
    clearInvocations(client);
    doThrow(new TException("Candidate is unreachable")).when(client).connect(any(), anyInt());

    try {
      client.connectAndSync();
      fail("All candidates are unreachable");
    } catch (TException expected) {
      // connectAndSync allows two passes, with at most one attempt per candidate in each pass.
      verify(client, times(2)).connect(eq(FIRST), anyInt());
      verify(client, times(2)).connect(eq(SECOND), anyInt());
      verify(client, times(2)).connect(eq(THIRD), anyInt());
      verify(client, times(6)).connect(any(), anyInt());
    }
  }

  @Test
  public void testSuccessfulRpcKeepsExistingConnection() throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    TestConfigNodeClient client = newClient(Collections.singletonList(FIRST), nodeInfo);
    clearInvocations(client);

    executeResponses(
        client, Collections.singletonList(FIRST), Collections.singletonList(success()));

    verify(client, never()).connect(any(), anyInt());
    verify(nodeInfo, never()).getLatestConfigNodes();
  }

  @Test
  public void testWarmingUpKeepsKnownLeaderWithoutRefreshingCandidates() throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    TestConfigNodeClient client = newClient(Arrays.asList(FIRST, SECOND), nodeInfo);

    executeResponses(
        client,
        Arrays.asList(FIRST, FIRST),
        Arrays.asList(
            new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()), success()));

    assertEquals(FIRST, client.configLeader);
    verify(nodeInfo, never()).getLatestConfigNodes();
    verify(client, never()).connect(eq(SECOND), anyInt());
  }

  private static TestConfigNodeClient newClient(List<TEndPoint> latestNodes) throws TException {
    AbstractConfigNodeInfo nodeInfo = mock(AbstractConfigNodeInfo.class);
    when(nodeInfo.getLatestConfigNodes()).thenReturn(latestNodes);
    return newClient(Collections.singletonList(FIRST), nodeInfo);
  }

  private static TestConfigNodeClient newClient(
      List<TEndPoint> nodes, AbstractConfigNodeInfo nodeInfo) {
    return mock(
        TestConfigNodeClient.class,
        withSettings().useConstructor(nodes, nodeInfo).defaultAnswer(CALLS_REAL_METHODS));
  }

  private static void executeResponses(
      TestConfigNodeClient client, List<TEndPoint> expectedNodes, List<TSStatus> responses)
      throws TException {
    List<TEndPoint> requestedNodes = new ArrayList<>();
    TSStatus result =
        client.executeRemoteCallWithRetry(
            () -> {
              int requestIndex = requestedNodes.size();
              assertTrue(requestIndex < expectedNodes.size());
              assertEquals(expectedNodes.get(requestIndex), client.configNode);
              requestedNodes.add(client.configNode);
              return responses.get(requestIndex);
            },
            status -> !client.updateConfigNodeLeader(status));

    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), result.getCode());
    assertEquals(expectedNodes, requestedNodes);
  }

  private static TSStatus redirectWithoutLeader() {
    return new TSStatus(TSStatusCode.REDIRECTION_RECOMMEND.getStatusCode());
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  abstract static class TestConfigNodeClient
      extends AbstractConfigNodeClient<TestConfigNodeClient> {

    private final AbstractConfigNodeInfo nodeInfo;

    TestConfigNodeClient(List<TEndPoint> nodes, AbstractConfigNodeInfo nodeInfo) throws TException {
      super(nodes, new ThriftClientProperty.Builder().build(), null);
      this.nodeInfo = nodeInfo;
    }

    @Override
    public void connect(TEndPoint endpoint, int timeoutMs) throws TException {
      // Model successful TCP connections while exercising the real selection and retry logic.
      configNode = endpoint;
    }

    @Override
    protected String getNodeTypeName() {
      return "DataNode";
    }

    @Override
    protected TEndPoint getAddressAndPort() {
      return new TEndPoint("127.0.0.1", 10740);
    }

    @Override
    protected AbstractConfigNodeInfo getConfigNodeInfo() {
      return nodeInfo;
    }
  }
}
