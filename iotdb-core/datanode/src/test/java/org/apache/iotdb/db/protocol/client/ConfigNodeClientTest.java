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

package org.apache.iotdb.db.protocol.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.AbstractConfigNodeClient;
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.confignode.rpc.thrift.TRegionRouteMapResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ConfigNodeClientTest {

  @Test
  public void testInvalidFrameDoesNotReconnectOrHideOriginalFailure() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    TDataPartitionReq request = new TDataPartitionReq();
    TTransportException failure =
        new TTransportException(TTransportException.CORRUPTED_DATA, "Frame size exceeded");
    when(service.getDataPartitionTable(request)).thenThrow(failure);
    AtomicInteger connections = new AtomicInteger();
    ConfigNodeClient client = newClient(service, connections, 1);

    assertSame(
        failure,
        assertThrows(TTransportException.class, () -> client.getDataPartitionTable(request)));
    assertEquals(1, connections.get());
    verify(service).getDataPartitionTable(request);
  }

  @Test
  public void testTransientTransportFailureStillRetries() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    TDataPartitionReq request = new TDataPartitionReq();
    TDataPartitionTableResp response =
        new TDataPartitionTableResp()
            .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(service.getDataPartitionTable(request))
        .thenThrow(new TTransportException(TTransportException.TIMED_OUT, "socket timeout"))
        .thenReturn(response);
    AtomicInteger connections = new AtomicInteger();
    ConfigNodeClient client = newClient(service, connections, 2);

    assertSame(response, client.getDataPartitionTable(request));
    assertEquals(2, connections.get());
    verify(service, times(2)).getDataPartitionTable(request);
  }

  @Test
  public void testWarmingUpRetriesExhaustWithStatusDetails() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    final String warmingMessage = "Region route map is not ready.";
    TRegionRouteMapResp response =
        new TRegionRouteMapResp()
            .setStatus(
                new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode())
                    .setMessage(warmingMessage));
    when(service.getLatestRegionRouteMap()).thenReturn(response);
    AtomicInteger connections = new AtomicInteger();
    AtomicInteger waits = new AtomicInteger();
    ConfigNodeClient client = newClient(service, connections, 1, nanos -> waits.incrementAndGet());

    TException failure = assertThrows(TException.class, client::getLatestRegionRouteMap);

    assertTrue(
        failure
            .getMessage()
            .contains(String.valueOf(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode())));
    assertTrue(failure.getMessage().contains(warmingMessage));
    verify(service, times(31)).getLatestRegionRouteMap();
    assertEquals(30, waits.get());
    assertEquals(1, connections.get());
  }

  @Test
  public void testWarmingUpRetriesBeyondGenericRetryLimit() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    TRegionRouteMapResp warmingResponse =
        new TRegionRouteMapResp()
            .setStatus(new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()));
    TRegionRouteMapResp successResponse =
        new TRegionRouteMapResp()
            .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    AtomicInteger attempts = new AtomicInteger();
    when(service.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> attempts.incrementAndGet() <= 15 ? warmingResponse : successResponse);
    AtomicInteger waits = new AtomicInteger();
    ConfigNodeClient client =
        newClient(service, new AtomicInteger(), 1, nanos -> waits.incrementAndGet());

    assertSame(successResponse, client.getLatestRegionRouteMap());
    verify(service, times(16)).getLatestRegionRouteMap();
    assertEquals(15, waits.get());
  }

  @Test
  public void testWarmingUpStopsWhenRetryBudgetExpires() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    AtomicLong nanos = new AtomicLong();
    final long deadline = TimeUnit.SECONDS.toNanos(60);
    final TSStatus warming =
        new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode());
    when(service.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> {
              nanos.set(deadline - 1);
              return new TRegionRouteMapResp(warming);
            });
    ConfigNodeClient client =
        newClient(
            service,
            new AtomicInteger(),
            1,
            pause -> {
              assertEquals(1, pause);
              nanos.addAndGet(pause + 1);
            });

    TException failure =
        assertThrows(TException.class, () -> client.getLatestRegionRouteMap(nanos::get));

    assertTrue(failure instanceof TTransportException);
    assertEquals(TTransportException.TIMED_OUT, ((TTransportException) failure).getType());
    assertEquals(ConfigNodeClient.MSG_RECONNECTION_FAIL, failure.getMessage());
    verify(service).getLatestRegionRouteMap();
  }

  @Test
  public void testLateWarmingUpResponseExceedsRetryDeadline() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    AtomicLong nanos = new AtomicLong();
    final TRegionRouteMapResp warmingResponse =
        new TRegionRouteMapResp(
            new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode()));
    when(service.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> {
              nanos.set(TimeUnit.SECONDS.toNanos(60) + 1);
              return warmingResponse;
            });
    AtomicInteger waits = new AtomicInteger();
    ConfigNodeClient client =
        newClient(service, new AtomicInteger(), 1, pause -> waits.incrementAndGet());

    final TTransportException failure =
        assertThrows(TTransportException.class, () -> client.getLatestRegionRouteMap(nanos::get));

    assertEquals(TTransportException.TIMED_OUT, failure.getType());
    assertEquals(ConfigNodeClient.MSG_RECONNECTION_FAIL, failure.getMessage());
    assertEquals(0, waits.get());
    verify(service).getLatestRegionRouteMap();
  }

  @Test
  public void testLateSuccessfulRouteResponseExceedsRetryDeadline() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    AtomicLong nanos = new AtomicLong();
    final TRegionRouteMapResp success =
        new TRegionRouteMapResp()
            .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    when(service.getLatestRegionRouteMap())
        .thenAnswer(
            invocation -> {
              nanos.set(TimeUnit.SECONDS.toNanos(60) + 1);
              return success;
            });
    ConfigNodeClient client = newClient(service, new AtomicInteger(), 1);

    assertThrows(TTransportException.class, () -> client.getLatestRegionRouteMap(nanos::get));
    verify(service).getLatestRegionRouteMap();
  }

  @Test
  public void testWarmingUpDoesNotMaskTerminalTransportFailure() throws Exception {
    IConfigNodeRPCService.Iface service = mock(IConfigNodeRPCService.Iface.class);
    TTransportException invalidFrame = new TTransportException(TTransportException.CORRUPTED_DATA);
    when(service.getLatestRegionRouteMap())
        .thenReturn(
            new TRegionRouteMapResp(
                new TSStatus(TSStatusCode.CONFIG_NODE_LEADER_WARMING_UP.getStatusCode())))
        .thenThrow(invalidFrame);
    ConfigNodeClient client = newClient(service, new AtomicInteger(), 1);

    assertSame(
        invalidFrame, assertThrows(TTransportException.class, client::getLatestRegionRouteMap));
    verify(service, times(2)).getLatestRegionRouteMap();
  }

  private ConfigNodeClient newClient(
      IConfigNodeRPCService.Iface service, AtomicInteger connections, int allowedConnections)
      throws Exception {
    return newClient(service, connections, allowedConnections, nanos -> {});
  }

  private ConfigNodeClient newClient(
      IConfigNodeRPCService.Iface service,
      AtomicInteger connections,
      int allowedConnections,
      LongConsumer routeWait)
      throws Exception {
    ConfigNodeClient client =
        new ConfigNodeClient(
            Collections.singletonList(new TEndPoint("127.0.0.1", 10710)),
            new ThriftClientProperty.Builder().build(),
            null) {
          @Override
          public void syncLatestConfigNodeList() {
            // Keep the test candidates independent of the process-wide ConfigNodeInfo.
          }

          @Override
          protected void waitBeforeRetry() {}

          @Override
          void waitBeforeRouteMapRetry(long nanos) {
            routeWait.accept(nanos);
          }

          @Override
          public void connect(TEndPoint endpoint, int timeoutMs) {
            assertTrue("Unexpected reconnect", connections.incrementAndGet() <= allowedConnections);
          }
        };
    Field field = AbstractConfigNodeClient.class.getDeclaredField("client");
    field.setAccessible(true);
    field.set(client, service);
    return client;
  }
}
