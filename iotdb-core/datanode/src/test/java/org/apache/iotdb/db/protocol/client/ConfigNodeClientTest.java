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
import org.apache.iotdb.commons.client.property.ThriftClientProperty;
import org.apache.iotdb.confignode.rpc.thrift.IConfigNodeRPCService;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataPartitionTableResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

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

  private ConfigNodeClient newClient(
      IConfigNodeRPCService.Iface service, AtomicInteger connections, int allowedConnections)
      throws Exception {
    ConfigNodeClient client =
        new ConfigNodeClient(
            Collections.singletonList(new TEndPoint("127.0.0.1", 10710)),
            new ThriftClientProperty.Builder().build(),
            null) {
          @Override
          public void connect(TEndPoint endpoint, int timeoutMs) {
            assertTrue("Unexpected reconnect", connections.incrementAndGet() <= allowedConnections);
          }
        };
    Field field = ConfigNodeClient.class.getDeclaredField("client");
    field.setAccessible(true);
    field.set(client, service);
    return client;
  }
}
