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

package org.apache.iotdb.commons.pipe.receiver.runtime;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PipeReceiverRuntimeRegistryTest {

  private final PipeReceiverRuntimeRegistry registry = PipeReceiverRuntimeRegistry.getInstance();

  @Before
  public void setUp() {
    registry.clear();
  }

  @After
  public void tearDown() {
    registry.clear();
  }

  @Test
  public void testAggregateAndSortSnapshots() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);
    registry.markTransfer("data-1", 200);
    registry.markRequest("data-1");
    registry.registerOrUpdateSession(
        "data-2",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        2,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9002,
        "root",
        "cluster-b",
        "pipe-b",
        2,
        150);
    registry.registerOrUpdateSession(
        "config-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_AIR_GAP,
        "127.0.0.2",
        9003,
        "root",
        PipeReceiverRuntimeRegistry.UNKNOWN,
        null,
        Long.MIN_VALUE,
        300);

    final List<PipeReceiverRuntimeSnapshot> snapshots = registry.snapshot();

    assertEquals(2, snapshots.size());
    assertEquals(
        PipeReceiverRuntimeRegistry.NODE_TYPE_CONFIG_NODE, snapshots.get(0).getReceiverNodeType());
    assertEquals(
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE, snapshots.get(1).getReceiverNodeType());

    final PipeReceiverRuntimeSnapshot dataSnapshot = snapshots.get(1);
    assertEquals(2, dataSnapshot.getConnectionCount());
    assertEquals("9001,9002", dataSnapshot.getSenderPorts());
    assertEquals(2, dataSnapshot.getPipeCount());
    assertTrue(dataSnapshot.getPipeIds().contains("pipe-a@"));
    assertTrue(dataSnapshot.getPipeIds().contains("pipe-b@"));
    assertEquals("cluster-a;cluster-b", dataSnapshot.getSenderClusterId());
    assertEquals(150, dataSnapshot.getLastHandshakeTime());
    assertEquals(200, dataSnapshot.getLastTransferTime());
    assertEquals(4, dataSnapshot.getRequestNum());
  }

  @Test
  public void testDeregisterSession() {
    registry.registerOrUpdateSession(
        "data-1",
        PipeReceiverRuntimeRegistry.NODE_TYPE_DATA_NODE,
        1,
        PipeReceiverRuntimeRegistry.PROTOCOL_THRIFT,
        "127.0.0.1",
        9001,
        "root",
        "cluster-a",
        "pipe-a",
        1,
        100);

    assertEquals(1, registry.snapshot().size());

    registry.deregister("data-1");

    assertTrue(registry.snapshot().isEmpty());
  }
}
