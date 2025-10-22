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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.session.endpointselector.RandomSelectionStrategy;
import org.apache.iotdb.session.endpointselector.SequentialSelectionStrategy;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class EndpointSelectionStrategyTest {

  private List<TEndPoint> endpoints;
  private Set<TEndPoint> triedEndpoints;
  private RandomSelectionStrategy randomStrategy;
  private SequentialSelectionStrategy sequentialStrategy;

  @Before
  public void setUp() {
    endpoints = new ArrayList<>();
    addEndpoint(endpoints, "node1", 6667);
    addEndpoint(endpoints, "node2", 6667);
    addEndpoint(endpoints, "node3", 6667);
    addEndpoint(endpoints, "node4", 6667);

    triedEndpoints = new HashSet<>();
    randomStrategy = new RandomSelectionStrategy();
    sequentialStrategy = new SequentialSelectionStrategy();
  }

  private void addEndpoint(List<TEndPoint> list, String ip, int port) {
    TEndPoint endpoint = new TEndPoint();
    endpoint.setIp(ip);
    endpoint.setPort(port);
    list.add(endpoint);
  }

  private TEndPoint createEndpoint(String ip, int port) {
    TEndPoint endpoint = new TEndPoint();
    endpoint.setIp(ip);
    endpoint.setPort(port);
    return endpoint;
  }

  // RandomSelectionStrategy
  @Test
  public void testRandomSelectWithNoTriedEndpoints() {
    TEndPoint selected = randomStrategy.selectNext(endpoints, triedEndpoints);
    assertNotNull(selected);
    assertTrue(endpoints.contains(selected));
  }

  @Test
  public void testRandomSelectWithSomeTriedEndpoints() {
    triedEndpoints.add(createEndpoint("node1", 6667));
    triedEndpoints.add(createEndpoint("node2", 6667));

    TEndPoint selected = randomStrategy.selectNext(endpoints, triedEndpoints);
    assertNotNull(selected);
    assertFalse(triedEndpoints.contains(selected));
    assertTrue(selected.getIp().equals("node3") || selected.getIp().equals("node4"));
  }

  @Test
  public void testRandomSelectWithAllTriedEndpoints() {
    triedEndpoints.addAll(endpoints);
    assertNull(randomStrategy.selectNext(endpoints, triedEndpoints));
  }

  @Test
  public void testRandomSelectWithEmptyOrNullEndpoints() {
    assertNull(randomStrategy.selectNext(new ArrayList<>(), triedEndpoints));
    assertNull(randomStrategy.selectNext(null, triedEndpoints));
  }

  @Test
  public void testRandomStrategyName() {
    assertEquals("random", randomStrategy.getName());
  }

  // SequentialSelectionStrategy
  @Test
  public void testSequentialSelectInOrder() {
    assertEquals("node1", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node2", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node3", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node4", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node1", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
  }

  @Test
  public void testSequentialSelectWithTriedEndpoints() {
    triedEndpoints.add(createEndpoint("node1", 6667));
    triedEndpoints.add(createEndpoint("node3", 6667));

    assertEquals("node2", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node4", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
    assertEquals("node2", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
  }

  @Test
  public void testSequentialSelectReset() {
    sequentialStrategy.selectNext(endpoints, triedEndpoints); // node1
    sequentialStrategy.selectNext(endpoints, triedEndpoints); // node2
    sequentialStrategy.reset();
    assertEquals("node1", sequentialStrategy.selectNext(endpoints, triedEndpoints).getIp());
  }

  @Test
  public void testSequentialStrategyName() {
    assertEquals("sequential", sequentialStrategy.getName());
  }
}
