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

import org.apache.iotdb.commons.client.util.IoTDBConnectorPortManager;

import org.apache.tsfile.utils.Pair;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

public class IoTDBConnectorPortManagerTest {

  private final IoTDBConnectorPortManager portManager = IoTDBConnectorPortManager.INSTANCE;

  @Test
  public void testAddIfPortAvailable() {
    portManager.resetPortManager();
    Set<Integer> set = new HashSet<>();
    addPort(set);
    Assert.assertEquals(generateExpectedRanges(set), portManager.getOccupiedPorts());
  }

  @Test
  public void releasePortIfUsed() {
    portManager.resetPortManager();
    Set<Integer> set = new HashSet<>();
    addPort(set);
    Random random = new Random();
    while (set.size() > 200) {
      int port = 1024 + random.nextInt(65535 - 1024);
      set.remove(port);
      portManager.releaseUsedPort(port);
    }
    Assert.assertEquals(generateExpectedRanges(set), portManager.getOccupiedPorts());
  }

  @Test
  public void testAvailablePortIterator() {
    portManager.resetPortManager();
    Set<Integer> set = new HashSet<>();
    addPort(set);
    testPortRange(1024, 65535, new HashSet<>(), set);
    Random r = new Random();
    Set<Integer> cp = new HashSet<>();
    for (int i = 0; i < 1; i++) {
      IoTDBConnectorPortManager.AvailablePortIterator iterator =
          portManager.createAvailablePortIterator(1024, 65535, new ArrayList<>());
      while (iterator.hasNext()) {
        int port = iterator.next();
        if (r.nextBoolean()) {
          iterator.updateOccupiedPort();
          set.add(port);
          break;
        }
      }
      testPortRange(1024, 6553, cp, set);
      int randomPort = 1024 + r.nextInt(65535 - 1024 - 200);
      cp.clear();
      for (int j = 0; j < 40; j++) {
        cp.add(1024 + r.nextInt(65535 - 1024));
      }
      testPortRange(randomPort, randomPort + 200, cp, set);
    }
  }

  private void testPortRange(int port, int maxPort, Set<Integer> cp, Set<Integer> set) {
    int start = port;
    int end = maxPort;
    List<Integer> candidatePorts = new ArrayList<>(cp);
    if (!candidatePorts.isEmpty()) {
      candidatePorts.sort(Integer::compare);
      start = Math.min(port, candidatePorts.get(0));
      end = Math.max(maxPort, candidatePorts.get(candidatePorts.size() - 1));
    }
    IoTDBConnectorPortManager.AvailablePortIterator iterator =
        portManager.createAvailablePortIterator(port, maxPort, candidatePorts);
    for (int i = start; i <= end; i++) {
      if (set.contains(i) || ((i < port || i > maxPort) && !cp.contains(i))) {
        continue;
      }
      Assert.assertTrue(iterator.hasNext());
      Assert.assertEquals(i, iterator.next().intValue());
    }
    Assert.assertFalse(iterator.hasNext());
  }

  private void addPort(final Set<Integer> set) {
    set.add(-1);
    set.add(65536);
    final Random random = new Random();
    while (set.size() < 300) {
      final int port = 1024 + random.nextInt(65535 - 1024);
      set.add(port);
      portManager.addPortIfAvailable(port);
    }
    for (int i = 0; i <= 100; i++) {
      int port = i + 1024;
      set.add(port);
      portManager.addPortIfAvailable(port);
      port = 65535 - i;
      if (!set.contains(port)) {
        set.add(port);
        set.add(port);
      }
      portManager.addPortIfAvailable(port);
    }
  }

  private List<Pair<Integer, Integer>> generateExpectedRanges(final Set<Integer> set) {
    List<Integer> ports = new ArrayList<>(set);
    ports.sort(Integer::compare);
    int start = ports.get(0);
    int end = start;
    List<Pair<Integer, Integer>> data = new LinkedList<>();
    for (int i = 1; i < ports.size(); i++) {
      if (ports.get(i) == end + 1) {
        end = ports.get(i);
      } else {
        data.add(new Pair<>(start, end));
        start = ports.get(i);
        end = start;
      }
    }
    data.add(new Pair<>(start, end));
    return data;
  }
}
