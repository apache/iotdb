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

  IoTDBConnectorPortManager portManager = IoTDBConnectorPortManager.INSTANCE;

  @Test
  public void testAddIfPortAvailable() {
    portManager.resetPortManager();
    Set<Integer> ports = new HashSet<>();
    portManager.resetPortManager();
    Set<Integer> set = new HashSet<>();
    addPort(set);
    Assert.assertEquals(greEX(set), portManager.getOccupiedPorts());
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
    Assert.assertEquals(greEX(set), portManager.getOccupiedPorts());
  }

  private void addPort(Set<Integer> set) {
    set.add(1023);
    set.add(65536);
    Random random = new Random();

    while (set.size() < 300) {
      int port = 1024 + random.nextInt(65535 - 1024);
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

  private List<Pair<Integer, Integer>> greEX(Set<Integer> set) {
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
