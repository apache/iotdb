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

package org.apache.iotdb.db.metadata.mtree.schemafile;

import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush.Monitor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class MonitorTest {
  @Test
  public void testGetRegionsToFlush() {
    Monitor monitor = new Monitor();
    // free = 500
    setRecord(
        monitor,
        1,
        Arrays.asList(0L, 2L, 3L, 3000L, 4000L),
        Arrays.asList(100L, 2500L, 200L, 5000L, 6000L));
    // free = 2000
    setRecord(
        monitor,
        2,
        Arrays.asList(0L, 2L, 3L, 3000L, 4000L),
        Arrays.asList(100L, 1000L, 200L, 5500L, 6000L));
    // free =700
    setRecord(monitor, 3, Arrays.asList(700L, 800L), Arrays.asList(900L, 6000L));
    // free =1700
    setRecord(monitor, 4, Arrays.asList(700L, 800L, 2500L), Arrays.asList(1000L, 1500L, 5000L));
    // free =2100
    setRecord(monitor, 5, Arrays.asList(0L, 2000L), Arrays.asList(1000L, 3900L));
    List<Integer> regions = monitor.getRegionsToFlush(5000);
    Assert.assertEquals(3, regions.size());
    Assert.assertEquals(5, regions.get(0).intValue());
    Assert.assertEquals(2, regions.get(1).intValue());
    Assert.assertEquals(4, regions.get(2).intValue());
  }

  private void setRecord(
      Monitor monitor, int regionId, List<Long> startTimes, List<Long> eneTimes) {
    for (int i = 0; i < startTimes.size(); i++) {
      Monitor.RecordNode node = monitor.recordTraverserTime(regionId);
      node.setStartTime(startTimes.get(i));
      node.setEndTime(eneTimes.get(i));
    }
  }
}
