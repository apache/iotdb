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

import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.lock.LockManager;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory.ReleaseFlushMonitor;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MonitorTest {
  private ReleaseFlushMonitor releaseFlushMonitor;

  @Before
  public void setUp() {
    releaseFlushMonitor = ReleaseFlushMonitor.getInstance();
  }

  @After
  public void tearDown() throws IOException {
    releaseFlushMonitor.clear();
  }

  private void mockCachedMTreeStore(int size) {
    for (int i = 1; i <= size; i++) {
      CachedMTreeStore mockStore = Mockito.mock(CachedMTreeStore.class);
      int finalI = i;
      CachedSchemaRegionStatistics mockStatistics =
          Mockito.mock(CachedSchemaRegionStatistics.class);
      Mockito.when(mockStore.getRegionStatistics()).then(o -> mockStatistics);
      Mockito.when(mockStatistics.getSchemaRegionId()).then(o -> finalI);
      LockManager lockManager = new LockManager();
      Mockito.when(mockStore.getLockManager()).then(o -> lockManager);
      releaseFlushMonitor.registerCachedMTreeStore(mockStore);
    }
  }

  @Test
  public void testGetRegionsToFlush() {
    mockCachedMTreeStore(5);
    // free = 500
    setRecord(
        1, Arrays.asList(0L, 2L, 3L, 3000L, 4000L), Arrays.asList(100L, 2500L, 200L, 5000L, 6000L));
    // free = 2000
    setRecord(
        2, Arrays.asList(0L, 2L, 3L, 3000L, 4000L), Arrays.asList(100L, 1000L, 200L, 5500L, 6000L));
    // free =700
    setRecord(3, Arrays.asList(700L, 800L), Arrays.asList(900L, 6000L));
    // free =1700
    setRecord(4, Arrays.asList(700L, 800L, 2500L), Arrays.asList(1000L, 1500L, 5000L));
    // free =2100
    setRecord(5, Arrays.asList(0L, 2000L), Arrays.asList(1000L, 3900L));
    List<Integer> regions = releaseFlushMonitor.getRegionsToFlush(5000);
    Assert.assertEquals(3, regions.size());
    Assert.assertEquals(5, regions.get(0).intValue());
    Assert.assertEquals(2, regions.get(1).intValue());
    Assert.assertEquals(4, regions.get(2).intValue());
  }

  @Test
  public void testGetRegionsToFlush2() {
    mockCachedMTreeStore(2);
    setRecord(1, Arrays.asList(0L, 2000L), Arrays.asList(100L, 7000L));
    setRecord(2, Collections.singletonList(3000L), Collections.singletonList(3500L));
    List<Integer> regions = releaseFlushMonitor.getRegionsToFlush(7000);
    Assert.assertEquals(1, regions.size());
    Assert.assertEquals(2, regions.get(0).intValue());
  }

  private void setRecord(int regionId, List<Long> startTimes, List<Long> eneTimes) {
    releaseFlushMonitor.initRecordList(regionId);
    for (int i = 0; i < startTimes.size(); i++) {
      ReleaseFlushMonitor.RecordNode node = releaseFlushMonitor.recordTraverserTime(regionId);
      node.setStartTime(startTimes.get(i));
      node.setEndTime(eneTimes.get(i));
    }
  }
}
