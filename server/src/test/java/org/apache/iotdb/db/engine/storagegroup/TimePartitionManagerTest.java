/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TimePartitionManagerTest {

  private final TimePartitionManager timePartitionManager = TimePartitionManager.getInstance();
  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();
  private long prevTimePartitionInfoMemoryThreshold;

  public TimePartitionManagerTest() throws QueryProcessException {}

  @Before
  public void setUp() throws IOException, WriteProcessException, MetadataException {
    prevTimePartitionInfoMemoryThreshold = CONFIG.getAllocateMemoryForTimePartitionInfo();
    timePartitionManager.setTimePartitionInfoMemoryThreshold(100L);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    timePartitionManager.setTimePartitionInfoMemoryThreshold(prevTimePartitionInfoMemoryThreshold);
    timePartitionManager.clear();
  }

  @Test
  public void testRegisterPartitionInfo() {
    TimePartitionInfo timePartitionInfo1 =
        new TimePartitionInfo(new DataRegionId(1), 0L, true, Long.MAX_VALUE, 0, true);
    timePartitionManager.registerTimePartitionInfo(timePartitionInfo1);

    assertEquals(
        timePartitionInfo1, timePartitionManager.getTimePartitionInfo(new DataRegionId(1), 0L));

    TimePartitionInfo timePartitionInfo2 =
        new TimePartitionInfo(new DataRegionId(1), 1L, true, Long.MAX_VALUE, 0, true);
    timePartitionManager.registerTimePartitionInfo(timePartitionInfo2);

    Assert.assertFalse(
        timePartitionManager.getTimePartitionInfo(new DataRegionId(1), 0L).isLatestPartition);
    Assert.assertTrue(
        timePartitionManager.getTimePartitionInfo(new DataRegionId(1), 1L).isLatestPartition);
  }

  @Test
  public void testUpdate() {
    TimePartitionInfo timePartitionInfo =
        new TimePartitionInfo(new DataRegionId(1), 0L, true, Long.MAX_VALUE, 0, true);
    timePartitionManager.registerTimePartitionInfo(timePartitionInfo);

    timePartitionManager.updateAfterFlushing(new DataRegionId(1), 0L, 100L, 100L, false);

    TimePartitionInfo timePartitionInfo1 =
        timePartitionManager.getTimePartitionInfo(new DataRegionId(1), 0L);

    assertTrue(timePartitionInfo1.isLatestPartition);
    assertEquals(timePartitionInfo1.lastSystemFlushTime, 100L);
    assertEquals(timePartitionInfo1.memSize, 100);
    assertFalse(timePartitionInfo1.isActive);

    timePartitionManager.updateAfterOpeningTsFileProcessor(new DataRegionId(1), 0L);
    TimePartitionInfo timePartitionInfo2 =
        timePartitionManager.getTimePartitionInfo(new DataRegionId(1), 0L);
    assertTrue(timePartitionInfo2.isActive);
  }

  @Test
  public void testMemoryControl() {
    for (int i = 0; i < 5; i++) {
      TimePartitionInfo timePartitionInfo =
          new TimePartitionInfo(new DataRegionId(i), 0L, true, Long.MAX_VALUE, 0, true);
      timePartitionManager.registerTimePartitionInfo(timePartitionInfo);
    }
    timePartitionManager.updateAfterFlushing(new DataRegionId(0), 0L, 100L, 20L, false);
    timePartitionManager.updateAfterFlushing(new DataRegionId(1), 0L, 101L, 20L, true);
    timePartitionManager.updateAfterFlushing(new DataRegionId(2), 0L, 102L, 20L, false);
    timePartitionManager.updateAfterFlushing(new DataRegionId(3), 0L, 103L, 20L, false);
    timePartitionManager.updateAfterFlushing(new DataRegionId(4), 0L, 104L, 20L, true);
    timePartitionManager.registerTimePartitionInfo(
        new TimePartitionInfo(new DataRegionId(0), 1L, true, Long.MAX_VALUE, 0, true));

    timePartitionManager.updateAfterFlushing(new DataRegionId(0), 1L, 105L, 20L, true);

    Assert.assertNull(timePartitionManager.getTimePartitionInfo(new DataRegionId(0), 0L));

    timePartitionManager.updateAfterFlushing(new DataRegionId(0), 1L, 106L, 40L, true);

    Assert.assertNull(timePartitionManager.getTimePartitionInfo(new DataRegionId(2), 0L));

    timePartitionManager.updateAfterFlushing(new DataRegionId(0), 1L, 107L, 60L, true);

    Assert.assertNull(timePartitionManager.getTimePartitionInfo(new DataRegionId(3), 0L));
  }
}
