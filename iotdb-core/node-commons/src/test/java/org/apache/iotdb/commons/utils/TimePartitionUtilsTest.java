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

package org.apache.iotdb.commons.utils;

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimePartitionUtilsTest {

  private static final long TEST_TIME_PARTITION_ORIGIN = 1000L;
  private static final long TEST_TIME_PARTITION_INTERVAL = 3600000L;

  @Before
  public void setUp() {
    CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(TEST_TIME_PARTITION_ORIGIN);
    CommonDescriptor.getInstance()
        .getConfig()
        .setTimePartitionInterval(TEST_TIME_PARTITION_INTERVAL);
  }

  @Test
  public void testGetTimePartitionSlot_StartOfInterval() {
    long testTime = TEST_TIME_PARTITION_ORIGIN;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testGetTimePartitionSlot_MiddleOfInterval() {
    long testTime = TEST_TIME_PARTITION_ORIGIN + (TEST_TIME_PARTITION_INTERVAL / 2);
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testGetTimePartitionSlot_EndOfInterval() {
    long testTime = TEST_TIME_PARTITION_ORIGIN + TEST_TIME_PARTITION_INTERVAL - 1;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testGetTimePartitionSlot_NegativeTime() {
    long testTime = TEST_TIME_PARTITION_ORIGIN - 1;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN - TEST_TIME_PARTITION_INTERVAL);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testGetTimePartitionSlot_NegativeBoundaryTime() {
    long testTime = TEST_TIME_PARTITION_ORIGIN - TEST_TIME_PARTITION_INTERVAL;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN - TEST_TIME_PARTITION_INTERVAL);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testOverflow() {
    long testTime = Long.MIN_VALUE;
    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    Assert.assertTrue(actualSlot.getStartTime() < 0);
    testTime += 1;
    long lowerBound = TimePartitionUtils.getTimePartitionLowerBound(testTime, "global");
    assertEquals(Long.MIN_VALUE, lowerBound);
    testTime = Long.MAX_VALUE;
    actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "global");
    Assert.assertTrue(actualSlot.getStartTime() > 0);
    long upperBound = TimePartitionUtils.getTimePartitionUpperBound(testTime, "global");
    assertEquals(Long.MAX_VALUE, upperBound);
  }

  @Test
  public void testDatabaseLevelTimePartition() {
    // Create a database schema with custom time partition settings
    TDatabaseSchema schema = new TDatabaseSchema();
    schema.setName("test.db");
    schema.setTimePartitionInterval(7200000L); // 2 hours
    schema.setTimePartitionOrigin(2000L);

    // Update database configuration
    TimePartitionUtils.updateDatabaseTimePartitionConfig("test.db", schema);

    // Test with custom database settings
    long testTime = 2000L;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(2000L);

    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "test.db");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());

    // Test with different time in the same partition
    testTime = 2000L + 3600000L; // 1 hour later
    actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "test.db");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());

    // Test with time in next partition
    testTime = 2000L + 7200000L; // 2 hours later
    expectedSlot.setStartTime(2000L + 7200000L);
    actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "test.db");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testDatabaseLevelTimePartitionFallbackToGlobal() {
    // Test with database that doesn't have custom settings
    long testTime = TEST_TIME_PARTITION_ORIGIN;
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN);

    TTimePartitionSlot actualSlot =
        TimePartitionUtils.getTimePartitionSlot(testTime, "nonexistent.db");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }

  @Test
  public void testRemoveDatabaseTimePartitionConfig() {
    // Create and update database configuration
    TDatabaseSchema schema = new TDatabaseSchema();
    schema.setName("test.db");
    schema.setTimePartitionInterval(7200000L);
    schema.setTimePartitionOrigin(2000L);
    TimePartitionUtils.updateDatabaseTimePartitionConfig("test.db", schema);

    // Verify custom configuration is used
    long testTime = 2000L;
    TTimePartitionSlot actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "test.db");
    assertEquals(2000L, actualSlot.getStartTime());

    // Remove database configuration
    TimePartitionUtils.removeDatabaseTimePartitionConfig("test.db");

    // Verify fallback to global settings
    TTimePartitionSlot expectedSlot = new TTimePartitionSlot();
    expectedSlot.setStartTime(TEST_TIME_PARTITION_ORIGIN);
    actualSlot = TimePartitionUtils.getTimePartitionSlot(testTime, "test.db");
    assertEquals(expectedSlot.getStartTime(), actualSlot.getStartTime());
  }
}
