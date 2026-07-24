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

package org.apache.iotdb.db.conf;

import org.apache.iotdb.commons.conf.TrimProperties;
import org.apache.iotdb.commons.memory.MemoryConfig;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DataNodeMemoryConfigTest {

  @Test
  public void testResolveSubscriptionQueryMemoryProportions() {
    final int[] defaultProportions = DataNodeMemoryConfig.resolveQueryMemoryProportions(null, true);
    assertArrayEquals(new int[] {1, 100, 200, 50, 200, 200, 200, 50, 250}, defaultProportions);
    assertEquals(
        0.2, (double) defaultProportions[8] / Arrays.stream(defaultProportions).sum(), 0.001);
    assertArrayEquals(
        new int[] {1, 100, 200, 50, 200, 200, 200, 50, 250},
        DataNodeMemoryConfig.resolveQueryMemoryProportions("1:100:200:50:200:200:200:50", true));
    assertArrayEquals(
        new int[] {1, 100, 200, 50, 200, 200, 200, 50, 0},
        DataNodeMemoryConfig.resolveQueryMemoryProportions(
            "1:100:200:50:200:200:200:50:1000", false));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectInvalidSubscriptionQueryMemoryProportions() {
    DataNodeMemoryConfig.resolveQueryMemoryProportions("1:100:200:50:200:200:200", true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectNegativeSubscriptionQueryMemoryProportion() {
    DataNodeMemoryConfig.resolveQueryMemoryProportions("1:100:200:50:200:200:200:50:-1", true);
  }

  @Test
  public void testRpcMemoryControlIsActivatedOnlyExplicitly() {
    DataNodeMemoryConfig memoryConfig = IoTDBDescriptor.getInstance().getMemoryConfig();

    assertEquals(0, MemoryConfig.getInstance().getAutoResizingBufferMemoryTotalSizeInBytes());

    memoryConfig.activateAutoResizingBufferMemoryControl();

    assertTrue(MemoryConfig.getInstance().getAutoResizingBufferMemoryTotalSizeInBytes() > 0);
  }

  @Test
  public void testDefaultAutoResizingBufferMemorySize() {
    assertEquals(
        Runtime.getRuntime().maxMemory() / 20,
        DataNodeMemoryConfig.getDefaultAutoResizingBufferMemorySizeInBytes());
  }

  @Test
  public void testCalculateAutoResizingBufferMemorySizeWithDataNodeMemoryProportion() {
    TrimProperties properties = new TrimProperties();
    properties.setProperty("datanode_memory_proportion", "1:1:1:1:1:5");

    assertEquals(
        Runtime.getRuntime().maxMemory() / 4,
        DataNodeMemoryConfig.calculateAutoResizingBufferMemorySizeInBytes(properties));
  }

  @Test
  public void testCalculateAutoResizingBufferMemorySizeWithDeprecatedMemoryProportion() {
    TrimProperties properties = new TrimProperties();
    properties.setProperty("storage_query_schema_consensus_free_memory_proportion", "1:1:1:1:1:2");

    assertEquals(
        Runtime.getRuntime().maxMemory() / 7,
        DataNodeMemoryConfig.calculateAutoResizingBufferMemorySizeInBytes(properties));
  }
}
