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
import org.apache.iotdb.commons.memory.MemoryManager;
import org.apache.iotdb.db.i18n.DataNodeMiscMessages;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DataNodeMemoryConfigTest {

  @Test
  public void testResolveSubscriptionQueryMemoryProportionsWhenEnabled() {
    final int[] defaultProportions = DataNodeMemoryConfig.resolveQueryMemoryProportions(null, true);
    assertArrayEquals(new int[] {1, 100, 200, 50, 200, 200, 200, 50, 250}, defaultProportions);
    assertEquals(
        0.2, (double) defaultProportions[8] / Arrays.stream(defaultProportions).sum(), 0.001);
    assertArrayEquals(
        new int[] {1, 100, 200, 50, 200, 200, 200, 50, 250},
        DataNodeMemoryConfig.resolveQueryMemoryProportions("1:100:200:50:200:200:200:50", true));
  }

  @Test
  public void testResolveSubscriptionQueryMemoryProportionsWhenDisabled() {
    assertArrayEquals(
        new int[] {1, 100, 200, 50, 200, 200, 200, 50, 0},
        DataNodeMemoryConfig.resolveQueryMemoryProportions(
            "1:100:200:50:200:200:200:50:1000", false));
    assertArrayEquals(
        new int[] {1, 100, 200, 50, 200, 200, 200, 50, 0},
        DataNodeMemoryConfig.resolveQueryMemoryProportions(null, false));
  }

  @Test
  public void testSubscriptionDoesNotReserveQueryMemoryWhenDisabledByDefault()
      throws ReflectiveOperationException {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("chunk_timeseriesmeta_free_memory_proportion", "0:0:0:0:1:0:0:0:1");
    final DataNodeMemoryConfig memoryConfig = initializeQueryEngineMemory(properties);

    assertEquals(0, memoryConfig.getSubscriptionMemoryManager().getTotalMemorySizeInBytes());
    assertEquals(1_000_000L, memoryConfig.getOperatorsMemoryManager().getTotalMemorySizeInBytes());
  }

  @Test
  public void testSubscriptionDoesNotReserveQueryMemoryWhenExplicitlyDisabled()
      throws ReflectiveOperationException {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("chunk_timeseriesmeta_free_memory_proportion", "0:0:0:0:1:0:0:0:1");
    properties.setProperty("subscription_enabled", Boolean.FALSE.toString());
    final DataNodeMemoryConfig memoryConfig = initializeQueryEngineMemory(properties);

    assertEquals(0, memoryConfig.getSubscriptionMemoryManager().getTotalMemorySizeInBytes());
    assertEquals(1_000_000L, memoryConfig.getOperatorsMemoryManager().getTotalMemorySizeInBytes());
  }

  @Test
  public void testSubscriptionReservesQueryMemoryWhenExplicitlyEnabled()
      throws ReflectiveOperationException {
    final TrimProperties properties = new TrimProperties();
    properties.setProperty("chunk_timeseriesmeta_free_memory_proportion", "0:0:0:0:1:0:0:0:1");
    properties.setProperty("subscription_enabled", Boolean.TRUE.toString());
    final DataNodeMemoryConfig memoryConfig = initializeQueryEngineMemory(properties);

    assertEquals(500_000L, memoryConfig.getSubscriptionMemoryManager().getTotalMemorySizeInBytes());
    assertEquals(500_000L, memoryConfig.getOperatorsMemoryManager().getTotalMemorySizeInBytes());
  }

  @Test
  public void testRejectInvalidSubscriptionQueryMemoryProportions() {
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DataNodeMemoryConfig.resolveQueryMemoryProportions(
                    "1:100:200:50:200:200:200", true));
    assertEquals(
        String.format(
            DataNodeMiscMessages
                .EXCEPTION_QUERY_MEMORY_PROPORTIONS_MUST_CONTAIN_8_OR_9_COLON_SEPARATED_VALUES_BUT_FOUND_ARG_03A03941,
            7),
        exception.getMessage());
  }

  @Test
  public void testRejectNegativeSubscriptionQueryMemoryProportion() {
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DataNodeMemoryConfig.resolveQueryMemoryProportions(
                    "1:100:200:50:200:200:200:50:-1", true));
    assertEquals(
        String.format(
            DataNodeMiscMessages
                .EXCEPTION_QUERY_MEMORY_PROPORTION_AT_POSITION_ARG_MUST_BE_NON_NEGATIVE_BUT_FOUND_ARG_DC69BC75,
            9,
            -1),
        exception.getMessage());
  }

  @Test
  public void testRejectNonPositiveSubscriptionQueryMemoryProportionSum() {
    final IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DataNodeMemoryConfig.resolveQueryMemoryProportions("0:0:0:0:0:0:0:0:0", true));
    assertEquals(
        String.format(
            DataNodeMiscMessages
                .EXCEPTION_THE_SUM_OF_QUERY_MEMORY_PROPORTIONS_MUST_BE_POSITIVE_BUT_WAS_ARG_407092B6,
            0),
        exception.getMessage());
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

  private DataNodeMemoryConfig initializeQueryEngineMemory(TrimProperties properties)
      throws ReflectiveOperationException {
    final DataNodeMemoryConfig memoryConfig = new DataNodeMemoryConfig();
    final Method initQueryEngineMemoryAllocate =
        DataNodeMemoryConfig.class.getDeclaredMethod(
            "initQueryEngineMemoryAllocate", MemoryManager.class, TrimProperties.class);
    initQueryEngineMemoryAllocate.setAccessible(true);
    initQueryEngineMemoryAllocate.invoke(memoryConfig, new MemoryManager(1_000_000L), properties);
    return memoryConfig;
  }
}
