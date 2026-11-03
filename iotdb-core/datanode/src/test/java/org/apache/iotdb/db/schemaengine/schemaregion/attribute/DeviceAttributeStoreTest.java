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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute;

import org.apache.iotdb.commons.schema.MemUsageUtil;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Constants;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeviceAttributeStoreTest {

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAttributeMemoryUsesValueSizesAcrossUpdates() {
    final MemSchemaRegionStatistics statistics = newStatistics();
    final DeviceAttributeStore store = new DeviceAttributeStore(statistics);
    final Binary first = new Binary("value", StandardCharsets.UTF_8);
    final Binary second = new Binary("a longer attribute value", StandardCharsets.UTF_8);
    final int pointer =
        store.createAttribute(Collections.singletonList("a"), new Object[] {first}, "t");
    final long emptySlotMemory =
        statistics.getRegionMemoryUsage() - MemUsageUtil.computeKVMemUsageInMap("a", first);
    assertEquals(first.ramBytesUsed(), statistics.getTableAttributeMemory("t"));

    store.alterAttribute(pointer, Collections.singletonList("a"), new Object[] {second}, "t");
    assertEquals(second.ramBytesUsed(), statistics.getTableAttributeMemory("t"));
    assertEquals(
        emptySlotMemory + MemUsageUtil.computeKVMemUsageInMap("a", second),
        statistics.getRegionMemoryUsage());

    store.alterAttribute(pointer, Arrays.asList("a", "b"), new Object[] {null, first}, "t");
    assertEquals(first.ramBytesUsed(), statistics.getTableAttributeMemory("t"));
    assertEquals(
        emptySlotMemory + MemUsageUtil.computeKVMemUsageInMap("b", first),
        statistics.getRegionMemoryUsage());

    store.alterAttribute(
        pointer, Collections.singletonList("b"), new Object[] {Constants.NONE}, "t");
    assertEquals(first.ramBytesUsed(), statistics.getTableAttributeMemory("t"));
    store.alterAttribute(pointer, Collections.singletonList("b"), new Object[] {null}, "t");
    assertEquals(0, statistics.getTableAttributeMemory("t"));
    assertEquals(emptySlotMemory, statistics.getRegionMemoryUsage());

    store.alterAttribute(pointer, Collections.singletonList("c"), new Object[] {second}, "t");
    store.removeAttribute(pointer, "c", "t");
    assertEquals(0, statistics.getTableAttributeMemory("t"));
    assertEquals(emptySlotMemory, statistics.getRegionMemoryUsage());

    store.alterAttribute(pointer, Collections.singletonList("c"), new Object[] {first}, "t");
    store.removeAttribute(pointer, "t");
    assertEquals(0, statistics.getTableAttributeMemory("t"));
    assertNull(store.getAttributes(pointer));
    final long releasedMemory = statistics.getRegionMemoryUsage();
    store.removeAttribute(pointer, "t");
    assertEquals(releasedMemory, statistics.getRegionMemoryUsage());
  }

  @Test
  public void testRepeatedCreateDeleteReusesSlotsAndBoundsSnapshot() throws Exception {
    final MemSchemaRegionStatistics statistics = newStatistics();
    final DeviceAttributeStore store = new DeviceAttributeStore(statistics);
    for (int i = 0; i < 10000; i++) {
      final int pointer = store.createAttribute(Collections.emptyList(), new Object[0], "t");
      assertEquals(0, pointer);
      store.removeAttribute(pointer, "t");
    }
    // One retained list reference and one bitmap word, independent of the number of deletions.
    assertEquals(
        RamUsageEstimator.NUM_BYTES_OBJECT_REF + Long.BYTES, statistics.getRegionMemoryUsage());
    final File snapshotDir = temporaryFolder.newFolder();
    assertTrue(store.createSnapshot(snapshotDir));
    assertEquals(2L * Integer.BYTES, snapshotFile(snapshotDir).length());

    final MemSchemaRegionStatistics recoveredStatistics = newStatistics();
    final DeviceAttributeStore recovered = new DeviceAttributeStore(recoveredStatistics);
    recovered.loadFromSnapshot(snapshotDir);
    assertEquals(statistics.getRegionMemoryUsage(), recoveredStatistics.getRegionMemoryUsage());
    assertEquals(0, recovered.createAttribute(Collections.emptyList(), new Object[0], "other"));
    assertEquals(0, recoveredStatistics.getTableAttributeMemory("other"));
  }

  @Test
  public void testSnapshotRebuildsFreeSlotsAndPreservesLivePointers() throws Exception {
    final DeviceAttributeStore store = new DeviceAttributeStore(null);
    final Binary value = new Binary("live", StandardCharsets.UTF_8);
    for (int i = 0; i < 130; i++) {
      assertEquals(
          i, store.createAttribute(Collections.singletonList("a"), new Object[] {value}, "t"));
    }
    store.removeAttribute(1, "t");
    store.removeAttribute(64, "t");
    store.removeAttribute(129, "t");
    final File snapshotDir = temporaryFolder.newFolder();
    assertTrue(store.createSnapshot(snapshotDir));
    try (final InputStream input = Files.newInputStream(snapshotFile(snapshotDir).toPath())) {
      assertEquals(130, ReadWriteIOUtils.readInt(input));
    }

    final DeviceAttributeStore recovered = new DeviceAttributeStore(null);
    recovered.loadFromSnapshot(snapshotDir);
    for (int i = 0; i < 130; i++) {
      if (i == 1 || i == 64 || i == 129) {
        assertNull(recovered.getAttributes(i));
      } else {
        assertEquals(value, recovered.getAttributes(i, "a"));
      }
    }
    assertEquals(1, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
    assertEquals(64, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
    recovered.removeAttribute(1, "t");
    assertEquals(1, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
    assertEquals(129, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
    assertEquals(130, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
    recovered.clear();
    assertEquals(0, recovered.createAttribute(Collections.emptyList(), new Object[0], "t"));
  }

  private static File snapshotFile(final File snapshotDir) {
    return new File(snapshotDir, SchemaConstant.DEVICE_ATTRIBUTE_SNAPSHOT);
  }

  private static MemSchemaRegionStatistics newStatistics() {
    return new MemSchemaRegionStatistics(0, new MemSchemaEngineStatistics());
  }

  @Test
  public void testCreateAttributeIgnoresNamesWithoutValues() {
    final DeviceAttributeStore store = new DeviceAttributeStore(null);
    final Binary expectedValue = new Binary("v1", StandardCharsets.UTF_8);

    final int pointer =
        store.createAttribute(
            Arrays.asList("attr1", "attr2"), new Object[] {expectedValue}, "table1");

    final Map<String, Binary> attributes = store.getAttributes(pointer);
    assertEquals(1, attributes.size());
    assertEquals(expectedValue, attributes.get("attr1"));
    assertFalse(attributes.containsKey("attr2"));
  }

  @Test
  public void testCreateAttributeSkipsNoneValue() {
    final DeviceAttributeStore store = new DeviceAttributeStore(null);
    final Binary expectedValue = new Binary("v2", StandardCharsets.UTF_8);

    final int pointer =
        store.createAttribute(
            Arrays.asList("attr1", "attr2"),
            new Object[] {Constants.NONE, expectedValue},
            "table1");

    final Map<String, Binary> attributes = store.getAttributes(pointer);
    assertEquals(1, attributes.size());
    assertFalse(attributes.containsKey("attr1"));
    assertTrue(attributes.containsKey("attr2"));
    assertEquals(expectedValue, attributes.get("attr2"));
  }
}
