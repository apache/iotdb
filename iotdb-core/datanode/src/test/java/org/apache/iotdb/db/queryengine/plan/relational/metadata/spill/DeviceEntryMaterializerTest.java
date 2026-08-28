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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class DeviceEntryMaterializerTest {

  private Path queryDirectory;
  private String originalSortTmpDir;

  @Before
  public void setUp() throws Exception {
    queryDirectory = Files.createTempDirectory("device-entry-spill-test");
    originalSortTmpDir = IoTDBDescriptor.getInstance().getConfig().getSortTmpDir();
    IoTDBDescriptor.getInstance().getConfig().setSortTmpDir(queryDirectory.toString());
  }

  @After
  public void tearDown() throws Exception {
    DeviceEntrySpillManager.getInstance().clearStaleData();
    Files.deleteIfExists(queryDirectory.resolve("device-entry"));
    Files.deleteIfExists(queryDirectory);
    IoTDBDescriptor.getInstance().getConfig().setSortTmpDir(originalSortTmpDir);
  }

  @Test
  public void testKeepSmallDataSetInline() throws Exception {
    List<DeviceEntry> expected = createEntries(3);
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-inline", new PlanNodeId("scan-0"), Long.MAX_VALUE, true)) {
      for (DeviceEntry entry : expected) {
        materializer.append(entry);
      }
      try (DeviceEntryDataSet dataSet = materializer.finish()) {
        assertFalse(dataSet.isSpilled());
        assertEquals(expected, dataSet.getInlineEntries());
      }
    }
  }

  @Test
  public void testSpillAndReadMultipleSegments() throws Exception {
    List<DeviceEntry> expected = createEntries(20);
    DeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-spill", new PlanNodeId("scan-0"), 128, true)) {
      for (DeviceEntry entry : expected) {
        materializer.append(entry);
      }
      materializer.forceSpill();
      dataSet = materializer.finish();
    }

    assertTrue(dataSet.isSpilled());
    assertEquals(expected.size(), dataSet.getEntryCount());
    List<DeviceEntry> actual = new ArrayList<>();
    try (DeviceEntryReader reader = dataSet.openReader()) {
      while (reader.hasNext()) {
        actual.add(reader.next());
      }
    }
    assertEquals(expected, actual);

    dataSet.close();
    assertFalse(Files.exists(queryDirectory.resolve("device-entry/q-spill/scan-0")));
  }

  @Test
  public void testControlledSegmentAccess() throws Exception {
    DeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-segment", new PlanNodeId("scan-0"), 128, true)) {
      for (DeviceEntry entry : createEntries(20)) {
        materializer.append(entry);
      }
      materializer.forceSpill();
      dataSet = materializer.finish();
    }

    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path rawDirectory = queryDirectory.resolve("device-entry/q-segment/scan-0/raw");
    List<Path> segments;
    try (java.util.stream.Stream<Path> stream = Files.list(rawDirectory)) {
      segments =
          stream
              .filter(path -> path.getFileName().toString().endsWith(".bin"))
              .sorted()
              .collect(Collectors.toList());
    }
    assertTrue(segments.size() > 1);
    assertTrue(Files.size(segments.get(0)) > 0);
    assertThrows(
        IllegalArgumentException.class, () -> manager.resolveSegment("q-segment", "../scan-0", 0));
    assertThrows(
        IllegalArgumentException.class, () -> manager.resolveSegment("q-segment", "scan-0", -1));
    assertThrows(
        NoSuchFileException.class,
        () -> manager.resolveSegment("q-segment", "scan-0", segments.size()));
    dataSet.close();
  }

  @Test
  public void testSpillFileUsesLengthPrefixedRecordsWithoutHeaderOrCrc() throws Exception {
    DeviceEntry entry = createEntries(1).get(0);
    DeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-format", new PlanNodeId("scan-0"), 1, true)) {
      materializer.append(entry);
      materializer.forceSpill();
      dataSet = materializer.finish();
    }

    Path segment = ((SpilledDeviceEntryDataSet) dataSet).getSegments().get(0);
    byte[] fileBytes = Files.readAllBytes(segment);
    byte[] payload = entry.serializeToBytes();
    assertEquals(Integer.BYTES + payload.length, fileBytes.length);
    assertEquals(payload.length, ByteBuffer.wrap(fileBytes).getInt());
    dataSet.close();
  }

  @Test
  public void testMemoryControllerSpillsLargestMaterializer() throws Exception {
    DeviceEntry first = createEntries(1).get(0);
    DeviceEntry second = createEntries(2).get(1);
    try (DeviceEntryMaterializer firstMaterializer =
            new DeviceEntryMaterializer("q-controller", new PlanNodeId("scan-0"), 128, false);
        DeviceEntryMaterializer secondMaterializer =
            new DeviceEntryMaterializer("q-controller", new PlanNodeId("scan-1"), 128, false)) {
      DeviceEntryMaterializationMemoryController controller =
          new DeviceEntryMaterializationMemoryController(
              first.ramBytesUsed() + second.ramBytesUsed() - 1);
      controller.append(firstMaterializer, first);
      controller.append(secondMaterializer, second);

      assertTrue(firstMaterializer.isSpilled() || secondMaterializer.isSpilled());
    }
  }

  @Test
  public void testMemoryControllerPreservesEntriesAfterInitialSpill() throws Exception {
    List<DeviceEntry> expected = createEntries(3);
    List<DeviceEntry> actual = new ArrayList<>();
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-controller-flush", new PlanNodeId("scan-0"), 1, false)) {
      DeviceEntryMaterializationMemoryController controller =
          new DeviceEntryMaterializationMemoryController(1);
      for (DeviceEntry entry : expected) {
        controller.append(materializer, entry);
      }
      try (DeviceEntryDataSet dataSet = materializer.finish();
          DeviceEntryReader reader = dataSet.openReader()) {
        while (reader.hasNext()) {
          actual.add(reader.next());
        }
      }
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testDirectSpillReleasesCurrentEntryReservation() throws Exception {
    DeviceEntry entry = createEntries(1).get(0);
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-release", new PlanNodeId("scan-0"), 1, false)) {
      assertEquals(entry.ramBytesUsed(), materializer.appendWithMemoryControl(entry));
    }
  }

  @Test
  public void testSpillPreservesEmptyBinaryAttribute() throws Exception {
    DeviceEntry expected =
        new AlignedDeviceEntry(
            IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table", "device"}),
            new Binary[] {new Binary(new byte[0]), null});
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-empty-binary", new PlanNodeId("scan-0"), 1, true)) {
      materializer.append(expected);
      materializer.forceSpill();
      try (DeviceEntryDataSet dataSet = materializer.finish();
          DeviceEntryReader reader = dataSet.openReader()) {
        assertEquals(expected, reader.next());
      }
    }
  }

  @Test
  public void testDistinctSpillDoesNotCollideDottedDeviceSegments() throws Exception {
    DeviceEntry first =
        new AlignedDeviceEntry(
            IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table", "a.b", "c"}),
            new Binary[0]);
    DeviceEntry second =
        new AlignedDeviceEntry(
            IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table", "a", "b.c"}),
            new Binary[0]);
    try (DeviceEntrySortedMaterializer materializer =
        new DeviceEntrySortedMaterializer(
            "q-dotted", new PlanNodeId("scan-0"), 1, DeviceEntry::compareDeviceId, true)) {
      materializer.appendWithMemoryControl(first);
      materializer.appendWithMemoryControl(second);
      try (DeviceEntryDataSet dataSet = materializer.finish()) {
        assertEquals(2, dataSet.getEntryCount());
      }
    }
  }

  @Test
  public void testSortedMaterializerMergesRunsInOrder() throws Exception {
    List<DeviceEntry> input = createEntries(40);
    input.sort(
        Comparator.comparing((DeviceEntry entry) -> entry.getDeviceID().toString()).reversed());
    List<DeviceEntry> actual = new ArrayList<>();
    try (DeviceEntrySortedMaterializer materializer =
        new DeviceEntrySortedMaterializer(
            "q-sorted",
            new PlanNodeId("scan-0"),
            128,
            Comparator.comparing((DeviceEntry entry) -> entry.getDeviceID().toString()))) {
      DeviceEntryMaterializationMemoryController controller =
          new DeviceEntryMaterializationMemoryController(128);
      for (DeviceEntry entry : input) {
        controller.append(materializer, entry);
      }
      try (DeviceEntryDataSet dataSet = materializer.finish();
          DeviceEntryReader reader = dataSet.openReader()) {
        while (reader.hasNext()) {
          actual.add(reader.next());
        }
      }
    }
    List<DeviceEntry> expected = new ArrayList<>(input);
    expected.sort(Comparator.comparing(entry -> entry.getDeviceID().toString()));
    assertEquals(expected, actual);
  }

  @Test
  public void testDistinctSortedMaterializerDeduplicatesAcrossRuns() throws Exception {
    List<DeviceEntry> expected = createEntries(20);
    List<DeviceEntry> input = new ArrayList<>(expected);
    input.addAll(expected);
    Comparator<DeviceEntry> comparator =
        Comparator.comparing(entry -> entry.getDeviceID().toString());
    try (DeviceEntrySortedMaterializer materializer =
        new DeviceEntrySortedMaterializer(
            "q-distinct", new PlanNodeId("scan-0"), 128, comparator, true)) {
      for (DeviceEntry entry : input) {
        materializer.appendWithMemoryControl(entry);
      }
      List<DeviceEntry> actual = new ArrayList<>();
      try (DeviceEntryDataSet dataSet = materializer.finish();
          DeviceEntryReader reader = dataSet.openReader()) {
        assertTrue(dataSet.isSpilled());
        assertEquals(expected.size(), dataSet.getEntryCount());
        while (reader.hasNext()) {
          actual.add(reader.next());
        }
      }
      expected.sort(comparator);
      assertEquals(expected, actual);
    }
  }

  @Test
  public void testDistinctSortedMaterializerRecordsRawDeviceEntryCount() throws Exception {
    MPPQueryContext queryContext = new MPPQueryContext(new QueryId("q_statistics"));
    List<DeviceEntry> entries = createEntries(3);
    entries.add(entries.get(0));
    queryContext.setStartTime(System.currentTimeMillis());
    queryContext.setTimeOut(Long.MAX_VALUE);
    try (DeviceEntrySortedMaterializer materializer =
        new DeviceEntrySortedMaterializer(
            "q_statistics",
            new PlanNodeId("scan-0"),
            Long.MAX_VALUE,
            Comparator.comparing(entry -> entry.getDeviceID().toString()),
            true,
            true,
            queryContext)) {
      for (DeviceEntry entry : entries) {
        materializer.append(entry);
      }
      try (DeviceEntryDataSet ignored = materializer.finish()) {
        assertEquals(3, ignored.getEntryCount());
      }
    }
    assertEquals(3, queryContext.getDeviceEntryCount());
  }

  @Test
  public void testDistributionMaterializerRecordsDiskIO() throws Exception {
    MPPQueryContext queryContext = new MPPQueryContext(new QueryId("q_distribution"));
    queryContext.setStartTime(System.currentTimeMillis());
    queryContext.setTimeOut(Long.MAX_VALUE);
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer(
            "q_distribution", new PlanNodeId("scan-0"), 128, false, queryContext)) {
      for (DeviceEntry entry : createEntries(20)) {
        materializer.appendWithMemoryControl(entry);
      }
      try (DeviceEntryDataSet ignored = materializer.finish()) {
        assertTrue(ignored.isSpilled());
      }
    }
    assertTrue(queryContext.getDiskIOTimeCostForDeviceEntryDuringDistributionPlan() > 0);
    assertEquals(0, queryContext.getDiskIOSizeForDeviceEntryDuringFetchSchema());
  }

  private static List<DeviceEntry> createEntries(int count) {
    List<DeviceEntry> entries = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      entries.add(
          new AlignedDeviceEntry(
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table", "device" + i}),
              new Binary[] {new Binary(("attribute" + i).getBytes(TSFileConfig.STRING_CHARSET))}));
    }
    return entries;
  }
}
