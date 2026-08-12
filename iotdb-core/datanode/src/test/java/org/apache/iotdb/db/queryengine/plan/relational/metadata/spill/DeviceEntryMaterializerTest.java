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
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DeviceEntryMaterializerTest {

  private Path queryDirectory;

  @Before
  public void setUp() throws Exception {
    queryDirectory = Files.createTempDirectory("device-entry-spill-test");
    IoTDBDescriptor.getInstance().getConfig().setQueryDir(queryDirectory.toString());
  }

  @After
  public void tearDown() throws Exception {
    DeviceEntrySpillManager.getInstance().clearStaleData();
    Files.deleteIfExists(queryDirectory.resolve("device-entry"));
    Files.deleteIfExists(queryDirectory);
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
      dataSet = materializer.finish();
    }

    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path rawDirectory = queryDirectory.resolve("device-entry/q-segment/scan-0/raw");
    List<Path> segments;
    try (java.util.stream.Stream<Path> stream = Files.list(rawDirectory)) {
      segments =
          stream.filter(path -> path.getFileName().toString().endsWith(".bin")).sorted().toList();
    }
    assertTrue(segments.size() > 1);
    assertTrue(Files.size(segments.get(0)) > 0);
    dataSet.close();
  }

  @Test
  public void testSpillFileUsesLengthPrefixedRecordsWithoutHeaderOrCrc() throws Exception {
    DeviceEntry entry = createEntries(1).get(0);
    DeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-format", new PlanNodeId("scan-0"), 1, true)) {
      materializer.append(entry);
      dataSet = materializer.finish();
    }

    Path segment =
        DeviceEntrySpillManager.getInstance().listSegments("q-format", "scan-0/raw").get(0);
    byte[] fileBytes = Files.readAllBytes(segment);
    byte[] payload = entry.serializeToBytes();
    assertEquals(Integer.BYTES + payload.length, fileBytes.length);
    assertEquals(payload.length, ByteBuffer.wrap(fileBytes).getInt());
    dataSet.close();
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
