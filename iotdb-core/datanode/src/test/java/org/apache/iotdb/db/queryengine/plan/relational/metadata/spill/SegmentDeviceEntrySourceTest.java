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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentDeviceEntrySourceTest {

  private Path queryDirectory;
  private String originalSortTmpDir;

  @Before
  public void setUp() throws Exception {
    queryDirectory = Files.createTempDirectory("device-entry-source-test");
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
  public void testLocalSourceConsumesSegmentsAndCleansDataSet() throws Exception {
    List<DeviceEntry> expected = createEntries(20);
    PlanNodeId planNodeId = new PlanNodeId("scan-local");
    SpilledDeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-local", planNodeId, 128, false)) {
      for (DeviceEntry entry : expected) {
        materializer.append(entry);
      }
      materializer.forceSpill();
      dataSet = (SpilledDeviceEntryDataSet) materializer.finish();
    }

    DeviceEntryDataSetHandle handle =
        new DeviceEntryDataSetHandle(
            "q-local",
            planNodeId,
            new TEndPoint("127.0.0.1", 1),
            dataSet.getSegments().size(),
            expected.size(),
            false);
    List<DeviceEntry> actual = new ArrayList<>();
    try (LocalSegmentDeviceEntrySource source = new LocalSegmentDeviceEntrySource(handle)) {
      while (source.hasNextBatch()) {
        actual.addAll(source.nextBatch());
      }
    }

    assertEquals(expected, actual);
    assertFalse(Files.exists(queryDirectory.resolve("device-entry/q-local/scan-local")));
  }

  @Test
  public void testRemoteSourceFetchesSegmentsAndFinishes() throws Exception {
    List<DeviceEntry> expected = createEntries(3);
    RecordingFetcher fetcher = new RecordingFetcher(expected);
    DeviceEntryDataSetHandle handle =
        new DeviceEntryDataSetHandle(
            "q-remote",
            new PlanNodeId("scan-remote"),
            new TEndPoint("127.0.0.2", 2),
            expected.size(),
            expected.size(),
            false);
    List<DeviceEntry> actual = new ArrayList<>();
    try (RemoteSegmentDeviceEntrySource source =
        new RemoteSegmentDeviceEntrySource(handle, fetcher)) {
      while (source.hasNextBatch()) {
        actual.addAll(source.nextBatch());
      }
    }

    assertEquals(expected, actual);
    assertEquals(List.of(0, 1, 2), fetcher.segmentIds);
    assertTrue(fetcher.finished);
  }

  @Test
  public void testFinishUnregisteredDataSetIsIdempotent() throws Exception {
    DeviceEntrySpillManager.getInstance()
        .finishSegmentDataSet("unregistered-query", "unregistered-scan");
    DeviceEntrySpillManager.getInstance()
        .finishSegmentDataSet("unregistered-query", "unregistered-scan");
  }

  private static List<DeviceEntry> createEntries(int count) {
    List<DeviceEntry> entries = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      entries.add(
          new AlignedDeviceEntry(
              IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table", "device" + i}),
              new org.apache.tsfile.utils.Binary[0]));
    }
    return entries;
  }

  private static byte[] serializeSegment(DeviceEntry entry) throws Exception {
    byte[] payload = entry.serializeToBytes();
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream output = new DataOutputStream(bytes)) {
      output.writeInt(payload.length);
      output.write(payload);
      return bytes.toByteArray();
    }
  }

  private static final class RecordingFetcher implements DeviceEntrySegmentFetcher {

    private final List<DeviceEntry> entries;
    private final List<Integer> segmentIds = new ArrayList<>();
    private boolean finished;

    private RecordingFetcher(List<DeviceEntry> entries) {
      this.entries = entries;
    }

    @Override
    public byte[] fetch(DeviceEntryDataSetHandle handle, int segmentId) throws java.io.IOException {
      segmentIds.add(segmentId);
      try {
        return serializeSegment(entries.get(segmentId));
      } catch (Exception e) {
        throw new java.io.IOException(e);
      }
    }

    @Override
    public void finish(DeviceEntryDataSetHandle handle) {
      finished = true;
    }
  }
}
