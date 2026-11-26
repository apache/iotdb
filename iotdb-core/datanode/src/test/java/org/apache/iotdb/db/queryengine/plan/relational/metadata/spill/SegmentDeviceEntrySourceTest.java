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
import org.apache.iotdb.db.exception.query.DeviceEntrySpillNotFoundException;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
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
  public void testClosingLocalSourceEarlyCleansRemainingSegments() throws Exception {
    PlanNodeId planNodeId = new PlanNodeId("scan-early-close");
    SpilledDeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer("q-early-close", planNodeId, 128, false)) {
      for (DeviceEntry entry : createEntries(20)) {
        materializer.append(entry);
      }
      materializer.forceSpill();
      dataSet = (SpilledDeviceEntryDataSet) materializer.finish();
    }

    DeviceEntryDataSetHandle handle =
        new DeviceEntryDataSetHandle(
            "q-early-close",
            planNodeId,
            new TEndPoint("127.0.0.1", 1),
            dataSet.getSegments().size(),
            20,
            false);
    try (LocalSegmentDeviceEntrySource source = new LocalSegmentDeviceEntrySource(handle)) {
      source.nextBatch();
    }

    assertFalse(
        Files.exists(queryDirectory.resolve("device-entry/q-early-close/scan-early-close")));
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

  @Test
  public void testSegmentReleaseIsIdempotentAfterForcedQueryCleanup() throws Exception {
    String queryId = "q-release-after-force-cleanup";
    PlanNodeId planNodeId = new PlanNodeId("scan-release");
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path ownerDirectory = manager.register(queryId, planNodeId);
    Path dataSetDirectory = ownerDirectory.resolve("fi");
    Files.createDirectories(dataSetDirectory);
    Files.write(dataSetDirectory.resolve("segment-000000.bin"), new byte[] {1});

    manager.deregisterQuery(queryId);

    DeviceEntryDataSetHandle handle =
        new DeviceEntryDataSetHandle(
            queryId, planNodeId, new TEndPoint("127.0.0.1", 1), 1, 1, false);
    try (LocalSegmentDeviceEntrySource source = new LocalSegmentDeviceEntrySource(handle)) {
      assertThrows(DeviceEntrySpillNotFoundException.class, source::nextBatch);
    }

    manager.deleteSegment(queryId, planNodeId, 0);
    manager.finishSegmentDataSet(queryId, planNodeId.getId());
  }

  @Test
  public void testRejectsInvalidHandleCountsAndTruncatedPayload() {
    TEndPoint endPoint = new TEndPoint("127.0.0.1", 1);
    PlanNodeId planNodeId = new PlanNodeId("scan-invalid");
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeviceEntryDataSetHandle("q-invalid", planNodeId, endPoint, -1, 0, false));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeviceEntryDataSetHandle("q-invalid", planNodeId, endPoint, 0, -1, false));

    DeviceEntryDataSetHandle handle =
        new DeviceEntryDataSetHandle("q-invalid", planNodeId, endPoint, 1, 1, false);
    SegmentDeviceEntrySource source =
        new SegmentDeviceEntrySource(handle) {
          @Override
          public List<DeviceEntry> nextBatch() throws IOException {
            return deserialize(new byte[] {0, 0, 0, 4, 1});
          }
        };
    assertThrows(IOException.class, source::nextBatch);
  }

  @Test
  public void testConcurrentOwnerAndQueryCleanupLeavesNoFiles() throws Exception {
    String queryId = "q-concurrent-cleanup";
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    int ownerCount = 32;
    for (int owner = 0; owner < ownerCount; owner++) {
      try (DeviceEntryMaterializer materializer =
          new DeviceEntryMaterializer(queryId, new PlanNodeId("scan-" + owner), 128, false)) {
        for (DeviceEntry entry : createEntries(20)) {
          materializer.append(entry);
        }
        materializer.forceSpill();
        materializer.finish();
      }
    }

    ExecutorService executor = Executors.newFixedThreadPool(ownerCount + 1);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> futures = new ArrayList<>();
    for (int owner = 0; owner < ownerCount; owner++) {
      String planNodeId = "scan-" + owner;
      futures.add(
          executor.submit(
              () -> {
                start.await();
                manager.finishSegmentDataSet(queryId, planNodeId);
                return null;
              }));
    }
    futures.add(
        executor.submit(
            () -> {
              start.await();
              manager.deregisterQuery(queryId);
              return null;
            }));
    start.countDown();
    executor.shutdown();
    assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    for (Future<?> future : futures) {
      future.get();
    }

    assertFalse(Files.exists(queryDirectory.resolve("device-entry").resolve(queryId)));
  }

  @Test
  public void testQueryCleanupClosesActiveSpillerBeforeDeletingDirectory() throws Exception {
    String queryId = "q-cleanup-active-spiller";
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer(queryId, new PlanNodeId("scan-active"), 1, false)) {
      materializer.append(createEntries(1).get(0));
      materializer.forceSpill();

      Path querySpillDirectory = queryDirectory.resolve("device-entry").resolve(queryId);
      assertTrue(Files.exists(querySpillDirectory));

      manager.deregisterQuery(queryId);

      assertFalse(Files.exists(querySpillDirectory));
      assertThrows(
          IOException.class, () -> materializer.appendWithMemoryControl(createEntries(1).get(0)));
    }
  }

  @Test
  public void testQueryCleanupCanRetryAfterSpillerCloseFailure() throws Exception {
    String queryId = "q-cleanup-spiller-close-retry";
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path ownerDirectory = manager.register(queryId, new PlanNodeId("scan-close-retry"));
    Path spillDirectory = ownerDirectory.resolve("fi");
    DeviceEntryDiskSpiller spiller =
        manager.createSpiller(queryId, ownerDirectory, spillDirectory, 1024, null, false);
    spiller.append(new byte[] {1});

    Field outputField = DeviceEntryDiskSpiller.class.getDeclaredField("output");
    outputField.setAccessible(true);
    ((DataOutputStream) outputField.get(spiller)).close();
    Field fileOutputField = DeviceEntryDiskSpiller.class.getDeclaredField("fileOutput");
    fileOutputField.setAccessible(true);
    AtomicInteger closeCalls = new AtomicInteger();
    OutputStream fileOutput =
        new OutputStream() {
          @Override
          public void write(int value) {}

          @Override
          public void close() throws IOException {
            if (closeCalls.incrementAndGet() == 1) {
              throw new IOException("Simulated first close failure");
            }
          }
        };
    fileOutputField.set(spiller, fileOutput);
    outputField.set(spiller, new DataOutputStream(new BufferedOutputStream(fileOutput)));

    assertThrows(IOException.class, () -> manager.deregisterQuery(queryId));
    assertEquals(1, closeCalls.get());
    assertTrue(Files.exists(spillDirectory));

    manager.deregisterQuery(queryId);

    assertEquals(2, closeCalls.get());
    assertFalse(Files.exists(queryDirectory.resolve("device-entry").resolve(queryId)));
  }

  @Test
  public void testSpillerDoesNotSealAfterBufferedOutputCloseFailure() throws Exception {
    String queryId = Integer.toHexString(System.identityHashCode(this));
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path ownerDirectory = manager.register(queryId, new PlanNodeId(queryId));
    Path spillDirectory = ownerDirectory.resolve(ownerDirectory.getFileName());
    DeviceEntryDiskSpiller spiller =
        manager.createSpiller(queryId, ownerDirectory, spillDirectory, 1024, null, false);
    spiller.append(new byte[] {1});

    Field outputField = null;
    Field fileOutputField = null;
    for (Field field : DeviceEntryDiskSpiller.class.getDeclaredFields()) {
      if (field.getType() == DataOutputStream.class) {
        outputField = field;
      } else if (field.getType() == OutputStream.class) {
        fileOutputField = field;
      }
    }
    outputField.setAccessible(true);
    ((DataOutputStream) outputField.get(spiller)).close();
    fileOutputField.setAccessible(true);
    AtomicInteger writeCalls = new AtomicInteger();
    AtomicInteger closeCalls = new AtomicInteger();
    OutputStream fileOutput =
        new OutputStream() {
          @Override
          public void write(int value) throws IOException {
            writeCalls.incrementAndGet();
            throw new IOException();
          }

          @Override
          public void close() {
            closeCalls.incrementAndGet();
          }
        };
    fileOutputField.set(spiller, fileOutput);
    DataOutputStream output = new DataOutputStream(new BufferedOutputStream(fileOutput));
    output.writeInt(1);
    outputField.set(spiller, output);

    assertThrows(IOException.class, spiller::finish);
    assertTrue(writeCalls.get() > 0);
    assertTrue(Files.exists(spillDirectory));
    int writeCallsAfterCloseFailure = writeCalls.get();
    IOException outputCloseFailure =
        assertThrows(IOException.class, () -> spiller.append(new byte[] {2}));
    assertNotNull(outputCloseFailure.getMessage());
    assertEquals(writeCallsAfterCloseFailure, writeCalls.get());
    assertThrows(IOException.class, spiller::finish);
    assertTrue(Files.exists(spillDirectory));

    manager.deregisterQuery(queryId);

    assertEquals(2, closeCalls.get());
    assertFalse(Files.exists(ownerDirectory.getParent()));
  }

  @Test
  public void testQueryCleanupClosesActiveReaderBeforeDeletingDirectory() throws Exception {
    String queryId = "q-cleanup-active-reader";
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    SpilledDeviceEntryDataSet dataSet;
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer(queryId, new PlanNodeId("scan-reader"), 1, false)) {
      materializer.append(createEntries(1).get(0));
      materializer.forceSpill();
      dataSet = (SpilledDeviceEntryDataSet) materializer.finish();
    }

    DeviceEntryReader reader = dataSet.openReader();
    assertTrue(reader.hasNext());
    Path querySpillDirectory = queryDirectory.resolve("device-entry").resolve(queryId);

    manager.deregisterQuery(queryId);

    assertFalse(Files.exists(querySpillDirectory));
    assertThrows(IOException.class, reader::hasNext);
    reader.close();
    dataSet.close();
  }

  @Test
  public void testQueryCleanupDeletesAllOwners() throws Exception {
    String queryId = "q-deferred-cleanup";
    PlanNodeId rawPlanNodeId = new PlanNodeId("scan-raw");
    PlanNodeId fragmentPlanNodeId = new PlanNodeId("scan-fragment");
    PlanNodeId emptyFragmentPlanNodeId = new PlanNodeId("scan-empty-fragment");
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    try (DeviceEntryMaterializer materializer =
        new DeviceEntryMaterializer(queryId, rawPlanNodeId, 128, true)) {
      for (DeviceEntry entry : createEntries(20)) {
        materializer.append(entry);
      }
      materializer.forceSpill();
      materializer.finish();
    }
    Path fragmentOwner = manager.register(queryId, fragmentPlanNodeId);
    Path fragmentDirectory = fragmentOwner.resolve("fi");
    Files.createDirectories(fragmentDirectory);
    Files.write(fragmentDirectory.resolve("segment-000000.bin"), new byte[] {1});
    Path emptyFragmentOwner = manager.register(queryId, emptyFragmentPlanNodeId);
    Files.createDirectories(emptyFragmentOwner.resolve("fi"));

    manager.deregisterQuery(queryId);

    assertFalse(Files.exists(queryDirectory.resolve("device-entry").resolve(queryId)));
  }

  @Test
  public void testFailedQueryCleanupDeletesActiveFragmentOwners() throws Exception {
    String queryId = "q-failed-cleanup";
    PlanNodeId fragmentPlanNodeId = new PlanNodeId("scan-fragment");
    DeviceEntrySpillManager manager = DeviceEntrySpillManager.getInstance();
    Path fragmentOwner = manager.register(queryId, fragmentPlanNodeId);
    Path fragmentDirectory = fragmentOwner.resolve("fi");
    Files.createDirectories(fragmentDirectory);
    Files.write(fragmentDirectory.resolve("segment-000000.bin"), new byte[] {1});

    manager.deregisterQuery(queryId);

    assertFalse(Files.exists(queryDirectory.resolve("device-entry").resolve(queryId)));
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
