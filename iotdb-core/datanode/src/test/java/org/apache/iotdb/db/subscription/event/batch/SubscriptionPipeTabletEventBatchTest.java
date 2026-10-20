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

package org.apache.iotdb.db.subscription.event.batch;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeMemoryManager.TsFileParserMemoryReservation;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.subscription.broker.SubscriptionPrefetchingTabletQueue;
import org.apache.iotdb.db.subscription.event.pipe.SubscriptionPipeTabletBatchEvents;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SubscriptionPipeTabletEventBatchTest {

  private static final String REGION_ID = "7";

  private final CommonConfig config = CommonDescriptor.getInstance().getConfig();
  private final PipeMemoryManager memoryManager = PipeDataNodeResourceManager.memory();
  private final List<PipeTsFileInsertionEvent> tsFileEvents = new ArrayList<>();

  private long originalParserMemory;
  private int originalGlobalLimit;
  private int originalRegionLimit;
  private File temporaryDirectory;
  private SubscriptionPipeTabletEventBatch batch;

  @Before
  public void setUp() throws Exception {
    originalParserMemory = config.getPipeTsFileParserMemory();
    originalGlobalLimit = config.getPipeTsFileParserInFlightMaxNum();
    originalRegionLimit = config.getPipeTsFileParserInFlightMaxNumPerPipeRegion();
    config.setPipeTsFileParserMemory(1);
    config.setPipeTsFileParserInFlightMaxNum(4);
    config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(1);

    temporaryDirectory = Files.createTempDirectory("subscription-tablet-batch").toFile();
    final SubscriptionPrefetchingTabletQueue queue =
        Mockito.mock(SubscriptionPrefetchingTabletQueue.class);
    Mockito.when(queue.getTopicName()).thenReturn("topic");
    Mockito.when(queue.getConsumerGroupId()).thenReturn("group");
    batch =
        new SubscriptionPipeTabletEventBatch(Integer.parseInt(REGION_ID), queue, 20, 1024 * 1024);
  }

  @After
  public void tearDown() throws Exception {
    if (batch != null) {
      batch.cleanUp(true);
    }
    for (final PipeTsFileInsertionEvent event : tsFileEvents) {
      event.close();
    }
    config.setPipeTsFileParserMemory(originalParserMemory);
    config.setPipeTsFileParserInFlightMaxNum(originalGlobalLimit);
    config.setPipeTsFileParserInFlightMaxNumPerPipeRegion(originalRegionLimit);
    if (temporaryDirectory != null) {
      FileUtils.deleteDirectory(temporaryDirectory);
    }
  }

  @Test(timeout = 10000)
  public void testSameRegionFilesReleaseParserBeforeBatchAck() throws Exception {
    final long baselineTabletMemory = memoryManager.getUsedMemorySizeInBytesOfTablets();
    batch.enrichedEvents.add(createTsFileEvent("first.tsfile", 1, false, false));
    batch.enrichedEvents.add(createTsFileEvent("second.tsfile", 2, false, false));
    batch.resetForIteration();

    Assert.assertTrue(batch.hasNext());
    final Pair<String, List<Tablet>> first = batch.next();
    assertTablet(first, "table_0", 1);
    Assert.assertTrue("The exhausted file must release its parser before ACK", canReserveParser());

    Assert.assertTrue(batch.hasNext());
    assertTablet(batch.next(), "table_0", 2);
    Assert.assertFalse(batch.hasNext());
    Assert.assertTrue(canReserveParser());

    // Closing a parser must leave its materialized tablets available until the response is ACKed.
    assertTablet(first, "table_0", 1);
    final SubscriptionPipeTabletIterationSnapshot snapshot = batch.sendIterationSnapshot();
    Assert.assertEquals(2, snapshot.getIteratedEnrichedEvents().size());
    snapshot.ack();
    Assert.assertEquals(baselineTabletMemory, memoryManager.getUsedMemorySizeInBytesOfTablets());
  }

  @Test(timeout = 10000)
  public void testFilteredFileReleasesParserWithoutProducingTablet() throws Exception {
    final long baselineTabletMemory = memoryManager.getUsedMemorySizeInBytesOfTablets();
    batch.enrichedEvents.add(createTsFileEvent("filtered.tsfile", 1, true, false));
    batch.resetForIteration();

    Assert.assertTrue(batch.hasNext());
    Assert.assertNull(batch.next());
    Assert.assertTrue("An empty iteration must also release its parser", canReserveParser());
    Assert.assertFalse(batch.hasNext());

    final SubscriptionPipeTabletIterationSnapshot snapshot = batch.sendIterationSnapshot();
    Assert.assertEquals(1, snapshot.getIteratedEnrichedEvents().size());
    snapshot.ack();
    Assert.assertEquals(baselineTabletMemory, memoryManager.getUsedMemorySizeInBytesOfTablets());
  }

  @Test(timeout = 10000)
  public void testDetachedSnapshotCleanupClosesPartiallyParsedFile() throws Exception {
    final long baselineTabletMemory = memoryManager.getUsedMemorySizeInBytesOfTablets();
    batch.enrichedEvents.add(createTsFileEvent("partial.tsfile", 1, false, true));
    batch.resetForIteration();

    Assert.assertNotNull(batch.next());
    Assert.assertTrue(batch.hasNext());
    Assert.assertFalse("A file still being parsed must retain its parser", canReserveParser());

    final SubscriptionPipeTabletBatchEvents events = new SubscriptionPipeTabletBatchEvents(batch);
    events.receiveIterationSnapshot(batch.sendIterationSnapshot());
    events.cleanUp(true);
    Assert.assertTrue(canReserveParser());
    Assert.assertFalse(batch.hasNext());
    Assert.assertEquals(baselineTabletMemory, memoryManager.getUsedMemorySizeInBytesOfTablets());
  }

  private boolean canReserveParser() {
    final TsFileParserMemoryReservation reservation = new TsFileParserMemoryReservation();
    final boolean reserved =
        memoryManager.tryReserveTsFileParserMemory(null, 0, REGION_ID, reservation);
    try {
      return reserved;
    } finally {
      if (reserved) {
        memoryManager.releaseTsFileParserMemory(null, 0, REGION_ID);
      } else {
        memoryManager.cancelTsFileParserMemoryReservation(null, 0, REGION_ID, reservation);
      }
    }
  }

  private PipeTsFileInsertionEvent createTsFileEvent(
      final String fileName, final long timestamp, final boolean filtered, final boolean twoTables)
      throws Exception {
    final File partitionDirectory = new File(temporaryDirectory, REGION_ID + File.separator + "0");
    Assert.assertTrue(partitionDirectory.isDirectory() || partitionDirectory.mkdirs());
    final File tsFile = new File(partitionDirectory, fileName);
    try (final TsFileWriter writer = new TsFileWriter(tsFile)) {
      writeTable(writer, "table_0", timestamp);
      if (twoTables) {
        writeTable(writer, "table_1", timestamp + 1);
      }
    }
    final TsFileResource resource = new TsFileResource(tsFile);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    final IDeviceID device =
        IDeviceID.Factory.DEFAULT_FACTORY.create(new String[] {"table_0", "device_0"});
    resource.updateStartTime(device, timestamp);
    resource.updateEndTime(device, timestamp + 1);
    final PipeTsFileInsertionEvent event =
        new PipeTsFileInsertionEvent(
            true,
            "test_sg_0",
            resource,
            null,
            false,
            false,
            false,
            Collections.singleton("table_0"),
            null,
            0,
            null,
            null,
            new TablePattern(true, null, filtered ? "other_table" : null),
            null,
            null,
            null,
            true,
            Long.MIN_VALUE,
            Long.MAX_VALUE);
    tsFileEvents.add(event);
    return event;
  }

  private static void writeTable(final TsFileWriter writer, final String tableName, final long time)
      throws Exception {
    final List<ColumnCategory> categories = Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);
    writer.registerTableSchema(
        new TableSchema(
            tableName,
            Arrays.asList(
                new MeasurementSchema("device_id", TSDataType.STRING),
                new MeasurementSchema("sensor", TSDataType.INT64)),
            categories));
    final Tablet tablet =
        new Tablet(
            tableName,
            Arrays.asList("device_id", "sensor"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            categories,
            1);
    tablet.addTimestamp(0, time);
    tablet.addValue(0, 0, "device_0");
    tablet.addValue(0, 1, time);
    writer.writeTable(tablet);
  }

  private static void assertTablet(
      final Pair<String, List<Tablet>> tablets, final String tableName, final long time) {
    Assert.assertNotNull(tablets);
    Assert.assertEquals("test_sg_0", tablets.left);
    Assert.assertEquals(1, tablets.right.size());
    final Tablet tablet = tablets.right.get(0);
    Assert.assertEquals(tableName, tablet.getTableName());
    Assert.assertEquals(1, tablet.getRowSize());
    Assert.assertEquals(time, tablet.getTimestamps()[0]);
    Assert.assertEquals(time, ((long[]) tablet.getValues()[1])[0]);
  }
}
