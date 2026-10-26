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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileDeviceQueryTask.DeviceOffset;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource.DeviceTaskPartition;
import org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile.ExternalTsFileQueryResource.DeviceTaskRunReader;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.AlignedDeviceEntry;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.DeviceEntry;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.utils.Binary;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExternalTsFileQueryResourceTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ExternalTsFileQueryResource resource;

  @After
  public void tearDown() {
    if (resource != null) {
      resource.closeByQueryExecution();
    }
  }

  @Test
  public void testDeviceTaskRunReaderMergesMultipleRunsWithComparator() throws Exception {
    resource = newResource("merge_comparator", Arrays.asList("file-0.tsfile", "file-1.tsfile"));
    addDevices("d1", "d2", "d3", "d4", "d5");
    resource.setDeviceEntryComparator(Comparator.comparing(DeviceEntry::getDeviceID));
    DeviceTaskPartition partition = newPartition();

    partition.add(task(2, offset(0, 30, 39)));
    partition.add(task(4, offset(1, 50, 59)));
    partition.flush();
    partition.add(task(0, offset(0, 10, 19)));
    partition.add(task(3, offset(1, 40, 49)));
    partition.flush();
    partition.add(task(1, offset(0, 20, 29)));
    partition.flush();

    try (DeviceTaskRunReader reader = newReader(partition)) {
      assertDeviceOrder(reader, "d1", "d2", "d3", "d4", "d5");
    }
  }

  @Test
  public void testDeviceTaskRunReaderReadsRunsInFifoOrderWithoutComparator() throws Exception {
    resource = newResource("fifo", Collections.singletonList("file-0.tsfile"));
    addDevices("d1", "d2", "d3", "d4");
    DeviceTaskPartition partition = newPartition();

    partition.add(task(2, offset(0, 30, 39)));
    partition.flush();
    partition.add(task(0, offset(0, 10, 19)));
    partition.add(task(1, offset(0, 20, 29)));
    partition.flush();
    partition.add(task(3, offset(0, 40, 49)));
    partition.flush();

    try (DeviceTaskRunReader reader = newReader(partition)) {
      assertDeviceOrder(reader, "d3", "d1", "d2", "d4");
    }
  }

  @Test
  public void testDeviceTaskRunReaderReadsDiskRunBeforePendingMemoryTasksWithoutComparator()
      throws Exception {
    resource = newResource("disk_memory", Collections.singletonList("file-0.tsfile"));
    addDevices("d1", "d2", "d3");
    DeviceTaskPartition partition = newPartition();

    partition.add(task(1, offset(0, 20, 29)));
    partition.flush();
    partition.add(task(0, offset(0, 10, 19)));
    partition.add(task(2, offset(0, 30, 39)));
    partition.finish();

    try (DeviceTaskRunReader reader = newReader(partition)) {
      assertDeviceOrder(reader, "d2", "d1", "d3");
    }
  }

  @Test
  public void testDeviceTaskRunReaderUsesSharedTsFileResourceAsOffsetMapKey() throws Exception {
    resource = newResource("offset_map", Arrays.asList("file-0.tsfile", "file-1.tsfile"));
    addDevices("d1");
    DeviceTaskPartition partition = newPartition();
    partition.add(task(0, offset(0, 11, 22), offset(1, 33, 44)));
    partition.finish();

    try (DeviceTaskRunReader reader = newReader(partition)) {
      assertTrue(reader.nextDevice());
      Map<TsFileResource, DeviceOffset> offsetMap = reader.getCurrentDeviceOffsetMap();
      List<TsFileResource> sharedResources = resource.getSharedTsFileResources();

      assertEquals(2, offsetMap.size());
      assertTrue(offsetMap.containsKey(sharedResources.get(0)));
      assertTrue(offsetMap.containsKey(sharedResources.get(1)));
      assertOffset(offsetMap.get(sharedResources.get(0)), 0, 11, 22);
      assertOffset(offsetMap.get(sharedResources.get(1)), 1, 33, 44);
      assertEquals(sharedResources, reader.getCurrentDeviceQueryDataSource().getUnseqResources());
      assertFalse(reader.nextDevice());
    }
  }

  private ExternalTsFileQueryResource newResource(String queryId, List<String> fileNames)
      throws Exception {
    File root = temporaryFolder.newFolder(queryId);
    List<String> tsFilePaths = new ArrayList<>(fileNames.size());
    for (String fileName : fileNames) {
      tsFilePaths.add(new File(root, fileName).getAbsolutePath());
    }
    MPPQueryContext queryContext = new MPPQueryContext(new QueryId(queryId));
    queryContext.setStartTime(System.currentTimeMillis());
    queryContext.setTimeOut(Long.MAX_VALUE);
    return new ExternalTsFileQueryResource(
        queryContext,
        root.toPath().resolve("tmp"),
        "table1",
        tsFilePaths,
        Collections.emptyMap(),
        ReadTsFileTableFunction.DEVICE_TASK_BUCKET_TARGET_SIZE_IN_BYTES);
  }

  private DeviceTaskRunReader newReader(DeviceTaskPartition partition) {
    resource.getDeviceTaskPartitions().add(partition);
    return resource.getDeviceTaskRunReader(partition.getPartitionIndex());
  }

  private DeviceTaskPartition newPartition() {
    return resource.new DeviceTaskPartition(0);
  }

  private void addDevices(String... deviceNames) {
    for (String deviceName : deviceNames) {
      resource
          .getSharedDeviceEntries()
          .add(
              new AlignedDeviceEntry(
                  IDeviceID.Factory.DEFAULT_FACTORY.create(deviceName), new Binary[0]));
    }
  }

  private ExternalTsFileDeviceQueryTask task(int deviceEntryIndex, DeviceOffset... offsets) {
    return new ExternalTsFileDeviceQueryTask(deviceEntryIndex, Arrays.asList(offsets));
  }

  private DeviceOffset offset(int fileIndex, long startOffset, long endOffset) {
    return new DeviceOffset(fileIndex, startOffset, endOffset);
  }

  private void assertDeviceOrder(DeviceTaskRunReader reader, String... expectedDeviceNames)
      throws Exception {
    for (String expectedDeviceName : expectedDeviceNames) {
      assertTrue(reader.nextDevice());
      assertEquals(expectedDeviceName, reader.getCurrentDevice().getDeviceID().toString());
    }
    assertFalse(reader.nextDevice());
  }

  private void assertOffset(
      DeviceOffset offset,
      int expectedFileIndex,
      long expectedStartOffset,
      long expectedEndOffset) {
    assertEquals(expectedFileIndex, offset.getFileIndex());
    assertEquals(expectedStartOffset, offset.getStartOffset());
    assertEquals(expectedEndOffset, offset.getEndOffset());
  }
}
