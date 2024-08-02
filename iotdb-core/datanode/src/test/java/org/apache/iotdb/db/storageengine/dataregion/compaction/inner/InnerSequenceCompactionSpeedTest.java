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

package org.apache.iotdb.db.storageengine.dataregion.compaction.inner;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class InnerSequenceCompactionSpeedTest extends AbstractCompactionTest {
  private final long compactionReadThroughputPerSec =
      IoTDBDescriptor.getInstance().getConfig().getCompactionReadThroughputMbPerSec();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    CompactionTaskManager.getInstance().setCompactionReadThroughputRate(1);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    CompactionTaskManager.getInstance()
        .setCompactionReadThroughputRate(compactionReadThroughputPerSec);
  }

  @Test
  public void testManyAlignedDeviceTsFile() throws IOException, InterruptedException {
    List<String> deviceNames = new ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      deviceNames.add("d" + i);
    }
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      for (String device : deviceNames) {
        writer.startChunkGroup(device);
        writer.generateSimpleAlignedSeriesToCurrentDevice(
            Collections.singletonList("s0"),
            new TimeRange[] {new TimeRange(1, 2)},
            TSEncoding.PLAIN,
            CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    seqResources.add(resource);
    tsFileManager.add(resource, true);
    long tsFileSize = resource.getTsFileSize();
    Thread thread =
        new Thread(
            () -> {
              InnerSpaceCompactionTask task =
                  new InnerSpaceCompactionTask(
                      0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
              task.start();
            });
    thread.start();
    thread.join(TimeUnit.SECONDS.toMillis(30 + tsFileSize / IoTDBConstant.MB));
  }

  @Test
  public void testManyNotAlignedDeviceTsFile() throws IOException {
    List<String> deviceNames = new ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      deviceNames.add("d" + i);
    }
    TsFileResource resource = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource)) {
      for (String device : deviceNames) {
        writer.startChunkGroup(device);
        writer.generateSimpleNonAlignedSeriesToCurrentDevice(
            "s0", new TimeRange[] {new TimeRange(1, 2)}, TSEncoding.PLAIN, CompressionType.LZ4);
        writer.endChunkGroup();
      }
      writer.endFile();
    }
    seqResources.add(resource);
    tsFileManager.add(resource, true);
    long tsFileSize = resource.getTsFileSize();
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    Assert.assertTrue(
        TimeUnit.SECONDS.toMillis(tsFileSize / IoTDBConstant.MB + 30) > task.getTimeCost());
  }
}
