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

package org.apache.iotdb.db.storageengine.dataregion.compaction.tablemodel;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.storageengine.dataregion.compaction.AbstractCompactionTest;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadPointCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.CrossSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class TableModelCrossSpaceCompactionTest extends AbstractCompactionTest {

  private final String oldThreadName = Thread.currentThread().getName();

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    super.setUp();
    Thread.currentThread().setName("pool-1-IoTDB-Compaction-Worker-1");
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    super.tearDown();
    Thread.currentThread().setName(oldThreadName);
    for (TsFileResource tsFileResource : seqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
    for (TsFileResource tsFileResource : unseqResources) {
      FileReaderManager.getInstance().closeFileAndRemoveReader(tsFileResource.getTsFilePath());
    }
  }

  @Test
  public void testCrossSpaceCompactionOfTwoTableModelWithFastCompactionPerformer()
      throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource resource2 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource2)) {
      writer.registerTableSchema("t2", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t2", Arrays.asList("id_field1", "id_field2", "id_field3"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    TsFileResource resource3 = createEmptyFileAndResource(false);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource3)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource resource4 = createEmptyFileAndResource(false);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource4)) {
      writer.registerTableSchema("t2", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t2", Arrays.asList("id_field1", "id_field2", "id_field3"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    seqResources.add(resource2);
    unseqResources.add(resource3);
    unseqResources.add(resource4);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new FastCompactionPerformer(true),
            0,
            0);
    Assert.assertTrue(task.start());
    TsFileResource targetResource0 = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource0.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
    TsFileResource targetResource1 = tsFileManager.getTsFileList(true).get(1);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource1.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
  }

  @Test
  public void testCrossSpaceCompactionOfTwoTableModelWithReadPointCompactionPerformer()
      throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource1)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource resource2 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource2)) {
      writer.registerTableSchema("t2", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t2", Arrays.asList("id_field1", "id_field2", "id_field3"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    TsFileResource resource3 = createEmptyFileAndResource(false);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource3)) {
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource resource4 = createEmptyFileAndResource(false);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource4)) {
      writer.registerTableSchema("t2", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t2", Arrays.asList("id_field1", "id_field2", "id_field3"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s2",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(20, 22)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    seqResources.add(resource1);
    seqResources.add(resource2);
    unseqResources.add(resource3);
    unseqResources.add(resource4);
    tsFileManager.addAll(seqResources, true);
    tsFileManager.addAll(unseqResources, false);

    CrossSpaceCompactionTask task =
        new CrossSpaceCompactionTask(
            0,
            tsFileManager,
            seqResources,
            unseqResources,
            new ReadPointCompactionPerformer(),
            0,
            0);
    Assert.assertTrue(task.start());
    TsFileResource targetResource0 = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource0.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
    TsFileResource targetResource1 = tsFileManager.getTsFileList(true).get(1);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource1.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
  }
}
