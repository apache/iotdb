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
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.compaction.utils.CompactionTestFileWriter;
import org.apache.iotdb.db.storageengine.dataregion.read.control.FileReaderManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.TimeRange;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TableModelReadChunkCompactionPerformerTest extends AbstractCompactionTest {

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
  public void testSequenceInnerSpaceCompactionOfTwoTableModel() throws IOException {
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
      writer.registerTableSchema("t1", Arrays.asList("id1", "id2", "id3"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field2", "id_field3"));
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
    tsFileManager.addAll(seqResources, true);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource targetResource = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      TableSchema tableSchema = tsFileMetadata.getTableSchemaMap().get("t1");
      Assert.assertEquals(5, tableSchema.getColumnTypes().size());
      Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(true);
      Assert.assertEquals(2, allTimeseriesMetadata.size());
    }
  }

  @Test
  public void testSequenceInnerSpaceCompactionOfTwoV4TreeModel() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource1)) {
      writer.startChunkGroup("d1");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }
    TsFileResource resource2 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource2)) {
      writer.startChunkGroup("d1");
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
    tsFileManager.addAll(seqResources, true);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource targetResource = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertTrue(tsFileMetadata.getTableSchemaMap().isEmpty());
      Map<IDeviceID, List<TimeseriesMetadata>> allTimeseriesMetadata =
          reader.getAllTimeseriesMetadata(true);
      for (Map.Entry<IDeviceID, List<TimeseriesMetadata>> entry :
          allTimeseriesMetadata.entrySet()) {
        Assert.assertEquals(2, entry.getValue().size());
      }
    }
  }

  @Test
  public void testSequenceInnerSpaceCompactionOfTableModelAndTreeModel() throws IOException {
    TsFileResource resource1 = createEmptyFileAndResource(true);
    try (CompactionTestFileWriter writer = new CompactionTestFileWriter(resource1)) {
      writer.startChunkGroup("d1");
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
    seqResources.add(resource1);
    seqResources.add(resource2);
    tsFileManager.addAll(seqResources, true);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    TsFileResource targetResource = tsFileManager.getTsFileList(true).get(0);
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(targetResource.getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
  }

  @Test
  public void testSequenceInnerSpaceCompactionOfTwoV4TreeModelCanNotMatchTableSchema()
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
      writer.registerTableSchema("t1", Arrays.asList("id1", "id3"));
      writer.startChunkGroup("t1", Arrays.asList("id_field1", "id_field3"));
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
    tsFileManager.addAll(seqResources, true);

    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertFalse(task.start());
  }

  @Test
  public void testCompactionWithV3Tsfile() throws IOException {
    String pathStr =
        this.getClass().getClassLoader().getResource("v3tsfile/compaction-test-tsfile").getFile();
    File v3TsFile = new File(pathStr);
    File v3TsFileResource = new File(pathStr + "-resource");
    TsFileResource resource1 = createEmptyFileAndResource(true);
    Files.copy(v3TsFile.toPath(), resource1.getTsFile().toPath());
    Files.copy(
        v3TsFileResource.toPath(), new File(resource1.getTsFilePath() + ".resource").toPath());
    resource1.deserialize();

    TsFileResource resource2 = createEmptyFileAndResource(true);
    try (CompactionTableModelTestFileWriter writer =
        new CompactionTableModelTestFileWriter(resource2)) {
      writer.registerTableSchema("db1.t1", Arrays.asList("id1", "id2"));
      writer.startChunkGroup("db1.t1", Arrays.asList("id_field1", "id_field2"));
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();

      writer.startChunkGroup("d3");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();

      writer.startChunkGroup("node1.node2.device");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();

      writer.startChunkGroup("node1.node2.node3.device");
      writer.generateSimpleNonAlignedSeriesToCurrentDevice(
          "s1",
          new TimeRange[][][] {new TimeRange[][] {new TimeRange[] {new TimeRange(10, 12)}}},
          TSEncoding.PLAIN,
          CompressionType.LZ4);
      writer.endChunkGroup();
      writer.endFile();
    }

    seqResources.add(resource1);
    seqResources.add(resource2);
    InnerSpaceCompactionTask task =
        new InnerSpaceCompactionTask(
            0, tsFileManager, seqResources, true, new ReadChunkCompactionPerformer(), 0);
    Assert.assertTrue(task.start());
    try (TsFileSequenceReader reader =
        new TsFileSequenceReader(
            tsFileManager.getTsFileList(true).get(0).getTsFile().getAbsolutePath())) {
      TsFileMetadata tsFileMetadata = reader.readFileMetadata();
      Assert.assertEquals(1, tsFileMetadata.getTableSchemaMap().size());
    }
  }
}
