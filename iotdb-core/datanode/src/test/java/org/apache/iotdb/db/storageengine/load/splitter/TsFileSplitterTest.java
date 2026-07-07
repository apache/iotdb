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

package org.apache.iotdb.db.storageengine.load.splitter;

import org.apache.iotdb.calc.utils.IObjectPath;
import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate.FullExactMatch;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.BatchData;
import org.apache.tsfile.read.common.Chunk;
import org.apache.tsfile.read.common.TimeRange;
import org.apache.tsfile.read.reader.chunk.AlignedChunkReader;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.apache.tsfile.write.writer.TsFileIOWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TsFileSplitterTest {

  @Test
  public void testSplitTableTimeOnlyAlignedChunk() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");

    try {
      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, deviceID);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              });
      splitter.splitTsFileByDataPartition();

      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema());
        IDeviceID currentDeviceID = null;
        for (final ChunkData chunkData : emittedChunkDataList) {
          if (!Objects.equals(currentDeviceID, chunkData.getDevice())) {
            if (Objects.nonNull(currentDeviceID)) {
              writer.endChunkGroup();
            }
            writer.startChunkGroup(chunkData.getDevice());
            currentDeviceID = chunkData.getDevice();
          }

          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        if (Objects.nonNull(currentDeviceID)) {
          writer.endChunkGroup();
        }
        writer.endFile();
      }

      Assert.assertEquals(1, emittedChunkDataList.size());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(1, chunkMetadataList.size());
        Assert.assertEquals(
            2, chunkMetadataList.get(0).getTimeChunkMetadata().getStatistics().getCount());
        Assert.assertTrue(chunkMetadataList.get(0).getValueChunkMetadataList().isEmpty());
      }
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitRewritesWritableViewTableName() throws Exception {
    final File sourceTsFile = new File("split-table-view-source.tsfile");
    final File targetTsFile = new File("split-table-view-target.tsfile");
    final IDeviceID viewDeviceID = new StringArrayDeviceID("view_table", "tagA");
    final IDeviceID sourceDeviceID = new StringArrayDeviceID("source_table", "tagA");

    try {
      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, viewDeviceID, "view_table");

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              },
              false,
              null,
              Collections.singletonMap("view_table", "source_table"));
      splitter.splitTsFileByDataPartition();

      Assert.assertEquals(1, emittedChunkDataList.size());
      Assert.assertEquals(sourceDeviceID, emittedChunkDataList.get(0).getDevice());

      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
        writer.setSchema(createSchema("source_table"));
        writer.startChunkGroup(emittedChunkDataList.get(0).getDevice());
        writeSerializedChunkDataToWriter(emittedChunkDataList.get(0), writer);
        writer.endChunkGroup();
        writer.endFile();
      }

      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(sourceDeviceID, false);
        Assert.assertEquals(1, chunkMetadataList.size());
      }
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitRewritesWritableViewTableDeletion() throws Exception {
    final File sourceTsFile = new File("split-table-view-mod-source.tsfile");
    final IDeviceID viewDeviceID = new StringArrayDeviceID("view_table", "tagA");
    final IDeviceID sourceDeviceID = new StringArrayDeviceID("source_table", "tagA");
    final String viewMeasurement = "temperature";
    final String sourceMeasurement = "source_temperature";

    try {
      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, viewDeviceID, "view_table");
      try (final ModificationFile modificationFile =
          new ModificationFile(ModificationFile.getExclusiveMods(sourceTsFile), false)) {
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate("view_table", new FullExactMatch(viewDeviceID)),
                new TimeRange(100, 101)));
        modificationFile.write(
            new TableDeletionEntry(
                new DeletionPredicate(
                    "view_table",
                    new FullExactMatch(viewDeviceID),
                    Collections.singletonList(viewMeasurement)),
                new TimeRange(102, 103)));
      }

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final List<DeletionData> emittedDeletionDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                } else if (tsFileData instanceof DeletionData) {
                  emittedDeletionDataList.add((DeletionData) tsFileData);
                }
                return true;
              },
              false,
              null,
              Collections.singletonMap("view_table", "source_table"),
              Collections.singletonMap(
                  "view_table", Collections.singletonMap(viewMeasurement, sourceMeasurement)));
      splitter.splitTsFileByDataPartition();

      Assert.assertEquals(1, emittedChunkDataList.size());
      Assert.assertEquals(sourceDeviceID, emittedChunkDataList.get(0).getDevice());
      Assert.assertEquals(2, emittedDeletionDataList.size());

      final TableDeletionEntry rewrittenDeletion =
          emittedDeletionDataList.stream()
              .map(deletionData -> (TableDeletionEntry) deletionData.getModEntry())
              .filter(deletion -> deletion.getPredicate().getMeasurementNames().isEmpty())
              .findFirst()
              .orElseThrow(AssertionError::new);
      Assert.assertEquals("source_table", rewrittenDeletion.getTableName());
      Assert.assertTrue(rewrittenDeletion.affects(sourceDeviceID));
      Assert.assertFalse(rewrittenDeletion.affects(viewDeviceID));

      final TableDeletionEntry rewrittenMeasurementDeletion =
          emittedDeletionDataList.stream()
              .map(deletionData -> (TableDeletionEntry) deletionData.getModEntry())
              .filter(deletion -> !deletion.getPredicate().getMeasurementNames().isEmpty())
              .findFirst()
              .orElseThrow(AssertionError::new);
      Assert.assertEquals("source_table", rewrittenMeasurementDeletion.getTableName());
      Assert.assertEquals(
          Collections.singletonList(sourceMeasurement),
          rewrittenMeasurementDeletion.getPredicate().getMeasurementNames());
      Assert.assertTrue(rewrittenMeasurementDeletion.affects(sourceMeasurement));
      Assert.assertFalse(rewrittenMeasurementDeletion.affects(viewMeasurement));
    } finally {
      Files.deleteIfExists(ModificationFile.getExclusiveMods(sourceTsFile).toPath());
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitRewritesWritableViewColumnName() throws Exception {
    final File sourceTsFile = new File("split-table-view-column-source.tsfile");
    final File targetTsFile = new File("split-table-view-column-target.tsfile");
    final IDeviceID viewDeviceID = new StringArrayDeviceID("view_table", "tagA");
    final IDeviceID sourceDeviceID = new StringArrayDeviceID("source_table", "tagA");
    final String viewMeasurement = "temperature";
    final String sourceMeasurement = "source_temperature";

    try {
      writeTableTsFileWithFieldChunk(sourceTsFile, viewDeviceID, "view_table", viewMeasurement);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              },
              false,
              null,
              Collections.singletonMap("view_table", "source_table"),
              Collections.singletonMap(
                  "view_table", Collections.singletonMap(viewMeasurement, sourceMeasurement)));
      splitter.splitTsFileByDataPartition();

      Assert.assertEquals(1, emittedChunkDataList.size());
      Assert.assertEquals(sourceDeviceID, emittedChunkDataList.get(0).getDevice());

      writeSingleChunkDataToTableFile(
          targetTsFile,
          emittedChunkDataList.get(0),
          createSchema("source_table", sourceMeasurement),
          sourceDeviceID);

      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(sourceDeviceID, false);
        Assert.assertEquals(1, chunkMetadataList.size());
        Assert.assertEquals(1, chunkMetadataList.get(0).getValueChunkMetadataList().size());
        Assert.assertEquals(
            sourceMeasurement,
            chunkMetadataList.get(0).getValueChunkMetadataList().get(0).getMeasurementUid());
      }
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  @Test
  public void testSplitRewritesWritableViewObjectPath() throws Exception {
    final File sourceTsFile = new File("split-table-view-object-source.tsfile");
    final File targetTsFile = new File("split-table-view-object-target.tsfile");
    final File objectRoot = new File("split-table-view-object-source");
    final IDeviceID viewDeviceID = new StringArrayDeviceID("view_table", "tagA");
    final IDeviceID sourceDeviceID = new StringArrayDeviceID("source_table", "tagA");
    final String viewMeasurement = "payload";
    final String sourceMeasurement = "source_payload";
    final long time = 100L;
    final byte[] objectBytes = new byte[] {1, 2, 3, 4};

    try {
      final IObjectPath viewObjectPath =
          IObjectPath.Factory.FACTORY.create(0, time, viewDeviceID, viewMeasurement);
      final IObjectPath sourceObjectPath =
          IObjectPath.Factory.FACTORY.create(0, time, sourceDeviceID, sourceMeasurement);
      final Binary objectBinary =
          ObjectTypeUtils.generateObjectBinary(objectBytes.length, viewObjectPath);

      writeObjectFile(objectRoot, viewObjectPath.toString(), objectBytes);
      writeTableTsFileWithObjectChunk(
          sourceTsFile, viewDeviceID, "view_table", viewMeasurement, objectBinary);

      final List<ChunkData> emittedChunkDataList = new ArrayList<>();
      final TsFileSplitter splitter =
          new TsFileSplitter(
              sourceTsFile,
              tsFileData -> {
                if (tsFileData instanceof ChunkData) {
                  emittedChunkDataList.add((ChunkData) tsFileData);
                }
                return true;
              },
              true,
              objectRoot,
              Collections.singletonMap("view_table", "source_table"),
              Collections.singletonMap(
                  "view_table", Collections.singletonMap(viewMeasurement, sourceMeasurement)));
      splitter.splitTsFileByDataPartition();

      Assert.assertEquals(1, emittedChunkDataList.size());
      final ChunkData chunkData = emittedChunkDataList.get(0);
      Assert.assertEquals(sourceDeviceID, chunkData.getDevice());
      Assert.assertEquals(1, chunkData.getObjectFiles().size());
      final Pair<File, String> sourceObjectReference = chunkData.getObjectFiles().iterator().next();
      Assert.assertEquals(objectRoot, sourceObjectReference.left);
      Assert.assertEquals(viewObjectPath.toString(), sourceObjectReference.right);

      final LoadTsFileObjectFileBatch objectFileBatch =
          chunkData.getObjectFileBatchIterator(Integer.MAX_VALUE).next();
      Assert.assertEquals(1, objectFileBatch.getObjectFileChunks().size());
      Assert.assertEquals(
          sourceObjectPath.toString(),
          objectFileBatch.getObjectFileChunks().get(0).getObjectRelativePath());

      writeSingleChunkDataToTableFile(
          targetTsFile,
          chunkData,
          createObjectSchema("source_table", sourceMeasurement),
          sourceDeviceID);
      final Binary rewrittenObjectBinary =
          readSingleAlignedObjectValue(targetTsFile, sourceDeviceID);
      Assert.assertEquals(
          sourceObjectPath.toString(),
          ObjectTypeUtils.parseObjectBinaryToSizeStringPathPair(rewrittenObjectBinary).getRight());
    } finally {
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
      deleteRecursively(objectRoot.toPath());
    }
  }

  private void writeTableTsFileWithTimeOnlyChunk(final File tsFile, final IDeviceID deviceID)
      throws Exception {
    writeTableTsFileWithTimeOnlyChunk(tsFile, deviceID, deviceID.getTableName());
  }

  private void writeTableTsFileWithTimeOnlyChunk(
      final File tsFile, final IDeviceID deviceID, final String tableName) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema(tableName));
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(Collections.emptyList());
      chunkWriter.write(100);
      chunkWriter.write(101);
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private void writeTableTsFileWithObjectChunk(
      final File tsFile,
      final IDeviceID deviceID,
      final String tableName,
      final Binary objectBinary)
      throws Exception {
    writeTableTsFileWithObjectChunk(tsFile, deviceID, tableName, "payload", objectBinary);
  }

  private void writeTableTsFileWithObjectChunk(
      final File tsFile,
      final IDeviceID deviceID,
      final String tableName,
      final String measurement,
      final Binary objectBinary)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createObjectSchema(tableName));
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(
              Collections.singletonList(new MeasurementSchema(measurement, TSDataType.OBJECT)));
      chunkWriter.getTimeChunkWriter().write(100L);
      chunkWriter.getValueChunkWriterByIndex(0).write(100L, objectBinary, false);
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private void writeTableTsFileWithFieldChunk(
      final File tsFile, final IDeviceID deviceID, final String tableName, final String measurement)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema(tableName, measurement));
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(
              Collections.singletonList(new MeasurementSchema(measurement, TSDataType.INT64)));
      chunkWriter.getTimeChunkWriter().write(100L);
      chunkWriter.getValueChunkWriterByIndex(0).write(100L, 1L, false);
      chunkWriter.getTimeChunkWriter().write(101L);
      chunkWriter.getValueChunkWriterByIndex(0).write(101L, 2L, false);
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private void writeSingleChunkDataToTableFile(
      final File targetTsFile,
      final ChunkData chunkData,
      final Schema schema,
      final IDeviceID deviceID)
      throws Exception {
    if (targetTsFile.exists()) {
      Assert.assertTrue(targetTsFile.delete());
    }
    try (final TsFileIOWriter writer = new TsFileIOWriter(targetTsFile)) {
      writer.setSchema(schema);
      writer.startChunkGroup(deviceID);
      writeSerializedChunkDataToWriter(chunkData, writer);
      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private Binary readSingleAlignedObjectValue(final File tsFile, final IDeviceID deviceID)
      throws Exception {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      final List<AbstractAlignedChunkMetadata> alignedChunkMetadataList =
          reader.getAlignedChunkMetadata(deviceID, true);
      Assert.assertEquals(1, alignedChunkMetadataList.size());

      final AbstractAlignedChunkMetadata alignedChunkMetadata = alignedChunkMetadataList.get(0);
      final Chunk timeChunk =
          reader.readMemChunk((ChunkMetadata) alignedChunkMetadata.getTimeChunkMetadata());
      final List<Chunk> valueChunks = new ArrayList<>();
      for (final IChunkMetadata valueChunkMetadata :
          alignedChunkMetadata.getValueChunkMetadataList()) {
        valueChunks.add(reader.readMemChunk((ChunkMetadata) valueChunkMetadata));
      }
      final AlignedChunkReader chunkReader = new AlignedChunkReader(timeChunk, valueChunks);
      Assert.assertTrue(chunkReader.hasNextSatisfiedPage());
      final BatchData batchData = chunkReader.nextPageData();
      Assert.assertTrue(batchData.hasCurrent());
      Assert.assertNotNull(batchData.getVector()[0]);
      return batchData.getVector()[0].getBinary();
    }
  }

  private void writeObjectFile(
      final File objectRoot, final String relativePath, final byte[] objectBytes) throws Exception {
    final Path objectPath = objectRoot.toPath().resolve(relativePath);
    Files.createDirectories(objectPath.getParent());
    Files.write(objectPath, objectBytes);
  }

  private Schema createSchema() {
    return createSchema("table1");
  }

  private Schema createSchema(final String tableName) {
    return createSchema(tableName, "s1");
  }

  private Schema createSchema(final String tableName, final String measurement) {
    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema(measurement, TSDataType.INT64));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema(tableName, tableSchemaList, columnCategoryList));
    return schema;
  }

  private Schema createObjectSchema(final String tableName) {
    return createObjectSchema(tableName, "payload");
  }

  private Schema createObjectSchema(final String tableName, final String measurement) {
    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema(measurement, TSDataType.OBJECT));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema(tableName, tableSchemaList, columnCategoryList));
    return schema;
  }

  private void deleteRecursively(final Path path) throws Exception {
    if (!Files.exists(path)) {
      return;
    }
    final List<Path> paths = new ArrayList<>();
    Files.walk(path).forEach(paths::add);
    Collections.reverse(paths);
    for (final Path p : paths) {
      Files.deleteIfExists(p);
    }
  }

  private void writeSerializedChunkDataToWriter(
      final ChunkData chunkData, final TsFileIOWriter writer) throws Exception {
    final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (final DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      chunkData.serialize(dataOutputStream);
    }
    ((ChunkData)
            TsFileData.deserialize(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())))
        .writeToFileWriter(writer);
  }
}
