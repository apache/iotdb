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

import org.apache.iotdb.common.rpc.thrift.TTimePartitionSlot;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.utils.TimePartitionUtils;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.read.TsFileSequenceReader;
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
      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, deviceID, 100, 101);

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
  public void testSplitTableTimeOnlyAlignedChunkAtLongMaxPartitionEnd() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-long-max-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-long-max-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final long lastPartitionStartTime =
        TimePartitionUtils.getTimePartitionSlot(Long.MAX_VALUE).getStartTime();

    try {
      writeTableTsFileWithTimeOnlyChunk(
          sourceTsFile,
          deviceID,
          lastPartitionStartTime - 1,
          lastPartitionStartTime,
          Long.MAX_VALUE);

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
        writer.startChunkGroup(deviceID);
        for (final ChunkData chunkData : emittedChunkDataList) {
          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        writer.endChunkGroup();
        writer.endFile();
      }

      Assert.assertEquals(2, emittedChunkDataList.size());
      Assert.assertEquals(
          TimePartitionUtils.getTimePartitionSlot(lastPartitionStartTime - 1),
          emittedChunkDataList.get(0).getTimePartitionSlot());
      Assert.assertEquals(
          new TTimePartitionSlot(lastPartitionStartTime),
          emittedChunkDataList.get(1).getTimePartitionSlot());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(2, chunkMetadataList.size());
        Assert.assertEquals(
            3,
            chunkMetadataList.stream()
                .mapToLong(metadata -> metadata.getTimeChunkMetadata().getStatistics().getCount())
                .sum());
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
  public void testSplitTableTimeOnlyAlignedChunkAtExactLongMaxUpperBound() throws Exception {
    final File sourceTsFile = new File("split-table-time-only-exact-long-max-source.tsfile");
    final File targetTsFile = new File("split-table-time-only-exact-long-max-target.tsfile");
    final IDeviceID deviceID = new StringArrayDeviceID("table1", "tagA");
    final long previousTimePartitionOrigin =
        CommonDescriptor.getInstance().getConfig().getTimePartitionOrigin();
    final long previousTimePartitionInterval =
        CommonDescriptor.getInstance().getConfig().getTimePartitionInterval();

    try {
      CommonDescriptor.getInstance().getConfig().setTimePartitionOrigin(0);
      CommonDescriptor.getInstance().getConfig().setTimePartitionInterval(1);
      TimePartitionUtils.setTimePartitionOrigin(0);
      TimePartitionUtils.setTimePartitionInterval(1);

      writeTableTsFileWithTimeOnlyChunk(sourceTsFile, deviceID, Long.MAX_VALUE - 1, Long.MAX_VALUE);

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
        writer.startChunkGroup(deviceID);
        for (final ChunkData chunkData : emittedChunkDataList) {
          writeSerializedChunkDataToWriter(chunkData, writer);
        }
        writer.endChunkGroup();
        writer.endFile();
      }

      Assert.assertEquals(2, emittedChunkDataList.size());
      Assert.assertEquals(
          new TTimePartitionSlot(Long.MAX_VALUE - 1),
          emittedChunkDataList.get(0).getTimePartitionSlot());
      Assert.assertEquals(
          new TTimePartitionSlot(Long.MAX_VALUE),
          emittedChunkDataList.get(1).getTimePartitionSlot());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(targetTsFile.getAbsolutePath())) {
        final List<AbstractAlignedChunkMetadata> chunkMetadataList =
            reader.getAlignedChunkMetadata(deviceID, false);
        Assert.assertEquals(2, chunkMetadataList.size());
        Assert.assertEquals(
            2,
            chunkMetadataList.stream()
                .mapToLong(metadata -> metadata.getTimeChunkMetadata().getStatistics().getCount())
                .sum());
      }
    } finally {
      TimePartitionUtils.setTimePartitionOrigin(previousTimePartitionOrigin);
      TimePartitionUtils.setTimePartitionInterval(previousTimePartitionInterval);
      CommonDescriptor.getInstance()
          .getConfig()
          .setTimePartitionOrigin(previousTimePartitionOrigin);
      CommonDescriptor.getInstance()
          .getConfig()
          .setTimePartitionInterval(previousTimePartitionInterval);
      if (sourceTsFile.exists()) {
        Assert.assertTrue(sourceTsFile.delete());
      }
      if (targetTsFile.exists()) {
        Assert.assertTrue(targetTsFile.delete());
      }
    }
  }

  private void writeTableTsFileWithTimeOnlyChunk(
      final File tsFile, final IDeviceID deviceID, final long... times) throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema());
      writer.startChunkGroup(deviceID);

      final AlignedChunkWriterImpl chunkWriter =
          new AlignedChunkWriterImpl(Collections.emptyList());
      for (final long time : times) {
        chunkWriter.write(time);
      }
      chunkWriter.writeToFileWriter(writer);

      writer.endChunkGroup();
      writer.endFile();
    }
  }

  private Schema createSchema() {
    final List<IMeasurementSchema> tableSchemaList =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("s1", TSDataType.INT64));
    final List<ColumnCategory> columnCategoryList =
        Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD);

    final Schema schema = new Schema();
    schema.registerTableSchema(new TableSchema("table1", tableSchemaList, columnCategoryList));
    return schema;
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
