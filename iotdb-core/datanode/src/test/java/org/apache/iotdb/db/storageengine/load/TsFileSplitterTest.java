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

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.MetaMarker;
import org.apache.tsfile.file.metadata.AbstractAlignedChunkMetadata;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MeasurementMetadataIndexEntry;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.file.metadata.statistics.Statistics;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
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
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class TsFileSplitterTest {

  // Verify the splitter initializes the v3 deserialize configuration for a valid v3 TsFile.
  @Test
  public void testSplitV3TsFile() throws Exception {
    final File sourceTsFile = constructV3TsFile();

    try {
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(sourceTsFile.getAbsolutePath())) {
        Assert.assertEquals(2, reader.getAllTimeseriesMetadata(true).size());
      }

      // Verify the buffered reader initializes the v3 deserialize configuration before reading
      // metadata.
      new TsFileSplitter(sourceTsFile, tsFileData -> true).splitTsFileByDataPartition();
    } finally {
      Assert.assertTrue(sourceTsFile.delete());
    }
  }

  private File constructV3TsFile() throws IOException {
    final File tsFile = Files.createTempFile("v3-tsfile-splitter", ".tsfile").toFile();
    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(TSFileConfig.MAGIC_STRING.getBytes());
    outputStream.write(TSFileConfig.VERSION_NUMBER_V3);

    final long metaOffset = outputStream.size();
    outputStream.write(MetaMarker.SEPARATOR);

    final MetadataIndexNode deviceIndexNode =
        new MetadataIndexNode(MetadataIndexNodeType.LEAF_DEVICE);
    for (final String device : Arrays.asList("root.sg.d1", "root.sg.d2")) {
      final long timeseriesMetadataOffset = outputStream.size();
      writeV3TimeseriesMetadata(outputStream);

      final long measurementIndexOffset = outputStream.size();
      final MetadataIndexNode measurementIndexNode =
          new MetadataIndexNode(MetadataIndexNodeType.LEAF_MEASUREMENT);
      measurementIndexNode.addEntry(
          new MeasurementMetadataIndexEntry("s1", timeseriesMetadataOffset));
      measurementIndexNode.setEndOffset(measurementIndexOffset);
      measurementIndexNode.serializeTo(outputStream);

      deviceIndexNode.addEntry(
          new DeviceMetadataIndexEntry(new PlainDeviceID(device), measurementIndexOffset));
    }
    deviceIndexNode.setEndOffset(outputStream.size());

    int metadataSize = deviceIndexNode.serializeTo(outputStream);
    metadataSize += ReadWriteIOUtils.write(metaOffset, outputStream);
    ReadWriteIOUtils.write(metadataSize, outputStream);
    outputStream.write(TSFileConfig.MAGIC_STRING.getBytes());
    Files.write(tsFile.toPath(), outputStream.toByteArray());
    return tsFile;
  }

  private void writeV3TimeseriesMetadata(final ByteArrayOutputStream outputStream)
      throws IOException {
    final Statistics<?> statistics = Statistics.getStatsByType(TSDataType.INT32);
    statistics.update(1, 1);

    final TimeseriesMetadata timeseriesMetadata = new TimeseriesMetadata();
    timeseriesMetadata.setTimeSeriesMetadataType((byte) 0);
    timeseriesMetadata.setMeasurementId("s1");
    timeseriesMetadata.setTsDataType(TSDataType.INT32);
    timeseriesMetadata.setDataSizeOfChunkMetaDataList(0);
    timeseriesMetadata.setStatistics(statistics);
    timeseriesMetadata.setChunkMetadataListBuffer(new PublicBAOS());
    timeseriesMetadata.serializeTo(outputStream);
  }

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

  private void writeTableTsFileWithTimeOnlyChunk(final File tsFile, final IDeviceID deviceID)
      throws Exception {
    if (tsFile.exists()) {
      Assert.assertTrue(tsFile.delete());
    }

    try (final TsFileIOWriter writer = new TsFileIOWriter(tsFile)) {
      writer.setSchema(createSchema());
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
