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
package org.apache.iotdb.tsfile.write;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.metadata.TimeSeriesMetadataTest;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TsFileIOWriterTest {

  private static String tsfile = TestConstant.BASE_OUTPUT_PATH.concat("tsfileIOWriterTest.tsfile");
  private static String deviceId = "device1";
  private static int chunkGroupNum = 2;

  @Before
  public void before() throws IOException {
    TsFileIOWriter writer = new TsFileIOWriter(new File(tsfile));

    // file schema
    UnaryMeasurementSchema measurementSchema = TestHelper.createSimpleMeasurementSchema("sensor01");
    VectorMeasurementSchema vectorMeasurementSchema =
        new VectorMeasurementSchema(
            "vector",
            new String[] {"s1", "s2"},
            new TSDataType[] {TSDataType.INT64, TSDataType.INT64});
    Schema schema = new Schema();
    schema.registerTimeseries(new Path(deviceId, "sensor01"), measurementSchema);
    schema.registerTimeseries(new Path(deviceId, "vector"), vectorMeasurementSchema);

    writeChunkGroup(writer, measurementSchema, vectorMeasurementSchema, chunkGroupNum);

    writer.setMinPlanIndex(100);
    writer.setMaxPlanIndex(10000);
    writer.writePlanIndices();
    // end file
    writer.endFile();
  }

  @After
  public void after() {
    File file = new File(tsfile);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void endFileTest() throws IOException {
    TsFileSequenceReader reader = new TsFileSequenceReader(tsfile);

    // magic_string
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readHeadMagic());
    Assert.assertEquals(TSFileConfig.VERSION_NUMBER, reader.readVersionNumber());
    Assert.assertEquals(TSFileConfig.MAGIC_STRING, reader.readTailMagic());

    reader.position(TSFileConfig.MAGIC_STRING.getBytes().length + 1);

    ChunkHeader header;
    ChunkGroupHeader chunkGroupHeader;
    for (int i = 0; i < chunkGroupNum; i++) {
      // chunk group header
      Assert.assertEquals(MetaMarker.CHUNK_GROUP_HEADER, reader.readMarker());
      chunkGroupHeader = reader.readChunkGroupHeader();
      Assert.assertEquals(deviceId, chunkGroupHeader.getDeviceID());
      // ordinary chunk header
      Assert.assertEquals(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER, reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      Assert.assertEquals(TimeSeriesMetadataTest.measurementUID, header.getMeasurementID());
      // vector chunk header (time)
      Assert.assertEquals(
          MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.TIME_COLUMN_MASK,
          reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      Assert.assertEquals("vector", header.getMeasurementID());
      // vector chunk header (values)
      Assert.assertEquals(
          MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK,
          reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      Assert.assertEquals("vector.s1", header.getMeasurementID());
      Assert.assertEquals(
          MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER | TsFileConstant.VALUE_COLUMN_MASK,
          reader.readMarker());
      header = reader.readChunkHeader(MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER);
      Assert.assertEquals("vector.s2", header.getMeasurementID());
    }

    Assert.assertEquals(MetaMarker.OPERATION_INDEX_RANGE, reader.readMarker());
    reader.readPlanIndex();
    Assert.assertEquals(100, reader.getMinPlanIndex());
    Assert.assertEquals(10000, reader.getMaxPlanIndex());

    Assert.assertEquals(MetaMarker.SEPARATOR, reader.readMarker());

    // make sure timeseriesMetadata is only
    Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap =
        reader.getAllTimeseriesMetadata();
    Set<String> pathSet = new HashSet<>();
    for (Map.Entry<String, List<TimeseriesMetadata>> entry :
        deviceTimeseriesMetadataMap.entrySet()) {
      for (TimeseriesMetadata timeseriesMetadata : entry.getValue()) {
        String seriesPath = entry.getKey() + "." + timeseriesMetadata.getMeasurementId();
        Assert.assertTrue(!pathSet.contains(seriesPath));
        pathSet.add(seriesPath);
      }
    }

    // FileMetaData
    TsFileMetadata metaData = reader.readFileMetadata();
    Assert.assertEquals(1, metaData.getMetadataIndex().getChildren().size());
  }

  private void writeChunkGroup(
      TsFileIOWriter writer,
      UnaryMeasurementSchema measurementSchema,
      VectorMeasurementSchema vectorMeasurementSchema,
      int groupNum)
      throws IOException {
    for (int i = 0; i < groupNum; i++) {
      // chunk group
      writer.startChunkGroup(deviceId);
      // ordinary chunk
      // chunk statistics
      Statistics statistics = Statistics.getStatsByType(measurementSchema.getType());
      statistics.updateStats(0L, 0L);
      writer.startFlushChunk(
          measurementSchema.getMeasurementId(),
          measurementSchema.getCompressor(),
          measurementSchema.getType(),
          measurementSchema.getEncodingType(),
          statistics,
          0,
          0,
          0);
      writer.endCurrentChunk();
      // vector chunk (time)
      Statistics vectorStatistics = Statistics.getStatsByType(vectorMeasurementSchema.getType());
      writer.startFlushChunk(
          vectorMeasurementSchema.getMeasurementId(),
          vectorMeasurementSchema.getCompressor(),
          vectorMeasurementSchema.getType(),
          vectorMeasurementSchema.getTimeTSEncoding(),
          vectorStatistics,
          0,
          0,
          TsFileConstant.TIME_COLUMN_MASK);
      writer.endCurrentChunk();
      // vector chunk (values)
      for (int j = 0; j < vectorMeasurementSchema.getSubMeasurementsCount(); j++) {
        Statistics subStatistics =
            Statistics.getStatsByType(
                vectorMeasurementSchema.getSubMeasurementsTSDataTypeList().get(j));
        subStatistics.updateStats(0L, 0L);
        writer.startFlushChunk(
            vectorMeasurementSchema.getMeasurementId()
                + "."
                + vectorMeasurementSchema.getSubMeasurementsList().get(j),
            vectorMeasurementSchema.getCompressor(),
            vectorMeasurementSchema.getSubMeasurementsTSDataTypeList().get(j),
            vectorMeasurementSchema.getSubMeasurementsTSEncodingList().get(j),
            subStatistics,
            0,
            0,
            TsFileConstant.VALUE_COLUMN_MASK);
        writer.endCurrentChunk();
      }
      writer.endChunkGroup();
    }
  }
}
