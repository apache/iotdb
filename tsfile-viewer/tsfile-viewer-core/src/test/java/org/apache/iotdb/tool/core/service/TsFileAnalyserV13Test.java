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

package org.apache.iotdb.tool.core.service;

import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsFileAnalyserV13Test {

  private static final String FILE_PATH = "TsFileIOWriterTest.tsfile";
  private static final String DEVICE_1 = "device1";
  private static final String DEVICE_2 = "device2";
  private static final String SENSOR_1 = "sensor1";

  private static final int CHUNK_GROUP_NUM = 2;

  @Before
  public void before() throws IOException {
    TsFileIOWriter writer = new TsFileIOWriter(new File(FILE_PATH));

    // file schema
    MeasurementSchema measurementSchema = createSimpleMeasurementSchema(SENSOR_1);
    VectorMeasurementSchema vectorMeasurementSchema =
        new VectorMeasurementSchema(
            "", new String[] {"s1", "s2"}, new TSDataType[] {TSDataType.INT64, TSDataType.INT64});
    List<MeasurementSchema> schemas = new ArrayList<>();
    schemas.add(new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
    schemas.add(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
    MeasurementGroup group = new MeasurementGroup(true, schemas);

    Schema schema = new Schema();
    schema.registerTimeseries(new Path(DEVICE_1), measurementSchema);
    schema.registerMeasurementGroup(new Path(DEVICE_2), group);

    writeChunkGroup(writer, measurementSchema);
    writeVectorChunkGroup(writer, vectorMeasurementSchema);
    writer.setMinPlanIndex(100);
    writer.setMaxPlanIndex(10000);
    writer.writePlanIndices();
    // end file
    writer.endFile();
  }

  private MeasurementSchema createSimpleMeasurementSchema(String measurementuid) {
    return new MeasurementSchema(measurementuid, TSDataType.INT64, TSEncoding.RLE);
  }

  @After
  public void after() {
    File file = new File(FILE_PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testTsFileAnalyserV13ForAligned() throws IOException, InterruptedException {

    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13(FILE_PATH);
    tsFileAnalyserV13.getTimeSeriesMetadataNode();
    // 是否有序
    assertTrue(tsFileAnalyserV13.isSeq());
    // 进度条完成
    assertEquals(1.0, tsFileAnalyserV13.getRateOfProcess(), 0);
    // 是否AlignedChunkMetadata
    assertTrue(tsFileAnalyserV13.getChunkGroupInfoList().get(3).getDeviceName().equals(DEVICE_2));
    // 判断node节点
    assertTrue(tsFileAnalyserV13.getTimeSeriesMetadataNode().getChildren().size() > 0);
  }

  @Test
  public void testTsFileAnalyserV13ForNonAligned() throws IOException, InterruptedException {

    TsFileAnalyserV13 tsFileAnalyserV13 = new TsFileAnalyserV13(FILE_PATH);
    tsFileAnalyserV13.getTimeSeriesMetadataNode();
    // 是否有序
    assertTrue(tsFileAnalyserV13.isSeq());
    // 进度条完成
    assertEquals(1.0, tsFileAnalyserV13.getRateOfProcess(), 0);
    // 是否AlignedChunkMetadata
    assertTrue(tsFileAnalyserV13.getChunkGroupInfoList().get(0).getDeviceName().equals(DEVICE_1));
    // 判断node节点
    assertTrue(tsFileAnalyserV13.getTimeSeriesMetadataNode().getChildren().size() > 0);
  }

  private void writeChunkGroup(TsFileIOWriter writer, MeasurementSchema measurementSchema)
      throws IOException {
    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group
      writer.startChunkGroup(DEVICE_1);
      // ordinary chunk, chunk statistics
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
      writer.endChunkGroup();
    }
  }

  private void writeVectorChunkGroup(
      TsFileIOWriter writer, VectorMeasurementSchema vectorMeasurementSchema) throws IOException {
    for (int i = 0; i < CHUNK_GROUP_NUM; i++) {
      // chunk group
      writer.startChunkGroup(DEVICE_2);
      // vector chunk (time)
      writer.startFlushChunk(
          vectorMeasurementSchema.getMeasurementId(),
          vectorMeasurementSchema.getCompressor(),
          vectorMeasurementSchema.getType(),
          vectorMeasurementSchema.getTimeTSEncoding(),
          Statistics.getStatsByType(vectorMeasurementSchema.getType()),
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
            vectorMeasurementSchema.getSubMeasurementsList().get(j),
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
