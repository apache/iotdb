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
package org.apache.iotdb.db.tools;

import org.apache.iotdb.db.exception.TsFileTimeseriesMetadataException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TsFileSelfCheckToolTest {

  String path =
      "target"
          .concat(File.separator)
          .concat("data")
          .concat(File.separator)
          .concat("sequence")
          .concat(File.separator)
          .concat("root.sg1")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator)
          .concat("0")
          .concat(File.separator);
  String device = "root.device_0";

  private static final Logger logger = LoggerFactory.getLogger(TsFileSelfCheckToolTest.class);

  public void setUp(String filePath) throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(filePath);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }

      Schema schema = new Schema();

      String sensorPrefix = "sensor_";
      // the number of rows to include in the tablet
      int rowNum = 1000000;
      // the number of values to include in the tablet
      int sensorNum = 10;

      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema measurementSchema =
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device),
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
      }

      // add measurements into TSFileWriter
      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

        // construct the tablet
        Tablet tablet = new Tablet(device, measurementSchemas);

        long[] timestamps = tablet.timestamps;
        Object[] values = tablet.values;

        long timestamp = 1;
        long value = 1000000L;

        for (int r = 0; r < rowNum; r++, value++) {
          int row = tablet.rowSize++;
          timestamps[row] = timestamp++;
          for (int i = 0; i < sensorNum; i++) {
            long[] sensor = (long[]) values[i];
            sensor[row] = value;
          }
          // write Tablet to TsFile
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.write(tablet);
            tablet.reset();
          }
        }
        // write Tablet to TsFile
        if (tablet.rowSize != 0) {
          tsFileWriter.write(tablet);
          tablet.reset();
        }
      }
    } catch (Exception e) {
      throw new Exception("meet error in TsFileWrite with tablet", e);
    }
  }

  @Test
  public void tsFileSelfCheckToolCompleteTest() throws Exception {
    String fileName = "1-0-0-1.tsfile";
    String filePath = path.concat(fileName);
    setUp(filePath);
    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(filePath, false);
    } catch (IOException
        | TsFileStatisticsMistakesException
        | TsFileTimeseriesMetadataException e) {
      fail(e.getMessage());
    }
    tearDown(filePath);
  }

  @Test
  public void tsFileSelfCheckToolWithStatisticsModifiedTest()
      throws IOException, TsFileTimeseriesMetadataException, Exception {
    String fileName = "1-0-0-2.tsfile";
    String filePath = path.concat(fileName);
    setUp(filePath);
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        new TsFileSelfCheckTool().getTimeseriesMetadataMapWithPath(filePath);
    for (Map.Entry<Long, Pair<Path, TimeseriesMetadata>> entry : timeseriesMetadataMap.entrySet()) {
      TimeseriesMetadata timeseriesMetadata = entry.getValue().right;
      Long pos = entry.getKey();
      LongStatistics statistics = (LongStatistics) timeseriesMetadata.getStatistics();
      statistics.initializeStats(666, 1999999, 1000000, 1999999, 0);

      RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      int serialLength = ReadWriteIOUtils.write(timeseriesMetadata.getTimeSeriesMetadataType(), bo);
      serialLength += ReadWriteIOUtils.writeVar(timeseriesMetadata.getMeasurementId(), bo);
      serialLength += ReadWriteIOUtils.write(timeseriesMetadata.getTsDataType(), bo);
      serialLength +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              timeseriesMetadata.getDataSizeOfChunkMetaDataList(), bo);
      serialLength += statistics.serialize(bo);
      logger.info("serialLength: " + serialLength);
      byte[] serialArr = bo.toByteArray();
      raf.seek(pos);
      raf.write(serialArr, 0, serialArr.length);
      bo.close();
      raf.close();

      // We only modify one statistics of TimeseriesMetadata in TsFile to test the check method, so
      // we break here
      break;
    }

    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(filePath, false);
      fail("No exception thrown.");
    } catch (TsFileStatisticsMistakesException e) {
      // In fact, what we are modifying is the Statistics of TimeseriesMetadata. It should be
      // reported that
      // TimeseriesMetadata is inconsistent with the Statistics of the subsequent ChunkMetadata
      // aggregation statistics.
      // But because the self check method first checks the aggregate statistics of ChunkMetadata
      // and the page behind
      // the chunk at its index position and TsFile is initialized to TimeseriesMetadata and
      // followed by a
      // ChunkMetadata, the Statistics of ChunkMetadata here uses the Statistics of
      // TimeseriesMetadata.
      // Therefore, Chunk's Statistics error will be reported.
      assertEquals("Chunk exists statistics mistakes at position 23", e.getMessage());
    }
    tearDown(filePath);
  }

  @Test
  public void tsFileSelfCheckToolWithRandomModifiedTest() throws IOException, Exception {

    String fileName = "1-0-0-3.tsfile";
    String filePath = path.concat(fileName);
    setUp(filePath);

    RandomAccessFile raf = new RandomAccessFile(filePath, "rw");
    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(100, bo);
    byte[] serialArr = bo.toByteArray();
    // timeseriesMetadata begins at 878364
    // randomly modify timeseriesMetadata region
    raf.seek(966123);
    raf.write(serialArr, 0, serialArr.length);
    bo.close();
    raf.close();

    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(filePath, false);
      fail("No exception thrown.");
    } catch (TsFileTimeseriesMetadataException e) {
      assertEquals(
          "Error occurred while getting all TimeseriesMetadata with offset in TsFile.",
          e.getMessage());
    }
    tearDown(filePath);
  }

  public void tearDown(String filePath) {
    try {
      FileUtils.forceDelete(new File(filePath));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
