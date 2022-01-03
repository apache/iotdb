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

import org.apache.iotdb.tsfile.exception.TsFileStatisticsMistakesException;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.LongStatistics;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class TsFileSelfCheckToolTest {

  String path =
      "data"
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
          .concat(File.separator)
          .concat("1-0-0-0.tsfile");
  String sketchOut = "sketch.out";
  String device = "root.device_0";

  @Before
  public void setUp() throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
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
        IMeasurementSchema measurementSchema =
            new UnaryMeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device),
            new UnaryMeasurementSchema(
                sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
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
  public void tsFileSelfCheckToolCompleteTest() {
    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(path, false);
    } catch (IOException | TsFileStatisticsMistakesException e) {
      fail(e.getMessage());
    }
  }

  @Test
  public void tsFileSelfCheckToolWithStatisticsModifiedTest() throws IOException {
    Map<Long, Pair<Path, TimeseriesMetadata>> timeseriesMetadataMap =
        new TsFileSelfCheckTool().getTimeseriesMetadataMap(path);
    for (Map.Entry<Long, Pair<Path, TimeseriesMetadata>> entry : timeseriesMetadataMap.entrySet()) {
      TimeseriesMetadata timeseriesMetadata = entry.getValue().right;
      Long pos = entry.getKey();
      LongStatistics statistics = (LongStatistics) timeseriesMetadata.getStatistics();
      statistics.initializeStats(666, 1999999, 1000000, 1999999, 0);

      RandomAccessFile raf = new RandomAccessFile(path, "rw");
      ByteArrayOutputStream bo = new ByteArrayOutputStream();
      int serialLength = ReadWriteIOUtils.write(timeseriesMetadata.getTimeSeriesMetadataType(), bo);
      serialLength += ReadWriteIOUtils.writeVar(timeseriesMetadata.getMeasurementId(), bo);
      serialLength += ReadWriteIOUtils.write(timeseriesMetadata.getTSDataType(), bo);
      serialLength +=
          ReadWriteForEncodingUtils.writeUnsignedVarInt(
              timeseriesMetadata.getDataSizeOfChunkMetaDataList(), bo);
      serialLength += statistics.serialize(bo);
      System.out.println("serialLength: " + serialLength);
      byte[] serialArr = bo.toByteArray();
      raf.seek(pos);
      raf.write(serialArr, 0, serialArr.length);
      raf.close();

      break;
    }

    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(path, false);
      fail("No exception thrown.");
    } catch (TsFileStatisticsMistakesException e) {
      assertEquals("Chunk exists statistics mistakes at position 22", e.getMessage());
    }
  }

  @Test
  public void tsFileSelfCheckToolWithRandomModifiedTest() throws IOException {

    RandomAccessFile raf = new RandomAccessFile(path, "rw");
    ByteArrayOutputStream bo = new ByteArrayOutputStream();
    ReadWriteIOUtils.write(100, bo);
    byte[] serialArr = bo.toByteArray();
    // timeseriesMetadata begins at 878364
    // randomly modify timeseriesMetadata region
    raf.seek(878375);
    raf.write(serialArr, 0, serialArr.length);
    raf.close();

    TsFileSelfCheckTool tool = new TsFileSelfCheckTool();
    try {
      tool.check(path, false);
      fail("No exception thrown.");
    } catch (BufferUnderflowException e) {
      assertNull(e.getMessage());
    }
  }

  @After
  public void tearDown() {
    try {
      FileUtils.forceDelete(new File(path));
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }
}
