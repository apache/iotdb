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

import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.MeasurementGroup;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TsFileSketchToolTest {
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
  String alignedDevice = "root.device_1";
  String sensorPrefix = "sensor_";
  // the number of rows to include in the tablet
  int rowNum = 1000000;
  // the number of values to include in the tablet
  int sensorNum = 10;

  @Before
  public void setUp() throws Exception {
    try {
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }

      Schema schema = new Schema();

      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema measurementSchema =
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device),
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF));
      }
      // add aligned measurements into file schema
      List<MeasurementSchema> schemas = new ArrayList<>();
      List<MeasurementSchema> alignedMeasurementSchemas = new ArrayList<>();
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema schema1 =
            new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.RLE);
        schemas.add(schema1);
        alignedMeasurementSchemas.add(schema1);
      }
      MeasurementGroup group = new MeasurementGroup(true, schemas);
      schema.registerMeasurementGroup(new Path(alignedDevice), group);

      try (TsFileWriter tsFileWriter = new TsFileWriter(f, schema)) {

        // add measurements into TSFileWriter
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

        // add aligned measurements into TSFileWriter
        // construct the tablet
        tablet = new Tablet(alignedDevice, alignedMeasurementSchemas);
        timestamps = tablet.timestamps;
        values = tablet.values;
        timestamp = 1;
        value = 1000000L;
        for (int r = 0; r < rowNum; r++, value++) {
          int row = tablet.rowSize++;
          timestamps[row] = timestamp++;
          for (int i = 0; i < sensorNum; i++) {
            long[] sensor = (long[]) values[i];
            sensor[row] = value;
          }
          // write Tablet to TsFile
          if (tablet.rowSize == tablet.getMaxRowNumber()) {
            tsFileWriter.writeAligned(tablet);
            tablet.reset();
          }
        }
        // write Tablet to TsFile
        if (tablet.rowSize != 0) {
          tsFileWriter.writeAligned(tablet);
          tablet.reset();
        }
      }
    } catch (Exception e) {
      throw new Exception("meet error in TsFileWrite with tablet", e);
    }
  }

  @Test
  public void tsFileSketchToolTest() throws IOException {
    String[] args = new String[2];
    args[0] = path;
    args[1] = sketchOut;
    TsFileSketchTool tool = new TsFileSketchTool(path, sketchOut);
    List<ChunkGroupMetadata> chunkGroupMetadataList = tool.getAllChunkGroupMetadata();
    Assert.assertEquals(2, chunkGroupMetadataList.size());
    for (ChunkGroupMetadata chunkGroupMetadata : chunkGroupMetadataList) {
      if (device.equals(chunkGroupMetadata.getDevice())) {
        Assert.assertEquals(sensorNum, chunkGroupMetadata.getChunkMetadataList().size());
      } else if (alignedDevice.equals(chunkGroupMetadata.getDevice())) {
        Assert.assertEquals(sensorNum + 1, chunkGroupMetadata.getChunkMetadataList().size());
      } else {
        Assert.fail();
      }
    }
    tool.close();
  }

  @After
  public void tearDown() {
    try {
      FileUtils.forceDelete(new File(path));
      FileUtils.forceDelete(new File(sketchOut));
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }
}
