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
package org.apache.iotdb.tsfile;

import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.Constant.*;

public class TsFileWriteAlignedWithTablet {
  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteAlignedWithTablet.class);

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("alignedTablet.tsfile");
    if (f.exists() && !f.delete()) {
      throw new RuntimeException("can not delete " + f.getAbsolutePath());
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(new MeasurementSchema(SENSOR_1, TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema(SENSOR_2, TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(new MeasurementSchema(SENSOR_3, TSDataType.INT64, TSEncoding.PLAIN));

      // register align timeseries
      tsFileWriter.registerAlignedTimeseries(new Path(DEVICE_1), measurementSchemas);

      List<MeasurementSchema> writeMeasurementScheams = new ArrayList<>();
      // example 1
      writeMeasurementScheams.add(measurementSchemas.get(0));
      writeMeasurementScheams.add(measurementSchemas.get(1));
      writeMeasurementScheams.add(measurementSchemas.get(2));
      writeAlignedWithTablet(tsFileWriter, DEVICE_1, writeMeasurementScheams, 20000, 0, 0);

      //      writeNonAlignedWithTablet(tsFileWriter); // write nonAligned timeseries
    } catch (WriteProcessException e) {
      logger.error("write Tablet failed", e);
    }
  }

  private static void writeAlignedWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<MeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, schemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    long sensorNum = schemas.size();

    for (long r = 0; r < rowNum; r++, startValue++) {
      int row = tablet.rowSize++;
      timestamps[row] = startTime++;
      for (int i = 0; i < sensorNum; i++) {
        long[] textSensor = (long[]) values[i];
        textSensor[row] = startValue;
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        tsFileWriter.writeAligned(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      tsFileWriter.writeAligned(tablet);
      tablet.reset();
    }
  }

  private static void writeNonAlignedWithTablet(TsFileWriter tsFileWriter)
      throws WriteProcessException, IOException {
    // register nonAlign timeseries
    tsFileWriter.registerTimeseries(
        new Path(DEVICE_2), new MeasurementSchema(SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
    tsFileWriter.registerTimeseries(
        new Path(DEVICE_2), new MeasurementSchema(SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
    // construct Tablet
    List<MeasurementSchema> measurementSchemas = new ArrayList<>();
    measurementSchemas.add(new MeasurementSchema(SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
    measurementSchemas.add(new MeasurementSchema(SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
    Tablet tablet = new Tablet(DEVICE_2, measurementSchemas);
    long[] timestamps = tablet.timestamps;
    Object[] values = tablet.values;
    int rowNum = 100;
    int sensorNum = measurementSchemas.size();
    long timestamp = 1;
    long value = 1000000L;
    for (int r = 0; r < rowNum; r++, value++) {
      int row = tablet.rowSize++;
      timestamps[row] = timestamp++;
      for (int i = 0; i < sensorNum; i++) {
        long[] sensor = (long[]) values[i];
        sensor[row] = value;
      }
      // write
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        tsFileWriter.write(tablet);
        tablet.reset();
      }
    }
    // write
    if (tablet.rowSize != 0) {
      tsFileWriter.write(tablet);
      tablet.reset();
    }
  }
}
