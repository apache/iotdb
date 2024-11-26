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

package org.apache.tsfile;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.fileSystem.FSFactoryProducer;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tsfile.Constant.DEVICE_1;
import static org.apache.tsfile.Constant.DEVICE_2;
import static org.apache.tsfile.Constant.SENSOR_1;
import static org.apache.tsfile.Constant.SENSOR_2;

public class TsFileWriteAlignedWithTablet {
  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriteAlignedWithTablet.class);

  public static void main(String[] args) throws IOException {
    File f = FSFactoryProducer.getFSFactory().getFile("alignedTablet.tsfile");
    if (f.exists()) {
      try {
        Files.delete(f.toPath());
      } catch (IOException e) {
        throw new IOException("can not delete " + f.getAbsolutePath());
      }
    }

    try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {

      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_1, TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_2, TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_3, TSDataType.INT64, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_4, TSDataType.BLOB, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_5, TSDataType.STRING, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_6, TSDataType.DATE, TSEncoding.PLAIN));
      measurementSchemas.add(
          new MeasurementSchema(Constant.SENSOR_7, TSDataType.TIMESTAMP, TSEncoding.PLAIN));

      // register align timeseries
      tsFileWriter.registerAlignedTimeseries(new Path(DEVICE_1), measurementSchemas);

      // example 1
      writeAlignedWithTablet(tsFileWriter, DEVICE_1, measurementSchemas, 10000, 0, 0);

      writeNonAlignedWithTablet(tsFileWriter); // write nonAligned timeseries
    } catch (WriteProcessException e) {
      LOGGER.error("write Tablet failed", e);
    }
  }

  private static void writeAlignedWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
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
        tablet.addValue(
            schemas.get(i).getMeasurementId(),
            row,
            DataGenerator.generate(schemas.get(i).getType(), (int) r));
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
    List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
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
