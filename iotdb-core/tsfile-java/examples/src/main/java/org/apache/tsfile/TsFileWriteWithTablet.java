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
import org.apache.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/** An example of writing data with Tablet to TsFile */
public class TsFileWriteWithTablet {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileWriteWithTablet.class);

  public static void main(String[] args) {
    try {
      String path = "Tablet.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists()) {
        Files.delete(f.toPath());
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

        // register nonAligned timeseries
        tsFileWriter.registerTimeseries(new Path(Constant.DEVICE_1), measurementSchemas);

        // example 1
        writeWithTablet(tsFileWriter, Constant.DEVICE_1, measurementSchemas, 10000, 0, 0);
      }
    } catch (Exception e) {
      LOGGER.error("meet error in TsFileWrite with tablet", e);
    }
  }

  private static void writeWithTablet(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowNum,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    Tablet tablet = new Tablet(deviceId, schemas);
    long[] timestamps = tablet.timestamps;
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
