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

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of writing data with Tablet to TsFile
 */
public class TsFileWriteWithTablet {

  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteWithTablet.class);

  public static void main(String[] args) {
    try {
      String path = "test.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists()) {
        if (!f.delete()) {
          throw new RuntimeException("can not delete " + f.getAbsolutePath());
        }
      }

      Schema schema = new Schema();

      String device = "root.sg.device_1";
      String sensorPrefix = "sensor_";
      // the number of rows to include in the tablet
      int rowNum = 1000000;
      // the number of values to include in the tablet
      int sensorNum = 10;

      List<MeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < sensorNum; i++) {
        MeasurementSchema measurementSchema = new MeasurementSchema(sensorPrefix + (i + 1), TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(new Path(device, sensorPrefix + (i + 1)),
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
      logger.error("meet error in TsFileWrite with tablet", e);
    }
  }
}
