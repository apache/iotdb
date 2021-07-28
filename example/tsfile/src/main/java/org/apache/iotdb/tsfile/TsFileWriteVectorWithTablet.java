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

import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An example of writing vector type timeseries with tablet */
public class TsFileWriteVectorWithTablet {

  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteVectorWithTablet.class);

  public static void main(String[] args) throws IOException {
    try {
      String path = "test.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);

      Schema schema = new Schema();

      String device = Constant.DEVICE_PREFIX + 1;
      String sensorPrefix = "sensor_";
      String vectorName = "vector1";
      // the number of rows to include in the tablet
      int rowNum = 10000;
      // the number of vector values to include in the tablet
      int multiSensorNum = 10;
      // the number of single values to include in the tablet
      int singleSensorNum = 5;

      String[] measurementNames = new String[multiSensorNum];
      TSDataType[] dataTypes = new TSDataType[multiSensorNum];

      List<IMeasurementSchema> measurementSchemas = new ArrayList<>();
      // add measurements into file schema (all with INT64 data type)
      for (int i = 0; i < multiSensorNum; i++) {
        measurementNames[i] = sensorPrefix + (i + 1);
        dataTypes[i] = TSDataType.INT64;
      }
      IMeasurementSchema vectorMeasurementSchema =
          new VectorMeasurementSchema(vectorName, measurementNames, dataTypes);
      measurementSchemas.add(vectorMeasurementSchema);
      schema.registerTimeseries(new Path(device, vectorName), vectorMeasurementSchema);
      for (int i = 0; i < singleSensorNum; i++) {
        String mid = sensorPrefix + (multiSensorNum + i + 1);
        IMeasurementSchema measurementSchema =
            new MeasurementSchema(mid, TSDataType.INT64, TSEncoding.TS_2DIFF);
        measurementSchemas.add(measurementSchema);
        schema.registerTimeseries(
            new Path(device, mid),
            new MeasurementSchema(mid, TSDataType.INT64, TSEncoding.TS_2DIFF));
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
          for (int i = 0; i < singleSensorNum + multiSensorNum; i++) {
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
