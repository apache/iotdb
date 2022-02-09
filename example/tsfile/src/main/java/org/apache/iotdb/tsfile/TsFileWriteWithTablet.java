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
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** An example of writing data with Tablet to TsFile */
public class TsFileWriteWithTablet {

  private static final Logger logger = LoggerFactory.getLogger(TsFileWriteWithTablet.class);

  public static void main(String[] args) {
    try {
      String path = "Tablet.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists() && !f.delete()) {
        throw new RuntimeException("can not delete " + f.getAbsolutePath());
      }
      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        List<UnaryMeasurementSchema> measurementSchemas = new ArrayList<>();
        measurementSchemas.add(new UnaryMeasurementSchema("s1", TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new UnaryMeasurementSchema("s2", TSDataType.TEXT, TSEncoding.PLAIN));
        measurementSchemas.add(new UnaryMeasurementSchema("s3", TSDataType.TEXT, TSEncoding.PLAIN));

        // register nonAligned timeseries
        tsFileWriter.registerTimeseries(new Path("root.sg.d1"), measurementSchemas);

        List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
        // example 1
        writeMeasurementScheams.add(measurementSchemas.get(0));
        writeMeasurementScheams.add(measurementSchemas.get(1));
        writeMeasurementScheams.add(measurementSchemas.get(2));
        writeWithTablet(tsFileWriter, "root.sg.d1", writeMeasurementScheams, 10000, 0, 0);
      }
    } catch (Exception e) {
      logger.error("meet error in TsFileWrite with tablet", e);
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
    Object[] values = tablet.values;
    long sensorNum = schemas.size();

    for (long r = 0; r < rowNum; r++, startValue++) {
      int row = tablet.rowSize++;
      timestamps[row] = startTime++;
      for (int i = 0; i < sensorNum; i++) {
        Binary[] textSensor = (Binary[]) values[i];
        textSensor[row] = new Binary("testString.........");
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
