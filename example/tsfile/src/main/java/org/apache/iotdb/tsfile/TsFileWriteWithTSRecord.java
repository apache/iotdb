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
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.tsfile.Constant.DEVICE_1;
import static org.apache.iotdb.tsfile.Constant.SENSOR_1;
import static org.apache.iotdb.tsfile.Constant.SENSOR_2;
import static org.apache.iotdb.tsfile.Constant.SENSOR_3;

/**
 * An example of writing data with TSRecord to TsFile It uses the interface: public void
 * addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
 */
public class TsFileWriteWithTSRecord {

  public static void main(String[] args) {
    try {
      String path = "Record.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists()) {
        f.delete();
      }

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        List<MeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema(SENSOR_1, TSDataType.INT64, TSEncoding.RLE));
        schemas.add(new MeasurementSchema(SENSOR_2, TSDataType.INT64, TSEncoding.RLE));
        schemas.add(new MeasurementSchema(SENSOR_3, TSDataType.INT64, TSEncoding.RLE));

        // register timeseries
        tsFileWriter.registerTimeseries(new Path(DEVICE_1), schemas);

        List<IMeasurementSchema> writeMeasurementScheams = new ArrayList<>();
        // example1
        writeMeasurementScheams.add(schemas.get(0));
        writeMeasurementScheams.add(schemas.get(1));
        writeMeasurementScheams.add(schemas.get(2));
        write(tsFileWriter, DEVICE_1, writeMeasurementScheams, 10000, 0, 0);
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }

  private static void write(
      TsFileWriter tsFileWriter,
      String deviceId,
      List<IMeasurementSchema> schemas,
      long rowSize,
      long startTime,
      long startValue)
      throws IOException, WriteProcessException {
    for (long time = startTime; time < rowSize + startTime; time++) {
      // construct TsRecord
      TSRecord tsRecord = new TSRecord(time, deviceId);
      for (IMeasurementSchema schema : schemas) {
        DataPoint dPoint = new LongDataPoint(schema.getMeasurementId(), startValue++);
        tsRecord.addTuple(dPoint);
      }
      // write
      tsFileWriter.write(tsRecord);
    }
  }
}
