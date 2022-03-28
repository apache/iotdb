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

package org.apache.iotdb.tsfile.test1832;

import org.apache.iotdb.tsfile.Constant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.File;
import java.util.Random;

/**
 * An example of writing data with TSRecord to TsFile It uses the interface: public void
 * addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
 */
public class TsFileWrite {

  public static void main(String[] args) {

    try {
      Random random = new Random();
      String path = "/Users/samperson1997/git/iotdb/data/data/sequence/root.sg/0/1832/test5.tsfile";
      File f = FSFactoryProducer.getFSFactory().getFile(path);
      if (f.exists()) {
        f.delete();
      }

      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        // only one timeseries
        tsFileWriter.registerTimeseries(
            new Path(Constant.DEVICE_PREFIX, Constant.SENSOR_1),
            new UnaryMeasurementSchema(Constant.SENSOR_1, TSDataType.INT64, TSEncoding.RLE));

        // construct TSRecord
        for (int i = 1; i <= 7977; i++) {
          TSRecord tsRecord = new TSRecord(i, Constant.DEVICE_PREFIX);
          DataPoint dPoint1 = new LongDataPoint(Constant.SENSOR_1, random.nextLong());
          tsRecord.addTuple(dPoint1);
          // write TSRecord
          tsFileWriter.write(tsRecord);
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
