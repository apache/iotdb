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

import java.io.File;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

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
      int rowSize = 100;
      int value = 0;
      try (TsFileWriter tsFileWriter = new TsFileWriter(f)) {
        // register timeseries
        tsFileWriter.registerTimeseries(
            new Path("root.sg.d1"),
            new UnaryMeasurementSchema("s1", TSDataType.INT64, TSEncoding.RLE));
        tsFileWriter.registerTimeseries(
            new Path("root.sg.d1"),
            new UnaryMeasurementSchema("s2", TSDataType.INT64, TSEncoding.RLE));
        for (int time = 0; time < rowSize; time++) {
          // construct TsRecord
          TSRecord tsRecord = new TSRecord(time, "root.sg.d1");
          DataPoint dPoint1 = new LongDataPoint("s1", value++);
          DataPoint dPoint2 = new LongDataPoint("s2", value++);
          tsRecord.addTuple(dPoint1);
          tsRecord.addTuple(dPoint2);
          // write
          tsFileWriter.write(tsRecord);
        }
        for (int time = rowSize; time < rowSize + rowSize; time++) {
          // construct TsRecord
          TSRecord tsRecord = new TSRecord(time, "root.sg.d1");
          DataPoint dPoint2 = new LongDataPoint("s2", value++);
          tsRecord.addTuple(dPoint2);
          // write
          tsFileWriter.write(tsRecord);
        }
      }
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
