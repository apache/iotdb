/**
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
package org.apache.iotdb.tool;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.IoTDBFile;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.BooleanDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

/**
 * An example of writing data to TsFile
 */
public class TsFileWriteTool {

  public static int largeNum = 1024 * 1024;

  public void create1(String tsfilePath) throws Exception {
    IoTDBFile f = new IoTDBFile(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);

    // add measurements into file schema
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "device_1");
    DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
    DataPoint dPoint3;
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "device_1");
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 50);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
    dPoint2 = new IntDataPoint("sensor_2", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 51);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(6, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
    dPoint2 = new IntDataPoint("sensor_2", 10);
    dPoint3 = new IntDataPoint("sensor_3", 11);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(7, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(8, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
    dPoint2 = new IntDataPoint("sensor_2", 30);
    dPoint3 = new IntDataPoint("sensor_3", 31);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(1, "device_2");
    dPoint1 = new FloatDataPoint("sensor_1", 2.3f);
    dPoint2 = new IntDataPoint("sensor_2", 11);
    dPoint3 = new IntDataPoint("sensor_3", 19);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "device_2");
    dPoint1 = new FloatDataPoint("sensor_1", 25.4f);
    dPoint2 = new IntDataPoint("sensor_2", 10);
    dPoint3 = new IntDataPoint("sensor_3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }

  public void create2(String tsfilePath) throws Exception {
    IoTDBFile f = new IoTDBFile(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);

    // add measurements into file schema
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    for (long i = 0; i < largeNum; i++) {
      // construct TSRecord
      TSRecord tsRecord = new TSRecord(i, "device_1");
      DataPoint dPoint1 = new FloatDataPoint("sensor_1", (float) i);
      tsRecord.addTuple(dPoint1);
      // write a TSRecord to TsFile
      tsFileWriter.write(tsRecord);
    }
    // close TsFile
    tsFileWriter.close();
  }

  public void create3(String tsfilePath) throws Exception {
    IoTDBFile f = new IoTDBFile(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);

    // add measurements into file schema
    // NOTE the measurments here are different from those defined in create1 and
    // create2 function, despite their names are the same.
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.BOOLEAN, TSEncoding.RLE));
    tsFileWriter
        .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.TEXT, TSEncoding.PLAIN));

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "device_1");
    DataPoint dPoint1 = new BooleanDataPoint("sensor_1", true);
    DataPoint dPoint2 = new StringDataPoint("sensor_2", new Binary("Monday"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "device_1");
    dPoint2 = new StringDataPoint("sensor_2", new Binary("Tuesday"));
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "device_1");
    dPoint1 = new BooleanDataPoint("sensor_1", false);
    dPoint2 = new StringDataPoint("sensor_2", new Binary("Wednesday"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "device_1");
    dPoint1 = new BooleanDataPoint("sensor_1", false);
    dPoint2 = new StringDataPoint("sensor_2", new Binary("Thursday"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(6, "device_1");
    dPoint1 = new BooleanDataPoint("sensor_1", true);
    dPoint2 = new StringDataPoint("sensor_2", new Binary("Saturday"));
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }
}