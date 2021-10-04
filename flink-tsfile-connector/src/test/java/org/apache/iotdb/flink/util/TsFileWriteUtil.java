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

package org.apache.iotdb.flink.util;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.io.File;

/** Utils used to prepare TsFiles for testing. */
public class TsFileWriteUtil {

  public static final String TMP_DIR = "target";
  public static final String DEFAULT_TEMPLATE = "template";

  public static void create1(String tsfilePath) throws Exception {
    File f = new File(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new UnaryMeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE,
        new UnaryMeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE,
        new UnaryMeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(f, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "device_1");
    DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "device_1");
    dPoint2 = new IntDataPoint("sensor_2", 20);
    DataPoint dPoint3 = new IntDataPoint("sensor_3", 50);
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

  public static void create2(String tsfilePath) throws Exception {
    File f = new File(tsfilePath);
    if (f.exists()) {
      f.delete();
    }
    Schema schema = new Schema();
    schema.extendTemplate(
        DEFAULT_TEMPLATE, new UnaryMeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    schema.extendTemplate(
        DEFAULT_TEMPLATE,
        new UnaryMeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    schema.extendTemplate(
        DEFAULT_TEMPLATE,
        new UnaryMeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));

    TsFileWriter tsFileWriter = new TsFileWriter(f, schema);

    // construct TSRecord
    TSRecord tsRecord = new TSRecord(9, "device_1");
    DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(10, "device_1");
    dPoint2 = new IntDataPoint("sensor_2", 20);
    DataPoint dPoint3 = new IntDataPoint("sensor_3", 50);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(11, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
    dPoint2 = new IntDataPoint("sensor_2", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(12, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 51);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(14, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
    dPoint2 = new IntDataPoint("sensor_2", 10);
    dPoint3 = new IntDataPoint("sensor_3", 11);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(15, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(16, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
    dPoint2 = new IntDataPoint("sensor_2", 30);
    dPoint3 = new IntDataPoint("sensor_3", 31);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(9, "device_2");
    dPoint1 = new FloatDataPoint("sensor_1", 2.3f);
    dPoint2 = new IntDataPoint("sensor_2", 11);
    dPoint3 = new IntDataPoint("sensor_3", 19);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(10, "device_2");
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
}
