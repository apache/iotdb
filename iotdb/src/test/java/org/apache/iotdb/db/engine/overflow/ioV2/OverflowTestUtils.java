/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.overflow.ioV2;

import java.io.IOException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class OverflowTestUtils {

  public static String deviceId1 = "d1";
  public static String deviceId2 = "d2";
  public static String measurementId1 = "s1";
  public static String measurementId2 = "s2";
  public static TSDataType dataType1 = TSDataType.INT32;
  public static TSDataType dataType2 = TSDataType.FLOAT;
  private static FileSchema fileSchema = new FileSchema();

  static {
    fileSchema
        .registerMeasurement(new MeasurementSchema(measurementId1, dataType1, TSEncoding.PLAIN));
    fileSchema
        .registerMeasurement(new MeasurementSchema(measurementId2, dataType2, TSEncoding.PLAIN));
  }

  public static FileSchema getFileSchema() {
    return fileSchema;
  }

  public static void produceInsertData(OverflowSupport support) {
    support.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(1), 1));
    support.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(3), 3));
    support.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(2), 2));

    support.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
    support.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
    support.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
  }

  private static TSRecord getData(String d, String m, TSDataType type, String value, long time) {
    TSRecord record = new TSRecord(time, d);
    record.addTuple(DataPoint.getDataPoint(type, m, value));
    return record;
  }

  public static void produceInsertData(OverflowProcessor processor) throws IOException {

    processor.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(1), 1));
    processor.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(3), 3));
    processor.insert(getData(deviceId1, measurementId1, dataType1, String.valueOf(2), 2));

    processor.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(5.5f), 1));
    processor.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(5.5f), 2));
    processor.insert(getData(deviceId2, measurementId2, dataType2, String.valueOf(10.5f), 2));
  }

}
