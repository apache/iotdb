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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MemTableTestUtils {

  public static String deviceId0 = "d0";

  public static String measurementId0 = "s0";

  public static TSDataType dataType0 = TSDataType.INT32;
  private static FileSchema fileSchema = new FileSchema();

  static {
    fileSchema
        .registerMeasurement(new MeasurementSchema(measurementId0, dataType0, TSEncoding.PLAIN));
  }

  public static void produceData(IMemTable iMemTable, long startTime, long endTime, String deviceId,
      String measurementId, TSDataType dataType) {
    if (startTime > endTime) {
      throw new RuntimeException(String.format("start time %d > end time %d", startTime, endTime));
    }
    for (long l = startTime; l <= endTime; l++) {
      iMemTable.write(deviceId, measurementId, dataType, l, String.valueOf(l));
    }
  }

  public static FileSchema getFileSchema() {
    return fileSchema;
  }

}
