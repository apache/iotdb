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

package org.apache.iotdb.generator;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import java.util.ArrayList;
import java.util.List;

public class Generator {

  public static Tablet generateTablet(
      String deviceId, String measurementId, long startTime, int pointNum) {
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    schemaList.add(new UnaryMeasurementSchema(measurementId, TSDataType.INT32));
    Tablet tablet = new Tablet(deviceId, schemaList, pointNum);

    long timestamp = startTime;
    for (long row = 0; row < pointNum; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      tablet.addValue(measurementId, rowIndex, 0);
      timestamp += 1;
    }
    return tablet;
  }
}
