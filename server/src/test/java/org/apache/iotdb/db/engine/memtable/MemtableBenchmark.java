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
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

/** Memtable insert benchmark. Bench the Memtable and get its performance. */
public class MemtableBenchmark {

  private static String deviceId = "d0";
  private static int numOfMeasurement = 10000;
  private static int numOfPoint = 1000;

  private static String[] measurementId = new String[numOfMeasurement];
  private static TSDataType tsDataType = TSDataType.INT64;

  static {
    for (int i = 0; i < numOfMeasurement; i++) {
      measurementId[i] = "m" + i;
    }
  }

  public static void main(String[] args) {
    IMemTable memTable = new PrimitiveMemTable();
    final long startTime = System.currentTimeMillis();
    // cpu not locality
    for (int i = 0; i < numOfPoint; i++) {
      for (int j = 0; j < numOfMeasurement; j++) {
        memTable.write(
            deviceId,
            new UnaryMeasurementSchema(measurementId[j], tsDataType, TSEncoding.PLAIN),
            System.nanoTime(),
            String.valueOf(System.currentTimeMillis()));
      }
    }

    final long endTime = System.currentTimeMillis();
    System.out.println(
        String.format(
            "Num of time series: %d, "
                + "Num of points for each time series: %d, "
                + "The total time: %d ms. ",
            numOfMeasurement, numOfPoint, endTime - startTime));
  }
}
