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
package org.apache.iotdb.doublewrite;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;

/** Thread for insertion. Normally you don't need to modify this java class. */
public class DoubleWriteThread implements Runnable {

  private final SessionPool sessionPool;

  private final String sg = "root.DOUBLEWRITESG";
  private final String deviceId;

  private final int batchCnt;
  private final int timeseriesCnt;
  private final int batchSize;

  DoubleWriteThread(
      SessionPool sessionPool, String deviceId, int batchCnt, int timeseriesCnt, int batchSize)
      throws IoTDBConnectionException, StatementExecutionException {
    this.sessionPool = sessionPool;

    this.deviceId = deviceId;

    this.batchCnt = batchCnt;
    this.timeseriesCnt = timeseriesCnt;
    this.batchSize = batchSize;

    for (int i = 0; i < timeseriesCnt; i++) {
      sessionPool.createTimeseries(
          sg + "." + deviceId + "." + "s" + i,
          TSDataType.INT32,
          TSEncoding.PLAIN,
          CompressionType.UNCOMPRESSED);
    }
  }

  @Override
  public void run() {
    long timestamp = 0;
    for (int i = 0; i < batchCnt; i++) {
      List<String> deviceList = new ArrayList<>();
      List<Long> timestampList = new ArrayList<>();
      List<List<String>> measurementList = new ArrayList<>();
      List<List<String>> valueList = new ArrayList<>();

      for (int j = 0; j < batchSize; j++) {
        deviceList.add(sg + "." + deviceId);
        timestampList.add(timestamp);
        List<String> measurements = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int k = 0; k < timeseriesCnt; k++) {
          measurements.add("s" + k);
          values.add(String.valueOf(timestamp));
        }
        measurementList.add(measurements);
        valueList.add(values);
        timestamp += 1;
      }

      try {
        if (batchSize == 1) {
          sessionPool.insertRecord(
              deviceList.get(0), timestampList.get(0), measurementList.get(0), valueList.get(0));
        } else {
          sessionPool.insertRecords(deviceList, timestampList, measurementList, valueList);
        }
      } catch (IoTDBConnectionException | StatementExecutionException ignored) {
        // ignored
      }
    }
  }
}
