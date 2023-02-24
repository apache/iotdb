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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Collections;

public class InsertData {
  private static Session session;
  private static final String DEVICE = "root.sg";

  private static final long TIME_PARTITION_INTERVAL = 10000 * 200;
  private static final long FILE_NUM = 20;

 private static final long RECORD_NUM = 100;

//  private static final long FILE_NUM = 200;
//
//  private static final long RECORD_NUM = 10000;

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session = new Session("127.0.0.1", 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    writeTargetData();
    writeTestData();
  }

  public static void writeTargetData()
      throws IoTDBConnectionException, StatementExecutionException {
    String target = "s0";
    for (int fileNum = 0; fileNum < FILE_NUM; fileNum++) {
      for (int recordNum = 0; recordNum < RECORD_NUM; recordNum++) {
        long time = RECORD_NUM * fileNum + recordNum;
        session.insertRecord(
            DEVICE,
            time,
            Collections.singletonList(target),
            Collections.singletonList(TSDataType.INT64),
            time);
      }
      session.executeNonQueryStatement("flush");
    }
  }

  public static void writeTestData() throws IoTDBConnectionException, StatementExecutionException {
    String[] measurements =
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"};
    for (int i = 0; i < measurements.length; i++) {
      String measurement = measurements[i];
      long startTime = (i + 1) * FILE_NUM * RECORD_NUM;
      for (int fileNum = 0; fileNum < 200; fileNum++) {
        for (int recordNum = 0; recordNum < 10000; recordNum++) {
          long time = startTime + RECORD_NUM * fileNum + recordNum;
          session.insertRecord(
              DEVICE,
              time,
              Collections.singletonList(measurement),
              Collections.singletonList(TSDataType.INT64),
              time);
        }
        session.executeNonQueryStatement("flush");
      }
    }
  }
}
