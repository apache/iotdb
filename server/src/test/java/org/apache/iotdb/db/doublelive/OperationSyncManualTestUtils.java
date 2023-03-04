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
package org.apache.iotdb.db.doublelive;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a manual test utils for DoubleLive. First start two IoTDB that enable OperationSync to
 * use this.
 */
public class OperationSyncManualTestUtils {

  private static final SessionPool sessionPool =
      new SessionPool("127.0.0.1", 6667, "root", "root", 3);

  private static final String sg = "root.sg";
  private static final int sgCnt = 10;
  private static final String d = ".d";
  private static final int dCnt = 20;
  private static final String s = ".s";
  private static final int sCnt = 100;
  private static final int dataCnt = 1000;

  public void setStorageGroups() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < sgCnt; i++) {
      sessionPool.setStorageGroup(sg + i);
    }
  }

  public void deleteStorageGroups() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < sgCnt; i++) {
      sessionPool.deleteStorageGroups(Collections.singletonList(sg + i));
    }
  }

  public void createTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < sgCnt; i++) {
      String SG = sg + i;
      for (int j = 0; j < dCnt; j++) {
        String D = d + j;
        for (int k = 0; k < sCnt; k++) {
          String S = s + k;
          sessionPool.createTimeseries(
              SG + D + S, TSDataType.INT32, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
        }
      }
    }
  }

  public void deleteTimeSeries() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < sgCnt; i++) {
      String SG = sg + i;
      for (int j = 0; j < dCnt; j++) {
        String D = d + j;
        for (int k = 0; k < sCnt; k++) {
          String S = s + k;
          sessionPool.deleteTimeseries(SG + D + S);
        }
      }
    }
  }

  public void insertData() throws IoTDBConnectionException, StatementExecutionException {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < sgCnt; i++) {
      String SG = sg + i;
      for (int j = 0; j < dCnt; j++) {
        String D = d + j;
        String device = SG + D;
        List<String> measurements = new ArrayList<>();
        List<TSDataType> types = new ArrayList<>();
        for (int k = 0; k < sCnt; k++) {
          measurements.add("s" + k);
          types.add(TSDataType.INT32);
        }
        for (int l = 0; l < dataCnt; l++) {
          List<Object> values = new ArrayList<>();
          for (int k = 0; k < sCnt; k++) {
            values.add(l);
          }
          sessionPool.insertRecord(device, l, measurements, types, values);
        }
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println(
        "Avg time per insert: "
            + ((endTime - startTime) / (double) (sgCnt + dCnt + dataCnt))
            + "ms");
  }

  public void deleteData() throws IoTDBConnectionException, StatementExecutionException {
    for (int i = 0; i < sgCnt; i++) {
      String SG = sg + i;
      for (int j = 0; j < dCnt; j++) {
        String D = d + j;
        for (int k = 0; k < sCnt; k++) {
          String S = s + k;
          sessionPool.deleteData(Collections.singletonList(SG + D + S), 0, dataCnt);
        }
      }
    }
  }
}
