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
package org.apache.iotdb.flink.it;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;

public class Utils {

  protected static void prepareData(String deviceId, String host, int port)
      throws IoTDBConnectionException, StatementExecutionException {
    Session session = new Session.Builder().host(host).port(port).build();
    session.open(false);
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();

    ArrayList<String> measurements =
        new ArrayList<String>() {
          {
            for (int i = 0; i < 6; i++) {
              add(String.format("s%d", i));
            }
          }
        };
    ArrayList<TSDataType> types =
        new ArrayList<TSDataType>() {
          {
            add(TSDataType.INT32);
            add(TSDataType.INT64);
            add(TSDataType.FLOAT);
            add(TSDataType.DOUBLE);
            add(TSDataType.BOOLEAN);
            add(TSDataType.TEXT);
          }
        };
    ArrayList<Object> values =
        new ArrayList<Object>() {
          {
            add(1);
            add(1L);
            add(1F);
            add(1D);
            add(true);
            add("hello world");
          }
        };
    for (int i = 1; i <= 1000; i++) {
      times.add(Long.valueOf(i));
      measurementsList.add(measurements);
      typesList.add(types);
      valuesList.add(values);
    }
    session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
  }
}
