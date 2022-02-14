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
import org.apache.iotdb.session.SessionDataSet.DataIterator;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SessionPoolExample {

  private static SessionPool pool;
  private static ExecutorService service;

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    pool = new SessionPool("127.0.0.1", 6667, "root", "root", 3);
    service = Executors.newFixedThreadPool(10);

    insertRecord();
    queryByRowRecord();
    Thread.sleep(1000);
    queryByIterator();
    pool.close();
    service.shutdown();
  }

  // more insert example, see SessionExample.java
  private static void insertRecord() throws StatementExecutionException, IoTDBConnectionException {
    String deviceId = "root.sg1.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);

    for (long time = 0; time < 10; time++) {
      List<Object> values = new ArrayList<>();
      values.add(1L);
      values.add(2L);
      values.add(3L);
      pool.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  private static void queryByRowRecord() {
    for (int i = 0; i < 1; i++) {
      service.submit(
          () -> {
            SessionDataSetWrapper wrapper = null;
            try {
              wrapper = pool.executeQueryStatement("select * from root.sg1.d1");
              System.out.println(wrapper.getColumnNames());
              System.out.println(wrapper.getColumnTypes());
              while (wrapper.hasNext()) {
                System.out.println(wrapper.next());
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              pool.closeResultSet(wrapper);
            }
          });
    }
  }

  private static void queryByIterator() {
    for (int i = 0; i < 1; i++) {
      service.submit(
          () -> {
            SessionDataSetWrapper wrapper = null;
            try {
              wrapper = pool.executeQueryStatement("select * from root.sg1.d1");
              // get DataIterator like JDBC
              DataIterator dataIterator = wrapper.iterator();
              System.out.println(wrapper.getColumnNames());
              System.out.println(wrapper.getColumnTypes());
              while (dataIterator.next()) {
                StringBuilder builder = new StringBuilder();
                for (String columnName : wrapper.getColumnNames()) {
                  builder.append(dataIterator.getString(columnName) + " ");
                }
                System.out.println(builder);
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              pool.closeResultSet(wrapper);
            }
          });
    }
  }
}
