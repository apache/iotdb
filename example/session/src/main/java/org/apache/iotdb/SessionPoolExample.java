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

import org.apache.iotdb.isession.SessionDataSet.DataIterator;
import org.apache.iotdb.isession.pool.SessionDataSetWrapper;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.enums.TSDataType;

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SuppressWarnings({"squid:S106", "squid:S1144"})
public class SessionPoolExample {
  private static final Logger logger = LoggerFactory.getLogger(SessionPoolExample.class);

  private static SessionPool sessionPool;
  private static ExecutorService service;

  /** Build a custom SessionPool for this example */
  private static void constructCustomSessionPool() {
    sessionPool =
        new SessionPool.Builder()
            .host("127.0.0.1")
            .port(6667)
            .user("root")
            .password("root")
            .maxSize(20)
            .build();
  }

  /** Build a redirect-able SessionPool for this example */
  private static void constructRedirectSessionPool() {
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("127.0.0.1:6667");
    nodeUrls.add("127.0.0.1:6668");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .maxSize(3)
            .build();
  }

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    // Choose the SessionPool you going to use
    constructCustomSessionPool();

    service = Executors.newFixedThreadPool(20);
    insertRecord();
    Thread.sleep(2000);
    queryByRowRecord();
    Thread.sleep(5000);
    sessionPool.close();
    service.shutdown();
  }

  // more insert example, see SessionExample.java
  private static void insertRecord() throws StatementExecutionException, IoTDBConnectionException {
    String deviceId = "root.sg.d1";
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    measurements.add("s1");
    measurements.add("s2");
    measurements.add("s3");
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    types.add(TSDataType.INT64);
    List<Object> values = new ArrayList<>();
    values.add(1L);
    values.add(2L);
    values.add(3L);
    for (int i = 0; i < 10; i++) {
      long time = i;
      service.submit(
          () -> {
            while (true) {
              try {
                sessionPool.insertRecord(deviceId, time, measurements, types, values);
                break;
              } catch (IoTDBConnectionException | StatementExecutionException ignored) {

              }
            }
          });
    }
  }

  private static void queryByRowRecord() {
    for (int i = 1; i < 4; i++) {
      int finalI = i;
      service.submit(
          () -> {
            try {
              sessionPool.createTimeseries("root.sg.d1.s" + finalI, TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              logger.error("   ", e);
            }
          });
    }
  }
}
