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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SessionPoolExample {

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
            .maxSize(3)
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

  /** Build a backup SessionPool for this example */
  private static void backupSessionPool() {
    List<String> backupList = new ArrayList<>();
    backupList.add("172.20.31.10:6667");
    backupList.add("172.20.31.10:6668");
    backupList.add("172.20.31.10:6669");
    List<String> nodeUrls = new ArrayList<>();
    nodeUrls.add("127.0.0.1:6667");
    //    nodeUrls.add("127.0.0.1:6668");
    //    nodeUrls.add("127.0.0.1:6669");
    sessionPool =
        new SessionPool.Builder()
            .nodeUrls(nodeUrls)
            .user("root")
            .password("root")
            .backupNodeUrls(backupList)
            .backupUser("root")
            .backupPassword("root")
            .checkPrimaryClusterIsConnectedTimeS(15L)
            .fillAllNodeUrlsStatus(true)
            .maxSize(30)
            .timeOut(1000)
            .connectionTimeoutInMs(1000)
            .build();
  }

  private static final String ROOT_SG1_D1_S1 = "root.sg1.d1.s1";
  private static final String ROOT_SG1_D1_S2 = "root.sg1.d1.s2";
  private static final String ROOT_SG1_D1_S3 = "root.sg1.d1.s3";
  private static final String ROOT_SG1_D1_S4 = "root.sg1.d1.s4";
  private static final String ROOT_SG1_D1_S5 = "root.sg1.d1.s5";

  public static void main(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    // Choose the SessionPool you going to use
    backupSessionPool();
    //    createTimeseries();

    new Thread(
            () -> {
              try {
                Thread.sleep(10000);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              sessionPool.fillAllNodeUrls();
            })
        .start();
    service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 300; i++) {
      try {
        insertRecord();
        Thread.sleep(100);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    for (int i = 0; i < 2000; i++) {
      try {
        queryByRowRecord();
        Thread.sleep(1000);
        queryByIterator();
        Thread.sleep(1000);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    sessionPool.close();
    service.shutdown();
  }

  private static void createTimeseries()
      throws IoTDBConnectionException, StatementExecutionException {

    if (!sessionPool.checkTimeseriesExists(ROOT_SG1_D1_S1)) {
      sessionPool.createTimeseries(
          ROOT_SG1_D1_S1, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!sessionPool.checkTimeseriesExists(ROOT_SG1_D1_S2)) {
      sessionPool.createTimeseries(
          ROOT_SG1_D1_S2, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!sessionPool.checkTimeseriesExists(ROOT_SG1_D1_S3)) {
      sessionPool.createTimeseries(
          ROOT_SG1_D1_S3, TSDataType.INT64, TSEncoding.RLE, CompressionType.SNAPPY);
    }

    // create timeseries with tags and attributes
    if (!sessionPool.checkTimeseriesExists(ROOT_SG1_D1_S4)) {
      Map<String, String> tags = new HashMap<>();
      tags.put("tag1", "v1");
      Map<String, String> attributes = new HashMap<>();
      attributes.put("description", "v1");
      sessionPool.createTimeseries(
          ROOT_SG1_D1_S4,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          null,
          tags,
          attributes,
          "temperature");
    }

    // create timeseries with SDT property, SDT will take place when flushing
    if (!sessionPool.checkTimeseriesExists(ROOT_SG1_D1_S5)) {
      // COMPDEV is required
      // COMPMAXTIME and COMPMINTIME are optional and their unit is ms
      Map<String, String> props = new HashMap<>();
      props.put("LOSS", "sdt");
      props.put("COMPDEV", "0.01");
      props.put("COMPMINTIME", "2");
      props.put("COMPMAXTIME", "10");
      sessionPool.createTimeseries(
          ROOT_SG1_D1_S5,
          TSDataType.INT64,
          TSEncoding.RLE,
          CompressionType.SNAPPY,
          props,
          null,
          null,
          null);
    }
  }

  public static void main1(String[] args)
      throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
    // Choose the SessionPool you going to use
    constructRedirectSessionPool();

    service = Executors.newFixedThreadPool(10);
    insertRecord();
    queryByRowRecord();
    Thread.sleep(1000);
    queryByIterator();
    sessionPool.close();
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
      sessionPool.insertRecord(deviceId, System.currentTimeMillis(), measurements, types, values);
    }
  }

  private static void queryByRowRecord() {
    for (int i = 0; i < 1; i++) {
      service.submit(
          () -> {
            SessionDataSetWrapper wrapper = null;
            try {
              wrapper = sessionPool.executeQueryStatement("select * from root.sg1.d1");
              //              System.out.println(wrapper.getColumnNames());
              //              System.out.println(wrapper.getColumnTypes());
              while (wrapper.hasNext()) {
                //                System.out.println(wrapper.next());
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              sessionPool.closeResultSet(wrapper);
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
              wrapper = sessionPool.executeQueryStatement("select * from root.sg1.d1");
              // get DataIterator like JDBC
              DataIterator dataIterator = wrapper.iterator();
              //              System.out.println(wrapper.getColumnNames());
              //              System.out.println(wrapper.getColumnTypes());
              while (dataIterator.next()) {
                StringBuilder builder = new StringBuilder();
                for (String columnName : wrapper.getColumnNames()) {
                  builder.append(dataIterator.getString(columnName) + " ");
                }
                //                System.out.println(builder);
              }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
              e.printStackTrace();
            } finally {
              // remember to close data set finally!
              sessionPool.closeResultSet(wrapper);
            }
          });
    }
  }
}
