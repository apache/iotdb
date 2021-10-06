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

package org.apache.iotdb.influxdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Before;

import java.util.*;

/**
 * The purpose of this class is to test the performance of two design schemes, store and read
 * large-scale data for the two schemes respectively, and compare the reading efficiency
 */
public class DesignSchemePerformanceTest {

  private static final String LOCAL_HOST = "127.0.0.1";
  private static Session session;
  private static final String ROOT_T1 = "root.teststress.test1";
  private static final String ROOT_T2 = "root.teststress.test2";

  @Before
  public void setUp() throws IoTDBConnectionException {
    // create session
    //
    session = new Session(LOCAL_HOST, 6667, "root", "root");
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);
  }

  public void Insert1() throws IoTDBConnectionException, StatementExecutionException { // 测试数据插入
    String deviceId = ROOT_T1;
    int fieldNum = 100;
    int tagNum = 10;
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    for (int i = 0; i < fieldNum; i++) {
      measurements.add(Util.generateWord(i));
      types.add(TSDataType.INT64);
    }
    for (int i = 0; i < tagNum; i++) {
      int baseline = 300;
      String tmpTag = Util.generateWord(i + baseline);
      System.out.println("tag:" + tmpTag);
      measurements.add(tmpTag);
      types.add(TSDataType.INT64);
    }

    for (long time = 0; time < 200000; time++) {
      List<Object> values = new ArrayList<>();
      for (long i = 0; i < fieldNum + tagNum; i++) {
        values.add(time);
      }
      session.insertRecord(deviceId, time, measurements, types, values);
    }
  }

  public void Insert2() throws IoTDBConnectionException, StatementExecutionException { // 测试数据插入
    String deviceId = ROOT_T2;
    int fieldNum = 100;
    int tagNum = 10;
    for (int i = 0; i < tagNum; i++) {
      List<String> measurements = new ArrayList<>();
      List<TSDataType> types = new ArrayList<>();
      int baseline = 300;
      String tmpTag = Util.generateWord(i + baseline);
      deviceId += "." + tmpTag;
      for (int j = 0; j < fieldNum; j++) {
        measurements.add(Util.generateWord(j));
        types.add(TSDataType.INT64);
      }

      for (long time = 0; time < 20000; time++) {
        List<Object> values = new ArrayList<>();
        for (long k = 0; k < fieldNum; k++) {
          values.add(time);
        }
        session.insertRecord(deviceId, time, measurements, types, values);
      }
    }
  }

  public void Query1() throws IoTDBConnectionException, StatementExecutionException { // 测试数据查询

    long before = System.currentTimeMillis();
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test1 where RL = 1 and A = 1 and B =1 and C=1");
    long after = System.currentTimeMillis();
    long duration = (after - before);
    System.out.println(duration);
    //        System.out.println(dataSet.getColumnNames());
    //        dataSet.setFetchSize(1024); // default is 10000
    //        int index = 0;
    //        while (dataSet.hasNext()) {
    ////            System.out.println(dataSet.next());
    //            index++;
    //        }
    //        System.out.println(index);

    //        dataSet.closeOperationHandle();
  }

  public void Query2() throws IoTDBConnectionException, StatementExecutionException { // 测试数据查询
    long before = System.currentTimeMillis();
    SessionDataSet dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test2.*.*.*.*.SL where A=1 and B=1 and C=1");
    dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test2.*.*.*.*.SL.* where A=1 and B=1 and C=1");
    dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test2.*.*.*.*.SL.*.*.* where A=1 and B=1 and C=1");
    dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test2.*.*.*.*.SL.*.*.*.* where A=1 and B=1 and C=1");
    dataSet =
        session.executeQueryStatement(
            "select * from root.teststress.test2.*.*.*.*.SL.*.*.*.*.* where A=1 and B=1 ");
    long after = System.currentTimeMillis();
    long duration = (after - before);
    System.out.println(duration);
    System.out.println(dataSet.getColumnNames());
    dataSet.setFetchSize(1024); // default is 10000
    int index = 0;
    while (dataSet.hasNext()) {
      //            System.out.println(dataSet.next());
      index++;
    }
    System.out.println(index);

    dataSet.closeOperationHandle();
  }
}
