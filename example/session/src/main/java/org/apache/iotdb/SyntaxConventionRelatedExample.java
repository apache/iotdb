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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;

/**
 * When using session API, measurement, device, database and path are represented by String. The
 * content of the String should be the same as what you would write in a SQL statement. This class
 * is an example to help you understand better.
 */
public class SyntaxConventionRelatedExample {
  private static Session session;
  private static final String LOCAL_HOST = "127.0.0.1";
  /**
   * if you want to create a time series named root.sg1.select, a possible SQL statement would be
   * like: create timeseries root.sg1.select with datatype=FLOAT, encoding=RLE As described before,
   * when using session API, path is represented using String. The path should be written as
   * "root.sg1.select".
   */
  private static final String ROOT_SG1_KEYWORD_EXAMPLE = "root.sg1.select";

  /**
   * if you want to create a time series named root.sg1.111, a possible SQL statement would be like:
   * create timeseries root.sg1.`111` with datatype=FLOAT, encoding=RLE The path should be written
   * as "root.sg1.`111`".
   */
  private static final String ROOT_SG1_DIGITS_EXAMPLE = "root.sg1.`111`";

  /**
   * if you want to create a time series named root.sg1.`a"b'c``, a possible SQL statement would be
   * like: create timeseries root.sg1.`a"b'c``` with datatype=FLOAT, encoding=RLE The path should be
   * written as "root.sg1.`a"b`c```".
   */
  private static final String ROOT_SG1_SPECIAL_CHARACTER_EXAMPLE = "root.sg1.`a\"b'c```";

  /**
   * if you want to create a time series named root.sg1.a, a possible SQL statement would be like:
   * create timeseries root.sg1.a with datatype=FLOAT, encoding=RLE The path should be written as
   * "root.sg1.a".
   */
  private static final String ROOT_SG1_NORMAL_NODE_EXAMPLE = "root.sg1.a";

  public static void main(String[] args)
      throws IoTDBConnectionException, StatementExecutionException {
    session =
        new Session.Builder()
            .host(LOCAL_HOST)
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    session.open(false);

    // set session fetchSize
    session.setFetchSize(10000);

    try {
      session.setStorageGroup("root.sg1");
    } catch (StatementExecutionException e) {
      if (e.getStatusCode() != TSStatusCode.PATH_ALREADY_EXIST.getStatusCode()) {
        throw e;
      }
    }

    // createTimeSeries
    createTimeSeries();
    SessionDataSet dataSet = session.executeQueryStatement("show timeseries root.sg1.*");
    // the expected paths would be:
    // [root.sg1.select, root.sg1.`111`, root.sg1.`a"b'c```, root.sg1.a]
    // You could see that time series in dataSet are exactly the same as
    // the initial String you used as path. Node names consist of digits or contain special
    // characters are quoted with ``, both in SQL statement and in header of result dataset.
    // It's convenient that you can use the result of show timeseries as input parameter directly
    // for other
    // session APIs such as insertRecord or executeRawDataQuery.
    List<String> paths = new ArrayList<>();
    while (dataSet.hasNext()) {
      paths.add(dataSet.next().getFields().get(0).toString());
    }

    long startTime = 1L;
    long endTime = 100L;
    long timeOut = 60000;

    try (SessionDataSet dataSet1 =
        session.executeRawDataQuery(paths, startTime, endTime, timeOut)) {

      System.out.println(dataSet1.getColumnNames());
      dataSet1.setFetchSize(1024);
      while (dataSet1.hasNext()) {
        System.out.println(dataSet1.next());
      }
    }
  }

  private static void createTimeSeries()
      throws IoTDBConnectionException, StatementExecutionException {
    if (!session.checkTimeseriesExists(ROOT_SG1_KEYWORD_EXAMPLE)) {
      session.createTimeseries(
          ROOT_SG1_KEYWORD_EXAMPLE, TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_DIGITS_EXAMPLE)) {
      session.createTimeseries(
          ROOT_SG1_DIGITS_EXAMPLE, TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_SPECIAL_CHARACTER_EXAMPLE)) {
      session.createTimeseries(
          ROOT_SG1_SPECIAL_CHARACTER_EXAMPLE,
          TSDataType.FLOAT,
          TSEncoding.RLE,
          CompressionType.SNAPPY);
    }
    if (!session.checkTimeseriesExists(ROOT_SG1_NORMAL_NODE_EXAMPLE)) {
      session.createTimeseries(
          ROOT_SG1_NORMAL_NODE_EXAMPLE, TSDataType.FLOAT, TSEncoding.RLE, CompressionType.SNAPPY);
    }
  }
}
