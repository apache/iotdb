/**
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
package org.apache.iotdb.db.sync.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SyncTestClient3 is used to generate data of another half timeseries (simulating jilian scene) which is
 * different to those in SyncTestClient2 to test stability of sync function.
 */
public class SyncTestClient3 {

  private static final int TIME_INTERVAL = 0;
  private static final int TOTAL_DATA = 2000000;
  private static final int ABNORMAL_MAX_INT = 0;
  private static final int ABNORMAL_MIN_INT = -10;
  private static final int ABNORMAL_MAX_FLOAT = 0;
  private static final int ABNORMAL_MIN_FLOAT = -10;
  private static final int ABNORMAL_FREQUENCY = Integer.MAX_VALUE;
  private static final int ABNORMAL_LENGTH = 0;
  private static final int MIN_INT = 0;
  private static final int MAX_INT = 14;
  private static final int MIN_FLOAT = 20;
  private static final int MAX_FLOAT = 30;
  private static final int STRING_LENGTH = 5;
  private static final int BATCH_SQL = 10000;
  private static final Logger logger = LoggerFactory.getLogger(SyncTestClient3.class);

  /**
   * generate time series map from file.
   *
   * @param inputFilePath input file path
   * @return map
   * @throws Exception Exception
   */
  public static Map<String, String> generateTimeseriesMapFromFile(String inputFilePath)
      throws IOException {

    Map<String, String> timeseriesMap = new HashMap<>();

    File file = new File(inputFilePath);
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        String timeseries = line.split(" ")[2];
        String dataType = line.split("DATATYPE = ")[1].split(",")[0].trim();
        String encodingType = line.split("ENCODING = ")[1].split(";")[0].trim();
        timeseriesMap.put(timeseries, dataType + "," + encodingType);
      }
    }

    return timeseriesMap;

  }

  /**
   * create time series.
   *
   * @param statement statement
   * @param timeseriesMap time series map
   * @throws SQLException SQLException
   */
  public static void createTimeseries(Statement statement, Statement statement1,
      Map<String, String> timeseriesMap)
      throws SQLException {

    try {
      String createTimeseriesSql = "CREATE TIMESERIES <timeseries> WITH DATATYPE=<datatype>, "
          + "ENCODING=<encode>";

      int sqlCount = 0;

      for (Map.Entry<String, String> entry : timeseriesMap.entrySet()) {
        String key = entry.getKey();
        String properties = entry.getValue();
        String sql = createTimeseriesSql.replace("<timeseries>", key)
            .replace("<datatype>", Utils.getType(properties))
            .replace("<encode>", Utils.getEncode(properties));

        statement.addBatch(sql);
        statement1.addBatch(sql);
        sqlCount++;
        if (sqlCount >= BATCH_SQL) {
          statement.executeBatch();
          statement.clearBatch();
          statement1.executeBatch();
          statement1.clearBatch();
          sqlCount = 0;
        }
      }
      statement.executeBatch();
      statement.clearBatch();
      statement1.executeBatch();
      statement1.clearBatch();
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  /**
   * set storage group.
   *
   * @param statement statement
   * @param storageGroupList storage group list
   * @throws SQLException SQLException
   */
  public static void setStorageGroup(Statement statement, Statement statement1,
      List<String> storageGroupList)
      throws SQLException {
    try {
      String setStorageGroupSql = "SET STORAGE GROUP TO <prefixpath>";
      for (String str : storageGroupList) {
        String sql = setStorageGroupSql.replace("<prefixpath>", str);
        statement.execute(sql);
        statement1.execute(sql);
      }
    } catch (Exception e) {
      logger.error("", e);
    }
  }

  /**
   * randomly insert data.
   *
   * @param statement statement
   * @param timeseriesMap time series map
   * @throws Exception Exception
   */
  public static void randomInsertData(Statement statement, Statement statement1,
      Map<String, String> timeseriesMap) throws InterruptedException, SQLException {
    String insertDataSql = "INSERT INTO %s (timestamp, %s) VALUES (%s, %s)";
    int abnormalCount = 0;
    int abnormalFlag = 1;

    int sqlCount = 0;

    for (int i = 0; i < TOTAL_DATA; i++) {

      long time = System.currentTimeMillis();

      if (i % ABNORMAL_FREQUENCY == 250) {
        abnormalFlag = 0;
      }

      for (Map.Entry<String, String> entry : timeseriesMap.entrySet()) {
        String key = entry.getKey();
        String type = Utils.getType(entry.getValue());
        String path = Utils.getPath(key);
        String sensor = Utils.getSensor(key);
        String sql = "";

        if (type.equals("INT32")) {
          int value;
          if (abnormalFlag == 0) {
            value = RandomNum.getRandomInt(ABNORMAL_MIN_INT, ABNORMAL_MAX_INT);
          } else {
            value = RandomNum.getRandomInt(MIN_INT, MAX_INT);
          }
          sql = String.format(insertDataSql, path, sensor, time, value);
        } else if (type.equals("FLOAT")) {
          float value;
          if (abnormalFlag == 0) {
            value = RandomNum.getRandomFloat(ABNORMAL_MIN_FLOAT, ABNORMAL_MAX_FLOAT);
          } else {
            value = RandomNum.getRandomFloat(MIN_FLOAT, MAX_FLOAT);
          }
          sql = String.format(insertDataSql, path, sensor, time, value);
        } else if (type.equals("TEXT")) {
          String value;
          value = RandomNum.getRandomText(STRING_LENGTH);
          sql = String.format(insertDataSql, path, sensor, time, "\"" + value + "\"");
        }

        statement.addBatch(sql);
        statement1.addBatch(sql);
        sqlCount++;
        if (sqlCount >= BATCH_SQL) {
          statement.executeBatch();
          statement.clearBatch();
          statement1.executeBatch();
          statement1.clearBatch();
          sqlCount = 0;
        }
      }

      if (abnormalFlag == 0) {
        abnormalCount += 1;
      }
      if (abnormalCount >= ABNORMAL_LENGTH) {
        abnormalCount = 0;
        abnormalFlag = 1;
      }
    }
    statement.executeBatch();
    statement.clearBatch();
    statement1.executeBatch();
    statement1.clearBatch();
  }

  /**
   * main function.
   *
   * @param args arguments
   * @throws Exception Exception
   */
  public static void main(String[] args) throws Exception {

    Statement statement = null;
    Statement statement1 = null;

    String path =
        new File(System.getProperty(IoTDBConstant.IOTDB_HOME, null)).getParent() + File.separator
            + "src"
            + File.separator + "test" + File.separator + "resources" + File.separator
            + "CreateTimeseries3.txt";
    Map<String, String> timeseriesMap = generateTimeseriesMapFromFile(path);

    List<String> storageGroupList = new ArrayList<>();
    storageGroupList.add("root.vehicle_history2");
    storageGroupList.add("root.vehicle_alarm2");
    storageGroupList.add("root.vehicle_temp2");
    storageGroupList.add("root.range_event2");

    try (Connection connection1 = DriverManager
        .getConnection("jdbc:iotdb://192.168.130.17:6667/", "root", "root")) {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      try (Connection connection = DriverManager
          .getConnection("jdbc:iotdb://localhost:6667/", "root", "root")) {
        statement = connection.createStatement();

        statement1 = connection1.createStatement();

        setStorageGroup(statement, statement1, storageGroupList);
        logger.debug("Finish set storage group.");
        createTimeseries(statement, statement1, timeseriesMap);
        logger.debug("Finish create timeseries.");
        while (true) {
          randomInsertData(statement, statement1, timeseriesMap);
        }
      }
    } catch (Exception e) {
      logger.error("", e);
    } finally {
      if (statement != null) {
        statement.close();
      }
      if (statement1 != null) {
        statement1.close();
      }
    }
  }
}