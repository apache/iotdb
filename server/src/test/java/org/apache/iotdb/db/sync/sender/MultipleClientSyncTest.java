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
package org.apache.iotdb.db.sync.sender;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.jdbc.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultipleClientSyncTest {

  Map<String, ArrayList<String>> timeseriesList = new HashMap();
  Map<String, ArrayList<String>> timeseriesList1 = new HashMap();
  private static final Logger logger = LoggerFactory.getLogger(MultipleClientSyncTest.class);
  private Set<String> dataSender = new HashSet<>();
  private Set<String> dataReceiver = new HashSet<>();

  public static void main(String[] args) throws IOException {
    MultipleClientSyncTest multipleClientSyncTest = new MultipleClientSyncTest();
    multipleClientSyncTest.testPostback();
  }

  public void testPostback() throws IOException {

    timeseriesList1.put("root.vehicle_history1", new ArrayList<>());
    timeseriesList1.put("root.vehicle_alarm1", new ArrayList<>());
    timeseriesList1.put("root.vehicle_temp1", new ArrayList<>());
    timeseriesList1.put("root.range_event1", new ArrayList<>());
    timeseriesList.put("root.vehicle_history", new ArrayList<>());
    timeseriesList.put("root.vehicle_alarm", new ArrayList<>());
    timeseriesList.put("root.vehicle_temp", new ArrayList<>());
    timeseriesList.put("root.range_event", new ArrayList<>());

    File file = new File("CreateTimeseries1.txt");
    BufferedReader reader = new BufferedReader(new FileReader(file));
    String line;
    while ((line = reader.readLine()) != null) {
      String timeseries = line.split(" ")[2];
      for (String storageGroup : timeseriesList.keySet()) {
        if (timeseries.startsWith(storageGroup + ".")) {
          String timesery = timeseries.substring((storageGroup + ".").length());
          timeseriesList.get(storageGroup).add(timesery);
          break;
        }
      }
    }

    file = new File("CreateTimeseries2.txt");
    reader = new BufferedReader(new FileReader(file));
    while ((line = reader.readLine()) != null) {
      String timeseries = line.split(" ")[2];
      for (String storageGroup : timeseriesList1.keySet()) {
        if (timeseries.startsWith(storageGroup + ".")) {
          String timesery = timeseries.substring((storageGroup + ".").length());
          timeseriesList1.get(storageGroup).add(timesery);
          break;
        }
      }
    }

    for (String storageGroup : timeseriesList.keySet()) {
      String sqlFormat = "select %s from %s";
      logger.debug(String.format("%s:", storageGroup));
      int count = 0;
      int count1 = 0;
      int count2 = 0;
      for (String timesery : timeseriesList.get(storageGroup)) {
        count++;
        count1 = 0;
        count2 = 0;
        dataSender.clear();
        dataReceiver.clear();
        try {
          Class.forName(Config.JDBC_DRIVER_NAME);
          Connection connection = null;
          Connection connection1 = null;
          try {
            connection = DriverManager
                .getConnection("jdbc:iotdb://192.168.130.14:6667/", "root", "root");
            connection1 = DriverManager
                .getConnection("jdbc:iotdb://192.168.130.16:6667/", "root", "root");
            Statement statement = connection.createStatement();
            Statement statement1 = connection1.createStatement();
            String sql = String.format(sqlFormat, timesery, storageGroup);
            boolean hasResultSet = statement.execute(sql);
            boolean hasResultSet1 = statement1.execute(sql);
            if (hasResultSet) {
              ResultSet res = statement.getResultSet();
              while (res.next()) {
                count1++;
                dataSender
                    .add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
              }
            }
            if (hasResultSet1) {
              ResultSet res = statement1.getResultSet();
              while (res.next()) {
                count2++;
                dataReceiver
                    .add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
              }
            }
            assert ((dataSender.size() == dataReceiver.size()) && dataSender
                .containsAll(dataReceiver));
            statement.close();
            statement1.close();
          } catch (Exception e) {
            logger.error("", e);
          } finally {
            if (connection != null) {
              connection.close();
            }
            if (connection1 != null) {
              connection1.close();
            }
          }
        } catch (ClassNotFoundException | SQLException e) {
          fail(e.getMessage());
        }
        if (count > 20) {
          break;
        }
        logger.debug(String.valueOf(count1));
        logger.debug(String.valueOf(count2));
      }
    }

    for (String storageGroup : timeseriesList1.keySet()) {
      String sqlFormat = "select %s from %s";
      logger.debug(String.format("%s:", storageGroup));
      int count = 0;
      int count1;
      int count2;
      for (String timesery : timeseriesList1.get(storageGroup)) {
        count++;
        count1 = 0;
        count2 = 0;
        dataSender.clear();
        dataReceiver.clear();
        try {
          Class.forName(Config.JDBC_DRIVER_NAME);
          Connection connection = null;
          Connection connection1 = null;
          try {
            connection = DriverManager
                .getConnection("jdbc:iotdb://192.168.130.15:6667/", "root", "root");
            connection1 = DriverManager
                .getConnection("jdbc:iotdb://192.168.130.16:6667/", "root", "root");
            Statement statement = connection.createStatement();
            Statement statement1 = connection1.createStatement();
            String sql = String.format(sqlFormat, timesery, storageGroup);
            boolean hasResultSet = statement.execute(sql);
            boolean hasResultSet1 = statement1.execute(sql);
            if (hasResultSet) {
              ResultSet res = statement.getResultSet();
              while (res.next()) {
                count1++;
                dataSender
                    .add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
              }
            }
            if (hasResultSet1) {
              ResultSet res = statement1.getResultSet();
              while (res.next()) {
                count2++;
                dataReceiver
                    .add(res.getString("Time") + res.getString(storageGroup + "." + timesery));
              }
            }
            assert ((dataSender.size() == dataReceiver.size()) && dataSender
                .containsAll(dataReceiver));
            statement.close();
            statement1.close();
          } catch (Exception e) {
            logger.error("", e);
          } finally {
            if (connection != null) {
              connection.close();
            }
            if (connection1 != null) {
              connection1.close();
            }
          }
        } catch (ClassNotFoundException | SQLException e) {
          fail(e.getMessage());
        }
        if (count > 20) {
          break;
        }
        logger.debug(String.valueOf(count1));
        logger.debug(String.valueOf(count2));
      }
    }
  }
}
