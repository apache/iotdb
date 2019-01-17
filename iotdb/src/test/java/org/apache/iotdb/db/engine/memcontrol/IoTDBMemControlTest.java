/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.engine.memcontrol;

import static junit.framework.TestCase.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.integration.Constant;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.LongDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.StringDataPoint;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Deprecated
public class IoTDBMemControlTest {

  private static final String TIMESTAMP_STR = "Time";
  private final String FOLDER_HEADER = "src/test/resources";
  private final String d0 = "root.vehicle.d0";
  private final String d1 = "root.house.d0";
  private final String s0 = "s0";
  private final String s1 = "s1";
  private final String s2 = "s2";
  private final String s3 = "s3";
  IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[] sqls = new String[]{"SET STORAGE GROUP TO root.vehicle",
      "SET STORAGE GROUP TO root.house",
      "CREATE TIMESERIES root.vehicle.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.vehicle.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.vehicle.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",

      "CREATE TIMESERIES root.house.d1.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.house.d0.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
      "CREATE TIMESERIES root.house.d0.s3 WITH DATATYPE=TEXT, ENCODING=PLAIN",
      "CREATE TIMESERIES root.house.d0.s0 WITH DATATYPE=INT32, ENCODING=RLE",
      "CREATE TIMESERIES root.house.d0.s1 WITH DATATYPE=INT64, ENCODING=RLE",
      "CREATE TIMESERIES root.house.d0.s4 WITH DATATYPE=BOOLEAN, ENCODING=PLAIN"};
  private IoTDB deamon;

  private boolean testFlag = false;
  private boolean exceptionCaught = false;

  public IoTDBMemControlTest() {
  }

  @Before
  public void setUp() throws Exception {
    if (testFlag) {
      deamon = IoTDB.getInstance();

      config.memThresholdWarning = 3 * IoTDBConstant.MB;
      config.memThresholdDangerous = 5 * IoTDBConstant.MB;

      BasicMemController.getInstance().setCheckInterval(15 * 1000);
      BasicMemController.getInstance()
          .setDangerousThreshold(config.memThresholdDangerous); // force initialize
      BasicMemController.getInstance().setWarningThreshold(config.memThresholdWarning);

      deamon.active();
      EnvironmentUtils.envSetUp();
      insertSQL();
    }
  }

  @After
  public void tearDown() throws Exception {
    if (testFlag) {
      deamon.stop();
      Thread.sleep(5000);
      EnvironmentUtils.cleanEnv();
    }
  }

  @Test
  public void test() throws ClassNotFoundException, SQLException, InterruptedException {
    // test a huge amount of write causes block
    if (!testFlag) {
      return;
    }
    Thread t1 = new Thread(() -> insert(d0));
    Thread t2 = new Thread(() -> insert(d1));
    t1.start();
    t2.start();
    t1.join();
    t2.join();
    assertEquals(exceptionCaught, true);
    assertEquals(BasicMemController.UsageLevel.WARNING,
        BasicMemController.getInstance().getCurrLevel());

    // test MemControlTread auto flush
    Thread.sleep(15000);
    assertEquals(BasicMemController.UsageLevel.SAFE,
        BasicMemController.getInstance().getCurrLevel());
  }

  public void insert(String deviceId) {
    try {
      Class.forName(Config.JDBC_DRIVER_NAME);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    Connection connection = null;
    TSRecord record = new TSRecord(0, deviceId);
    record.addTuple(new IntDataPoint(s0, 0));
    record.addTuple(new LongDataPoint(s1, 0));
    record.addTuple(new FloatDataPoint(s2, 0.0f));
    record.addTuple(new StringDataPoint(s3,
        new Binary(
            "\"sadasgagfdhdshdhdfhdfhdhdhdfherherdfsdfbdfsherhedfjerjerdfshfdshxzcvenerhreherjnfdgntrnt"
                + "ddfhdsf,joreinmoidnfh\"")));
    long recordMemSize = MemUtils.getTsRecordMemBufferwrite(record);
    long insertCnt = config.memThresholdDangerous / recordMemSize * 2;
    System.out.println(Thread.currentThread().getId() + " to insert " + insertCnt);
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      for (int i = 0; i < insertCnt; i++) {
        record.time = i + 1;
        statement.execute(Constant.recordToInsert(record));
        if (i % 1000 == 0) {
          System.out.println(Thread.currentThread().getId() + " inserting " + i);
        }
      }
      statement.close();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      if (e.getMessage().contains("exceeded dangerous threshold")) {
        exceptionCaught = true;
      }
    } finally {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void insertSQL() throws ClassNotFoundException, SQLException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    Connection connection = null;
    try {
      connection = DriverManager
          .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
      Statement statement = connection.createStatement();
      for (String sql : sqls) {
        statement.execute(sql);
      }
      statement.close();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
