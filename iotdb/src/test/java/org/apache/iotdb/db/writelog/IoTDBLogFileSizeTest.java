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
package org.apache.iotdb.db.writelog;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.writelog.manager.MultiFileLogNodeManager;
import org.apache.iotdb.db.writelog.node.ExclusiveWriteLogNode;
import org.apache.iotdb.db.writelog.node.WriteLogNode;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IoTDBLogFileSizeTest {

  private IoTDB deamon;

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private TSFileConfig fileConfig = TSFileDescriptor.getInstance().getConfig();

  private boolean skip = true;

  private int groupSize;
  private long runtime = 600000;

  private String[] setUpSqls = new String[]{"SET STORAGE GROUP TO root.logFileTest.bufferwrite",
      "SET STORAGE GROUP TO root.logFileTest.overflow",
      "CREATE TIMESERIES root.logFileTest.bufferwrite.val WITH DATATYPE=INT32, ENCODING=PLAIN",
      "CREATE TIMESERIES root.logFileTest.overflow.val WITH DATATYPE=INT32, ENCODING=PLAIN",
      // overflow baseline
      "INSERT INTO root.logFileTest.overflow(timestamp,val) VALUES (1000000000, 0)"};

  private String[] tearDownSqls = new String[]{"DELETE TIMESERIES root.logFileTest.*"};

  @Before
  public void setUp() throws Exception {
    if (skip) {
      return;
    }
    groupSize = fileConfig.groupSizeInByte;
    fileConfig.groupSizeInByte = 8 * 1024 * 1024;
    EnvironmentUtils.closeStatMonitor();
    deamon = IoTDB.getInstance();
    deamon.active();
    EnvironmentUtils.envSetUp();
    executeSQL(setUpSqls);
  }

  @After
  public void tearDown() throws Exception {
    if (skip) {
      return;
    }
    fileConfig.groupSizeInByte = groupSize;
    executeSQL(tearDownSqls);
    deamon.stop();
    Thread.sleep(5000);
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testBufferwrite() throws InterruptedException {
    if (skip) {
      return;
    }
    final long[] maxLength = {0};
    Thread writeThread = new Thread(() -> {
      int cnt = 0;
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        return;
      }
      Connection connection = null;
      try {
        connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();
        while (true) {
          if (Thread.interrupted()) {
            System.out.println("Exit after " + cnt + " insertion");
            break;
          }
          String sql = String.format(
              "INSERT INTO root.logFileTest.bufferwrite(timestamp,val) VALUES (%d, %d)", ++cnt,
              cnt);
          statement.execute(sql);
          WriteLogNode logNode = MultiFileLogNodeManager.getInstance().getNode(
              "root.logFileTest.bufferwrite" + IoTDBConstant.BUFFERWRITE_LOG_NODE_SUFFIX, null,
              null);
          File bufferWriteWALFile = new File(
              logNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
          if (bufferWriteWALFile.exists() && bufferWriteWALFile.length() > maxLength[0]) {
            maxLength[0] = bufferWriteWALFile.length();
          }
        }
        statement.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            e.printStackTrace();
            return;
          }
        }
      }
    });
    writeThread.start();
    Thread.sleep(runtime);
    writeThread.interrupt();
    while (writeThread.isAlive()) {

    }
    System.out.println(
        "Max size of bufferwrite wal is " + MemUtils.bytesCntToStr(maxLength[0]) + " after "
            + runtime + "ms continuous writing");
  }

  @Test
  public void testOverflow() throws InterruptedException {
    if (skip) {
      return;
    }
    final long[] maxLength = {0};
    Thread writeThread = new Thread(() -> {
      int cnt = 0;
      try {
        Class.forName(Config.JDBC_DRIVER_NAME);
      } catch (ClassNotFoundException e) {
        e.printStackTrace();
        return;
      }
      Connection connection = null;
      try {
        connection = DriverManager
            .getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement();
        while (true) {
          if (Thread.interrupted()) {
            System.out.println("Exit after " + cnt + " insertion");
            break;
          }
          String sql = String
              .format("INSERT INTO root.logFileTest.overflow(timestamp,val) VALUES (%d, %d)",
                  ++cnt, cnt);
          statement.execute(sql);
          WriteLogNode logNode = MultiFileLogNodeManager.getInstance()
              .getNode("root.logFileTest.overflow" + IoTDBConstant.OVERFLOW_LOG_NODE_SUFFIX, null,
                  null);
          File WALFile = new File(
              logNode.getLogDirectory() + File.separator + ExclusiveWriteLogNode.WAL_FILE_NAME);
          if (WALFile.exists() && WALFile.length() > maxLength[0]) {
            maxLength[0] = WALFile.length();
          }
        }
        statement.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        if (connection != null) {
          try {
            connection.close();
          } catch (SQLException e) {
            e.printStackTrace();
            return;
          }
        }
      }
    });
    writeThread.start();
    Thread.sleep(runtime);
    writeThread.interrupt();
    while (writeThread.isAlive()) {

    }
    System.out.println(
        "Max size of overflow wal is " + MemUtils.bytesCntToStr(maxLength[0]) + " after " + runtime
            + "ms continuous writing");
  }

  private void executeSQL(String[] sqls) throws ClassNotFoundException, SQLException {
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
