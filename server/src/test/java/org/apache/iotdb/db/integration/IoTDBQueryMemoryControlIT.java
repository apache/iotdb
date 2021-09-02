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

package org.apache.iotdb.db.integration;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class IoTDBQueryMemoryControlIT {

  private static final String[] sqls =
      new String[] {
        "set storage group to root.ln",
        "create timeseries root.ln.wf01.wt01 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf01.wt02 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf01.wt03 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf01.wt04 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf01.wt05 with datatype=BOOLEAN,encoding=PLAIN",
        "create timeseries root.ln.wf02.wt01 with datatype=FLOAT,encoding=RLE",
        "create timeseries root.ln.wf02.wt02 with datatype=FLOAT,encoding=RLE",
        "create timeseries root.ln.wf02.wt03 with datatype=FLOAT,encoding=RLE",
        "create timeseries root.ln.wf02.wt04 with datatype=FLOAT,encoding=RLE",
        "create timeseries root.ln.wf02.wt05 with datatype=FLOAT,encoding=RLE",
        "create timeseries root.ln.wf03.wt01 with datatype=TEXT,encoding=PLAIN",
        "create timeseries root.ln.wf03.wt02 with datatype=TEXT,encoding=PLAIN",
        "create timeseries root.ln.wf03.wt03 with datatype=TEXT,encoding=PLAIN",
        "create timeseries root.ln.wf03.wt04 with datatype=TEXT,encoding=PLAIN",
        "create timeseries root.ln.wf03.wt05 with datatype=TEXT,encoding=PLAIN",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setMaxQueryDeduplicatedPathNum(10);
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
  }

  private static void createTimeSeries() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String sql : sqls) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    IoTDBDescriptor.getInstance().getConfig().setMaxQueryDeduplicatedPathNum(1000);
  }

  @Test
  public void selectWildcard() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select * from root.**");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }

      try {
        statement.execute("select count(*) from root");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardSlimit10() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select * from root.** slimit 10");
      statement.execute("select count(*) from root slimit 10");
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardSlimit11() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select * from root.** slimit 11");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }

      try {
        statement.execute("select count(*) from root slimit 11");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWildcardWildcardSlimit5Soffset7() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select wf01.*, wf02.*, wf03.* from root.ln slimit 5 soffset 7");
      ResultSetMetaData resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(1 + 5, resultSetMetaData.getColumnCount());
      for (int i = 2; i < 3 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }
      for (int i = 3 + 2; i < 5 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf03.wt0"));
      }

      statement.execute(
          "select count(wf01.*), avg(wf02.*), sum(wf03.*) from root.ln slimit 5 soffset 7");
      resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(5, resultSetMetaData.getColumnCount());
      for (int i = 1; i < 3 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }
      for (int i = 3 + 1; i < 5 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf03.wt0"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWildcardWildcardSlimit5Soffset5() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select wf01.*, wf02.*, wf03.* from root.ln slimit 5 soffset 5");
      ResultSetMetaData resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(1 + 5, resultSetMetaData.getColumnCount());
      for (int i = 2; i < 5 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }

      statement.execute(
          "select count(wf01.*), sum(wf02.*), avg(wf03.*) from root.ln slimit 5 soffset 5");
      resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(5, resultSetMetaData.getColumnCount());
      for (int i = 1; i < 5 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWildcardWildcardSlimit15Soffset5() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select wf01.*, wf03.*, wf02.* from root.ln slimit 15 soffset 5");
      ResultSetMetaData resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(1 + 10, resultSetMetaData.getColumnCount());
      for (int i = 2; i < 5 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf03.wt0"));
      }
      for (int i = 5 + 2; i < 10 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }

      statement.execute(
          "select sum(wf01.*), avg(wf03.*), count(wf02.*) from root.ln slimit 15 soffset 5");
      resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(10, resultSetMetaData.getColumnCount());
      for (int i = 1; i < 5 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf03.wt0"));
      }
      for (int i = 5 + 1; i < 10 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWildcardWildcardSlimit15Soffset4() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      try {
        statement.execute("select wf01.*, wf02.*, wf03.* from root.ln slimit 15 soffset 4");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }
      try {
        statement.execute(
            "select count(wf01.*), avg(wf02.*), sum(wf03.*) from root.ln slimit 15 soffset 4");
      } catch (SQLException e) {
        assertTrue(e.getMessage().contains("Too many paths in one query!"));
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectWildcardWildcardWildcardSlimit3Soffset4() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("select wf01.*, wf02.*, wf03.* from root.ln slimit 3 soffset 4");
      ResultSetMetaData resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(1 + 3, resultSetMetaData.getColumnCount());
      for (int i = 2; i < 1 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf01.wt0"));
      }
      for (int i = 1 + 2; i < 3 + 2; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }

      statement.execute(
          "select sum(wf01.*), count(wf02.*), avg(wf03.*) from root.ln slimit 3 soffset 4");
      resultSetMetaData = statement.getResultSet().getMetaData();
      assertEquals(3, resultSetMetaData.getColumnCount());
      for (int i = 1; i < 1 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf01.wt0"));
      }
      for (int i = 1 + 1; i < 3 + 1; ++i) {
        assertTrue(resultSetMetaData.getColumnName(i).contains("root.ln.wf02.wt0"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
