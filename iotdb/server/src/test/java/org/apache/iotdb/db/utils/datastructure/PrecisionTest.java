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
package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

public class PrecisionTest {

  @Before
  public void setUp() {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testDoublePrecision1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=PLAIN, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.2345678";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testDoublePrecision2() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=DOUBLE, encoding=RLE, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.23";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFloatPrecision1() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=FLOAT, encoding=PLAIN, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.2345678";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testFloatPrecision2() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      statement.execute("SET STORAGE GROUP TO root.turbine1");
      statement.execute(
          "create timeseries root.turbine1.d1.s1 with datatype=FLOAT, encoding=RLE, compression=SNAPPY");

      statement.execute("insert into root.turbine1.d1(timestamp,s1) values(1,1.2345678)");

      ResultSet resultSet = statement.executeQuery("select * from root.turbine1.**");

      String str = "1.23";
      while (resultSet.next()) {
        assertEquals(str, resultSet.getString("root.turbine1.d1.s1"));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
