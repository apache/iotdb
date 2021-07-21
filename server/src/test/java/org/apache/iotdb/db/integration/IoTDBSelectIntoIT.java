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

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.jdbc.Config;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class IoTDBSelectIntoIT {

  private static final double E = 0.0001;

  private static final String[] INSERTION_SQLS = {
    "insert into root.sg.d1(time, s2, s3, s4, s5, s6) values (0, 0, 0, 0, true, '0')",
    "insert into root.sg.d1(time, s1, s3, s4, s5, s6) values (1, 1, 1, 1, false, '1')",
    "insert into root.sg.d1(time, s1, s2, s4, s5, s6) values (2, 2, 2, 2, true, '2')",
    "insert into root.sg.d1(time, s1, s2, s3, s5, s6) values (3, 3, 3, 3, false, '3')",
    "insert into root.sg.d1(time, s1, s2, s3, s4, s5, s6) values (4, 4, 4, 4, 4, true, '4')",
  };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    Class.forName(Config.JDBC_DRIVER_NAME);
    createTimeSeries();
    generateData();
  }

  private static void createTimeSeries() throws MetadataException {
    IoTDB.metaManager.setStorageGroup(new PartialPath("root.sg"));
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s1"),
        TSDataType.INT32,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s2"),
        TSDataType.INT64,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s3"),
        TSDataType.FLOAT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s4"),
        TSDataType.DOUBLE,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s5"),
        TSDataType.BOOLEAN,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
    IoTDB.metaManager.createTimeseries(
        new PartialPath("root.sg.d1.s6"),
        TSDataType.TEXT,
        TSEncoding.PLAIN,
        CompressionType.UNCOMPRESSED,
        null);
  }

  private static void generateData() {
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      for (String dataGenerationSql : INSERTION_SQLS) {
        statement.execute(dataGenerationSql);
      }
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void selectIntoSameDevice() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into s7, s8, s9, s10, s11, s12 from root.sg.d1");

      ResultSet resultSet =
          statement.executeQuery("select s7, s8, s9, s10, s11, s12 from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < 6 + 1; ++j) {
          stringBuilder.append(resultSet.getString(j + 1)).append(',');
        }
        System.out.println(stringBuilder.toString());
      }

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void selectIntoDifferentDevices() {
    try (Statement statement =
        DriverManager.getConnection(Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root")
            .createStatement()) {
      statement.execute(
          "select s1, s2, s3, s4, s5, s6 into s1.${2}, s2.${2}, s3.${2}, s4.${2}, s5.${2}, s6.${2} from root.sg.d1");

      ResultSet resultSet =
          statement.executeQuery("select s1.d1, s2.d1, s3.d1, s4.d1, s5.d1, s6.d1 from root.sg.d1");

      int columnCount = resultSet.getMetaData().getColumnCount();
      assertEquals(1 + 6, columnCount);

      for (int i = 0; i < INSERTION_SQLS.length; ++i) {
        resultSet.next();
        StringBuilder stringBuilder = new StringBuilder();
        for (int j = 0; j < 6 + 1; ++j) {
          stringBuilder.append(resultSet.getString(j + 1)).append(',');
        }
        System.out.println(stringBuilder.toString());
      }

      assertFalse(resultSet.next());
    } catch (SQLException throwable) {
      throwable.printStackTrace();
      fail(throwable.getMessage());
    }
  }
}
