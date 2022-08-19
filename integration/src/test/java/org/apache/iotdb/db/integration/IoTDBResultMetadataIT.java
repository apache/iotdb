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

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.ClusterTest;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;

import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Notice that, all test begins with "IoTDB" is integration test. All test which will start the
 * IoTDB server should be defined as integration test.
 */
@Category({LocalStandaloneTest.class, ClusterTest.class})
public class IoTDBResultMetadataIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("create timeseries root.sg.dev.status with datatype=text,encoding=PLAIN;");
      statement.execute("insert into root.sg.dev(time,status) values(1,3.14);");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void columnTypeTest() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      long lastTimestamp;
      try (ResultSet resultSet = statement.executeQuery("select status from root.sg.dev")) {
        Assert.assertTrue(resultSet.next());
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(2, metaData.getColumnCount());
        assertEquals("Time", metaData.getColumnName(1));
        assertEquals(Types.TIMESTAMP, metaData.getColumnType(1));
        assertEquals("TIME", metaData.getColumnTypeName(1));
        assertEquals("root.sg.dev.status", metaData.getColumnName(2));
        assertEquals(Types.VARCHAR, metaData.getColumnType(2));
        assertEquals("TEXT", metaData.getColumnTypeName(2));
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
