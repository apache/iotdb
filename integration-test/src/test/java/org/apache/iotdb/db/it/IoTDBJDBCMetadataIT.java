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

package org.apache.iotdb.db.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBJDBCMetadataIT {

  protected static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg_type;",
        "CREATE TIMESERIES root.sg_type.d_0.s_boolean BOOLEAN;",
        "CREATE TIMESERIES root.sg_type.d_0.s_int32 INT32;",
        "CREATE TIMESERIES root.sg_type.d_0.s_int64 INT64;",
        "CREATE TIMESERIES root.sg_type.d_0.s_float FLOAT;",
        "CREATE TIMESERIES root.sg_type.d_0.s_double DOUBLE;",
        "CREATE TIMESERIES root.sg_type.d_0.s_text TEXT;",
        "CREATE TIMESERIES root.sg_type.d_0.s_timestamp TIMESTAMP;",
        "CREATE TIMESERIES root.sg_type.d_0.s_date DATE;",
        "CREATE TIMESERIES root.sg_type.d_0.s_blob BLOB;",
        "CREATE TIMESERIES root.sg_type.d_0.s_string STRING;",
        "INSERT INTO root.sg_type.d_0(time, s_int32, s_int64, s_float, s_double, s_text) VALUES (0, 0, 0, 0.000000, 0.000000, 'text0');"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // without lastCache
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      prepareData(SQLs);
    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testMetadata() throws Exception {
    String[] columnNames =
        new String[] {
          "Time",
          "Device",
          "s_boolean",
          "s_int32",
          "s_int64",
          "s_float",
          "s_double",
          "s_text",
          "s_timestamp",
          "s_date",
          "s_blob",
          "s_string"
        };
    String[] columnTypes =
        new String[] {
          "TIMESTAMP",
          "TEXT",
          "BOOLEAN",
          "INT32",
          "INT64",
          "FLOAT",
          "DOUBLE",
          "TEXT",
          "TIMESTAMP",
          "DATE",
          "BLOB",
          "STRING"
        };
    String[] values =
        new String[] {
          "0", "root.sg_type.d_0", null, "0", "0", "0.0", "0.0", "text0", null, null, null, null
        };
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select s_boolean, s_int32, s_int64, s_float, s_double, s_text, s_timestamp, s_date, s_blob, s_string from root.sg_type.d_0 align by device")) {
        ResultSetMetaData metaData = resultSet.getMetaData();
        assertEquals(columnNames.length, metaData.getColumnCount());
        assertTrue(resultSet.next());
        for (int i = 1; i <= columnNames.length; i++) {
          assertEquals(columnNames[i - 1], metaData.getColumnName(i));
          assertEquals(columnTypes[i - 1], metaData.getColumnTypeName(i));
          assertEquals(values[i - 1], resultSet.getString(i));
        }
      }
    }
  }
}
