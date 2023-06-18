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
package org.apache.iotdb.db.it.schema.view;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBShowDevicesContainsViewIT {

  private static final String showDevicesSQL = "show devices;";
  private static final String deleteAllTimeSeriesSQL = "delete timeseries root.**;";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private void validateResultSetAndStandard(ResultSet resultSet, Set<String> standard)
      throws SQLException {
    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
    while (resultSet.next()) {
      StringBuilder builder = new StringBuilder();
      for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
        builder.append(resultSet.getString(i)).append(",");
      }
      String string = builder.toString();
      Assert.assertTrue(standard.contains(string));
      standard.remove(string);
    }
  }

  @Test
  public void testShowDevicesContainedView01() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // step 1. create normal time series and the alignment is true or false
      String[] sqls =
          new String[] {
            "create timeseries root.db.d01.s01 INT32 encoding=RLE;",
            "create timeseries root.db.d01.s02 INT32 encoding=RLE;",
            "create aligned timeseries root.db.d02(s01 INT32 encoding=RLE, s02 INT32 encoding=RLE);"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      Set<String> standard =
          new HashSet<>(Arrays.asList("root.db.d01,false,", "root.db.d02,true,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 2. create views under these devices and the alignment remain unchanged
      sqls =
          new String[] {
            "create VIEW root.db.d01.s_view AS root.db.d01.s01",
            "create VIEW root.db.d02.s_view AS root.db.d02.s01",
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard = new HashSet<>(Arrays.asList("root.db.d01,false,", "root.db.d02,true,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 3. create view and auto create device, the alignment of these devices are null
      sqls =
          new String[] {
            "create VIEW root.db.d03.s_view AS root.db.d01.s01",
            "create VIEW root.db.d04.s_view AS root.db.d01.s01",
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList(
                  "root.db.d01,false,",
                  "root.db.d02,true,",
                  "root.db.d03,null,",
                  "root.db.d04,null,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 4. create time series under those devices who just have views, and alignment should be
      // true or false
      sqls =
          new String[] {
            "create timeseries root.db.d03.s01 INT32 encoding=RLE;",
            "create aligned timeseries root.db.d04(s01 INT32 encoding=RLE, s02 INT32 encoding=RLE);"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList(
                  "root.db.d01,false,",
                  "root.db.d02,true,",
                  "root.db.d03,false,",
                  "root.db.d04,true,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 5. delete all non-view time series created at last step, and the alignment of devices
      // should be null
      sqls =
          new String[] {
            "delete timeseries root.db.d03.s01;",
            "delete timeseries root.db.d04.s01;",
            "delete timeseries root.db.d04.s02;"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList(
                  "root.db.d01,false,",
                  "root.db.d02,true,",
                  "root.db.d03,null,",
                  "root.db.d04,null,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // clean environment
      statement.execute(deleteAllTimeSeriesSQL);
    } // end of try
  }

  @Test
  public void testShowDevicesContainedView02() throws SQLException {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      // step 1. create normal time series (prepared for views)
      String[] sqls =
          new String[] {
            "create timeseries root.db.d01.s01 INT32 encoding=RLE;",
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      Set<String> standard = new HashSet<>(Collections.singletonList("root.db.d01,false,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 2. create view and auto create device, the alignment of these devices are null
      sqls =
          new String[] {
            "create VIEW root.db.d05.s_view AS root.db.d01.s01",
            "create VIEW root.db.d06.s_view AS root.db.d01.s01",
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList("root.db.d01,false,", "root.db.d05,null,", "root.db.d06,null,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 3. insert data under those devices who have only views, the alignment should be true
      // or false
      sqls =
          new String[] {
            "insert into root.db.d05(time,s01) values(1, 36);",
            "insert into root.db.d06(time,s01) aligned values(1, 36);"
          };
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList("root.db.d01,false,", "root.db.d05,false,", "root.db.d06,true,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // step 4. delete all auto created timeseries at last step, and the alignment of devices
      // should be null
      sqls =
          new String[] {"delete timeseries root.db.d05.s01;", "delete timeseries root.db.d06.s01;"};
      for (String sql : sqls) {
        statement.execute(sql);
      }
      standard =
          new HashSet<>(
              Arrays.asList("root.db.d01,false,", "root.db.d05,null,", "root.db.d06,null,"));
      validateResultSetAndStandard(statement.executeQuery(showDevicesSQL), standard);

      // clean environment
      statement.execute(deleteAllTimeSeriesSQL);
    } // end of try
  }
}
