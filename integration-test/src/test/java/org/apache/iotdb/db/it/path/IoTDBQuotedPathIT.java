/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.it.path;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.constant.TestConstant;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBQuotedPathIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void test() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      String[] exp =
          new String[] {
            "1509465600000,true", "1509465600001,true", "1509465600002,false", "1509465600003,false"
          };
      statement.execute("CREATE DATABASE root.ln");
      statement.execute(
          "CREATE TIMESERIES root.ln.`wf+01`.wt01.`status+2+3` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "INSERT INTO root.ln.`wf+01`.wt01(timestamp,`status+2+3`) values(1509465600000,true)");
      statement.execute(
          "INSERT INTO root.ln.`wf+01`.wt01(timestamp,`status+2+3`) values(1509465600001,true)");
      statement.execute(
          "INSERT INTO root.ln.`wf+01`.wt01(timestamp,`status+2+3`) values(1509465600002,false)");
      statement.execute(
          "INSERT INTO root.ln.`wf+01`.wt01(timestamp,`status+2+3`) values(1509465600003,false)");
      statement.execute(
          "CREATE TIMESERIES root.ln.`wf+01`.wt02.`abd` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");
      statement.execute(
          "CREATE TIMESERIES root.ln.`wf+01`.wt02.`asd12` WITH DATATYPE=BOOLEAN, ENCODING=PLAIN");

      int cnt = 0;
      try (ResultSet resultSet = statement.executeQuery("SELECT * FROM root.ln.`wf+01`.wt01")) {
        while (resultSet.next()) {
          String result =
              resultSet.getString(TestConstant.TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }
      }
      try (ResultSet resultSet =
          statement.executeQuery("SELECT * FROM root.ln.`wf+01`.wt01 WHERE `status+2+3` = false")) {
        exp = new String[] {"1509465600002,false", "1509465600003,false"};
        cnt = 0;
        while (resultSet.next()) {
          String result =
              resultSet.getString(TestConstant.TIMESTAMP_STR) + "," + resultSet.getString(2);
          assertEquals(exp[cnt++], result);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }

  @Test
  public void testIllegalStorageGroup() {
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      statement.execute("CREATE DATABASE root.`\"ln`");
    } catch (final SQLException e) {
      Assert.assertTrue(
          e.getMessage().contains("Error StorageGroup name")
              || e.getMessage()
                  .contains(
                      "The database name can only contain english or chinese characters, numbers, backticks and underscores."));
    } catch (final Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
