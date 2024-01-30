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

package org.apache.iotdb.db.it.udaf;

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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBUDAFMiscIT {
  private static final String[] dataset =
      new String[] {
        "CREATE DATABASE root.sg;",
        "CREATE TIMESERIES root.sg.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "CREATE TIMESERIES root.sg.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN;",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES(1, 1, 1);",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES(2, 2, 2);",
        "INSERT INTO root.sg.d1(time, s1, s2) VALUES(3, 3, 3);",
        "flush;",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setPartitionInterval(1000);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(dataset);
    registerUDF();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static void registerUDF() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(
          "CREATE FUNCTION udaf AS 'org.apache.iotdb.db.query.udf.example.UDAFCount'");
      statement.execute("CREATE FUNCTION udf AS 'org.apache.iotdb.db.query.udf.example.Adder'");
    } catch (SQLException throwable) {
      fail(throwable.getMessage());
    }
  }

  @Test
  public void mixUDAFAndUDTFTest() throws Exception {
    String[] expected = new String[] {"3"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int count = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT udaf(udf(s1, s2)) AS res " + "FROM root.sg.d1 ")) {
        while (resultSet.next()) {
          String actual = resultSet.getString("res");
          Assert.assertEquals(expected[count], actual);
          count++;
        }
      }

      assertEquals(expected.length, count);
    }
  }

  @Test
  public void UDAFAsOperandTest() throws Exception {
    String[] expected = new String[] {"6.0"};
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      int count = 0;
      try (ResultSet resultSet =
          statement.executeQuery("SELECT udaf(s1) + udaf(s2) AS res " + "FROM root.sg.d1 ")) {
        while (resultSet.next()) {
          String actual = resultSet.getString("res");
          Assert.assertEquals(expected[count], actual);
          count++;
        }
      }

      assertEquals(expected.length, count);
    }
  }
}
