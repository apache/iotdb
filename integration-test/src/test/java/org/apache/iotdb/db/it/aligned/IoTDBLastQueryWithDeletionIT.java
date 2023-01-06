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
package org.apache.iotdb.db.it.aligned;

import org.apache.iotdb.db.it.utils.AlignedWriteUtil;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.itbase.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryWithDeletionIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertData();

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("delete timeseries root.sg1.d1.s2");
      statement.execute("delete from root.sg1.d1.s1 where time <= 27");
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void selectAllAlignedLastTest() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select last * from root.sg1.d1 order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedAndNonAlignedLastTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "20,root.sg1.d2.s1,20.0,FLOAT",
                "40,root.sg1.d2.s2,40,INT32",
                "30,root.sg1.d2.s3,30,INT64",
                "30,root.sg1.d2.s4,false,BOOLEAN",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select last * from root.sg1.* order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(Collections.singletonList("40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select last * from root.sg1.d1 where time > 30 order by timeseries asc")) {
      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest1() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "30,root.sg1.d1.s4,false,BOOLEAN", "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select last s1, s4, s5 from root.sg1.d1 order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest2() {
    Set<String> retSet =
        new HashSet<>(Collections.singletonList("30,root.sg1.d1.s4,false,BOOLEAN"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery("select last s1, s4 from root.sg1.d1 order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(Collections.singletonList("40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select last s1, s4, s5 from root.sg1.d1 where time > 30 order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedAndNonAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet =
            statement.executeQuery(
                "select last d2.s5, d1.s4, d2.s1, d1.s5, d2.s4, d1.s1 from root.sg1 where time > 30 order by timeseries asc")) {

      int cnt = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(TIMESTAMP_STR)
                + ","
                + resultSet.getString(TIMESEIRES_STR)
                + ","
                + resultSet.getString(VALUE_STR)
                + ","
                + resultSet.getString(DATA_TYPE_STR);
        assertTrue(ans, retSet.contains(ans));
        cnt++;
      }
      assertEquals(retSet.size(), cnt);

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
