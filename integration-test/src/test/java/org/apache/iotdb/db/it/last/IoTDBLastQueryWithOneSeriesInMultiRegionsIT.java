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

package org.apache.iotdb.db.it.last;

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
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
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryWithOneSeriesInMultiRegionsIT {

  private static final String[] NON_ALIGNED_SERIES =
      new String[] {
        "create database root.sg",
        "insert into root.sg.d1(time,s1) values (1,2)",
        "insert into root.sg.d1(time,s1) values (6048000,3)",
        "insert into root.sg.d1(time,s1) values (6048000000,5)",
        "insert into root.sg.d1(time,s2) values (1,2)",
        "insert into root.sg.d1(time,s2) values (6048000,3)",
        "insert into root.sg.d1(time,s2) values (6048000000,5)",
        "insert into root.sg.d1(time,s3) values (1,2)",
        "insert into root.sg.d1(time,s3) values (6048000,3)",
        "insert into root.sg.d1(time,s3) values (6048000000,5)"
      };

  private static final String[] ALIGNED_SERIES =
      new String[] {
        "insert into root.sg.d2(time,s1,s2,s3) aligned values (1,2,2,2)",
        "insert into root.sg.d2(time,s1,s2,s3) aligned values (6048000,3,3,3)",
        "insert into root.sg.d2(time,s1,s2,s3) aligned values (6048000000,5,5,5)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false);
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(NON_ALIGNED_SERIES);
    prepareData(ALIGNED_SERIES);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void testNonAlignedSeriesInMultiRegion() {
    Set<String> retArray1 =
        new HashSet<String>() {
          {
            add("6048000000,root.sg.d1.s1,5.0,FLOAT");
            add("6048000000,root.sg.d1.s2,5.0,FLOAT");
            add("6048000000,root.sg.d1.s3,5.0,FLOAT");
          }
        };

    Set<String> retArray2 =
        new HashSet<String>() {
          {
            add("6048000,root.sg.d1.s1,3.0,FLOAT");
            add("6048000,root.sg.d1.s2,3.0,FLOAT");
            add("6048000,root.sg.d1.s3,3.0,FLOAT");
          }
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last * from root.sg.d1")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          cnt++;
          assertTrue(retArray1.contains(ans));
        }
        assertEquals(retArray1.size(), cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg.d1 where time < 6048000000")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          cnt++;
          assertTrue(retArray2.contains(ans));
        }
        assertEquals(retArray2.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testAlignedSeriesInMultiRegion() {
    Set<String> retArray1 =
        new HashSet<String>() {
          {
            add("6048000000,root.sg.d2.s1,5.0,FLOAT");
            add("6048000000,root.sg.d2.s2,5.0,FLOAT");
            add("6048000000,root.sg.d2.s3,5.0,FLOAT");
          }
        };

    Set<String> retArray2 =
        new HashSet<String>() {
          {
            add("6048000,root.sg.d2.s1,3.0,FLOAT");
            add("6048000,root.sg.d2.s2,3.0,FLOAT");
            add("6048000,root.sg.d2.s3,3.0,FLOAT");
          }
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last * from root.sg.d2")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          cnt++;
          assertTrue(retArray1.contains(ans));
        }
        assertEquals(retArray1.size(), cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg.d2 where time < 6048000000")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          assertTrue(retArray2.contains(ans));
          cnt++;
        }
        assertEquals(retArray2.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testNonAlignedAndAlignedSeriesInMultiRegion() {
    Set<String> retArray1 =
        new HashSet<String>() {
          {
            add("6048000000,root.sg.d1.s1,5.0,FLOAT");
            add("6048000000,root.sg.d1.s2,5.0,FLOAT");
            add("6048000000,root.sg.d1.s3,5.0,FLOAT");
            add("6048000000,root.sg.d2.s1,5.0,FLOAT");
            add("6048000000,root.sg.d2.s2,5.0,FLOAT");
            add("6048000000,root.sg.d2.s3,5.0,FLOAT");
          }
        };

    Set<String> retArray2 =
        new HashSet<String>() {
          {
            add("6048000,root.sg.d1.s1,3.0,FLOAT");
            add("6048000,root.sg.d1.s2,3.0,FLOAT");
            add("6048000,root.sg.d1.s3,3.0,FLOAT");
            add("6048000,root.sg.d2.s1,3.0,FLOAT");
            add("6048000,root.sg.d2.s2,3.0,FLOAT");
            add("6048000,root.sg.d2.s3,3.0,FLOAT");
          }
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet = statement.executeQuery("select last * from root.sg.**")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          cnt++;
          assertTrue(retArray1.contains(ans));
        }
        assertEquals(retArray1.size(), cnt);
      }

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg.** where time < 6048000000")) {
        int cnt = 0;
        while (resultSet.next()) {
          String ans =
              resultSet.getString(ColumnHeaderConstant.TIME)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.VALUE)
                  + ","
                  + resultSet.getString(ColumnHeaderConstant.DATATYPE);
          cnt++;
          assertTrue(retArray2.contains(ans));
        }
        assertEquals(retArray2.size(), cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
