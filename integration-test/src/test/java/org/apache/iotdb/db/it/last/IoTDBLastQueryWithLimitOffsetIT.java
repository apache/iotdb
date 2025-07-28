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

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryWithLimitOffsetIT {
  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("insert into root.sg.d1(time, s1, s2) values(1, 1, 1)");
      statement.execute("insert into root.sg.d2(time, s1, s2) aligned values(2, 1, 1)");
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
  public void testWithLimit() {
    String[] retArray =
        new String[] {
          "1,root.sg.d1.s1,1.0,DOUBLE",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg.* order by timeseries asc limit 1")) {
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
          assertEquals(retArray[cnt++], ans);
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testWithOffset() {
    String[] retArray = new String[] {"2,root.sg.d2.s2,1.0,DOUBLE"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg.* order by timeseries asc offset 3")) {
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
          assertEquals(retArray[cnt++], ans);
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testWithLimitAndOffset() {
    String[] retArray =
        new String[] {
          "1,root.sg.d1.s2,1.0,DOUBLE", "2,root.sg.d2.s1,1.0,DOUBLE",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last * from root.sg.* order by timeseries asc limit 2 offset 1")) {
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
          assertEquals(retArray[cnt++], ans);
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testWithSLimitOrSOffset() {

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try {
        statement.executeQuery("select last * from root.sg.* order by timeseries asc slimit 1");
        fail();
      } catch (Exception ignored) {

      }

      try {
        statement.executeQuery("select last * from root.sg.* order by timeseries asc soffset 1");
        fail();
      } catch (Exception ignored) {

      }

      try {
        statement.executeQuery(
            "select last * from root.sg.* order by timeseries asc slimit 1 soffset 1");
        fail();
      } catch (Exception ignored) {

      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testWithSortLimit() {
    String[] retArray =
        new String[] {
          "2,root.sg.d2.s2,1.0,DOUBLE",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery(
              "select last * from root.sg.** order by time desc, timeseries desc limit 1")) {
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
          assertEquals(retArray[cnt++], ans);
        }
        assertEquals(retArray.length, cnt);
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
