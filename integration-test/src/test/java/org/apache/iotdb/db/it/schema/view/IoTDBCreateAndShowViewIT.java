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

import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** This is an example for integration test. */
@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBCreateAndShowViewIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.db;",
        "CREATE DATABASE root.myview;",
        "CREATE DATABASE root.cal_view;",
        "CREATE TIMESERIES root.db.d01.s01 INT32 encoding=RLE compression=SNAPPY;",
        "CREATE TIMESERIES root.db.d01.s02 INT32 encoding=RLE compression=SNAPPY;",
        "CREATE TIMESERIES root.db.d02.s01 INT32 encoding=RLE compression=SNAPPY;",
        "CREATE TIMESERIES root.db.d02.s02 INT32 encoding=RLE compression=SNAPPY;",
        "CREATE VIEW root.myview.d01.s01 AS root.db.d01.s01;",
        "CREATE VIEW root.myview.d01.s02 AS SELECT s02 FROM root.db.d01;",
        "CREATE VIEW root.myview.d02(s01, s02) AS SELECT s01, s02 FROM root.db.d02;",
        "CREATE VIEW root.cal_view.avg AS SELECT (s01+s02)/2 FROM root.db.d01;",
        "CREATE VIEW root.cal_view(multiple, divide) AS SELECT s01*s02, s01/s02 FROM root.db.d02;",
        "CREATE VIEW root.cal_view.cast_view AS SELECT CAST(s01 as TEXT) FROM root.db.d01;",
      };

  private static final String[] unsupportedSQLs =
      new String[] {
        "CREATE VIEW root.myview.nested_view AS root.myview.d01.s01;",
        "CREATE VIEW root.cal_view(agg_avg1, agg_avg2) AS SELECT AVG(s01)+1 FROM root.db.d01, root.db.d02;",
        "CREATE VIEW root.cal_view(agg_max1, agg_max2) AS SELECT MAX_VALUE(s01) FROM root.db.d01, root.db.d02;",
        "CREATE VIEW root.myview.illegal_view AS root.myview.d01.s01 + 1;",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    createSchema();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // region Test show timesereis
  @Test
  public void testShowOriginTimeseries() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "root.db.d01.s01,null,root.db,INT32,RLE,SNAPPY,null,null,;",
                "root.db.d01.s02,null,root.db,INT32,RLE,SNAPPY,null,null,;",
                "root.db.d02.s01,null,root.db,INT32,RLE,SNAPPY,null,null,;",
                "root.db.d02.s02,null,root.db,INT32,RLE,SNAPPY,null,null,;"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("SHOW TiMESERIES root.db.**;");
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ALIAS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATABASE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION)
                + ","
                + resultSet.getString(ColumnHeaderConstant.TAGS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                + ";";

        System.out.println("actual result:" + ans);
        assertTrue(retSet.contains(ans));
        count++;
      }
      assertEquals(retSet.size(), count);
      resultSet.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowAliasViewsWithShowTimeseries() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "root.myview.d01.s01,null,root.myview,INT32,null,null,null,null,logical;",
                "root.myview.d01.s02,null,root.myview,INT32,null,null,null,null,logical;",
                "root.myview.d02.s01,null,root.myview,INT32,null,null,null,null,logical;",
                "root.myview.d02.s02,null,root.myview,INT32,null,null,null,null,logical;"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("SHOW TiMESERIES root.myview.**;");
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ALIAS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATABASE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION)
                + ","
                + resultSet.getString(ColumnHeaderConstant.TAGS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                + ";";

        System.out.println("actual result:" + ans);
        assertTrue(retSet.contains(ans));
        count++;
      }
      assertEquals(retSet.size(), count);
      resultSet.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void testShowViewsWithCalculationWithShowTimeseries() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "root.cal_view.avg,null,root.cal_view,DOUBLE,null,null,null,null,logical;",
                "root.cal_view.multiple,null,root.cal_view,DOUBLE,null,null,null,null,logical;",
                "root.cal_view.divide,null,root.cal_view,DOUBLE,null,null,null,null,logical;",
                "root.cal_view.cast_view,null,root.cal_view,TEXT,null,null,null,null,logical;"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("SHOW TiMESERIES root.cal_view.*;");
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ALIAS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATABASE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ENCODING)
                + ","
                + resultSet.getString(ColumnHeaderConstant.COMPRESSION)
                + ","
                + resultSet.getString(ColumnHeaderConstant.TAGS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                + ";";

        System.out.println("actual result:" + ans);
        assertTrue(retSet.contains(ans));
        count++;
      }
      assertEquals(retSet.size(), count);
      resultSet.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
  // endregion

  // region Test Show View
  @Test
  public void testShowAllViewsWithShowView() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "root.myview.d01.s01,root.myview,INT32,null,null,logical,root.db.d01.s01;",
                "root.myview.d01.s02,root.myview,INT32,null,null,logical,root.db.d01.s02;",
                "root.myview.d02.s01,root.myview,INT32,null,null,logical,root.db.d02.s01;",
                "root.myview.d02.s02,root.myview,INT32,null,null,logical,root.db.d02.s02;",
                "root.cal_view.avg,root.cal_view,DOUBLE,null,null,logical,(root.db.d01.s01 + root.db.d01.s02) / 2;",
                "root.cal_view.multiple,root.cal_view,DOUBLE,null,null,logical,root.db.d02.s01 * root.db.d02.s02;",
                "root.cal_view.divide,root.cal_view,DOUBLE,null,null,logical,root.db.d02.s01 / root.db.d02.s02;",
                "root.cal_view.cast_view,root.cal_view,TEXT,null,null,logical,cast(type=TEXT)(root.db.d01.s01);"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      ResultSet resultSet = statement.executeQuery("SHOW VIEW root.**;");
      int count = 0;
      while (resultSet.next()) {
        String ans =
            resultSet.getString(ColumnHeaderConstant.TIMESERIES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATABASE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.DATATYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.TAGS)
                + ","
                + resultSet.getString(ColumnHeaderConstant.ATTRIBUTES)
                + ","
                + resultSet.getString(ColumnHeaderConstant.VIEW_TYPE)
                + ","
                + resultSet.getString(ColumnHeaderConstant.SOURCE)
                + ";";

        System.out.println("actual result:" + ans);
        assertTrue(retSet.contains(ans));
        count++;
      }
      assertEquals(retSet.size(), count);
      resultSet.close();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
  // endregion

  // region unsupported SQLs
  @Test
  public void testUnsupportedSQLs() {
    for (String unsupportedSQL : unsupportedSQLs) {
      try (Connection connection = EnvFactory.getEnv().getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(String.format(unsupportedSQL));
        Assert.fail(String.format("SQL [%s] should fail but no exception thrown.", unsupportedSQL));
      } catch (SQLException ignored) {
      }
    }
  }

  // endregion

  private static void createSchema() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : SQLs) {
        statement.execute(sql);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
