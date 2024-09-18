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
import org.apache.iotdb.db.queryengine.common.header.ColumnHeaderConstant;
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
public class IoTDBAlignedLastQueryIT {

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setEnableSeqSpaceCompaction(false)
        .setEnableUnseqSpaceCompaction(false)
        .setEnableCrossSpaceCompaction(false)
        .setMaxTsBlockLineNumber(3);
    EnvFactory.getEnv().initClusterEnvironment();
    AlignedWriteUtil.insertData();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ----------------------------------------Last Query-----------------------------------------
  @Test
  public void selectAllAlignedLastTest() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg1.d1 order by timeseries asc")) {
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
          assertTrue(ans, retSet.contains(ans));
          cnt++;
        }
        assertEquals(retSet.size(), cnt);
      }

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
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "40,root.sg1.d1.s2,40,INT32",
                "30,root.sg1.d1.s3,30,INT64",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT",
                "20,root.sg1.d2.s1,20.0,FLOAT",
                "40,root.sg1.d2.s2,40,INT32",
                "30,root.sg1.d2.s3,30,INT64",
                "30,root.sg1.d2.s4,false,BOOLEAN",
                "40,root.sg1.d2.s5,non_aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last * from root.sg1.*  order by timeseries asc")) {
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectAllAlignedLastWithTimeFilterTest() {

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("40,root.sg1.d1.s2,40,INT32", "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

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
                "23,root.sg1.d1.s1,230000.0,FLOAT",
                "30,root.sg1.d1.s4,false,BOOLEAN",
                "40,root.sg1.d1.s5,aligned_test40,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void selectSomeAlignedLastTest2() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList("23,root.sg1.d1.s1,230000.0,FLOAT", "30,root.sg1.d1.s4,false,BOOLEAN"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

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
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
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
      }

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
        Statement statement = connection.createStatement()) {

      // 1 4 5
      try (ResultSet resultSet =
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void cacheHitTest() {
    selectAllAlignedLastTest();
    selectAllAlignedAndNonAlignedLastTest();
    selectSomeAlignedAndNonAlignedLastWithTimeFilterTest();
    selectSomeAlignedLastTest1();
    selectSomeAlignedLastTest2();
    selectSomeAlignedLastWithTimeFilterTest();
    selectSomeAlignedAndNonAlignedLastWithTimeFilterTest();
  }

  @Test
  public void testNullInMemtable() {
    String[] sqls =
        new String[] {
          "create aligned timeseries root.ln_1.tb_6141(风机退出_BOOLEAN BOOLEAN encoding=RLE,`出水NH4-N_DOUBLE` DOUBLE encoding=GORILLA,膜产水状态_BOOLEAN BOOLEAN encoding=RLE,11_TEXT TEXT encoding=PLAIN,产水间歇运行时间设置_DOUBLE DOUBLE encoding=GORILLA,文本_TEXT TEXT encoding=PLAIN, 风机投入_BOOLEAN BOOLEAN encoding=RLE,枚举_INT32 INT32 encoding=RLE,出水TP_DOUBLE DOUBLE encoding=GORILLA,水管流速_DOUBLE DOUBLE encoding=GORILLA,CO2_DOUBLE DOUBLE encoding=GORILLA,`开关量-运行_BOOLEAN` BOOLEAN encoding=RLE,功能码_DOUBLE DOUBLE encoding=GORILLA);",
          "alter timeseries root.ln_1.tb_6141.风机退出_BOOLEAN upsert alias=风机退出;",
          "alter timeseries root.ln_1.tb_6141.水管流速_DOUBLE upsert alias=水管流速;",
          "alter timeseries root.ln_1.tb_6141.CO2_DOUBLE upsert alias=CO2;",
          "alter timeseries root.ln_1.tb_6141.风机投入_BOOLEAN upsert alias=风机投入;",
          "alter timeseries root.ln_1.tb_6141.产水间歇运行时间设置_DOUBLE upsert alias=产水间歇运行时间设置;",
          "alter timeseries root.ln_1.tb_6141.膜产水状态_BOOLEAN upsert alias=膜产水状态;",
          "alter timeseries root.ln_1.tb_6141.枚举_INT32 upsert alias=枚举;",
          "alter timeseries root.ln_1.tb_6141.出水TP_DOUBLE upsert alias=出水TP;",
          "alter timeseries root.ln_1.tb_6141.文本_TEXT upsert alias=文本;",
          "alter timeseries root.ln_1.tb_6141.`出水NH4-N_DOUBLE` upsert alias=`出水NH4-N`;",
          "alter timeseries root.ln_1.tb_6141.功能码_DOUBLE upsert alias=功能码;",
          "alter timeseries root.ln_1.tb_6141.11_TEXT upsert alias=`11`;",
          "alter timeseries root.ln_1.tb_6141.`开关量-运行_BOOLEAN` upsert alias=`开关量-运行`;",
          "insert into root.ln_1.tb_6141(time,产水间歇运行时间设置_DOUBLE) aligned values(1679365910000,10.0);",
          "insert into root.ln_1.tb_6141(time,出水TP_DOUBLE) aligned values(1679365910000,15.0);",
          "insert into root.ln_1.tb_6141(time,功能码_DOUBLE) aligned values(1679477545000,2.0);",
          "insert into root.ln_1.tb_6141(time,文本_TEXT) aligned values(1675995566000,52);",
          "insert into root.ln_1.tb_6141(time,枚举_INT32) aligned values(1675995566000,2);",
          "insert into root.ln_1.tb_6141(time,水管流速_DOUBLE) aligned values(1679365910000,15.0);",
          "insert into root.ln_1.tb_6141(time,膜产水状态_BOOLEAN) aligned values(1677033625000,true);",
          "insert into root.ln_1.tb_6141(time,风机投入_BOOLEAN) aligned values(1675995566000,true);",
          "insert into root.ln_1.tb_6141(time,风机退出_BOOLEAN) aligned values(1675995566000,false);",
          "insert into root.ln_1.tb_6141(time,11_TEXT) aligned values(1679365910000,13);",
          "insert into root.ln_1.tb_6141(time,CO2_DOUBLE) aligned values(1679365910000,12.0);",
          "insert into root.ln_1.tb_6141(time,`出水NH4-N_DOUBLE`) aligned values(1679365910000,12.0);",
          "insert into root.ln_1.tb_6141(time,`开关量-运行_BOOLEAN`) aligned values(1675995566000,false);",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      // create aligned and non-aligned time series
      for (String sql : sqls) {
        statement.addBatch(sql);
      }
      statement.executeBatch();
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "1679477545000,root.ln_1.tb_6141.功能码_DOUBLE,2.0,DOUBLE",
                "1675995566000,root.ln_1.tb_6141.文本_TEXT,52,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last 功能码,文本 from root.ln_1.tb_6141;")) {
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    retSet =
        new HashSet<>(
            Arrays.asList(
                "1679477545000,root.ln_1.tb_6141.功能码_DOUBLE,2.0,DOUBLE",
                "1677033625000,root.ln_1.tb_6141.膜产水状态_BOOLEAN,true,BOOLEAN",
                "1675995566000,root.ln_1.tb_6141.文本_TEXT,52,TEXT"));

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      try (ResultSet resultSet =
          statement.executeQuery("select last 功能码,膜产水状态,文本 from root.ln_1.tb_6141;")) {
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
      }

    } catch (SQLException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
