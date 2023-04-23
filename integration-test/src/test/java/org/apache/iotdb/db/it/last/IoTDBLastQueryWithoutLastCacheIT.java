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
import java.sql.SQLException;
import java.sql.Statement;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.apache.iotdb.itbase.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.itbase.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBLastQueryWithoutLastCacheIT {
  private static final String[] SQLs =
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
        "insert into root.ln_1.tb_6141(time,`出水NH4-N_DOUBLE`) aligned values(1679365910000,12.0);",
        "insert into root.ln_1.tb_6141(time,`开关量-运行_BOOLEAN`) aligned values(1675995566000,false);"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableLastCache(false);
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
  public void testLastWithoutLastCache() {
    // cast to int
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};
    String[] retArray =
        new String[] {
          "1679365910000,root.ln_1.tb_6141.产水间歇运行时间设置_DOUBLE,10.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.出水TP_DOUBLE,15.0,DOUBLE,",
          "1679477545000,root.ln_1.tb_6141.功能码_DOUBLE,2.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.文本_TEXT,52,TEXT,",
          "1675995566000,root.ln_1.tb_6141.枚举_INT32,2,INT32,",
          "1679365910000,root.ln_1.tb_6141.水管流速_DOUBLE,15.0,DOUBLE,",
          "1677033625000,root.ln_1.tb_6141.膜产水状态_BOOLEAN,true,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.风机投入_BOOLEAN,true,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.风机退出_BOOLEAN,false,BOOLEAN,",
          "1679365910000,root.ln_1.tb_6141.11_TEXT,13,TEXT,",
          "1679365910000,root.ln_1.tb_6141.CO2_DOUBLE,12.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.`出水NH4-N_DOUBLE`,12.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.`开关量-运行_BOOLEAN`,false,BOOLEAN,"
        };
    resultSetEqualTest("select last * from root.ln_1.tb_6141;", expectedHeader, retArray);
  }
}
