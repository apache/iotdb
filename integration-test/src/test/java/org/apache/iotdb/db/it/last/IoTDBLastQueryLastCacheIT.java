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
public class IoTDBLastQueryLastCacheIT {
  protected static final String[] SQLs =
      new String[] {
        "create aligned timeseries root.ln_1.tb_6141(machineExit_BOOLEAN BOOLEAN encoding=RLE,`waterNH4-N_DOUBLE` DOUBLE encoding=GORILLA,status_BOOLEAN BOOLEAN encoding=RLE,11_TEXT TEXT encoding=PLAIN,waterInterval_DOUBLE DOUBLE encoding=GORILLA,content_TEXT TEXT encoding=PLAIN, machineOn_BOOLEAN BOOLEAN encoding=RLE,enum_INT32 INT32 encoding=RLE,waterTP_DOUBLE DOUBLE encoding=GORILLA,fluidVelocity_DOUBLE DOUBLE encoding=GORILLA,CO2_DOUBLE DOUBLE encoding=GORILLA,`switch_BOOLEAN` BOOLEAN encoding=RLE,code_DOUBLE DOUBLE encoding=GORILLA);",
        "alter timeseries root.ln_1.tb_6141.machineExit_BOOLEAN upsert alias=machineExit;",
        "alter timeseries root.ln_1.tb_6141.fluidVelocity_DOUBLE upsert alias=fluidVelocity;",
        "alter timeseries root.ln_1.tb_6141.CO2_DOUBLE upsert alias=CO2;",
        "alter timeseries root.ln_1.tb_6141.machineOn_BOOLEAN upsert alias=machineOn;",
        "alter timeseries root.ln_1.tb_6141.waterInterval_DOUBLE upsert alias=waterInterval;",
        "alter timeseries root.ln_1.tb_6141.status_BOOLEAN upsert alias=status;",
        "alter timeseries root.ln_1.tb_6141.enum_INT32 upsert alias=enum;",
        "alter timeseries root.ln_1.tb_6141.waterTP_DOUBLE upsert alias=waterTP;",
        "alter timeseries root.ln_1.tb_6141.content_TEXT upsert alias=content;",
        "alter timeseries root.ln_1.tb_6141.`waterNH4-N_DOUBLE` upsert alias=`waterNH4-N`;",
        "alter timeseries root.ln_1.tb_6141.code_DOUBLE upsert alias=code;",
        "alter timeseries root.ln_1.tb_6141.11_TEXT upsert alias=`11`;",
        "alter timeseries root.ln_1.tb_6141.`switch_BOOLEAN` upsert alias=`switch`;",
        "insert into root.ln_1.tb_6141(time,waterInterval_DOUBLE) aligned values(1679365910000,10.0);",
        "insert into root.ln_1.tb_6141(time,waterTP_DOUBLE) aligned values(1679365910000,15.0);",
        "insert into root.ln_1.tb_6141(time,code_DOUBLE) aligned values(1679477545000,2.0);",
        "insert into root.ln_1.tb_6141(time,content_TEXT) aligned values(1675995566000,52);",
        "insert into root.ln_1.tb_6141(time,enum_INT32) aligned values(1675995566000,2);",
        "insert into root.ln_1.tb_6141(time,fluidVelocity_DOUBLE) aligned values(1679365910000,15.0);",
        "insert into root.ln_1.tb_6141(time,status_BOOLEAN) aligned values(1677033625000,true);",
        "insert into root.ln_1.tb_6141(time,machineOn_BOOLEAN) aligned values(1675995566000,true);",
        "insert into root.ln_1.tb_6141(time,machineExit_BOOLEAN) aligned values(1675995566000,false);",
        "insert into root.ln_1.tb_6141(time,11_TEXT) aligned values(1679365910000,13);",
        "insert into root.ln_1.tb_6141(time,CO2_DOUBLE) aligned values(1679365910000,12.0);",
        "insert into root.ln_1.tb_6141(time,`waterNH4-N_DOUBLE`) aligned values(1679365910000,12.0);",
        "insert into root.ln_1.tb_6141(time,`waterNH4-N_DOUBLE`) aligned values(1679365910000,12.0);",
        "insert into root.ln_1.tb_6141(time,`switch_BOOLEAN`) aligned values(1675995566000,false);",
        "create aligned timeseries root.sg(风机退出_BOOLEAN BOOLEAN encoding=RLE,`NH4-N_DOUBLE` DOUBLE encoding=GORILLA,膜产水状态_BOOLEAN BOOLEAN encoding=RLE,11_TEXT TEXT encoding=PLAIN,产水间歇运行时间设置_DOUBLE DOUBLE encoding=GORILLA,文本_TEXT TEXT encoding=PLAIN, 风机投入_BOOLEAN BOOLEAN encoding=RLE,枚举_INT32 INT32 encoding=RLE,出水TP_DOUBLE DOUBLE encoding=GORILLA,水管流速_DOUBLE DOUBLE encoding=GORILLA,CO2_DOUBLE DOUBLE encoding=GORILLA,`开关量-运行_BOOLEAN` BOOLEAN encoding=RLE,code_DOUBLE DOUBLE encoding=GORILLA);",
        "insert into root.sg(time,code_DOUBLE) aligned values(1679477545000,2.0);",
        "insert into root.sg(time,`NH4-N_DOUBLE`) aligned values(1679365910000,12.0);"
      };

  @BeforeClass
  public static void setUp() throws Exception {
    // without lastCache
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
  public void testLastQuery() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};
    String[] retArray =
        new String[] {
          "1679365910000,root.ln_1.tb_6141.11_TEXT,13,TEXT,",
          "1679365910000,root.ln_1.tb_6141.CO2_DOUBLE,12.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.`waterNH4-N_DOUBLE`,12.0,DOUBLE,",
          "1679477545000,root.ln_1.tb_6141.code_DOUBLE,2.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.content_TEXT,52,TEXT,",
          "1675995566000,root.ln_1.tb_6141.enum_INT32,2,INT32,",
          "1679365910000,root.ln_1.tb_6141.fluidVelocity_DOUBLE,15.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.machineExit_BOOLEAN,false,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.machineOn_BOOLEAN,true,BOOLEAN,",
          "1677033625000,root.ln_1.tb_6141.status_BOOLEAN,true,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.switch_BOOLEAN,false,BOOLEAN,",
          "1679365910000,root.ln_1.tb_6141.waterInterval_DOUBLE,10.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.waterTP_DOUBLE,15.0,DOUBLE,",
        };
    resultSetEqualTest(
        "select last * from root.ln_1.tb_6141 order by timeseries;", expectedHeader, retArray);
  }

  @Test
  public void testLastQueryOrderByTimeDesc() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};
    String[] retArray =
        new String[] {
          "1679365910000,root.ln_1.tb_6141.waterTP_DOUBLE,15.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.waterInterval_DOUBLE,10.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.switch_BOOLEAN,false,BOOLEAN,",
          "1677033625000,root.ln_1.tb_6141.status_BOOLEAN,true,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.machineOn_BOOLEAN,true,BOOLEAN,",
          "1675995566000,root.ln_1.tb_6141.machineExit_BOOLEAN,false,BOOLEAN,",
          "1679365910000,root.ln_1.tb_6141.fluidVelocity_DOUBLE,15.0,DOUBLE,",
          "1675995566000,root.ln_1.tb_6141.enum_INT32,2,INT32,",
          "1675995566000,root.ln_1.tb_6141.content_TEXT,52,TEXT,",
          "1679477545000,root.ln_1.tb_6141.code_DOUBLE,2.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.`waterNH4-N_DOUBLE`,12.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.CO2_DOUBLE,12.0,DOUBLE,",
          "1679365910000,root.ln_1.tb_6141.11_TEXT,13,TEXT,",
        };
    resultSetEqualTest(
        "select last * from root.ln_1.tb_6141 order by timeseries desc;", expectedHeader, retArray);
  }

  @Test
  public void testLastQuery1() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};
    String[] retArray =
        new String[] {
          "1679365910000,root.sg.`NH4-N_DOUBLE`,12.0,DOUBLE,",
          "1679477545000,root.sg.code_DOUBLE,2.0,DOUBLE,",
        };
    resultSetEqualTest("select last * from root.sg;", expectedHeader, retArray);
  }

  @Test
  public void cacheHitTest() {
    testLastQuery();
    testLastQueryOrderByTimeDesc();
    testLastQuery1();
  }

  @Test
  public void testLastQuerySortWithLimit() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};
    String[] retArray =
        new String[] {
          "1679477545000,root.ln_1.tb_6141.code_DOUBLE,2.0,DOUBLE,",
        };
    resultSetEqualTest(
        "select last * from root.ln_1.tb_6141 order by time desc, timeseries desc limit 1;",
        expectedHeader,
        retArray);
  }
}
