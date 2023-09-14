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
public class IoTDBLastViewIT {

  @BeforeClass
  public static void setUp() throws Exception {
    // without lastCache
    EnvFactory.getEnv().getConfig().getCommonConfig().setEnableLastCache(false);
    EnvFactory.getEnv().initClusterEnvironment();
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      prepareData(SQL_LIST);
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
  public void lastViewTest() {
    String[] expectedHeader =
        new String[] {TIMESTAMP_STR, TIMESEIRES_STR, VALUE_STR, DATA_TYPE_STR};

    // order by time
    String[] retArray =
        new String[] {
          "1,root.sg.d3.s2,1.0,DOUBLE,",
          "1,root.sg.d4.vs4,2.0,DOUBLE,",
          "1,root.sg.d4.vs3,0.5,DOUBLE,",
          "2,root.sg.d4.vs2,102.0,DOUBLE,",
          "106048000000,root.sg.d1.s1,10000,INT32,",
          "106048000000,root.sg.d3.s1,2,INT32,",
          "106048000000,root.sg.d4.vs1,10000,INT32,",
          "106058000000,root.sg.d2.s1,10001,INT32,",
        };
    resultSetEqualTest("select last * from root.** order by time", expectedHeader, retArray);

    // order by timeseries
    retArray =
        new String[] {
          "1,root.sg.d4.vs4,2.0,DOUBLE,",
          "1,root.sg.d4.vs3,0.5,DOUBLE,",
          "2,root.sg.d4.vs2,102.0,DOUBLE,",
          "106048000000,root.sg.d4.vs1,10000,INT32,",
          "1,root.sg.d3.s2,1.0,DOUBLE,",
          "106048000000,root.sg.d3.s1,2,INT32,",
          "106058000000,root.sg.d2.s1,10001,INT32,",
          "106048000000,root.sg.d1.s1,10000,INT32,",
        };
    resultSetEqualTest(
        "select last * from root.** order by timeseries desc", expectedHeader, retArray);

    // time filter
    retArray =
        new String[] {
          "106048000000,root.sg.d1.s1,10000,INT32,",
          "106058000000,root.sg.d2.s1,10001,INT32,",
          "106048000000,root.sg.d3.s1,2,INT32,",
          "106048000000,root.sg.d4.vs1,10000,INT32,",
          "2,root.sg.d4.vs2,102.0,DOUBLE,",
        };
    resultSetEqualTest(
        "select last * from root.** where time > 1 order by timeseries", expectedHeader, retArray);

    retArray =
        new String[] {
          "106048000000,root.sg.d4.vs2,10000,INT32,", "1,root.sg.d4.vs3,0.5,DOUBLE,",
        };
    resultSetEqualTest(
        "select last vs2,vs3 from root.** order by timeseries", expectedHeader, retArray);
  }

  private static final String[] SQL_LIST =
      new String[] {
        "create timeseries root.sg.d1.s1 INT32;",
        "create timeseries root.sg.d2.s1 INT32;",
        "CREATE ALIGNED TIMESERIES root.sg.d3(s1 INT32, s2 DOUBLE);",
        "create view root.sg.d4.vs1 as root.sg.d1.s1;",
        "create view root.sg.d4.vs2 as select d1.s1 + d2.s1 from root.sg;",
        "create view root.sg.d4.vs3 as select d3.s2 / 2 from root.sg;",
        "create view root.sg.d4.vs4 as select d3.s1 + d3.s2 from root.sg;",
        "insert into root.sg.d1(time, s1) values (1, 1);",
        "insert into root.sg.d1(time, s1) values (2, 2);",
        "insert into root.sg.d1(time, s1) values (106048000000, 10000);",
        "insert into root.sg.d2(time, s1) values (2, 100);",
        "insert into root.sg.d2(time, s1) values (106058000000, 10001);",
        "insert into root.sg.d3(time, s1, s2) aligned values (1, 1, 1.0);",
        "insert into root.sg.d3(time, s1, s2) aligned values (106048000000, 2, null);",
      };
}
