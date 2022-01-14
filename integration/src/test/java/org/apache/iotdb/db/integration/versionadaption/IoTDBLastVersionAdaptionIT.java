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
package org.apache.iotdb.db.integration.versionadaption;

import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.integration.env.EnvFactory;
import org.apache.iotdb.itbase.category.LocalStandaloneTest;
import org.apache.iotdb.jdbc.Constant;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.apache.iotdb.db.constant.TestConstant.DATA_TYPE_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESEIRES_STR;
import static org.apache.iotdb.db.constant.TestConstant.TIMESTAMP_STR;
import static org.apache.iotdb.db.constant.TestConstant.VALUE_STR;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings("ALL")
@Category({LocalStandaloneTest.class})
public class IoTDBLastVersionAdaptionIT {

  private static final String[] dataSet1 =
      new String[] {
        "CREATE TIMESERIES root.ln.wf01.wt01.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt01.id WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
            + "values(100, 25.1, false, 7)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
            + "values(200, 25.2, true, 8)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
            + "values(300, 15.7, false, 9)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
            + "values(400, 16.2, false, 6)",
        "INSERT INTO root.ln.wf01.wt01(timestamp,temperature,status, id) "
            + "values(500, 22.1, false, 5)",
        "flush",
      };

  private static final String[] dataSet2 =
      new String[] {
        "CREATE TIMESERIES root.ln.wf01.wt02.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt02.id WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
            + "values(100, 18.6, false, 7)",
        "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
            + "values(300, 23.1, true, 8)",
        "INSERT INTO root.ln.wf01.wt02(timestamp,temperature,status, id) "
            + "values(500, 15.7, false, 9)",
        "flush",
      };

  private static final String[] dataSet3 =
      new String[] {
        "CREATE TIMESERIES root.ln.wf01.wt03.status WITH DATATYPE=BOOLEAN, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt03.temperature WITH DATATYPE=DOUBLE, ENCODING=PLAIN",
        "CREATE TIMESERIES root.ln.wf01.wt03.id WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.ln.wf01.wt03(timestamp,temperature,status, id) "
            + "values(100, 18.6, false, 7)",
        "INSERT INTO root.ln.wf01.wt03(timestamp,temperature,status, id) "
            + "values(300, 23.1, true, 8)",
        "flush",
      };

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    prepareData();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void lastDescTimeTest() {
    Set<String> retSet =
        new HashSet<>(
            Arrays.asList(
                "500,root.ln.wf01.wt01.status,false,BOOLEAN",
                "500,root.ln.wf01.wt01.temperature,22.1,DOUBLE",
                "500,root.ln.wf01.wt01.id,5,INT32",
                "500,root.ln.wf01.wt02.status,false,BOOLEAN",
                "500,root.ln.wf01.wt02.temperature,15.7,DOUBLE",
                "500,root.ln.wf01.wt02.id,9,INT32",
                "300,root.ln.wf01.wt03.status,true,BOOLEAN",
                "300,root.ln.wf01.wt03.temperature,23.1,DOUBLE",
                "300,root.ln.wf01.wt03.id,8,INT32"));

    try (Connection connection = EnvFactory.getEnv().getConnection(Constant.Version.V_0_12);
        Statement statement = connection.createStatement()) {

      boolean hasResultSet = statement.execute("select last * from root order by time desc");
      assertTrue(hasResultSet);
      ResultSet resultSet = statement.getResultSet();
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
        Assert.assertTrue(retSet.contains(ans));
        cnt++;
      }
      Assert.assertEquals(retSet.size(), cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private void prepareData() {
    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {

      for (String sql : dataSet1) {
        statement.execute(sql);
      }
      for (String sql : dataSet2) {
        statement.execute(sql);
      }
      for (String sql : dataSet3) {
        statement.execute(sql);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
