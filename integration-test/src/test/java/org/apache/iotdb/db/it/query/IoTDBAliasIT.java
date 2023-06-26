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

package org.apache.iotdb.db.it.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import static org.apache.iotdb.db.it.utils.TestUtils.assertResultSetEqual;
import static org.apache.iotdb.db.it.utils.TestUtils.assertTestFail;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;
import static org.apache.iotdb.db.it.utils.TestUtils.resultSetEqualTest;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class})
public class IoTDBAliasIT {

  private static final String[] SQLs =
      new String[] {
        "CREATE DATABASE root.sg",
        "CREATE TIMESERIES root.sg.d1.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d1.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s1(speed) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s2(temperature) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg.d2.s3(power) WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg.d1(timestamp,speed,temperature) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg.d2(timestamp,speed,temperature,power) values(400, 73.4, 26.3, 83.0)",
        "CREATE DATABASE root.sg1",
        "CREATE TIMESERIES root.sg1.d1.s1 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "CREATE TIMESERIES root.sg1.d1.s2 WITH DATATYPE=INT32, ENCODING=PLAIN",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (0, -1, 1)",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (1, -2, 2)",
        "INSERT INTO root.sg1.d1(timestamp, s1, s2) VALUES (2, -3, 3)",
        "CREATE DATABASE root.sg2",
        "CREATE TIMESERIES root.sg2.d1.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d1.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s1 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s2 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg2.d2.s3 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(100, 10.1, 20.7)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(200, 15.2, 22.9)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(300, 30.3, 25.1)",
        "INSERT INTO root.sg2.d1(timestamp,s1,s2) values(400, 50.4, 28.3)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(100, 11.1, 20.2, 80.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(200, 20.2, 21.8, 81.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(300, 45.3, 23.4, 82.0)",
        "INSERT INTO root.sg2.d2(timestamp,s1,s2,s3) values(400, 73.4, 26.3, 83.0)"
      };

  private static final String LAST_QUERY_HEADER = "Time,timeseries,value,dataType,";

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();
    prepareData(SQLs);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  // ---------------------------------- Use timeseries alias ---------------------------------

  @Test
  public void rawDataQueryAliasTest() {
    String expectedHeader = "Time,root.sg.d1.speed,root.sg.d1.temperature,";
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    resultSetEqualTest("select speed, temperature from root.sg.d1", expectedHeader, retArray);
  }

  @Test
  public void rawDataQueryWithDuplicatedColumnsAliasTest() {
    String expectedHeader = "Time,root.sg.d1.speed,root.sg.d1.speed,root.sg.d1.s2,";
    String[] retArray =
        new String[] {
          "100,10.1,10.1,20.7,", "200,15.2,15.2,22.9,", "300,30.3,30.3,25.1,", "400,50.4,50.4,28.3,"
        };

    resultSetEqualTest("select speed, speed, s2 from root.sg.d1", expectedHeader, retArray);
  }

  @Test
  @Ignore // TODO: remove @Ignore after support alias in last query
  public void lastQueryAliasTest() {
    String[] retArray =
        new String[] {"400,root.sg.d1.speed,50.4", "400,root.sg.d1.temperature,28.3"};

    resultSetEqualTest(
        "select last speed, temperature from root.sg.d1", LAST_QUERY_HEADER, retArray);
  }

  @Test
  @Ignore // TODO: remove @Ignore after support alias in last query
  public void lastQueryWithDuplicatedColumnsAliasTest() {
    String[] retArray =
        new String[] {
          "400,root.sg.d1.speed,50.4", "400,root.sg.d1.s1,50.4", "400,root.sg.d1.s2,28.3"
        };

    resultSetEqualTest(
        "select last speed, s1, speed, s2 from root.sg.d1", LAST_QUERY_HEADER, retArray);
  }

  @Test
  public void aggregationQueryAliasTest() {
    String expectedHeader =
        "count(root.sg.d1.speed),count(root.sg.d2.speed),max_value(root.sg.d1.temperature),"
            + "max_value(root.sg.d2.temperature),";
    String[] retArray = new String[] {"4,4,28.3,26.3,"};

    resultSetEqualTest(
        "select count(speed), max_value(temperature) from root.sg.*", expectedHeader, retArray);
  }

  @Test
  public void alterAliasTest() {
    String[] retArray = {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    try (Connection connection = EnvFactory.getEnv().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER timeseries root.sg.d2.s3 UPSERT ALIAS='powerNew'");

      try (ResultSet resultSet = statement.executeQuery("select powerNew from root.sg.d2")) {
        assertResultSetEqual(resultSet, "Time,root.sg.d2.powerNew,", retArray);
      }
    } catch (SQLException e) {
      fail(e.getMessage());
      e.printStackTrace();
    }
  }

  // ---------------------------------------- Use AS -----------------------------------------

  @Test
  public void rawDataQueryAsTest1() {
    String expectedHeader = "Time,power,";
    String[] retArray = new String[] {"100,80.0,", "200,81.0,", "300,82.0,", "400,83.0,"};

    // root.sg.*.s3 matches root.sg.d2.s3 exactly
    resultSetEqualTest("select s3 as power from root.sg2.*", expectedHeader, retArray);
  }

  @Test
  public void rawDataQueryAsTest2() {
    String expectedHeader = "Time,speed,temperature,";
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    resultSetEqualTest(
        "select s1 as speed, s2 as temperature from root.sg2.d1", expectedHeader, retArray);
  }

  @Test
  public void rawDataQueryAsTest3() {
    String expectedHeader = "Time,speed,root.sg2.d1.s2,";
    String[] retArray =
        new String[] {"100,10.1,20.7,", "200,15.2,22.9,", "300,30.3,25.1,", "400,50.4,28.3,"};

    resultSetEqualTest("select s1 as speed, s2 from root.sg2.d1", expectedHeader, retArray);
  }

  @Test
  public void rawDataQueryAsFailTest() {
    assertTestFail(
        "select s1 as speed from root.sg2.*",
        "alias 'speed' can only be matched with one time series");
  }

  @Test
  public void aggregationQueryAsTest() {
    String expectedHeader = "s1_num,s2_max,";
    String[] retArray =
        new String[] {
          "4,28.3,",
        };

    resultSetEqualTest(
        "select count(s1) as s1_num, max_value(s2) as s2_max from root.sg2.d1",
        expectedHeader,
        retArray);
  }

  @Test
  public void aggregationQueryAsFailTest() {
    // root.sg2.*.s1 matches root.sg2.d1.s1 and root.sg2.d2.s1 both
    assertTestFail(
        "select count(s1) as s1_num from root.sg2.*",
        "alias 's1_num' can only be matched with one time series");
  }

  @Test
  public void groupByQueryAsTest() {
    String expectedHeader = "Time,s1_num,";
    String[] retArray =
        new String[] {
          "100,1,", "180,1,", "260,1,", "340,1,", "420,0,",
        };

    resultSetEqualTest(
        "select count(s1) as 's1_num' from root.sg2.d1 group by ([100,500), 80ms)",
        expectedHeader,
        retArray);
  }

  @Test
  public void alignByDeviceQueryAsTest1() {
    String expectedHeader = "Time,Device,speed,temperature,";
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,20.7,",
          "200,root.sg2.d1,15.2,22.9,",
          "300,root.sg2.d1,30.3,25.1,",
          "400,root.sg2.d1,50.4,28.3,"
        };

    resultSetEqualTest(
        "select s1 as speed, s2 as temperature from root.sg2.d1 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void alignByDeviceQueryAsTest2() {
    String expectedHeader = "Time,Device,speed,s2,";
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,20.7,",
          "200,root.sg2.d1,15.2,22.9,",
          "300,root.sg2.d1,30.3,25.1,",
          "400,root.sg2.d1,50.4,28.3,",
          "100,root.sg2.d2,11.1,20.2,",
          "200,root.sg2.d2,20.2,21.8,",
          "300,root.sg2.d2,45.3,23.4,",
          "400,root.sg2.d2,73.4,26.3,"
        };

    resultSetEqualTest(
        "select s1 as speed, s2 from root.sg2.* align by device", expectedHeader, retArray);
  }

  @Test
  @Ignore // TODO incompatible feature with 0.13
  public void alignByDeviceQueryAsTest3() {
    String expectedHeader = "Time,Device,speed,s1,";
    String[] retArray =
        new String[] {
          "100,root.sg2.d1,10.1,10.1,",
          "200,root.sg2.d1,15.2,15.2,",
          "300,root.sg2.d1,30.3,30.3,",
          "400,root.sg2.d1,50.4,50.4,"
        };

    resultSetEqualTest(
        "select s1 as speed, s1 from root.sg2.d1 align by device", expectedHeader, retArray);
  }

  @Test
  public void alignByDeviceQueryAsTest4() {
    String expectedHeader = "Device,s1_num,count(s2),s3_num,";
    String[] retArray = new String[] {"root.sg2.d2,4,4,4,"};

    resultSetEqualTest(
        "select count(s1) as s1_num, count(s2), count(s3) as s3_num from root.sg2.d2 align by device",
        expectedHeader,
        retArray);
  }

  @Test
  public void alignByDeviceQueryAsFailTest() {
    // root.sg.*.s1 matches root.sg.d1.s1 and root.sg.d2.s1 both
    assertTestFail(
        "select * as speed from root.sg2.d1 align by device",
        "can only be matched with one time series");
  }

  @Test
  @Ignore // TODO: remove @Ignore after support alias in last query
  public void lastQueryAsTest() {
    String[] retArray = new String[] {"400,speed,50.4,FLOAT,", "400,root.sg2.d1.s2,28.3,FLOAT,"};

    resultSetEqualTest("select last s1 as speed, s2 from root.sg2.d1", LAST_QUERY_HEADER, retArray);
  }

  @Test
  @Ignore // TODO: remove @Ignore after support alias in last query
  public void lastQueryAsTest2() {
    String[] retArray =
        new String[] {
          "400,speed,50.4,FLOAT,", "400,root.sg2.d1.s1,50.4,FLOAT,", "400,temperature,28.3,FLOAT,"
        };

    resultSetEqualTest(
        "select last s1 as speed, s1, s2 as temperature from root.sg2.d1",
        LAST_QUERY_HEADER,
        retArray);
  }

  @Test
  @Ignore // TODO: remove @Ignore after support alias in last query
  public void lastQueryAsFailTest() {
    // root.sg2.*.s1 matches root.sg2.d1.s1 and root.sg2.d2.s1 both
    assertTestFail(
        "select last s1 as speed from root.sg2.*",
        "alias 'speed' can only be matched with one time series");
  }

  @Test
  public void UDFQueryAsTest() {
    List<String> sqls =
        Arrays.asList(
            "select -s1, sin(cos(tan(s1))) as a, cos(s2), top_k(s1 + s1, 'k'='1') as b from root.sg1.d1 WHERE time >= 1509466140000",
            "select -s1, sin(cos(tan(s1))) as a, cos(s2), top_k(s1 + s1, 'k'='1') as b from root.sg1.d1",
            "select -s1, -s1, sin(cos(tan(s1))) as a, sin(cos(tan(s1))), cos(s2), top_k(s1 + s1, 'k'='1') as b, cos(s2) from root.sg1.d1",
            "select s1, s2, sin(s1+s2) as a from root.sg1.d1");
    List<String> expectHeaders =
        Arrays.asList(
            "Time,-root.sg1.d1.s1,a,cos(root.sg1.d1.s2),b,",
            "Time,-root.sg1.d1.s1,a,cos(root.sg1.d1.s2),b,",
            "Time,-root.sg1.d1.s1,-root.sg1.d1.s1,a,sin(cos(tan(root.sg1.d1.s1))),cos(root.sg1.d1.s2),b,cos(root.sg1.d1.s2),",
            "Time,root.sg1.d1.s1,root.sg1.d1.s2,a,");
    List<String[]> retArrays =
        Arrays.asList(
            new String[] {},
            new String[] {
              "0,1,0.013387802193205699,0.5403023058681398,-2.0,",
              "1,2,-0.5449592372801408,-0.4161468365471424,null,",
              "2,3,0.8359477452180156,-0.9899924966004454,null,"
            },
            new String[] {
              "0,1,1,0.013387802193205699,0.013387802193205699,0.5403023058681398,-2.0,0.5403023058681398,",
              "1,2,2,-0.5449592372801408,-0.5449592372801408,-0.4161468365471424,null,-0.4161468365471424,",
              "2,3,3,0.8359477452180156,0.8359477452180156,-0.9899924966004454,null,-0.9899924966004454,"
            },
            new String[] {
              "0,-1,1,0.0,", "1,-2,2,0.0,", "2,-3,3,0.0,",
            });

    for (int i = 0; i < sqls.size(); i++) {
      resultSetEqualTest(sqls.get(i), expectHeaders.get(i), retArrays.get(i));
    }
  }

  // ------------------------------------ Function name --------------------------------------

  @Test
  public void aggregationFuncNameTest() {
    String expectedHeader =
        "count(root.sg.d1.temperature),count(root.sg.d2.temperature),"
            + "COUNT(root.sg.d1.s2),COUNT(root.sg.d2.s2),"
            + "CoUnT(root.sg.d1.temperature),CoUnT(root.sg.d2.temperature),";
    String[] retArray = new String[] {"4,4,4,4,4,4,"};

    resultSetEqualTest(
        "select count(temperature),COUNT(s2),CoUnT(temperature) from root.sg.*",
        expectedHeader,
        retArray);
  }

  @Test
  public void groupByLevelFuncNameTest() {
    String expectedHeader = "count(root.sg.*.s2),COUNT(root.sg.*.temperature),CoUnT(root.sg.*.s2),";
    String[] retArray = new String[] {"8,8,8,"};

    resultSetEqualTest(
        "select count(s2),COUNT(temperature),CoUnT(s2) from root.sg.* group by level = 1",
        expectedHeader,
        retArray);
  }
}
