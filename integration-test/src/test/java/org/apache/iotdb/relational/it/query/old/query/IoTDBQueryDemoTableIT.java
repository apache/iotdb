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
package org.apache.iotdb.relational.it.query.old.query;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.it.utils.TestUtils.defaultFormatDataTime;
import static org.apache.iotdb.db.it.utils.TestUtils.prepareTableData;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBQueryDemoTableIT {
  private static final String DATABASE_NAME = "test";
  private static String[] sqls =
      new String[] {
        "CREATE DATABASE " + DATABASE_NAME,
        "USE " + DATABASE_NAME,
        "CREATE TABLE wf(device STRING ID, status BOOLEAN MEASUREMENT, temperature FLOAT MEASUREMENT, hardware TEXT MEASUREMENT)",
        "insert into wf(time,device,status) values(1509465600000,'wt01',true)",
        "insert into wf(time,device,status) values(1509465660000,'wt01',true)",
        "insert into wf(time,device,status) values(1509465720000,'wt01',false)",
        "insert into wf(time,device,status) values(1509465780000,'wt01',false)",
        "insert into wf(time,device,status) values(1509465840000,'wt01',false)",
        "insert into wf(time,device,status) values(1509465900000,'wt01',false)",
        "insert into wf(time,device,status) values(1509465960000,'wt01',false)",
        "insert into wf(time,device,status) values(1509466020000,'wt01',false)",
        "insert into wf(time,device,status) values(1509466080000,'wt01',false)",
        "insert into wf(time,device,status) values(1509466140000,'wt01',false)",
        "insert into wf(time,device,temperature) values(1509465600000,'wt01',25.957603)",
        "insert into wf(time,device,temperature) values(1509465660000,'wt01',24.359503)",
        "insert into wf(time,device,temperature) values(1509465720000,'wt01',20.092794)",
        "insert into wf(time,device,temperature) values(1509465780000,'wt01',20.182663)",
        "insert into wf(time,device,temperature) values(1509465840000,'wt01',21.125198)",
        "insert into wf(time,device,temperature) values(1509465900000,'wt01',22.720892)",
        "insert into wf(time,device,temperature) values(1509465960000,'wt01',20.71)",
        "insert into wf(time,device,temperature) values(1509466020000,'wt01',21.451046)",
        "insert into wf(time,device,temperature) values(1509466080000,'wt01',22.57987)",
        "insert into wf(time,device,temperature) values(1509466140000,'wt01',20.98177)",
        "insert into wf(time,device,hardware) values(1509465600000,'wt02','v2')",
        "insert into wf(time,device,hardware) values(1509465660000,'wt02','v2')",
        "insert into wf(time,device,hardware) values(1509465720000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509465780000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509465840000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509465900000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509465960000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509466020000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509466080000,'wt02','v1')",
        "insert into wf(time,device,hardware) values(1509466140000,'wt02','v1')",
        "insert into wf(time,device,status) values(1509465600000,'wt02',true)",
        "insert into wf(time,device,status) values(1509465660000,'wt02',true)",
        "insert into wf(time,device,status) values(1509465720000,'wt02',false)",
        "insert into wf(time,device,status) values(1509465780000,'wt02',false)",
        "insert into wf(time,device,status) values(1509465840000,'wt02',false)",
        "insert into wf(time,device,status) values(1509465900000,'wt02',false)",
        "insert into wf(time,device,status) values(1509465960000,'wt02',false)",
        "insert into wf(time,device,status) values(1509466020000,'wt02',false)",
        "insert into wf(time,device,status) values(1509466080000,'wt02',false)",
        "insert into wf(time,device,status) values(1509466140000,'wt02',false)",
        "insert into wf(time,device,status) values(1509465600000,'wt03',true)",
        "insert into wf(time,device,status) values(1509465660000,'wt03',true)",
        "insert into wf(time,device,status) values(1509465720000,'wt03',false)",
        "insert into wf(time,device,status) values(1509465780000,'wt03',false)",
        "insert into wf(time,device,status) values(1509465840000,'wt03',false)",
        "insert into wf(time,device,status) values(1509465900000,'wt03',false)",
        "insert into wf(time,device,status) values(1509465960000,'wt03',false)",
        "insert into wf(time,device,status) values(1509466020000,'wt03',false)",
        "insert into wf(time,device,status) values(1509466080000,'wt03',false)",
        "insert into wf(time,device,status) values(1509466140000,'wt03',false)",
        "insert into wf(time,device,temperature) values(1509465600000,'wt03',25.957603)",
        "insert into wf(time,device,temperature) values(1509465660000,'wt03',24.359503)",
        "insert into wf(time,device,temperature) values(1509465720000,'wt03',20.092794)",
        "insert into wf(time,device,temperature) values(1509465780000,'wt03',20.182663)",
        "insert into wf(time,device,temperature) values(1509465840000,'wt03',21.125198)",
        "insert into wf(time,device,temperature) values(1509465900000,'wt03',22.720892)",
        "insert into wf(time,device,temperature) values(1509465960000,'wt03',20.71)",
        "insert into wf(time,device,temperature) values(1509466020000,'wt03',21.451046)",
        "insert into wf(time,device,temperature) values(1509466080000,'wt03',22.57987)",
        "insert into wf(time,device,temperature) values(1509466140000,'wt03',20.98177)",
      };

  @BeforeClass
  public static void setUp() throws Exception {
    EnvFactory.getEnv().initClusterEnvironment();

    prepareTableData(sqls);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Test
  public void selectTest() {
    String[] retArray =
        new String[] {
          defaultFormatDataTime(1509466140000L) + ",wt03,false,20.98177,null,",
          defaultFormatDataTime(1509466140000L) + ",wt02,false,null,v1,",
          defaultFormatDataTime(1509466140000L) + ",wt01,false,20.98177,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery("select * from wf where time>1509466080000 order by device desc");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void LimitTest() {
    String[] retArray =
        new String[] {
          defaultFormatDataTime(1509466140000L) + ",wt02,false,null,v1,",
          defaultFormatDataTime(1509466140000L) + ",wt01,false,20.98177,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery(
              "select * from wf where time>1509466080000 order by device desc offset 1 limit 2");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(2, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void InTest() {
    String[] retArray =
        new String[] {
          defaultFormatDataTime(1509465780000L) + ",wt03,false,20.182663,null,",
          defaultFormatDataTime(1509465780000L) + ",wt02,false,null,v1,",
          defaultFormatDataTime(1509465780000L) + ",wt01,false,20.182663,null,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery(
              "select * from wf where time in(1509465780000) order by device desc");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(3, cnt);

      resultSet =
          statement.executeQuery(
              "select * from wf where time in(1509465780000) order by device desc");
      resultSetMetaData = resultSet.getMetaData();
      actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(3, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  private List<Integer> checkHeader(
      ResultSetMetaData resultSetMetaData, String expectedHeaderStrings, int[] expectedTypes)
      throws SQLException {
    String[] expectedHeaders = expectedHeaderStrings.split(",");
    Map<String, Integer> expectedHeaderToTypeIndexMap = new HashMap<>();
    for (int i = 0; i < expectedHeaders.length; ++i) {
      expectedHeaderToTypeIndexMap.put(expectedHeaders[i], i);
    }

    List<Integer> actualIndexToExpectedIndexList = new ArrayList<>();
    for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
      Integer typeIndex = expectedHeaderToTypeIndexMap.get(resultSetMetaData.getColumnName(i));
      Assert.assertNotNull(typeIndex);
      Assert.assertEquals(expectedTypes[typeIndex], resultSetMetaData.getColumnType(i));
      actualIndexToExpectedIndexList.add(typeIndex);
    }
    return actualIndexToExpectedIndexList;
  }

  @Test
  public void testRightTextQuery() {
    String[] retArray =
        new String[] {
          defaultFormatDataTime(1509465600000L) + ",wt02,true,null,v2,",
          defaultFormatDataTime(1509465660000L) + ",wt02,true,null,v2,"
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet = statement.executeQuery("select * from wf where hardware='v2'");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(2, cnt);

      resultSet = statement.executeQuery("select * from wf where hardware>'v1'");
      resultSetMetaData = resultSet.getMetaData();
      actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "time,device," + "status," + "temperature,hardware",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN, Types.FLOAT, Types.VARCHAR,
              });

      cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(2, cnt);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void RegexpTest() {
    String[] retArray =
        new String[] {
          "1509465600000,v2,true,", "1509465660000,v2,true,", "1509465720000,v1,false,",
        };

    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      statement.setFetchSize(4);
      Assert.assertEquals(4, statement.getFetchSize());
      // Matches a string consisting of one lowercase letter and one digit. such as: "v1","v2"
      ResultSet resultSet =
          statement.executeQuery(
              "select hardware,status from wf where hardware regexp '^[a-z][0-9]$' and time < 1509465780000");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "Time," + "wf.hardware,wf.status,",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(3, cnt);

      retArray =
          new String[] {
            "1509465600000,v2,true,",
            "1509465660000,v2,true,",
            "1509465720000,v1,false,",
            "1509465780000,v1,false,",
            "1509465840000,v1,false,",
            "1509465900000,v1,false,",
            "1509465960000,v1,false,",
            "1509466020000,v1,false,",
            "1509466080000,v1,false,",
            "1509466140000,v1,false,",
          };
      resultSet =
          statement.executeQuery("select hardware,status from wf where hardware regexp 'v*' ");

      resultSetMetaData = resultSet.getMetaData();
      actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "Time," + "wf.hardware,wf.status,",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
              });

      cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(10, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Ignore
  @Test
  public void RegexpNonExistTest() {

    // Match nonexistent string.'s.' is indicates that the starting with s and the last is any
    // single character
    String[] retArray =
        new String[] {
          "1509465600000,v2,true,",
          "1509465660000,v2,true,",
          "1509465720000,v1,false,",
          "1509465780000,v1,false,",
          "1509465840000,v1,false,",
          "1509465900000,v1,false,",
          "1509465960000,v1,false,",
          "1509466020000,v1,false,",
          "1509466080000,v1,false,",
          "1509466140000,v1,false,",
        };
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("USE " + DATABASE_NAME);
      ResultSet resultSet =
          statement.executeQuery("select hardware,status from wf where hardware regexp 's.' ");
      ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
      List<Integer> actualIndexToExpectedIndexList =
          checkHeader(
              resultSetMetaData,
              "Time," + "wf.hardware,wf.status,",
              new int[] {
                Types.TIMESTAMP, Types.VARCHAR, Types.BOOLEAN,
              });

      int cnt = 0;
      while (resultSet.next()) {
        String[] expectedStrings = retArray[cnt].split(",");
        StringBuilder expectedBuilder = new StringBuilder();
        StringBuilder actualBuilder = new StringBuilder();
        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
          actualBuilder.append(resultSet.getString(i)).append(",");
          expectedBuilder
              .append(expectedStrings[actualIndexToExpectedIndexList.get(i - 1)])
              .append(",");
        }
        Assert.assertEquals(expectedBuilder.toString(), actualBuilder.toString());
        cnt++;
      }
      Assert.assertEquals(0, cnt);

    } catch (Exception e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
