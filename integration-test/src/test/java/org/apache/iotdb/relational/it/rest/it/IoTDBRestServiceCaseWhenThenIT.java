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
package org.apache.iotdb.relational.it.rest.it;

import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRestServiceCaseWhenThenIT {

  private int port = 18080;
  private CloseableHttpClient httpClient = null;

  @Before
  public void setUp() throws Exception {
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableRestService(true);
    baseEnv.initClusterEnvironment();
    DataNodeWrapper portConflictDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    port = portConflictDataNodeWrapper.getRestServicePort();
    httpClient = HttpClientBuilder.create().build();
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (httpClient != null) {
        httpClient.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private static final String DATABASE = "test";

  private static final String[] expectedHeader = {"_col0"};

  private static final String[] SQLs =
      new String[] {
        // normal cases
        "CREATE DATABASE " + DATABASE,
        "CREATE table table1 (device_id STRING TAG, s1 INT32 FIELD, s5 BOOLEAN FIELD, s6 TEXT FIELD)",
        "CREATE table table2 (device_id STRING TAG, s3 FLOAT FIELD, s4 DOUBLE FIELD)",
        "CREATE table table3 (device_id STRING TAG, s2 INT64 FIELD)",
        "INSERT INTO table1(time, device_id, s1) values(100, 'd1', 0)",
        "INSERT INTO table1(time, device_id, s1) values(200, 'd1', 11)",
        "INSERT INTO table1(time, device_id, s1) values(300, 'd1', 22)",
        "INSERT INTO table1(time, device_id, s1) values(400, 'd1', 33)",
        "INSERT INTO table2(time, device_id, s3) values(100, 'd1', 0)",
        "INSERT INTO table2(time, device_id, s3) values(200, 'd1', 11)",
        "INSERT INTO table2(time, device_id, s3) values(300, 'd1', 22)",
        "INSERT INTO table2(time, device_id, s3) values(400, 'd1', 33)",
        "INSERT INTO table2(time, device_id, s4) values(100, 'd1', 44)",
        "INSERT INTO table2(time, device_id, s4) values(200, 'd1', 55)",
        "INSERT INTO table2(time, device_id, s4) values(300, 'd1', 66)",
        "INSERT INTO table2(time, device_id, s4) values(400, 'd1', 77)",
      };

  @Test
  public void test() {
    ping();
    prepareTableData();
    testKind1Basic();
    testKind2Basic();
    testShortCircuitEvaluation();
    testKind1InputTypeRestrict();
    testKind2InputTypeRestrict();
    testKind1OutputTypeRestrict();
    testKind2OutputTypeRestrict();
    testKind1UsedInOtherOperation();
    testKind2UsedInOtherOperation();
    testKind1UseOtherOperation();
    testKind2UseOtherOperation();
    testKind1UseInWhereClause();
    testKind1CaseInCase();
    testKind2CaseInCase();
    testKind1Logic();
    testKind2UseOtherOperation();
    testKind1UseInWhereClause();
  }

  public void ping() {
    HttpGet httpGet = new HttpGet("http://127.0.0.1:" + port + "/ping");
    CloseableHttpResponse response = null;
    try {
      for (int i = 0; i < 30; i++) {
        try {
          response = httpClient.execute(httpGet);
          break;
        } catch (Exception e) {
          if (i == 29) {
            throw e;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
          }
        }
      }

      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(200, response.getStatusLine().getStatusCode());
      assertEquals(200, Integer.parseInt(result.get("code").toString()));
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {

      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  public void testKind1Basic() {
    String[] retArray = new String[] {"99,", "9999,", "9999,", "999,"};
    tableResultSetEqualTest(
        "select case when s1=0 then 99 when s1>22 then 999 else 9999 end from table1",
        expectedHeader,
        retArray,
        DATABASE);
  }

  public void testKind2Basic() {
    String sql = "select case s1 when 0 then 99 when 22 then 999 else 9999 end from table1";
    String[] retArray = new String[] {"99,", "9999,", "999,", "9999,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // without ELSE clause
    sql = "select case s1 when 0 then 99 when 22 then 999 end from table1";
    retArray = new String[] {"99,", "null,", "999,", "null,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testShortCircuitEvaluation() {
    String[] retArray = new String[] {"0,", "11,", "22,", "33,"};
    tableResultSetEqualTest(
        "select case when 1=0 then s1/0 when 1!=0 then s1 end from table1",
        expectedHeader,
        retArray,
        DATABASE);
  }

  public void testKind1InputTypeRestrict() {
    // WHEN clause must return BOOLEAN
    String sql = "select case when s1+1 then 20 else 22 end from table1";
    String msg = "701: CASE WHEN clause must evaluate to a BOOLEAN (actual: INT32)";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  public void testKind2InputTypeRestrict() {
    // the expression in CASE clause must be able to be equated with the expression in WHEN clause
    String sql = "select case s1 when '1' then 20 else 22 end from table1";
    String msg = "701: CASE operand type does not match WHEN clause operand type: INT32 vs STRING";
    tableAssertTestFail(sql, msg, DATABASE);
  }

  public void testKind1OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] retArray = new String[] {"true,", "false,", "true,", "true,"};
    // success
    tableResultSetEqualTest(
        "select case when s1<=0 then true when s1=11 then false else true end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case when s1<=0 then true else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between BOOLEAN and INT32, all types (without duplicates): [BOOLEAN, INT32]",
        DATABASE);

    // TEXT and other types cannot exist at the same time
    retArray = new String[] {"good,", "bad,", "okok,", "okok,"};
    // success
    tableResultSetEqualTest(
        "select case when s1<=0 then 'good' when s1=11 then 'bad' else 'okok' end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case when s1<=0 then 'good' else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between STRING and INT32, all types (without duplicates): [STRING, INT32]",
        DATABASE);
  }

  public void testKind2OutputTypeRestrict() {
    // BOOLEAN and other types cannot exist at the same time
    String[] retArray =
        new String[] {
          "true,", "false,", "true,", "true,",
        };
    // success
    tableResultSetEqualTest(
        "select case s1 when 0 then true when 11 then false else true end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case s1 when 0 then true else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between BOOLEAN and INT32, all types (without duplicates): [BOOLEAN, INT32]",
        DATABASE);

    // TEXT and other types cannot exist at the same time
    retArray = new String[] {"good,", "bad,", "okok,", "okok,"};
    // success
    tableResultSetEqualTest(
        "select case s1 when 0 then 'good' when 11 then 'bad' else 'okok' end from table1",
        expectedHeader,
        retArray,
        DATABASE);
    // fail
    tableAssertTestFail(
        "select case s1 when 0 then 'good' else 22 end from table1",
        "701: All CASE results must be the same type or coercible to a common type. Cannot find common type between STRING and INT32, all types (without duplicates): [STRING, INT32]",
        DATABASE);
  }

  public void testKind1UsedInOtherOperation() {
    String sql;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case when s1=0 then 99 when s1=22.0 then 999 else 9999 end from table1";
    retArray = new String[] {"198,", "19998,", "1998,", "19998,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // add
    sql =
        "select "
            + "case when s1=0 then 99 when s1=22.0 then 999 else 9999 end "
            + "+"
            + "case when s1=11 then 99 else 9999 end "
            + "from table1";
    retArray =
        new String[] {
          "10098,", "10098,", "10998,", "19998,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // function
    sql = "select diff(case when s1=0 then 99 when s1>22 then 999 else 9999 end) from table1";
    retArray = new String[] {"null,", "9900.0,", "0.0,", "-9000.0,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind2UsedInOtherOperation() {
    String sql;
    String[] retArray;

    // use in scalar operation

    // multiply
    sql = "select 2 * case s1 when 0 then 99 when 22 then 999 else 9999 end from table1";
    retArray = new String[] {"198,", "19998,", "1998,", "19998,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
    sql = "select diff(case s1 when 0 then 99 when 22 then 999 else 9999 end) from table1";
    retArray =
        new String[] {
          "null,", "9900.0,", "-9000.0,", "9000.0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind1UseOtherOperation() {
    // WHEN-clause use scalar function
    String sql = "select case when sin(s1)>=0 then '>0' else '<0' end from table1";
    String[] retArray =
        new String[] {
          ">0,", "<0,", "<0,", ">0,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // THEN-clause and ELSE-clause use scalar function

    // TODO: align by is not supported.

    //    sql =
    //        "select case when s1<=11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from
    // table1 align by device";
    //
    //    retArray =
    //        new String[] {
    //          "0,table1,null,",
    //          "1000000,table1,11.0,",
    //          "20000000,table1,21.0,",
    //          "210000000,table1,32.0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind2UseOtherOperation() {
    // CASE-clause use scalar function
    String sql =
        "select case round(sin(s1)) when 0 then '=0' when -1 then '<0' else '>0' end from table1";

    tableAssertTestFail(
        sql,
        "701: CASE operand type does not match WHEN clause operand type: DOUBLE vs INT32",
        DATABASE);
    //    String[] retArray =
    //        new String[] {
    //          "=0,", "<0,", ">0,", ">0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // WHEN-clause use scalar function
    sql = "select case 0 when sin(s1) then '=0' else '!=0' end from table1";
    tableAssertTestFail(
        sql,
        "701: CASE operand type does not match WHEN clause operand type: INT32 vs DOUBLE",
        DATABASE);
    //    retArray =
    //        new String[] {
    //          "=0,", "!=0,", "!=0,", "!=0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // THEN-clause and ELSE-clause use scalar function
    //    sql =
    //        "select case s1 when 11 then CAST(diff(s1) as TEXT) else CAST(s1-1 as TEXT) end from
    // table1 align by device";
    //
    //    retArray =
    //        new String[] {
    //          "table1,-1.0,", "table1,11.0,", "table1,21.0,", "table1,32.0,",
    //        };
    //    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);

    // UDF is not allowed
    //    sql = "select case s1 when 0 then change_points(s1) end from table1";
    //    String msg = "301: CASE expression cannot be used with non-mappable UDF";
    //    tableAssertTestFail(sql, msg, DATABASE);
  }

  public void testKind1UseInWhereClause() {
    String sql =
        "select s4 from table2 where case when s3=0 then s4>44 when s3=22 then s4>0 when time>300 then true end";
    String[] retArray = new String[] {"66.0,", "77.0,"};
    tableResultSetEqualTest(sql, new String[] {"s4"}, retArray, DATABASE);

    sql =
        "select case when s3=0 then s4>44 when s3=22 then s4>0 when time>300 then true end from table2";
    retArray =
        new String[] {
          "false,", "null,", "true,", "true,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind1CaseInCase() {
    String sql =
        "select case when s1=0 OR s1=22 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) else 'xxx' end from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind2CaseInCase() {
    String sql =
        "select case s1 when 0 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) when 22 then cast(case when s1=0 then 99 when s1>22 then 999 end as STRING) else 'xxx' end from table1";
    String[] retArray =
        new String[] {
          "99,", "xxx,", "null,", "xxx,",
        };
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void testKind1Logic() {
    String sql =
        "select case when s3 >= 0 and s3 < 20 and s4 >= 50 and s4 < 60 then 'just so so~~~' when s3 >= 20 and s3 < 40 and s4 >= 70 and s4 < 80 then 'very well~~~' end from table2";
    String[] retArray = new String[] {"null,", "just so so~~~,", "null,", "very well~~~,"};
    tableResultSetEqualTest(sql, expectedHeader, retArray, DATABASE);
  }

  public void tableAssertTestFail(String sql, String errMsg, String database) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("database", database);
    jsonObject.addProperty("sql", sql);
    JsonObject result = query(jsonObject.toString());
    assertEquals(errMsg, result.get("code") + ": " + result.get("message").getAsString());
  }

  public void tableResultSetEqualTest(
      String sql, String[] expectedHeader, String[] expectedRetArray, String database) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("database", database);
    jsonObject.addProperty("sql", sql);
    JsonObject result = query(jsonObject.toString());
    JsonArray columnNames = result.get("column_names").getAsJsonArray();
    JsonArray valuesList = result.get("values").getAsJsonArray();
    for (int i = 0; i < columnNames.size(); i++) {
      assertEquals(expectedHeader[i], columnNames.get(i).getAsString());
    }
    assertEquals(expectedHeader.length, columnNames.size());
    int cnt = 0;
    for (int i = 0; i < valuesList.size(); i++) {
      StringBuilder builder = new StringBuilder();
      JsonArray values = valuesList.get(i).getAsJsonArray();
      for (int c = 0; c < values.size(); c++) {
        if (!values.get(c).isJsonNull()) {
          builder.append(values.get(c).getAsString()).append(",");
        } else {
          builder.append(values.get(c).toString()).append(",");
        }
      }
      assertEquals(expectedRetArray[i], builder.toString());
      cnt++;
    }
    assertEquals(expectedRetArray.length, cnt);
  }

  public void prepareTableData() {
    for (int i = 0; i < SQLs.length; i++) {
      JsonObject jsonObject = new JsonObject();
      if (i > 0) {
        jsonObject.addProperty("database", DATABASE);
      } else {
        jsonObject.addProperty("database", "");
      }
      jsonObject.addProperty("sql", SQLs[i]);
      nonQuery(jsonObject.toString());
    }
  }

  public JsonObject query(String json) {
    return RestUtils.query(httpClient, port, json);
  }

  public JsonObject nonQuery(String json) {
    return RestUtils.nonQuery(httpClient, port, json);
  }
}
