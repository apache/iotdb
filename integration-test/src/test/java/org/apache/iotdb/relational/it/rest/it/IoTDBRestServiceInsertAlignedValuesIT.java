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
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;
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
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBRestServiceInsertAlignedValuesIT {

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

  private static final String[] sqls = new String[] {"CREATE DATABASE t1"};

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

  @Test
  public void test() {
    ping();
    prepareTableData();
    testInsertAlignedValues();
    testUpdatingAlignedValues();
    testInsertAlignedValuesWithSameTimestamp();
    testInsertWithWrongMeasurementNum1();
    testInsertWithWrongMeasurementNum2();
    testInsertWithDuplicatedMeasurements();
    testInsertMultiRows();
    testInsertLargeNumber();
    testExtendTextColumn();
  }

  public String sqlHandler(String database, String sql) {
    JsonObject json = new JsonObject();
    json.addProperty("database", database);
    json.addProperty("sql", sql);
    return json.toString();
  }

  public void testInsertAlignedValues() {
    List<String> sqls =
        Arrays.asList(
            "create table wf01 (id1 string id, status boolean measurement, temperature float measurement)",
            "insert into wf01(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)",
            "insert into wf01(id1, time, status, temperature) values ('wt01', 5000, true, 20.1)",
            "insert into wf01(id1, time, status, temperature) values ('wt01', 6000, true, 22)");
    for (String sql : sqls) {
      nonQuery(sqlHandler("t1", sql));
    }
    JsonObject jsonObject = query(sqlHandler("t1", "select time, status from wf01"));
    JsonArray valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      assertTrue(jsonArray.get(1).getAsBoolean());
    }

    jsonObject = query(sqlHandler("t1", "select time, status, temperature from wf01"));
    valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      if (i == 0) {
        assertEquals(4000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).getAsBoolean());
        assertEquals(17.1, jsonArray.get(2).getAsDouble(), 0.1);
      } else if (i == 1) {
        assertEquals(5000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).getAsBoolean());
        assertEquals(20.1, jsonArray.get(2).getAsDouble(), 0.1);
      } else if (i == 2) {
        assertEquals(6000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).getAsBoolean());
        assertEquals(22.0, jsonArray.get(2).getAsDouble(), 0.1);
      }
    }
  }

  public void testUpdatingAlignedValues() {
    List<String> sqls =
        Arrays.asList(
            "create table wf03 (id1 string id, status boolean measurement, temperature float measurement)",
            "insert into wf03(id1, time, status, temperature) values ('wt01', 4000, true, 17.1)",
            "insert into wf03(id1, time, status) values ('wt01', 5000, true)",
            "insert into wf03(id1, time, temperature)values ('wt01', 5000, 20.1)",
            "insert into wf03(id1, time, temperature)values ('wt01', 6000, 22)");
    for (String sql : sqls) {
      nonQuery(sqlHandler("t1", sql));
    }

    JsonObject jsonObject = query(sqlHandler("t1", "select time, status from wf03"));
    JsonArray valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      if (i >= 2) {
        assertTrue(jsonArray.get(1).isJsonNull());
      } else {
        assertTrue(jsonArray.get(1).getAsBoolean());
      }
    }
    jsonObject = query(sqlHandler("t1", "select time, status, temperature from wf03"));
    valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      if (i == 0) {
        assertEquals(4000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).getAsBoolean());
        assertEquals(17.1, jsonArray.get(2).getAsDouble(), 0.1);
      } else if (i == 1) {
        assertEquals(5000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).getAsBoolean());
        assertEquals(20.1, jsonArray.get(2).getAsDouble(), 0.1);
      } else if (i == 2) {
        assertEquals(6000, jsonArray.get(0).getAsLong());
        assertTrue(jsonArray.get(1).isJsonNull());
        assertEquals(22.0f, jsonArray.get(2).getAsFloat(), 0.1);
      }
    }
  }

  public void testInsertAlignedValuesWithSameTimestamp() {
    List<String> sqls =
        Arrays.asList(
            "create table sg3 (id1 string id, s2 double measurement, s1 double measurement)",
            "insert into sg3(id1,time,s2) values('d1',1,2)",
            "insert into sg3(id1,time,s1) values('d1',1,2)");
    for (String sql : sqls) {
      nonQuery(sqlHandler("t1", sql));
    }

    JsonObject jsonObject = query(sqlHandler("t1", "select time, s1, s2 from sg3"));
    JsonArray valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      for (int c = 0; c < jsonArray.size(); c++) {
        assertEquals(1, jsonArray.get(0).getAsLong());
        assertEquals(2.0d, jsonArray.get(1).getAsDouble(), 0.1);
        assertEquals(2.0d, jsonArray.get(2).getAsDouble(), 0.1);
      }
    }
  }

  public void testInsertWithWrongMeasurementNum1() {
    nonQuery(
        sqlHandler(
            "t1",
            "create table wf04 (id1 string id, status int32, temperature int32 measurement)"));
    JsonObject jsonObject =
        nonQuery(
            sqlHandler(
                "t1",
                "insert into wf04(id1, time, status, temperature) values('wt01', 11000, 100)"));
    assertEquals(
        "701: Inconsistent numbers of non-time column names and values: 3-2",
        jsonObject.get("code") + ": " + jsonObject.get("message").getAsString());
  }

  public void testInsertWithWrongMeasurementNum2() {
    nonQuery(
        sqlHandler(
            "t1",
            "create table wf04 (id1 string id, status int32, temperature int32 measurement)"));
    JsonObject jsonObject =
        nonQuery(
            sqlHandler(
                "t1",
                "insert into wf05(id1, time, status, temperature) values('wt01', 11000, 100, 300, 400)"));
    assertEquals(
        "701: Inconsistent numbers of non-time column names and values: 3-4",
        jsonObject.get("code") + ": " + jsonObject.get("message").getAsString());
  }

  public void testInsertWithDuplicatedMeasurements() {
    nonQuery(
        sqlHandler("t1", "create table wf07(id1 string id, s3 boolean measurement, status int32)"));
    JsonObject jsonObject =
        nonQuery(
            sqlHandler(
                "t1",
                "insert into wf07(id1, time, s3, status, status) values('wt01', 100, true, 20.1, 20.2)"));
    assertEquals(
        "701: Insertion contains duplicated measurement: status",
        jsonObject.get("code") + ": " + jsonObject.get("message").getAsString());
  }

  public void testInsertMultiRows() {
    nonQuery(
        sqlHandler(
            "t1", "create table sg8 (id1 string id, s1 int32 measurement, s2 int32 measurement)"));
    JsonObject jsonObject =
        nonQuery(
            sqlHandler(
                "t1",
                "insert into sg8(id1, time, s1, s2) values('d1', 10, 2, 2), ('d1', 11, 3, '3'), ('d1', 12,12.11,false)"));
    assertEquals(
        "507: Fail to insert measurements [s1, s2] caused by [data type is not consistent, input 12.11, registered INT32, data type is not consistent, input false, registered INT32]",
        jsonObject.get("code") + ": " + jsonObject.get("message").getAsString());
  }

  public void testInsertLargeNumber() {
    nonQuery(
        sqlHandler(
            "t1",
            "create table sg9 (id1 string id, s98 int64 measurement, s99 int64 measurement)"));
    JsonObject jsonObject =
        nonQuery(
            sqlHandler(
                "t1",
                "insert into sg9(id1, time, s98, s99) values('d1', 10, 2, 271840880000000000000000)"));
    assertEquals(
        "700: line 1:58: Invalid numeric literal: 271840880000000000000000",
        jsonObject.get("code") + ": " + jsonObject.get("message").getAsString());
  }

  public void testExtendTextColumn() {
    List<String> sqls =
        Arrays.asList(
            "use t1",
            "create table sg14 (id1 string id, s1 string measurement, s2 string measurement)",
            "insert into sg14(id1,time,s1,s2) values('d1',1,'test','test')",
            "insert into sg14(id1,time,s1,s2) values('d1',3,'test','test')",
            "insert into sg14(id1,time,s1,s2) values('d1',3,'test','test')",
            "insert into sg14(id1,time,s1,s2) values('d1',4,'test','test')",
            "insert into sg14(id1,time,s1,s3) values('d1',5,'test','test')",
            "insert into sg14(id1,time,s1,s2) values('d1',6,'test','test')",
            "flush",
            "insert into sg14(id1,time,s1,s3) values('d1',7,'test','test')");
    try {
      for (String sql : sqls) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.addProperty("database", DATABASE);
        jsonObject.addProperty("sql", sql);
        nonQuery(jsonObject.toString());
      }
    } catch (Exception ignore) {

    }
  }

  public void prepareTableData() {
    for (int i = 0; i < sqls.length; i++) {
      JsonObject jsonObject = new JsonObject();
      if (i > 0) {
        jsonObject.addProperty("database", DATABASE);
      } else {
        jsonObject.addProperty("database", "");
      }
      jsonObject.addProperty("sql", sqls[i]);
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
