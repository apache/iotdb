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
import org.apache.iotdb.itbase.category.*;
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
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRestServiceFlushQueryIT {

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

  private static final String[] sqls =
      new String[] {
        "CREATE DATABASE test",
        "CREATE TABLE vehicle (tag1 string tag, s0 int32 field)",
        "insert into vehicle(tag1,time,s0) values('d0',1,101)",
        "insert into vehicle(tag1,time,s0) values('d0',2,198)",
        "insert into vehicle(tag1,time,s0) values('d0',100,99)",
        "insert into vehicle(tag1,time,s0) values('d0',101,99)",
        "insert into vehicle(tag1,time,s0) values('d0',102,80)",
        "insert into vehicle(tag1,time,s0) values('d0',103,99)",
        "insert into vehicle(tag1,time,s0) values('d0',104,90)",
        "insert into vehicle(tag1,time,s0) values('d0',105,99)",
        "insert into vehicle(tag1,time,s0) values('d0',106,99)",
        "flush",
        "insert into vehicle(tag1,time,s0) values('d0',2,10000)",
        "insert into vehicle(tag1,time,s0) values('d0',50,10000)",
        "insert into vehicle(tag1,time,s0) values('d0',1000,22222)",
      };

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
    selectAllSQLTest();
    testFlushGivenGroup();
    testFlushGivenGroupNoData();
  }

  public String sqlHandler(String database, String sql) {
    JsonObject json = new JsonObject();
    json.addProperty("database", database);
    json.addProperty("sql", sql);
    return json.toString();
  }

  public void selectAllSQLTest() {
    String sql = sqlHandler("test", "SELECT * FROM vehicle");
    JsonObject jsonObject = query(sql);
    JsonArray valuesList = jsonObject.getAsJsonArray("values");
    for (int i = 0; i < valuesList.size(); i++) {
      JsonArray jsonArray = valuesList.get(i).getAsJsonArray();
      for (int j = 0; j < jsonArray.size(); j++) {
        jsonArray.get(j);
      }
    }
  }

  public void testFlushGivenGroup() {
    List<String> list =
        Arrays.asList("CREATE DATABASE group1", "CREATE DATABASE group2", "CREATE DATABASE group3");
    for (String sql : list) {
      nonQuery(sqlHandler("", sql));
    }

    String insertTemplate =
        "INSERT INTO vehicle(tag1, time, s1, s2, s3) VALUES (%s, %d, %d, %f, %s)";
    for (int i = 1; i <= 3; i++) {
      nonQuery(sqlHandler("", String.format("USE \"group%d\"", i)));
      nonQuery(
          sqlHandler(
              String.format("group%d", i),
              "CREATE TABLE vehicle (tag1 string tag, s1 int32 field, s2 float field, s3 string field)"));

      for (int j = 10; j < 20; j++) {
        nonQuery(String.format(Locale.CHINA, insertTemplate, i, j, j, j * 0.1, j));
      }
    }
    nonQuery(sqlHandler("", "FLUSH"));

    for (int i = 1; i <= 3; i++) {
      nonQuery(sqlHandler("", String.format("USE \"group%d\"", i)));
      nonQuery(
          sqlHandler(
              String.format("group%d", i),
              "CREATE TABLE vehicle (tag1 string tag, s1 int32 field, s2 float field, s3 string field)"));
    }
    nonQuery(sqlHandler("", "FLUSH group1"));
    nonQuery(sqlHandler("", "FLUSH group2,group3"));

    for (int i = 1; i <= 3; i++) {
      nonQuery(sqlHandler("", String.format("USE \"group%d\"", i)));
      nonQuery(
          sqlHandler(
              String.format("group%d", i),
              "CREATE TABLE vehicle (tag1 string tag, s1 int32 field, s2 float field, s3 string field)"));

      for (int j = 0; j < 30; j++) {
        nonQuery(String.format(Locale.CHINA, insertTemplate, i, j, j, j * 0.1, j));
      }
    }
    nonQuery(sqlHandler("", "FLUSH group1 TRUE"));
    nonQuery(sqlHandler("", "FLUSH group2,group3 FALSE"));

    for (int i = 1; i <= 3; i++) {
      int count = 0;
      nonQuery(sqlHandler("", String.format("USE \"group%d\"", i)));
      JsonObject jsonObject =
          query(sqlHandler(String.format("group%d", i), "SELECT * FROM vehicle"));

      JsonArray valuesList = jsonObject.getAsJsonArray("values");
      for (int c = 0; c < valuesList.size(); c++) {
        count++;
        JsonArray jsonArray = valuesList.get(c).getAsJsonArray();
        for (int j = 0; j < jsonArray.size(); j++) {
          jsonArray.get(j);
        }
        assertEquals(30, count);
      }
    }
  }

  public void testFlushGivenGroupNoData() {
    List<String> list =
        Arrays.asList(
            "CREATE DATABASE nodatagroup1",
            "CREATE DATABASE nodatagroup2",
            "CREATE DATABASE nodatagroup3",
            "FLUSH nodatagroup1",
            "FLUSH nodatagroup2",
            "FLUSH nodatagroup3",
            "FLUSH nodatagroup1, nodatagroup2");
    for (String sql : list) {
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("database", "");
      jsonObject.addProperty("sql", sql);
      nonQuery(jsonObject.toString());
    }
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
