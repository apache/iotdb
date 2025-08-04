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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBRestServiceIT {

  private int port = 18080;
  private CloseableHttpClient httpClient = null;

  @Before
  public void setUp() throws Exception {
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableRestService(true);
    baseEnv.getConfig().getCommonConfig().setEnforceStrongPassword(false);
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
        "create database if not exists test",
        "use test",
        "CREATE TABLE sg10(tag1 string tag, s1 int64 field, s2 float field, s3 string field)",
        "CREATE TABLE sg11(tag1 string tag, s1 int64 field, s2 float field, s3 string field)"
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
    rightNonQuery();
    rightNonQuery2();
    rightNonQuery3();
    rightNonQuery4();
    errorNonQuery();
    errorNonQuery1();
    errorNonQuery3();
    testInsertMultiPartition();
    testInsertTablet();
    testInsertTabletNoDatabase();
    testInsertTablet1();
    testInsertTablet2();
    testQuery();
    testQuery1();
    testQuery2();
    inertDateAndBlob();
  }

  public void testQuery() {
    String sql = "insert into sg11(tag1,s1,s2,s3,time) values('aa',11,1.1,1,1),('aa2',21,2.1,2,2)";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    assertEquals(200, result.get("code").getAsInt());
    JsonObject queryResult =
        RestUtils.query(
            httpClient,
            port,
            sqlHandler("test", "select tag1,s1,s2,s3,time from sg11 order by time"));
    JsonArray jsonArray = queryResult.get("values").getAsJsonArray();
    for (int i = 0; i < jsonArray.size(); i++) {
      JsonArray jsonArray1 = jsonArray.get(i).getAsJsonArray();

      if (i == 0) {
        assertEquals("aa", jsonArray1.get(0).getAsString());
        assertEquals(11, jsonArray1.get(1).getAsInt());
        assertEquals(1.1, jsonArray1.get(2).getAsFloat(), 0.000001f);
        assertEquals("1", jsonArray1.get(3).getAsString());
      } else if (i == 1) {
        assertEquals("aa2", jsonArray1.get(0).getAsString());
        assertEquals(21, jsonArray1.get(1).getAsInt());
        assertEquals(2.1, jsonArray1.get(2).getAsFloat(), 0.000001f);
        assertEquals("2", jsonArray1.get(3).getAsString());
        assertEquals(2, jsonArray1.get(4).getAsLong());
      }
    }
  }

  public void testQuery1() {
    JsonObject result =
        RestUtils.query(
            httpClient,
            port,
            sqlHandler(null, "select tag1,s1,s2,s3,time from sg11 order by time"));
    assertEquals(701, result.get("code").getAsInt());
    assertEquals(
        "Database must be specified when session database is not set",
        result.get("message").getAsString());
  }

  public void testQuery2() {
    JsonObject result = RestUtils.query(httpClient, port, sqlHandler("test", null));
    assertEquals(305, result.get("code").getAsInt());
    assertEquals("sql should not be null", result.get("message").getAsString());
  }

  public void rightNonQuery() {
    String sql = "create database test1";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("", sql));
    assertEquals(200, result.get("code").getAsInt());
  }

  public void rightNonQuery2() {
    String sql = "insert into sg10(tag1,s1,time,s2) values('aa',1,1,1.1)";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    assertEquals(200, result.get("code").getAsInt());
  }

  public void rightNonQuery4() {
    String sql = "insert into sg10(tag1,s1,time,s2) values('aa',1,1,1.1),('bb',2,2,2.1)";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    assertEquals(200, result.get("code").getAsInt());
  }

  public void rightNonQuery3() {
    String sql = "drop database test1";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    assertEquals(200, result.get("code").getAsInt());
  }

  public void errorNonQuery() {
    String sql = "create database test";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("", sql));
    assertEquals(501, result.get("code").getAsInt());
    assertEquals("Database test already exists", result.get("message").getAsString());
  }

  public void errorNonQuery1() {
    String sql =
        "CREATE TABLE sg10(tag1 string tag, s1 int64 field, s2 float field, s3 string field)";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler(null, sql));
    assertEquals(701, result.get("code").getAsInt());
    assertEquals("database is not specified", result.get("message").getAsString());
  }

  public void errorNonQuery2() {
    String sql = "create database test";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler(null, sql));
    assertEquals(701, result.get("code").getAsInt());
    assertEquals("database is not specified", result.get("message").getAsString());
  }

  public void errorNonQuery3() {
    String sql = "select * from sg10";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    assertEquals(301, result.get("code").getAsInt());
    assertEquals("EXECUTE_STATEMENT_ERROR", result.get("message").getAsString());
  }

  public String sqlHandler(String database, String sql) {
    JsonObject json = new JsonObject();
    json.addProperty("database", database);
    json.addProperty("sql", sql);
    return json.toString();
  }

  public void testInsertMultiPartition() {
    List<String> sqls =
        Arrays.asList(
            "create table sg1 (tag1 string tag, s1 int32 field)",
            "insert into sg1(tag1,time,s1) values('d1',1,2)",
            "flush",
            "insert into sg1(tag1,time,s1) values('d1',2,2)",
            "insert into sg1(tag1,time,s1) values('d1',604800001,2)",
            "flush");
    for (String sql : sqls) {
      RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    }
  }

  public void testInsertTablet() {
    List<String> sqls =
        Collections.singletonList(
            "create table sg211 (tag1 string tag,t1 STRING ATTRIBUTE, s1 FLOAT field)");
    for (String sql : sqls) {
      RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    }
    String json =
        "{\"database\":\"test\",\"column_categories\":[\"TAG\",\"ATTRIBUTE\",\"FIELD\"],\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232173960,1635232183960],\"column_names\":[\"tag1\",\"t1\",\"s1\"],\"data_types\":[\"STRING\",\"STRING\",\"FLOAT\"],\"values\":[[\"a11\",\"true\",11],[\"a11\",\"false\",22],[\"a13\",\"false1\",23],[\"a14\",\"false2\",24],[\"a15\",\"false3\",25]],\"table\":\"sg211\"}";
    rightInsertTablet(json);
  }

  public void testInsertTabletNoDatabase() {
    List<String> sqls =
        Collections.singletonList(
            "create table sg211 (tag1 string tag,t1 STRING ATTRIBUTE, s1 FLOAT field)");
    for (String sql : sqls) {
      RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    }
    String json =
        "{\"database\":\"\",\"column_categories\":[\"TAG\",\"ATTRIBUTE\",\"FIELD\"],\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232173960,1635232183960],\"column_names\":[\"tag1\",\"t1\",\"s1\"],\"data_types\":[\"STRING\",\"STRING\",\"FLOAT\"],\"values\":[[\"a11\",\"true\",11],[\"a11\",\"false\",22],[\"a13\",\"false1\",23],[\"a14\",\"false2\",24],[\"a15\",\"false3\",25]],\"table\":\"sg211\"}";
    JsonObject result = RestUtils.insertTablet(httpClient, port, json);
    assertEquals(305, Integer.parseInt(result.get("code").toString()));
  }

  public void testInsertTablet1() {
    List<String> sqls =
        Collections.singletonList(
            "create table sg211 (tag1 string tag,t1 STRING ATTRIBUTE, s1 FLOAT field)");
    for (String sql : sqls) {
      RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    }
    String json =
        "{\"database\":\"test\",\"column_categories\":[\"ATTRIBUTE\",\"FIELD\"],\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232173960,1635232183960],\"column_names\":[\"id1\",\"t1\",\"s1\"],\"data_types\":[\"STRING\",\"STRING\",\"FLOAT\"],\"values\":[[\"a11\",\"true\",11],[\"a11\",\"false\",22],[\"a13\",\"false1\",23],[\"a14\",\"false2\",24],[\"a15\",\"false3\",25]],\"table\":\"sg211\"}";
    JsonObject result = RestUtils.insertTablet(httpClient, port, json);
    assertEquals(305, Integer.parseInt(result.get("code").toString()));
    assertEquals(
        "column_names and column_categories should have the same size,column_categories and data_types should have the same size",
        result.get("message").getAsString());
  }

  public void testInsertTablet2() {
    List<String> sqls =
        Collections.singletonList(
            "create table sg211 (tag1 string tag,t1 STRING ATTRIBUTE, s1 FLOAT field)");
    for (String sql : sqls) {
      RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));
    }
    String json =
        "{\"database\":\"test\",\"column_categories\":[\"TAG\",\"ATTRIBUTE\",\"FIELD\"],\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232183960],\"column_names\":[\"tag1\",\"t1\",\"s1\"],\"data_types\":[\"STRING\",\"STRING\",\"FLOAT\"],\"values\":[[\"a11\",\"true\",11],[\"a11\",\"false\",22],[\"a13\",\"false1\",23],[\"a14\",\"false2\",24],[\"a15\",\"false3\",25]],\"table\":\"sg211\"}";
    JsonObject result = RestUtils.insertTablet(httpClient, port, json);
    assertEquals(305, Integer.parseInt(result.get("code").toString()));
    assertEquals(
        "values and timestamps should have the same size", result.get("message").getAsString());
  }

  public void rightInsertTablet(String json) {
    JsonObject result = RestUtils.insertTablet(httpClient, port, json);
    assertEquals(200, Integer.parseInt(result.get("code").toString()));
    JsonObject queryResult =
        RestUtils.query(
            httpClient, port, sqlHandler("test", "select tag1,t1,s1 from sg211 order by time"));
    JsonArray jsonArray = queryResult.get("values").getAsJsonArray();
    JsonArray jsonArray1 = jsonArray.get(0).getAsJsonArray();
    assertEquals("a11", jsonArray1.get(0).getAsString());
    assertEquals("false", jsonArray1.get(1).getAsString());
    assertEquals(11f, jsonArray1.get(2).getAsFloat(), 0f);
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
      RestUtils.nonQuery(httpClient, port, jsonObject.toString());
    }
  }

  public void inertDateAndBlob() {
    RestUtils.nonQuery(httpClient, port, sqlHandler("", "create database test"));
    String sql = "CREATE TABLE tt (time TIMESTAMP TIME,d Blob FIELD,e date FIELD)";
    JsonObject result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", sql));

    assertEquals(200, Integer.parseInt(result.get("code").toString()));
    String insertSql = "insert into tt(time,e,d) values(1,'2025-07-14',X'cafebabe')";
    result = RestUtils.nonQuery(httpClient, port, sqlHandler("test", insertSql));
    System.out.println(result);
    assertEquals(200, Integer.parseInt(result.get("code").toString()));

    JsonObject queryResult =
        RestUtils.query(httpClient, port, sqlHandler("test", "select time,e,d from tt"));
    JsonArray jsonArray = queryResult.get("values").getAsJsonArray();
    System.out.println(jsonArray);
    JsonArray jsonArray1 = jsonArray.get(0).getAsJsonArray();
    assertEquals(1, jsonArray1.get(0).getAsInt());
    assertEquals("2025-07-14", jsonArray1.get(1).getAsString());
    assertEquals("0xcafebabe", jsonArray1.get(2).getAsString());
  }
}
