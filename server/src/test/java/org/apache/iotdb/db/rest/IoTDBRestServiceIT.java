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
package org.apache.iotdb.db.rest;

import org.apache.iotdb.db.utils.EnvironmentUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@FixMethodOrder(MethodSorters.JVM)
public class IoTDBRestServiceIT {
  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  private String getAuthorization(String username, String password) {
    return Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void ping() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    HttpGet httpGet = new HttpGet("http://127.0.0.1:18080/ping");
    CloseableHttpResponse response = null;
    try {
      String authorization = getAuthorization("root", "root");
      httpGet.setHeader("Authorization", authorization);
      response = httpClient.execute(httpGet);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(200, Integer.parseInt(result.get("code").toString()));
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    } finally {
      try {
        if (httpClient != null) {
          httpClient.close();
        }
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
  }

  private HttpPost getHttpPost(String url) {
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-type", "application/json; charset=utf-8");
    httpPost.setHeader("Accept", "application/json");
    String authorization = getAuthorization("root", "root");
    httpPost.setHeader("Authorization", authorization);
    return httpPost;
  }

  public void insertTablet(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232161960,1635232162960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\"],\"dataType\":[\"TEXT\",\"INT32\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[\"11\",2],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":false,\"deviceId\":\"root.sg25\",\"rowSize\":2}";
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
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
  public void insertAndQuery() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    insertTablet(httpClient);
    query(httpClient);
    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  public void query(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/v1/query");
      String sql = "{\"sql\":\"select * from root.sg25\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      Gson json = new Gson();
      List<Map> list = json.fromJson(message, List.class);
      Assert.assertTrue(list.size() > 0);
      Assert.assertEquals("root.sg25.s3", list.get(0).get("measurements"));
      Assert.assertEquals("root.sg25.s4", list.get(1).get("measurements"));
      Assert.assertEquals("root.sg25.s5", list.get(2).get("measurements"));
      Assert.assertEquals("root.sg25.s6", list.get(3).get("measurements"));
      Assert.assertEquals("root.sg25.s7", list.get(4).get("measurements"));
      Assert.assertEquals("2aa", ((List<List>) list.get(0).get("dataValues")).get(0).get(0));
      Assert.assertEquals(
          1635232143960L,
          new BigDecimal(((List<List>) list.get(0).get("dataValues")).get(0).get(1).toString())
              .longValue());
      Assert.assertEquals(
          11,
          new BigDecimal(((List<List>) list.get(1).get("dataValues")).get(0).get(0).toString())
              .intValue());
      Assert.assertEquals(1.41, ((List<List>) list.get(2).get("dataValues")).get(0).get(0));
      Assert.assertNull(((List<List>) list.get(2).get("dataValues")).get(1).get(0));
      Assert.assertFalse(
          Boolean.parseBoolean(
              ((List<List>) list.get(3).get("dataValues")).get(1).get(0).toString()));
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
}
