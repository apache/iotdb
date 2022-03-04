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
package org.apache.iotdb.db.protocol.rest;

import org.apache.iotdb.db.utils.EnvironmentUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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

  public void rightInsertTablet(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":false,\"deviceId\":\"root.sg25\"}";
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
  public void errorInsertTablet() {
    CloseableHttpResponse response = null;
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":false,\"deviceId\":\"root.sg25\"}";
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(413, Integer.parseInt(result.get("code").toString()));
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
    rightInsertTablet(httpClient);
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
      String sql = "{\"sql\":\"select *,s4+1,s4+1 from root.sg25\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map map = mapper.readValue(message, Map.class);
      List<Long> timestampsResult = (List<Long>) map.get("timestamps");
      List<Long> expressionsResult = (List<Long>) map.get("expressions");
      List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
      Assert.assertTrue(map.size() > 0);
      List<Object> expressions =
          new ArrayList<Object>() {
            {
              add("root.sg25.s3");
              add("root.sg25.s4");
              add("root.sg25.s5");
              add("root.sg25.s6");
              add("root.sg25.s7");
              add("root.sg25.s8");
              add("root.sg25.s4 + 1");
              add("root.sg25.s4 + 1");
            }
          };
      List<Object> timestamps =
          new ArrayList<Object>() {
            {
              add(1635232143960l);
              add(1635232153960l);
            }
          };
      List<Object> values1 =
          new ArrayList<Object>() {
            {
              add("2aa");
              add("");
            }
          };
      List<Object> values2 =
          new ArrayList<Object>() {
            {
              add(11);
              add(2);
            }
          };
      List<Object> values3 =
          new ArrayList<Object>() {
            {
              add(1635000012345555l);
              add(1635000012345556l);
            }
          };

      List<Object> values4 =
          new ArrayList<Object>() {
            {
              add(1.41);
              add(null);
            }
          };
      List<Object> values5 =
          new ArrayList<Object>() {
            {
              add(null);
              add(false);
            }
          };
      List<Object> values6 =
          new ArrayList<Object>() {
            {
              add(null);
              add(3.5555);
            }
          };

      Assert.assertEquals(expressions, expressionsResult);
      Assert.assertEquals(timestamps, timestampsResult);
      Assert.assertEquals(values1, valuesResult.get(0));
      Assert.assertEquals(values2, valuesResult.get(1));
      Assert.assertEquals(values3, valuesResult.get(2));
      Assert.assertEquals(values4, valuesResult.get(3));
      Assert.assertEquals(values5, valuesResult.get(4));
      Assert.assertEquals(values6, valuesResult.get(5));
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
