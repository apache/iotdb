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
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

// move to integration-test
@Ignore
public class GrafanaApiServiceTest {
  @Before
  public void setUp() throws Exception {
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

  private HttpPost getHttpPost(String url) {
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-type", "application/json; charset=utf-8");
    httpPost.setHeader("Accept", "application/json");
    String authorization = getAuthorization("root", "root");
    httpPost.setHeader("Authorization", authorization);
    return httpPost;
  }

  @Test
  public void login() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    HttpGet httpGet = new HttpGet("http://127.0.0.1:18080/grafana/v1/login");
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

  public void rightInsertTablet(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s4\",\"s5\"],\"dataTypes\":[\"INT32\",\"INT32\"],\"values\":[[11,2],[15,13]],\"isAligned\":false,\"deviceId\":\"root.sg25\"}";
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

  public void expressionGroupByLevel(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/grafana/v1/query/expression");
      String sql =
          "{\"expression\":[\"count(s4)\"],\"prefixPath\":[\"root.sg25\"],\"startTime\":1635232143960,\"endTime\":1635232153960,\"control\":\"group by([1635232143960,1635232153960),1s),level=1\"}";
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
      Assert.assertTrue(timestampsResult.size() == 10);
      Assert.assertTrue(valuesResult.size() == 1);
      Assert.assertTrue("count(root.sg25.s4)".equals(expressionsResult.get(0)));
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

  public void expression(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/grafana/v1/query/expression");
      String sql =
          "{\"expression\":[\"s4\",\"s5\"],\"prefixPath\":[\"root.sg25\"],\"startTime\":1635232133960,\"endTime\":1635232163960}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map<String, List> map = mapper.readValue(message, Map.class);
      String[] expressionsResult = {"root.sg25.s4", "root.sg25.s5"};
      Long[] timestamps = {1635232143960L, 1635232153960L};
      Object[] values1 = {11, 2};
      Object[] values2 = {15, 13};
      Assert.assertArrayEquals(
          expressionsResult, (map.get("expressions")).toArray(new String[] {}));
      Assert.assertArrayEquals(timestamps, (map.get("timestamps")).toArray(new Long[] {}));
      Assert.assertArrayEquals(
          values1, ((List) (map.get("values")).get(0)).toArray(new Object[] {}));
      Assert.assertArrayEquals(
          values2, ((List) (map.get("values")).get(1)).toArray(new Object[] {}));
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

  public void expressionWithControl(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/grafana/v1/query/expression");
      String sql =
          "{\"expression\":[\"sum(s4)\",\"avg(s5)\"],\"prefixPath\":[\"root.sg25\"],\"startTime\":1635232133960,\"endTime\":1635232163960,\"control\":\"group by([1635232133960,1635232163960),20s)\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map<String, List> map = mapper.readValue(message, Map.class);
      String[] expressionsResult = {"sum(root.sg25.s4)", "avg(root.sg25.s5)"};
      Long[] timestamps = {1635232133960L, 1635232153960L};
      Object[] values1 = {11.0, 2.0};
      Object[] values2 = {15.0, 13.0};
      Assert.assertArrayEquals(
          expressionsResult, (map.get("expressions")).toArray(new String[] {}));
      Assert.assertArrayEquals(timestamps, (map.get("timestamps")).toArray(new Long[] {}));
      Assert.assertArrayEquals(
          values1, ((List) (map.get("values")).get(0)).toArray(new Object[] {}));
      Assert.assertArrayEquals(
          values2, ((List) (map.get("values")).get(1)).toArray(new Object[] {}));
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

  public void expressionWithConditionControl(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/grafana/v1/query/expression");
      String sql =
          "{\"expression\":[\"sum(s4)\",\"avg(s5)\"],\"prefixPath\":[\"root.sg25\"],\"condition\":\"timestamp=1635232143960\",\"startTime\":1635232133960,\"endTime\":1635232163960,\"control\":\"group by([1635232133960,1635232163960),20s)\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map<String, List> map = mapper.readValue(message, Map.class);
      String[] expressionsResult = {"sum(root.sg25.s4)", "avg(root.sg25.s5)"};
      Long[] timestamps = {1635232133960L, 1635232153960L};
      Object[] values1 = {11.0, null};
      Object[] values2 = {15.0, null};
      Assert.assertArrayEquals(expressionsResult, map.get("expressions").toArray(new String[] {}));
      Assert.assertArrayEquals(timestamps, (map.get("timestamps")).toArray(new Long[] {}));
      Assert.assertArrayEquals(
          values1, ((List) (map.get("values")).get(0)).toArray(new Object[] {}));
      Assert.assertArrayEquals(
          values2, ((List) (map.get("values")).get(1)).toArray(new Object[] {}));
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

  public void variable(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/grafana/v1/variable");
      String sql = "{\"sql\":\"show child paths root.sg25\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      List list = mapper.readValue(message, List.class);
      String[] expectedResult = {"s4", "s5"};
      Assert.assertArrayEquals(expectedResult, list.toArray(new String[] {}));
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
  public void expressionWithConditionControlTest() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    rightInsertTablet(httpClient);
    expressionWithConditionControl(httpClient);
    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void expressionTest() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    rightInsertTablet(httpClient);
    expression(httpClient);
    expressionGroupByLevel(httpClient);
    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void expressionWithControlTest() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    rightInsertTablet(httpClient);
    expressionWithControl(httpClient);
    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void variableTest() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    rightInsertTablet(httpClient);
    variable(httpClient);
    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }
}
