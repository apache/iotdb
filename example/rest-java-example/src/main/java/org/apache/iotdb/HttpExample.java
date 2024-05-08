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

package org.apache.iotdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

public class HttpExample {
  private String getAuthorization(String username, String password) {
    return Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  public static void main(String[] args) {
    HttpExample httpExample = new HttpExample();
    httpExample.ping();
    httpExample.insertTablet();
    httpExample.query();
  }

  public void ping() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
    HttpGet httpGet = new HttpGet("http://127.0.0.1:18080/ping");
    CloseableHttpResponse response = null;
    try {
      response = httpClient.execute(httpGet);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();

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

  public void insertTablet() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
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
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();

    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void query() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
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
      System.out.println(map);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
