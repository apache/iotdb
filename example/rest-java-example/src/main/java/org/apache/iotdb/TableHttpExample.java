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

public class TableHttpExample {

  private static final String UTF8 = "utf-8";

  private String getAuthorization(String username, String password) {
    return Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  public static void main(String[] args) {
    TableHttpExample httpExample = new TableHttpExample();
    httpExample.ping();
    httpExample.createDatabase();
    httpExample.createTable();
    httpExample.nonQuery();
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
      String message = EntityUtils.toString(responseEntity, UTF8);
      String result = JsonParser.parseString(message).getAsJsonObject().toString();
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("The ping rest api failed");
    } finally {
      try {
        httpClient.close();
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Http Client close error");
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
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/table/v1/insertTablet");
      String json =
          "{\"database\":\"test\",\"column_categories\":[\"TAG\",\"ATTRIBUTE\",\"FIELD\"],\"timestamps\":[1635232143960,1635232153960,1635232163960,1635232173960,1635232183960],\"column_names\":[\"id1\",\"t1\",\"s1\"],\"data_types\":[\"STRING\",\"STRING\",\"FLOAT\"],\"values\":[[\"a11\",\"true\",11333],[\"a11\",\"false\",22333],[\"a13\",\"false1\",23333],[\"a14\",\"false2\",24],[\"a15\",\"false3\",25]],\"table\":\"sg211\"}";
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, UTF8);
      String result = JsonParser.parseString(message).getAsJsonObject().toString();
      System.out.println(result);
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("The insertTablet rest api failed");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Response close error");
      }
    }
  }

  public void nonQuery() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/table/v1/nonQuery");
      String sql =
          "{\"database\":\"test\",\"sql\":\"INSERT INTO sg211(time, id1, s1) values(100, 'd1', 0)\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, UTF8);
      System.out.println(JsonParser.parseString(message).getAsJsonObject().toString());
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("The non query rest api failed");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Response close error");
      }
    }
  }

  public void createDatabase() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/table/v1/nonQuery");
      String sql = "{\"database\":\"\",\"sql\":\"create database if not exists test\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, UTF8);
      System.out.println(JsonParser.parseString(message).getAsJsonObject().toString());
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("The non query rest api failed");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Response close error");
      }
    }
  }

  public void createTable() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/table/v1/nonQuery");
      String sql =
          "{\"database\":\"test\",\"sql\":\"create table sg211 (id1 string TAG,t1 STRING ATTRIBUTE, s1 FLOAT FIELD)\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, UTF8);
      System.out.println(JsonParser.parseString(message).getAsJsonObject().toString());
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("create table failed");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Response close error");
      }
    }
  }

  public void query() {
    CloseableHttpClient httpClient = SSLClient.getInstance().getHttpClient();
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:18080/rest/table/v1/query");
      String sql = "{\"database\":\"test\",\"sql\":\"select * from sg211\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, UTF8);
      System.out.println(JsonParser.parseString(message).getAsJsonObject().toString());
    } catch (IOException e) {
      e.printStackTrace();
      System.out.println("The query rest api failed");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
        System.out.println("Response close error");
      }
    }
  }
}
