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

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.Assert.fail;

public class RestUtils {

  public static JsonObject insertTablet(CloseableHttpClient httpClient, int port, String json) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/table/v1/insertTablet");
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      return JsonParser.parseString(message).getAsJsonObject();
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
    return new JsonObject();
  }

  private static HttpPost getHttpPost(String url) {
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-type", "application/json; charset=utf-8");
    httpPost.setHeader("Accept", "application/json");
    String authorization = getAuthorization("root", "root");
    httpPost.setHeader("Authorization", authorization);
    return httpPost;
  }

  private static String getAuthorization(String username, String password) {
    return Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  public static JsonObject query(CloseableHttpClient httpClient, int port, String json) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/table/v1/query");
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      return JsonParser.parseString(message).getAsJsonObject();
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
    return new JsonObject();
  }

  public static JsonObject nonQuery(CloseableHttpClient httpClient, int port, String json) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/table/v1/nonQuery");
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      return JsonParser.parseString(message).getAsJsonObject();
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
    return new JsonObject();
  }
}
