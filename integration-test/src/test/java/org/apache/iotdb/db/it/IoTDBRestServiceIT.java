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
package org.apache.iotdb.db.it;

import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.env.cluster.node.DataNodeWrapper;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.itbase.category.RemoteIT;
import org.apache.iotdb.itbase.env.BaseEnv;

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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.commons.schema.column.ColumnHeaderConstant.COLUMN_TTL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class, ClusterIT.class, RemoteIT.class})
public class IoTDBRestServiceIT {

  private int port = 18080;

  @Before
  public void setUp() throws Exception {
    BaseEnv baseEnv = EnvFactory.getEnv();
    baseEnv.getConfig().getDataNodeConfig().setEnableRestService(true);
    baseEnv.initClusterEnvironment();
    DataNodeWrapper portConflictDataNodeWrapper = EnvFactory.getEnv().getDataNodeWrapper(0);
    port = portConflictDataNodeWrapper.getRestServicePort();
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  private String getAuthorization(String username, String password) {
    return Base64.getEncoder()
        .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void ping() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
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

  private HttpPost getHttpPost_1(String url) {
    HttpPost httpPost = new HttpPost(url);
    httpPost.addHeader("Content-type", "application/json; charset=utf-8");
    httpPost.setHeader("Accept", "application/json");
    String authorization = getAuthorization("root1", "root1");
    httpPost.setHeader("Authorization", authorization);
    return httpPost;
  }

  public void nonQuery(CloseableHttpClient httpClient, String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    try {
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

  public void errorNonQuery(CloseableHttpClient httpClient, String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    try {
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(700, Integer.parseInt(result.get("code").toString()));
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

  public void rightInsertRecords(CloseableHttpClient httpClient, String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    try {
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

  public void errorInsertRecords(CloseableHttpClient httpClient, String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    try {
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(509, Integer.parseInt(result.get("code").toString()));
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

  public void rightInsertTablet(CloseableHttpClient httpClient, String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    try {
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

  public void rightInsertTablet(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":false,\"deviceId\":\"root.sg25\"}";
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      for (int i = 0; i < 30; i++) {
        try {
          response = httpClient.execute(httpPost);
          break;
        } catch (Exception e) {
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

  public void errorInsertTablet(String json, HttpPost httpPost) {
    CloseableHttpResponse response = null;
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    try {
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(606, Integer.parseInt(result.get("code").toString()));
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
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/insertTablet");
      String json =
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":false,\"deviceId\":\"root.sg25\"}";
      httpPost.setEntity(new StringEntity(json, Charset.defaultCharset()));
      for (int i = 0; i < 30; i++) {
        try {
          response = httpClient.execute(httpPost);
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
      assertEquals(606, Integer.parseInt(result.get("code").toString()));
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

  public void perData(CloseableHttpClient httpClient) {
    HttpPost httpPost2 = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/nonQuery");
    HttpPost httpPostV2 = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/nonQuery");

    nonQuery(httpClient, "{\"sql\":\"CREATE USER `root1` 'root1'\"}", httpPost2);
    nonQuery(httpClient, "{\"sql\":\"GRANT WRITE ON  root.** to user root1\"}", httpPostV2);
  }

  @Test
  public void insertAndQuery() {
    CloseableHttpClient httpClient = HttpClientBuilder.create().build();
    //
    rightInsertTablet(httpClient);
    query(httpClient);
    queryGroupByLevel(httpClient);
    queryRowLimit(httpClient);
    queryShowChildPaths(httpClient);
    queryShowNodes(httpClient);
    showAllTTL(httpClient);
    showStorageGroup(httpClient);
    showFunctions(httpClient);
    showTimeseries(httpClient);

    showLastTimeseries(httpClient);
    countTimeseries(httpClient);
    countNodes(httpClient);
    showDevices(httpClient);

    showDevicesWithStroage(httpClient);
    listUser(httpClient);
    selectCount(httpClient);
    selectLast(httpClient);

    queryV2(httpClient);
    queryGroupByLevelV2(httpClient);
    queryRowLimitV2(httpClient);
    queryShowChildPathsV2(httpClient);
    queryShowNodesV2(httpClient);
    showAllTTLV2(httpClient);
    showStorageGroupV2(httpClient);
    showFunctionsV2(httpClient);
    showTimeseriesV2(httpClient);

    showLastTimeseriesV2(httpClient);
    countTimeseriesV2(httpClient);
    countNodesV2(httpClient);
    showDevicesV2(httpClient);

    showDevicesWithStroageV2(httpClient);
    listUserV2(httpClient);
    selectCountV2(httpClient);
    selectLastV2(httpClient);
    perData(httpClient);
    List<String> insertTablet_right_json_list = new ArrayList<>();
    List<String> insertTablet_error_json_list = new ArrayList<>();

    List<String> insertRecords_right_json_list = new ArrayList<>();
    List<String> insertRecords_error_json_list = new ArrayList<>();

    List<String> nonQuery_right_json_list = new ArrayList<>();
    List<String> nonQuery_error_json_list = new ArrayList<>();

    List<String> insertTablet_right_json_list_v2 = new ArrayList<>();
    List<String> insertTablet_error_json_list_v2 = new ArrayList<>();

    List<String> insertRecords_right_json_list_v2 = new ArrayList<>();
    List<String> insertRecords_error_json_list_v2 = new ArrayList<>();

    List<String> nonQuery_right_json_list_v2 = new ArrayList<>();
    List<String> nonQuery_error_json_list_v2 = new ArrayList<>();
    for (int i = 0; i <= 1; i++) {
      boolean isAligned = false;
      if (i == 0) {
        isAligned = true;
      }
      insertTablet_right_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":\""
              + isAligned
              + "\",\"deviceId\":\"root.sg21"
              + i
              + "\"}");
      insertTablet_right_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":\""
              + isAligned
              + "\",\"deviceId\":\"root.sg22"
              + i
              + "\"}");
      insertTablet_right_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":\""
              + isAligned
              + "\",\"deviceId\":\"root.`sg23"
              + i
              + "`\"}");
      insertTablet_right_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"isAligned\":\""
              + isAligned
              + "\",\"deviceId\":\"root.`sg24"
              + i
              + "`\"}");
      insertTablet_error_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":"
              + isAligned
              + ",\"deviceId\":\"root.sg25"
              + i
              + "\"}");
      insertTablet_error_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":"
              + isAligned
              + ",\"deviceId\":\"root.sg36"
              + i
              + "\"}");
      insertTablet_error_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"a123123\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":"
              + isAligned
              + ",\"deviceId\":\"root.`sg46"
              + i
              + "`\"}");
      insertTablet_error_json_list.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`1231231`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"dataTypes\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"isAligned\":"
              + isAligned
              + ",\"deviceId\":\"root.`3333a"
              + i
              + "`\"}");
      insertRecords_right_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"s33\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.s3\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"`s33`\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.s3\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"s33\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.`s3`\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"`s33`\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.`s11`\",\"root.s11\",\"root.s1\",\"root.`s3`\"],\"isAligned\":"
              + isAligned
              + "}");

      insertRecords_error_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"root\",\"442\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.time\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"time\",\"a123123\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.time\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"time\",\"a12321\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.111\",\"root.`s11`\",\"root.s1\",\"root.s3\"],\"isAligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurementsList\":[[\"`root`\",\"1111\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"dataTypesList\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"valuesList\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"deviceIds\":[\"root.s11\",\"root.s11\",\"root.time\",\"root.`s3`\"],\"isAligned\":"
              + isAligned
              + "}");

      nonQuery_right_json_list.add("{\"sql\":\"insert into root.aa(time,`bb`) values(111,1)\"}");
      nonQuery_right_json_list.add("{\"sql\":\"insert into root.`aa`(time,bb) values(111,1)\"}");
      nonQuery_right_json_list.add("{\"sql\":\"insert into root.`aa`(time,`bb`) values(111,1)\"}");
      nonQuery_right_json_list.add("{\"sql\":\"insert into root.aa(time,bb) values(111,1)\"}");

      nonQuery_error_json_list.add("{\"sql\":\"insert into root.time(time,1`bb`) values(111,1)\"}");
      nonQuery_error_json_list.add(
          "{\"sql\":\"insert into root.time.`aa`(time,bb) values(111,1)\"}");
      nonQuery_error_json_list.add(
          "{\"sql\":\"insert into root.time.`aa`(time,`bb`) values(111,1)\"}");
      nonQuery_error_json_list.add("{\"sql\":\"insert into root.aa(time,root) values(111,1)\"}");

      insertTablet_right_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"is_aligned\":\""
              + isAligned
              + "\",\"device\":\"root.sg21"
              + i
              + "\"}");
      insertTablet_right_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.sg22"
              + i
              + "\"}");
      insertTablet_right_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.`sg23"
              + i
              + "`\"}");
      insertTablet_right_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[11,2],[1635000012345555,1635000012345556],[1.41,null],[null,false],[null,3.5555]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.`sg24"
              + i
              + "`\"}");
      insertTablet_error_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"s3\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.sg25"
              + i
              + "\"}");
      insertTablet_error_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`s3`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.sg26"
              + i
              + "\"}");
      insertTablet_error_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"cc123123\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.`sg26"
              + i
              + "`\"}");
      insertTablet_error_json_list_v2.add(
          "{\"timestamps\":[1635232143960,1635232153960],\"measurements\":[\"`1231231cc`\",\"s4\",\"s5\",\"s6\",\"s7\",\"s8\"],\"data_types\":[\"TEXT\",\"INT32\",\"INT64\",\"FLOAT\",\"BOOLEAN\",\"DOUBLE\"],\"values\":[[\"2aa\",\"\"],[111111112312312442352545452323123,2],[16,15],[1.41,null],[null,false],[null,3.55555555555555555555555555555555555555555555312234235345123127318927461482308478123645555555555555555555555555555555555555555555531223423534512312731892746148230847812364]],\"is_aligned\":"
              + isAligned
              + ",\"device\":\"root.`3333a"
              + i
              + "`\"}");
      insertRecords_right_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"s33\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.s3\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"`s33`\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.s3\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"s33\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.`s3`\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_right_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"`s33`\",\"s44\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[1,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.`s11`\",\"root.s11\",\"root.s1\",\"root.`s3`\"],\"is_aligned\":"
              + isAligned
              + "}");

      insertRecords_error_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"root\",\"442\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.time\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"time\",\"123123\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.s1\",\"root.time\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"time\",\"12321\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.111\",\"root.`s11`\",\"root.s1\",\"root.s3\"],\"is_aligned\":"
              + isAligned
              + "}");
      insertRecords_error_json_list_v2.add(
          "{\"timestamps\":[1635232113960,1635232151960,1123,10],\"measurements_list\":[[\"`root`\",\"1111\"],[\"s55\",\"s66\"],[\"s77\",\"s88\"],[\"s771\",\"s881\"]],\"data_types_list\":[[\"INT32\",\"INT64\"],[\"float\",\"double\"],[\"float\",\"double\"],[\"boolean\",\"text\"]],\"values_list\":[[true,11],[2.1,2],[4,6],[false,\"cccccc\"]],\"devices\":[\"root.s11\",\"root.s11\",\"root.time\",\"root.`s3`\"],\"is_aligned\":"
              + isAligned
              + "}");

      nonQuery_right_json_list_v2.add("{\"sql\":\"insert into root.aa(time,`bb`) values(111,1)\"}");
      nonQuery_right_json_list_v2.add("{\"sql\":\"insert into root.`aa`(time,bb) values(111,1)\"}");
      nonQuery_right_json_list_v2.add(
          "{\"sql\":\"insert into root.`aa`(time,`bb`) values(111,1)\"}");
      nonQuery_right_json_list_v2.add("{\"sql\":\"insert into root.aa(time,bb) values(111,1)\"}");

      nonQuery_error_json_list_v2.add(
          "{\"sql\":\"insert into root.time(time,1`bb`) values(111,1)\"}");
      nonQuery_error_json_list_v2.add(
          "{\"sql\":\"insert into root.time.`aa`(time,bb) values(111,1)\"}");
      nonQuery_error_json_list_v2.add(
          "{\"sql\":\"insert into root.time.`aa`(time,`bb`) values(111,1)\"}");
      nonQuery_error_json_list_v2.add("{\"sql\":\"insert into root.aa(time,root) values(111,1)\"}");
    }
    HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/insertTablet");
    HttpPost httpPost1 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v1/insertTablet");

    List<HttpPost> httpPosts = new ArrayList<>();
    httpPosts.add(httpPost);
    httpPosts.add(httpPost1);
    for (HttpPost hp : httpPosts) {
      for (String json : insertTablet_right_json_list) {
        rightInsertTablet(httpClient, json, hp);
      }
      for (String json : insertTablet_error_json_list) {
        errorInsertTablet(json, hp);
      }
    }

    HttpPost httpPost3 = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/nonQuery");
    HttpPost httpPost4 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v1/nonQuery");
    List<HttpPost> httpPosts1 = new ArrayList<>();
    httpPosts1.add(httpPost3);
    httpPosts1.add(httpPost4);

    for (HttpPost hp : httpPosts1) {
      for (String json : nonQuery_right_json_list) {
        nonQuery(httpClient, json, hp);
      }
      for (String json : nonQuery_error_json_list) {
        errorNonQuery(httpClient, json, hp);
      }
    }

    HttpPost httpPost5 = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/insertRecords");
    HttpPost httpPost6 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v1/insertRecords");
    List<HttpPost> httpPosts2 = new ArrayList<>();
    httpPosts2.add(httpPost5);
    httpPosts2.add(httpPost6);

    HttpPost httpPostV2 = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/insertTablet");
    HttpPost httpPost1V2 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v2/insertTablet");

    List<HttpPost> httpPostsV2 = new ArrayList<>();
    httpPostsV2.add(httpPostV2);
    httpPostsV2.add(httpPost1V2);
    for (HttpPost hp : httpPostsV2) {
      for (String json : insertTablet_right_json_list_v2) {
        rightInsertTablet(httpClient, json, hp);
      }
      for (String json : insertTablet_error_json_list_v2) {
        errorInsertTablet(json, hp);
      }
    }

    HttpPost httpPost3V2 = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/nonQuery");
    HttpPost httpPost4V2 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v2/nonQuery");
    List<HttpPost> httpPosts1V2 = new ArrayList<>();
    httpPosts1V2.add(httpPost3V2);
    httpPosts1V2.add(httpPost4V2);
    for (HttpPost hp : httpPosts1V2) {
      for (String json : nonQuery_right_json_list_v2) {
        nonQuery(httpClient, json, hp);
      }
      for (String json : nonQuery_error_json_list_v2) {
        errorNonQuery(httpClient, json, hp);
      }
    }

    HttpPost httpPost5V2 = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/insertRecords");
    HttpPost httpPost6V2 = getHttpPost_1("http://127.0.0.1:" + port + "/rest/v2/insertRecords");
    List<HttpPost> httpPosts2V2 = new ArrayList<>();
    httpPosts2V2.add(httpPost5V2);
    httpPosts2V2.add(httpPost6V2);
    for (HttpPost hp : httpPosts2V2) {
      for (String json : insertRecords_right_json_list_v2) {
        rightInsertRecords(httpClient, json, hp);
      }
      for (String json : insertRecords_error_json_list_v2) {
        errorInsertRecords(httpClient, json, hp);
      }
    }

    try {
      httpClient.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void queryWithUnsetAuthorization() {
    CloseableHttpResponse response = null;
    try {
      CloseableHttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
      httpPost.addHeader("Content-type", "application/json; charset=utf-8");
      httpPost.setHeader("Accept", "application/json");
      String sql = "{\"sql\":\"select *,s4+1,s4+1 from root.sg25\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      for (int i = 0; i < 30; i++) {
        try {
          response = httpClient.execute(httpPost);
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

      Assert.assertEquals(401, response.getStatusLine().getStatusCode());
      String message = EntityUtils.toString(response.getEntity(), "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(800, Integer.parseInt(result.get("code").toString()));
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
  public void queryWithWrongAuthorization() {
    CloseableHttpResponse response = null;
    try {
      CloseableHttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
      httpPost.addHeader("Content-type", "application/json; charset=utf-8");
      httpPost.setHeader("Accept", "application/json");
      String authorization = getAuthorization("abc", "def");
      httpPost.setHeader("Authorization", authorization);
      String sql = "{\"sql\":\"select *,s4+1,s4+1 from root.sg25\"}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      for (int i = 0; i < 30; i++) {
        try {
          response = httpClient.execute(httpPost);
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

      Assert.assertEquals(401, response.getStatusLine().getStatusCode());
      String message = EntityUtils.toString(response.getEntity(), "utf-8");
      JsonObject result = JsonParser.parseString(message).getAsJsonObject();
      assertEquals(801, Integer.parseInt(result.get("code").toString()));
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

  public void query(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
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

  public void queryGroupByLevel(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
      String sql =
          "{\"sql\":\"select count(s4) from root.sg25 group by([1635232143960,1635232153960),1s),level=1\"}";
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

  public void queryRowLimit(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
      String sql = "{\"sql\":\"select *,s4+1,s4+1 from root.sg25\",\"rowLimit\":1}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      assertTrue(message.contains("row size exceeded the given max row size"));
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

  public Map queryMetaData(CloseableHttpClient httpClient, String sql) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v1/query");
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map map = mapper.readValue(message, Map.class);
      return map;

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
    return null;
  }

  public void queryShowChildPaths(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show child paths root\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("ChildPaths");
            add("NodeTypes");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void queryShowNodes(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show child nodes root\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("ChildNodes");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showAllTTL(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show all ttl\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.**");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("INF");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
  }

  public void showStorageGroup(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"SHOW DATABASES root.*\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Database");
            add("SchemaReplicationFactor");
            add("DataReplicationFactor");
            add("TimePartitionOrigin");
            add("TimePartitionInterval");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showFunctions(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show functions\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    assertEquals(4, columnNamesResult.size());
    assertEquals(4, valuesResult.size());
  }

  public void showTimeseries(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show timeseries\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Alias");
            add("Database");
            add("DataType");
            add("Encoding");
            add("Compression");
            add("Tags");
            add("Attributes");
            add("Deadband");
            add("DeadbandParameters");
            add("ViewType");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s3");
            add("root.sg25.s4");
            add("root.sg25.s5");
            add("root.sg25.s6");
            add("root.sg25.s7");
            add("root.sg25.s8");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
    Assert.assertEquals(values3, valuesResult.get(2));
  }

  public void showLastTimeseries(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"SHOW LATEST TIMESERIES\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Alias");
            add("Database");
            add("DataType");
            add("Encoding");
            add("Compression");
            add("Tags");
            add("Attributes");
            add("Deadband");
            add("DeadbandParameters");
            add("ViewType");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s3");
            add("root.sg25.s4");
            add("root.sg25.s5");
            add("root.sg25.s7");
            add("root.sg25.s8");
            add("root.sg25.s6");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
    Assert.assertEquals(values3, valuesResult.get(2));
  }

  public void countTimeseries(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"COUNT TIMESERIES root.** GROUP BY LEVEL=1\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Column");
            add("count(timeseries)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(6);
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
  }

  public void countNodes(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"count nodes root.** level=2\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("count(nodes)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add(6);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showDevices(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show devices\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add("IsAligned");
            add("Template");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Boolean> values2 =
        new ArrayList<Boolean>() {
          {
            add(false);
          }
        };
    List<String> values3 =
        new ArrayList<String>() {
          {
            add("null");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showDevicesWithStroage(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show devices with database\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add("Database");
            add("IsAligned");
            add("Template");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("false");
          }
        };
    List<Object> values4 =
        new ArrayList<Object>() {
          {
            add(null);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
    Assert.assertEquals(values3, valuesResult.get(2));
    Assert.assertEquals(values4, valuesResult.get(3));
  }

  public void listUser(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"list user\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("columnNames");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add(ColumnHeaderConstant.USER);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void selectCount(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"select count(s3) from root.** group by level = 1\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("expressions");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("count(root.sg25.s3)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add(2);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void selectLast(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"select last s4 from root.sg25\"}";
    Map map = queryMetaData(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("expressions");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    List<Long> timestampsResult = (List<Long>) map.get("timestamps");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Value");
            add("DataType");
          }
        };
    List<Long> timestamps =
        new ArrayList<Long>() {
          {
            add(1635232153960l);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s4");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("2");
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("INT32");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(timestamps, timestampsResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2.get(0), valuesResult.get(1).get(0));
    Assert.assertEquals(values3, valuesResult.get(2));
  }

  public void queryV2(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/query");
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

  public void queryGroupByLevelV2(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/query");
      String sql =
          "{\"sql\":\"select count(s4) from root.sg25 group by([1635232143960,1635232153960),1s),level=1\"}";
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

  public void queryRowLimitV2(CloseableHttpClient httpClient) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/query");
      String sql = "{\"sql\":\"select *,s4+1,s4+1 from root.sg25\",\"row_limit\":1}";
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      assertTrue(message.contains("row size exceeded the given max row size"));
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

  public Map queryMetaDataV2(CloseableHttpClient httpClient, String sql) {
    CloseableHttpResponse response = null;
    try {
      HttpPost httpPost = getHttpPost("http://127.0.0.1:" + port + "/rest/v2/query");
      httpPost.setEntity(new StringEntity(sql, Charset.defaultCharset()));
      response = httpClient.execute(httpPost);
      HttpEntity responseEntity = response.getEntity();
      String message = EntityUtils.toString(responseEntity, "utf-8");
      ObjectMapper mapper = new ObjectMapper();
      Map map = mapper.readValue(message, Map.class);
      return map;

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
    return null;
  }

  public void queryShowChildPathsV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show child paths root\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("ChildPaths");
            add("NodeTypes");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void queryShowNodesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show child nodes root\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("ChildNodes");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showAllTTLV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show all ttl\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.**");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("INF");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
  }

  public void showStorageGroupV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"SHOW DATABASES root.*\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Database");
            add("SchemaReplicationFactor");
            add("DataReplicationFactor");
            add("TimePartitionOrigin");
            add("TimePartitionInterval");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showFunctionsV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show functions\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    assertEquals(4, columnNamesResult.size());
    assertEquals(4, valuesResult.size());
  }

  public void showTimeseriesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show timeseries\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Alias");
            add("Database");
            add("DataType");
            add("Encoding");
            add("Compression");
            add("Tags");
            add("Attributes");
            add("Deadband");
            add("DeadbandParameters");
            add("ViewType");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s3");
            add("root.sg25.s4");
            add("root.sg25.s5");
            add("root.sg25.s6");
            add("root.sg25.s7");
            add("root.sg25.s8");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    //    Assert.assertEquals(values1, valuesResult.get(0));
    //    Assert.assertEquals(values2, valuesResult.get(1));
    //    Assert.assertEquals(values3, valuesResult.get(2));
  }

  public void showLastTimeseriesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"SHOW LATEST TIMESERIES\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Alias");
            add("Database");
            add("DataType");
            add("Encoding");
            add("Compression");
            add("Tags");
            add("Attributes");
            add("Deadband");
            add("DeadbandParameters");
            add("ViewType");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s3");
            add("root.sg25.s4");
            add("root.sg25.s5");
            add("root.sg25.s7");
            add("root.sg25.s8");
            add("root.sg25.s6");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
            add(null);
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
            add("root.sg25");
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
    Assert.assertEquals(values3, valuesResult.get(2));
  }

  public void countTimeseriesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"COUNT TIMESERIES root.** GROUP BY LEVEL=1\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Column");
            add("count(timeseries)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add(6);
          }
        };

    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
  }

  public void countNodesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"count nodes root.** level=2\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("count(nodes)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add(6);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void showDevicesV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show devices\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add("IsAligned");
            add("Template");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Boolean> values2 =
        new ArrayList<Boolean>() {
          {
            add(false);
          }
        };
    List<String> values3 =
        new ArrayList<String>() {
          {
            add("null");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    // Assert.assertEquals(values2, valuesResult.get(1));
  }

  public void showDevicesWithStroageV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"show devices with database\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Device");
            add("Database");
            add("IsAligned");
            add("Template");
            add(COLUMN_TTL);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("root.sg25");
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("false");
          }
        };
    List<Object> values4 =
        new ArrayList<Object>() {
          {
            add(null);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2, valuesResult.get(1));
    Assert.assertEquals(values3, valuesResult.get(2));
    Assert.assertEquals(values4, valuesResult.get(3));
  }

  public void listUserV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"list user\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("column_names");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add(ColumnHeaderConstant.USER);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void selectCountV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"select count(s3) from root.** group by level = 1\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("expressions");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("count(root.sg25.s3)");
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add(2);
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(values1, valuesResult.get(0));
  }

  public void selectLastV2(CloseableHttpClient httpClient) {
    String sql = "{\"sql\":\"select last s4 from root.sg25\"}";
    Map map = queryMetaDataV2(httpClient, sql);
    List<String> columnNamesResult = (List<String>) map.get("expressions");
    List<List<Object>> valuesResult = (List<List<Object>>) map.get("values");
    List<Long> timestampsResult = (List<Long>) map.get("timestamps");
    Assert.assertTrue(map.size() > 0);
    List<Object> columnNames =
        new ArrayList<Object>() {
          {
            add("Timeseries");
            add("Value");
            add("DataType");
          }
        };
    List<Long> timestamps =
        new ArrayList<Long>() {
          {
            add(1635232153960l);
          }
        };
    List<Object> values1 =
        new ArrayList<Object>() {
          {
            add("root.sg25.s4");
          }
        };
    List<Object> values2 =
        new ArrayList<Object>() {
          {
            add("2");
          }
        };
    List<Object> values3 =
        new ArrayList<Object>() {
          {
            add("INT32");
          }
        };
    Assert.assertEquals(columnNames, columnNamesResult);
    Assert.assertEquals(timestamps, timestampsResult);
    Assert.assertEquals(values1, valuesResult.get(0));
    Assert.assertEquals(values2.get(0), valuesResult.get(1).get(0));
    Assert.assertEquals(values3, valuesResult.get(2));
  }
}
