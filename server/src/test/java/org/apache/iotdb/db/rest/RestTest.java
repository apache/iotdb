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

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RestTest {

  private Client client = ClientBuilder.newClient();

  private static final String QUERY_URI
      = "http://localhost:8181/rest/query";

  private static final String SET_STORAGE_GROUP_URI
      = "http://localhost:8181/rest/setStorageGroup";

  private static final String CREATE_TIME_SERIES_URI
      = "http://localhost:8181/rest/createTimeSeries";

  private static final String INSERT_URI
      = "http://localhost:8181/rest/insert";

  private static final String SERVER_INFORMATION_URL
      = "http://localhost:8181/rest/server_information";

  private static final String VERSION_URL
      = "http://localhost:8181/rest/version";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.setEnableHttpService(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testSQL() throws InterruptedException {
    TimeUnit.SECONDS.sleep(5);
    // set storage group
    String file1 = RestTest.class.getClassLoader().getResource("setStorageGroup.json").getFile();
    String json1 = readToString(file1);
    String userAndPassword = "root:root";
    String encodedUserPassword = new String(Base64.getEncoder().encode(userAndPassword.getBytes()));
    Response response1 = client.target(SET_STORAGE_GROUP_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json1), MediaType.APPLICATION_JSON));
    String result1 = response1.readEntity(String.class);
    Assert.assertEquals("[\"root.ln.wf01.wt01:success\"]", result1);

    //create time series
    String file2 = RestTest.class.getClassLoader().getResource("createTimeSeries.json").getFile();
    String json2 = readToString(file2);
    Response response2 = client.target(CREATE_TIME_SERIES_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json2), MediaType.APPLICATION_JSON));
    String result2 = response2.readEntity(String.class);
    Assert.assertEquals("[\"root.ln.wf01.wt01.status:success\",\"root.ln.wf01.wt01.temperature:success\",\"root.ln.wf01.wt01.hardware:success\"]", result2);

    //insert
    String file3 = RestTest.class.getClassLoader().getResource("insert.json").getFile();
    String json3 = readToString(file3);
    Response response3 = client.target(INSERT_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json3), MediaType.APPLICATION_JSON));
    String result3 = response3.readEntity(String.class);
    Assert.assertEquals("[\"root.ln.wf01.wt01:success\",\"root.ln.wf01.wt01:success\",\"root.ln.wf01.wt01:success\",\"root.ln.wf01.wt01:success\",\"root.ln.wf01.wt01:success\"]", result3);

    //query
    String file4 = RestTest.class.getClassLoader().getResource("query.json").getFile();
    String json4 = readToString(file4);
    Response response4 = client.target(QUERY_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json4), MediaType.APPLICATION_JSON));
    String result4 = response4.readEntity(String.class);
    Assert.assertEquals("[{\"datapoints\":[[1,\"1.1\"],[2,\"2.2\"],[3,\"3.3\"],[4,\"4.4\"],[5,\"5.5\"]],\"target\":\"root.ln.wf01.wt01.temperature\"}]", result4);

    Response response5 = client.target(VERSION_URL).request(MediaType.TEXT_PLAIN).get();
    String result = response5.readEntity(String.class);
    Assert.assertEquals(IoTDBConstant.VERSION, result);

    Response response6 = client.target(SERVER_INFORMATION_URL).request(MediaType.APPLICATION_JSON).get();
    String result6 = response6.readEntity(String.class);
    JSONObject serverInfo = (JSONObject) JSONObject.parse(result6);
    if((int) serverInfo.get("cpu_ratio") < 0 ||
        (int) serverInfo.get("cores") < 0 ||
        (int) serverInfo.get("total_memory") < 0 ||
        (int) serverInfo.get("port") < 0 ||
        Integer.parseInt((String) serverInfo.get("totalPhysical_memory")) < 0 ||
        (int) serverInfo.get("free_memory") < 0 ||
        Integer.parseInt((String) serverInfo.get("freePhysical_memory")) < 0 ||
        Integer.parseInt((String) serverInfo.get("usedPhysical_memory")) < 0 ||
        (int) serverInfo.get("max_memory") < 0) {
      Assert.fail();
    }
  }

  @Test
  public void testWrongType() throws InterruptedException {
    TimeUnit.SECONDS.sleep(5);
    String file = RestTest.class.getClassLoader().getResource("setStorageGroupTypeError.json").getFile();
    String json = readToString(file);
    String userAndPassword = "root:root";
    String encodedUserPassword = new String(Base64.getEncoder().encode(userAndPassword.getBytes()));
    Response response = client.target(SET_STORAGE_GROUP_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json), MediaType.APPLICATION_JSON));
    String result = response.readEntity(String.class);
    Assert.assertEquals("Type is wrong", result);
  }

  @Test
  public void testWrongSQL() throws InterruptedException {
    TimeUnit.SECONDS.sleep(5);
    String file = RestTest.class.getClassLoader().getResource("setStorageGroupTypeError.json").getFile();
    String json = readToString(file);
    String userAndPassword = "root:root";
    String encodedUserPassword = new String(Base64.getEncoder().encode(userAndPassword.getBytes()));
    Response response = client.target(SET_STORAGE_GROUP_URI)
        .request(MediaType.APPLICATION_JSON).header("Authorization", "Basic " + encodedUserPassword)
        .post(Entity.entity(JSONObject.parse(json), MediaType.APPLICATION_JSON));
    String result = response.readEntity(String.class);
    Assert.assertEquals("Type is wrong", result);
  }


  private static String readToString(String fileName) {
    String encoding = "UTF-8";
    File file = new File(fileName);
    long fileLength = file.length();
    byte[] fileContent = new byte[(int) fileLength];
    try {
      FileInputStream in = new FileInputStream(file);
      in.read(fileContent);
      in.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      return new String(fileContent, encoding);
    } catch (UnsupportedEncodingException e) {
      System.err.println("The OS does not support " + encoding);
      e.printStackTrace();
      return null;
    }
  }
}
