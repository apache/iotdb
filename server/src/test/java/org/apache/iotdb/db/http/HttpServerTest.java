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
package org.apache.iotdb.db.http;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpServerTest extends HttpPrepData {

  private final Client client = ClientBuilder.newClient();

  private static final String QUERY_URI
      = "http://localhost:8282/query";

  private static final String STORAGE_GROUPS_URI
      = "http://localhost:8282/storageGroups";

  private static final String STORAGE_GROUPS_DELETE_URI
      = "http://localhost:8282/storageGroups/delete";


  private static final String TIME_SERIES_URI
      = "http://localhost:8282/timeSeries";

  private static final String TIME_SERIES_DELETE_URI
      = "http://localhost:8282/timeSeries/delete";

  private static final String INSERT_URI
      = "http://localhost:8282/insert";

  private static final String GET_TIME_SERIES_URI
      = "http://localhost:8282/getTimeSeries";

  private static final String USER_LOGIN
      = "http://localhost:8282/user/login?username=root&password=root";


  @Before
  public void before() {
    EnvironmentUtils.setIsEnableHttpService(true);
    EnvironmentUtils.envSetUp();
  }

  @After
  public void after() throws Exception {
    EnvironmentUtils.cleanEnv();
  }


  private void login() {
    Response response = client.target(USER_LOGIN).request(MediaType.APPLICATION_JSON).get();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
  }

  @Test
  public void setStorageGroupsByHttp() {
    login();
    JsonArray jsonArray = postStorageGroupsJsonExample();
    Response response1 = client.target(STORAGE_GROUPS_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(jsonArray.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response1.readEntity(String.class));
    Assert.assertEquals("[root.ln, root.sg]", mmanager.getAllStorageGroupPaths().toString());
  }

  @Test
  public void getStorageGroupsByHttp() throws Exception {
    login();
    mmanager.setStorageGroup(new PartialPath("root.ln"));
    mmanager.setStorageGroup(new PartialPath("root.sg"));
    Response response = client.target(STORAGE_GROUPS_URI)
        .request(MediaType.APPLICATION_JSON).get();
    Assert.assertEquals("[{\"storage group\":\"root.ln\"},{\"storage group\":\"root.sg\"}]",
        response.readEntity(String.class));
  }

  @Test
  public void deleteStorageGroupsByHttp() throws Exception {
    login();
    prepareData();
    JsonArray jsonArray = deleteStorageGroupsJsonExample();
    Response response = client.target(STORAGE_GROUPS_DELETE_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(jsonArray.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    Assert.assertEquals(0, mmanager.getAllStorageGroupPaths().size());
  }

  @Test
  public void createTimeSeriesByHttp() throws Exception {
    login();
    JsonArray jsonArray = createTimeSeriesJsonExample();
    Response response = client.target(TIME_SERIES_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(jsonArray.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    List<PartialPath> paths = mmanager.getAllTimeseriesPathWithAlias(new PartialPath("root.sg.*"));
    Assert.assertEquals("root.sg.d1.s1", paths.get(0).getFullPath());
    Assert.assertEquals("root.sg.d1.s2", paths.get(1).getFullPath());
  }

  @Test
  public void getTimeSeriesByHttp() throws Exception {
    login();
    buildMetaDataForGetTimeSeries();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("root.laptop.*");
    Response response = client.target(GET_TIME_SERIES_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(jsonArray.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals("[" +
            "[\"root.laptop.d1.1_2\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"],"
            +
            "[\"root.laptop.d1.\\\"1.2.3\\\"\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"],"
            +
            "[\"root.laptop.d1.s1\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"]]",
        response.readEntity(String.class));
  }

  @Test
  public void deleteTimeSeriesByHttp() throws Exception {
    login();
    prepareData();
    JsonArray timeSeries = deleteTimeSeriesJsonExample();
    Response response = client.target(TIME_SERIES_DELETE_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(timeSeries.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    checkDataAfterDeletingTimeSeries();
  }

  @Test
  public void insertByHttp() throws Exception {
    login();
    JsonArray inserts = insertJsonExample(1);
    Response response = client.target(INSERT_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(inserts.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    checkDataAfterInserting(1);
  }

  @Test
  public void queryByHttp() throws Exception {
    login();
    prepareData();
    JsonObject query = queryJsonExample();
    Response response = client.target(QUERY_URI)
        .request(MediaType.APPLICATION_JSON)
        .post(Entity.entity(query.toString(), MediaType.APPLICATION_JSON));
    Assert.assertEquals("[{\"name\":\"root.test.m0\"," +
            "\"series\":[{\"Time\":2,\"Value\":1}," +
            "{\"Time\":3,\"Value\":1}," +
            "{\"Time\":4,\"Value\":1}," +
            "{\"Time\":5,\"Value\":1}," +
            "{\"Time\":6,\"Value\":1}," +
            "{\"Time\":7,\"Value\":1}," +
            "{\"Time\":8,\"Value\":1}," +
            "{\"Time\":9,\"Value\":1}," +
            "{\"Time\":10,\"Value\":1}," +
            "{\"Time\":11,\"Value\":1}," +
            "{\"Time\":12,\"Value\":1}," +
            "{\"Time\":13,\"Value\":1}," +
            "{\"Time\":14,\"Value\":1}," +
            "{\"Time\":15,\"Value\":1}," +
            "{\"Time\":16,\"Value\":1}," +
            "{\"Time\":17,\"Value\":1}," +
            "{\"Time\":18,\"Value\":1}," +
            "{\"Time\":19,\"Value\":1}," +
            "{\"Time\":20,\"Value\":0}]}," +
            "{\"name\":\"root.test.m9\"," +
            "\"series\":[{\"Time\":2,\"Value\":1}," +
            "{\"Time\":3,\"Value\":1}," +
            "{\"Time\":4,\"Value\":1}," +
            "{\"Time\":5,\"Value\":1}," +
            "{\"Time\":6,\"Value\":1}," +
            "{\"Time\":7,\"Value\":1}," +
            "{\"Time\":8,\"Value\":1}," +
            "{\"Time\":9,\"Value\":1}," +
            "{\"Time\":10,\"Value\":1}," +
            "{\"Time\":11,\"Value\":1}," +
            "{\"Time\":12,\"Value\":1}," +
            "{\"Time\":13,\"Value\":1}," +
            "{\"Time\":14,\"Value\":1}," +
            "{\"Time\":15,\"Value\":1}," +
            "{\"Time\":16,\"Value\":1}," +
            "{\"Time\":17,\"Value\":1}," +
            "{\"Time\":18,\"Value\":1}," +
            "{\"Time\":19,\"Value\":1}," +
            "{\"Time\":20,\"Value\":0}]}]"
        , response.readEntity(String.class));
  }

  @Test
  public void multiThreadInsertTest() {
    login();
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      int finalI = i;
      service.submit(() -> {
        JsonArray inserts = insertJsonExample(finalI);
        Response response = client.target(INSERT_URI)
            .request(MediaType.APPLICATION_JSON)
            .post(Entity.entity(inserts.toString(), MediaType.APPLICATION_JSON));
        Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
        try {
          checkDataAfterInserting(finalI);
        } catch (Exception e) {
          e.printStackTrace();
        }
      });
    }
  }

}
