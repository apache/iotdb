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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
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
  private Client client = ClientBuilder.newClient();

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
  public void after() throws Exception{
    EnvironmentUtils.cleanEnv();
  }


  private void login() {
    Response response = client.target(USER_LOGIN).request(MediaType.APPLICATION_JSON).get();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
  }

  @Test
  public void setStorageGroupsByHttp() {
    login();
    JSONArray jsonArray = postStorageGroupsJsonExample();
    Response response1 = client.target(STORAGE_GROUPS_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(jsonArray, MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response1.readEntity(String.class));
    Assert.assertEquals("[root.ln, root.sg]",mmanager.getAllStorageGroupPaths().toString());
  }

  @Test
  public void getStorageGroupsByHttp() throws Exception {
    login();
    mmanager.setStorageGroup(new PartialPath("root.ln"));
    mmanager.setStorageGroup(new PartialPath("root.sg"));
    Response response = client.target(STORAGE_GROUPS_URI)
        .request(MediaType.APPLICATION_JSON).get();
    Assert.assertEquals("[{\"storage group\":\"root.ln\"},{\"storage group\":\"root.sg\"}]", response.readEntity(String.class));
  }

  @Test
  public void deleteStorageGroupsByHttp() throws Exception{
    login();
    prepareData();
    JSONArray jsonArray = deleteStorageGroupsJsonExample();
    Response response = client.target(STORAGE_GROUPS_DELETE_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(jsonArray, MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    Assert.assertEquals(0, mmanager.getAllStorageGroupPaths().size());
  }

  @Test
  public void createTimeSeriesByHttp() throws Exception {
    login();
    JSONArray jsonArray = createTimeSeriesJsonExample();
    Response response = client.target(TIME_SERIES_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(jsonArray, MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    List<PartialPath> paths = mmanager.getAllTimeseriesPathWithAlias(new PartialPath("root.sg.*"));
    Assert.assertEquals("root.sg.d1.temperature" ,paths.get(0).getFullPathWithAlias());
    Assert.assertEquals("root.sg.d1.s2", paths.get(1).getFullPath());
  }

  @Test
  public void getTimeSeriesByHttp() throws Exception {
    login();
    buildMetaDataForGetTimeSeries();
    JSONArray jsonArray = new JSONArray();
    jsonArray.add("root.laptop.*");
    Response response = client.target(GET_TIME_SERIES_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(jsonArray, MediaType.APPLICATION_JSON));
    Assert.assertEquals("[[\"root.laptop.d1.s1\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
        + ",[\"root.laptop.d1.1_2\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
        + ",[\"root.laptop.d1.\\\"1.2.3\\\"\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]]", response.readEntity(String.class));
  }

  @Test
  public void deleteTimeSeriesByHttp() throws Exception {
    login();
    prepareData();
    JSONArray timeSeries = deleteTimeSeriesJsonExample();
    Response response = client.target(TIME_SERIES_DELETE_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(timeSeries, MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    checkDataAfterDeletingTimeSeries();
  }

  @Test
  public void insertByHttp() throws Exception {
    login();
    JSONArray inserts = insertJsonExample();
    Response response = client.target(INSERT_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(inserts, MediaType.APPLICATION_JSON));
    Assert.assertEquals(SUCCESSFUL_RESPONSE, response.readEntity(String.class));
    checkDataAfterInserting();
  }

  @Test
  public void queryByHttp() throws Exception {
    login();
    prepareData();
    JSONObject query = queryJsonExample();
    Response response = client.target(QUERY_URI)
        .request(MediaType.APPLICATION_JSON).post(Entity.entity(query, MediaType.APPLICATION_JSON));
    Assert.assertEquals("[{\"timestamps\":1,\"value\":1.0},{\"timestamps\":2,\"value\":2.0},{\"timestamps\":3,\"value\":3.0},{\"timestamps\":4,\"value\":4.0},{\"timestamps\":5,\"value\":5.0}]"
        , response.readEntity(String.class));
  }

}
