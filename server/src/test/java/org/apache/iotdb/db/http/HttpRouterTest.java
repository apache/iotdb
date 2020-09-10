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
import io.netty.handler.codec.http.HttpMethod;
import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.router.Router;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpRouterTest extends HttpPrepData{

  @Before
  public void before() {
    EnvironmentUtils.envSetUp();
    router = new Router();
  }

  @After
  public void after() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void setStorageGroupsByRouter() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    JSONArray jsonArray = postStorageGroupsJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_STORAGE_GROUPS, jsonArray).toString());
    Assert.assertEquals("[root.ln, root.sg]",mmanager.getAllStorageGroupPaths().toString());
  }

  @Test
  public void getStorageGroupsByRouter() throws Exception {
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    mmanager.setStorageGroup(new PartialPath("root.ln"));
    mmanager.setStorageGroup(new PartialPath("root.sg"));
    Assert.assertEquals("[{\"storage group\":\"root.ln\"},{\"storage group\":\"root.sg\"}]", router.route(HttpMethod.GET, HttpConstant.ROUTING_STORAGE_GROUPS, null).toString());
  }

  @Test
  public void deleteStorageGroupsByRouter()
      throws Exception {
    prepareData();
    JSONArray jsonArray = deleteStorageGroupsJsonExample();
    Assert.assertEquals(1, mmanager.getAllStorageGroupPaths().size());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    router.route(HttpMethod.POST, HttpConstant.ROUTING_STORAGE_GROUPS_DELETE, jsonArray);
    Assert.assertEquals(0, mmanager.getAllStorageGroupPaths().size());
  }

  @Test
  public void createTimeSeriesByRouter() throws Exception{
    JSONArray jsonArray = createTimeSeriesJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_TIME_SERIES, jsonArray).toString());
    List<PartialPath> paths = mmanager.getAllTimeseriesPathWithAlias(new PartialPath("root.sg.*"));
    Assert.assertEquals("root.sg.d1.temperature" ,paths.get(0).getFullPathWithAlias());
    Assert.assertEquals("root.sg.d1.s2", paths.get(1).getFullPath());
  }

  @Test
  public void deleteTimeSeriesByRouter() throws Exception{
    prepareData();
    JSONArray timeSeries = deleteTimeSeriesJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_TIME_SERIES_DELETE, timeSeries).toString());
    checkDataAfterDeletingTimeSeries();
  }

  @Test
  public void getTimeSeriesByRouter() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    buildMetaDataForGetTimeSeries();
    JSONArray jsonArray = new JSONArray();
    jsonArray.add("root.laptop.*");
    Assert.assertEquals("[[\"root.laptop.d1.s1\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
            + ",[\"root.laptop.d1.1_2\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
            + ",[\"root.laptop.d1.\\\"1.2.3\\\"\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]]",
        router.route(HttpMethod.POST, HttpConstant.ROUTING_GET_TIME_SERIES, jsonArray).toString());
  }

  @Test
  public void insertByRouter() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    JSONArray inserts = insertJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_INSERT, inserts).toString());
    checkDataAfterInserting();
  }

  @Test
  public void queryByRouter() throws Exception{
    prepareData();
    JSONObject query = queryJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    Assert.assertEquals("[{\"timestamps\":1,\"value\":1.0},{\"timestamps\":2,\"value\":2.0},{\"timestamps\":3,\"value\":3.0},{\"timestamps\":4,\"value\":4.0},{\"timestamps\":5,\"value\":5.0}]"
        , router.route(HttpMethod.POST, HttpConstant.ROUTING_QUERY, query).toString());
  }

}
