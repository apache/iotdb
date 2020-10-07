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
    JsonArray jsonArray = postStorageGroupsJsonExample();
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
    JsonArray jsonArray = deleteStorageGroupsJsonExample();
    Assert.assertEquals("[root.test]", mmanager.getAllStorageGroupPaths().toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    router.route(HttpMethod.POST, HttpConstant.ROUTING_STORAGE_GROUPS_DELETE, jsonArray);
    Assert.assertEquals(0, mmanager.getAllStorageGroupPaths().size());
  }

  @Test
  public void createTimeSeriesByRouter() throws Exception{
    JsonArray jsonArray = createTimeSeriesJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_TIME_SERIES, jsonArray).toString());
    List<PartialPath> paths = mmanager.getAllTimeseriesPathWithAlias(new PartialPath("root.sg.*"));
    Assert.assertEquals("root.sg.d1.s1" ,paths.get(0).getFullPath());
    Assert.assertEquals("root.sg.d1.s2", paths.get(1).getFullPath());
  }

  @Test
  public void deleteTimeSeriesByRouter() throws Exception{
    prepareData();
    JsonArray timeSeries = deleteTimeSeriesJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_TIME_SERIES_DELETE, timeSeries).toString());
    checkDataAfterDeletingTimeSeries();
  }

  @Test
  public void getTimeSeriesByRouter() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    buildMetaDataForGetTimeSeries();
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("root.laptop.*");
    Assert.assertEquals("[[\"root.laptop.d1.s1\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"],[\"root.laptop.d1.1_2\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"],[\"root.laptop.d1.\\\"1.2.3\\\"\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\",\"null\",\"null\"]]",
        router.route(HttpMethod.POST, HttpConstant.ROUTING_GET_TIME_SERIES, jsonArray).toString());
  }

  @Test
  public void insertByRouter() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, LOGIN_URI, null).toString());
    JsonArray inserts = insertJsonExample(1);
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, HttpConstant.ROUTING_INSERT, inserts).toString());
    checkDataAfterInserting(1);
  }

  @Test
  public void queryByRouter() throws Exception{
    prepareData();
    JsonObject query = queryJsonExample();
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, LOGIN_URI, null).toString());
    Assert.assertEquals("[{\"timestamp\":\"timestamp\",\"values\":[2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20]}," +
                    "{\"timeSeries\":\"root.test.m0\",\"values\":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0]}," +
                    "{\"timeSeries\":\"root.test.m9\",\"values\":[1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0]}]"
        , router.route(HttpMethod.POST, HttpConstant.ROUTING_QUERY, query).toString());
  }

}
