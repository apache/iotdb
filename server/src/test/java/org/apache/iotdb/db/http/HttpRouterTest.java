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

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import io.netty.handler.codec.http.HttpMethod;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.router.Router;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.MNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HttpRouterTest {

  private String processorName = "root.test";
  private static String[] measurements = new String[10];
  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  private MNode deviceMNode;
  private TSDataType dataType = TSDataType.DOUBLE;
  private TSEncoding encoding = TSEncoding.PLAIN;
  private static MManager mmanager = null;
  private static Router router;
  private static final String SUCCESSFUL_RESPONSE = "{\"result\":\"successful operation\"}";
  private QueryRouter queryRouter = new QueryRouter();

  @Before
  public void before() {
    EnvironmentUtils.envSetUp();
    mmanager = IoTDB.metaManager;
    router = new Router();
  }

  @After
  public void after() throws IOException, StorageEngineException {
    EnvironmentUtils.cleanEnv();
  }


  @Test
  public void setStorageGroupsByHttp() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    JSONArray jsonArray = new JSONArray();
    jsonArray.add("root.ln");
    jsonArray.add("root.sg");
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, "/storageGroups", jsonArray).toString());
    Assert.assertEquals("[root.ln, root.sg]",mmanager.getAllStorageGroupPaths().toString());
  }

  @Test
  public void getStorageGroupsByHttp() throws Exception {
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    mmanager.setStorageGroup(new PartialPath("root.ln"));
    mmanager.setStorageGroup(new PartialPath("root.sg"));
    Assert.assertEquals("[{\"storage group\":\"root.ln\"},{\"storage group\":\"root.sg\"}]", router.route(HttpMethod.GET, "/storageGroups", null).toString());
  }

  @Test
  public void deleteStorageGroupsByHttp()
      throws Exception {
    prepareData();
    JSONArray jsonArray = new JSONArray();
    jsonArray.add(processorName);
    Assert.assertEquals(1, mmanager.getAllStorageGroupPaths().size());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    router.route(HttpMethod.DELETE, "/storageGroups", jsonArray);
    Assert.assertEquals(0, mmanager.getAllStorageGroupPaths().size());
  }

  @Test
  public void createTimeSeriesByHttp() throws Exception{
    JSONArray jsonArray = new JSONArray();
    JSONObject timeSeries1 = new JSONObject();

    timeSeries1.put("timeSeries", "root.sg.d1.s1");
    timeSeries1.put("alias", "temperature");
    timeSeries1.put("dataType", "DOUBLE");
    timeSeries1.put("encoding", "PLAIN");

    JSONArray props = new JSONArray();
    JSONObject keyValue = new JSONObject();
    keyValue.put("key", "hello");
    keyValue.put("value", "world");
    props.add(keyValue);

    // same props
    timeSeries1.put("properties", props);
    timeSeries1.put("tags", props);
    timeSeries1.put("attributes", props);

    JSONObject timeSeries2 = new JSONObject();
    timeSeries2.put("timeSeries", "root.sg.d1.s2");
    timeSeries2.put("dataType", "DOUBLE");
    timeSeries2.put("encoding", "PLAIN");

    jsonArray.add(timeSeries1);
    jsonArray.add(timeSeries2);
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, "/timeSeries", jsonArray).toString());
    List<PartialPath> paths = mmanager.getAllTimeseriesPathWithAlias(new PartialPath("root.sg.*"));
    Assert.assertEquals("root.sg.d1.temperature" ,paths.get(0).getFullPathWithAlias());
    Assert.assertEquals("root.sg.d1.s2", paths.get(1).getFullPath());
  }

  @Test
  public void deleteTimeSeriesByHttp() throws Exception{
    prepareData();
    JSONArray timeSeries = new JSONArray();
    for (String measurement : measurements) {
      timeSeries.add(processorName + TsFileConstant.PATH_SEPARATOR + measurement);
    }
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.DELETE, "/timeSeries", timeSeries).toString());
    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[3]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[4]));
    pathList.add(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[5]));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(dataType);
    dataTypes.add(dataType);
    dataTypes.add(dataType);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(pathList);
    QueryDataSet dataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);
    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    assertEquals(0, count);
  }

  @Test
  public void getTimeSeriesByHttp() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    mmanager.setStorageGroup(new PartialPath("root.laptop"));
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    mmanager.createTimeseries(new PartialPath("root.laptop.d1.s1"), TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    mmanager.createTimeseries(new PartialPath("root.laptop.d1.1_2"), TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), new HashMap<>());
    mmanager.createTimeseries(new PartialPath("root.laptop.d1.\"1.2.3\""), TSDataType.INT32, TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), new HashMap<>());

    JSONArray jsonArray = new JSONArray();
    jsonArray.add("root.laptop.*");
    Assert.assertEquals("[[\"root.laptop.d1.s1\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
            + ",[\"root.laptop.d1.1_2\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]"
            + ",[\"root.laptop.d1.\\\"1.2.3\\\"\",\"null\",\"root.laptop\",\"INT32\",\"RLE\",\"SNAPPY\"]]",
        router.route(HttpMethod.GET, "/timeSeries", jsonArray).toString());
  }

  @Test
  public void insertByHttp() throws Exception{
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    JSONArray inserts = new JSONArray();
    JSONObject insert = new JSONObject();
    insert.put("deviceId", "root.ln.wf01.wt01");
    JSONArray measurements = new JSONArray();
    measurements.add("temperature");
    measurements.add("status");
    measurements.add("hardware");
    insert.put("measurements", measurements);
    JSONArray timestamps = new JSONArray();
    JSONArray values = new JSONArray();
    for(int i = 0; i < 6; i++) {
      timestamps.add(i);
      JSONArray value = new JSONArray();
      value.add(String.valueOf(i));
      value.add("true");
      value.add(String.valueOf(i));
      values.add(value);
    }
    insert.put("timestamps", timestamps);
    insert.put("values", values);
    inserts.add(insert);
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.POST, "/insert", inserts).toString());

    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath("root.ln.wf01.wt01.temperature"));
    pathList.add(new PartialPath("root.ln.wf01.wt01.status"));
    pathList.add(new PartialPath("root.ln.wf01.wt01.hardware"));
    List<TSDataType> dataTypes = new ArrayList<>();
    dataTypes.add(TSDataType.FLOAT);
    dataTypes.add(TSDataType.BOOLEAN);
    dataTypes.add(TSDataType.FLOAT);

    RawDataQueryPlan queryPlan = new RawDataQueryPlan();
    queryPlan.setDeduplicatedDataTypes(dataTypes);
    queryPlan.setDeduplicatedPaths(pathList);
    QueryDataSet dataSet = queryRouter.rawDataQuery(queryPlan, TEST_QUERY_CONTEXT);
    int count = 0;
    while (dataSet.hasNext()) {
      dataSet.next();
      count++;
    }
    Assert.assertEquals(6, count);
  }

  @Test
  public void queryByHttp() throws Exception{
    prepareData();
    JSONObject query = new JSONObject();
    query.put(HttpConstant.SELECT, measurements[1]);
    query.put(HttpConstant.FROM, processorName);
    JSONObject range = new JSONObject();
    range.put(HttpConstant.START, "0");
    range.put(HttpConstant.END, "6");
    query.put(HttpConstant.RANGE, range);
    Assert.assertEquals(SUCCESSFUL_RESPONSE, router.route(HttpMethod.GET, "/user/login?username=root&password=root", null).toString());
    Assert.assertEquals("[{\"timestamps\":1,\"value\":1.0},{\"timestamps\":2,\"value\":2.0},{\"timestamps\":3,\"value\":3.0},{\"timestamps\":4,\"value\":4.0},{\"timestamps\":5,\"value\":5.0}]"
        , router.route(HttpMethod.POST, "/query", query).toString());
  }

  private void prepareData() throws MetadataException, StorageEngineException {
    String test = "test";
    deviceMNode = new MNode(null, test);
    IoTDB.metaManager.setStorageGroup(new PartialPath(processorName));
    for (int i = 0; i < 10; i++) {
      deviceMNode.addChild(measurements[i], new MeasurementMNode(null, null, null, null));
      IoTDB.metaManager.createTimeseries(new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[i]), dataType,
          encoding, TSFileDescriptor.getInstance().getConfig().getCompressor(), Collections.emptyMap());
    }
    for (int i = 1; i <= 100; i++) {
      TSRecord record = new TSRecord(i, processorName);
      for (int j = 0; j < 10; j++) {
        record.addTuple(new DoubleDataPoint(measurements[j], i * 1.0));
      }
      insertToStorageEngine(record);
    }
  }

  private void insertToStorageEngine(TSRecord record)
      throws StorageEngineException, IllegalPathException {
    InsertRowPlan insertRowPlan = new InsertRowPlan(record);
    insertRowPlan.setDeviceMNode(deviceMNode);
    StorageEngine.getInstance().insert(insertRowPlan);
  }

}
