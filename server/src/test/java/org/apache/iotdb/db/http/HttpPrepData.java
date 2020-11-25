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
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.http.constant.HttpConstant;
import org.apache.iotdb.db.http.router.HttpRouter;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.RawDataQueryPlan;
import org.apache.iotdb.db.query.executor.QueryRouter;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DoubleDataPoint;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.apache.iotdb.db.utils.EnvironmentUtils.TEST_QUERY_CONTEXT;
import static org.junit.Assert.assertEquals;

// prepare data for http test

public abstract class HttpPrepData {

  private final String processorName = "root.test";
  protected static String[] measurements = new String[10];

  static {
    for (int i = 0; i < 10; i++) {
      measurements[i] = "m" + i;
    }
  }

  protected TSDataType dataType = TSDataType.DOUBLE;
  protected TSEncoding encoding = TSEncoding.PLAIN;
  protected static MManager mmanager = IoTDB.metaManager;
  static HttpRouter router;
  static final String SUCCESSFUL_RESPONSE = "{\"result\":\"successful operation\"}";
  private final QueryRouter queryRouter = new QueryRouter();
  static final String LOGIN_URI = "/user/login?username=root&password=root";
  static final String GET_CHILD_PATH_URL = "/getChildPaths?path=root.test";

  protected void prepareData() throws MetadataException, StorageEngineException {
    IoTDB.metaManager.setStorageGroup(new PartialPath(processorName));
    for (int i = 0; i < 10; i++) {
      IoTDB.metaManager.createTimeseries(
          new PartialPath(processorName + TsFileConstant.PATH_SEPARATOR + measurements[i]),
          dataType,
          encoding, TSFileDescriptor.getInstance().getConfig().getCompressor(),
          Collections.emptyMap());
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
    StorageEngine.getInstance().insert(insertRowPlan);
  }

  JsonArray postStorageGroupsJsonExample() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add("root.ln");
    jsonArray.add("root.sg");
    return jsonArray;
  }

  JsonArray deleteStorageGroupsJsonExample() {
    JsonArray jsonArray = new JsonArray();
    jsonArray.add(processorName);
    return jsonArray;
  }

  JsonArray createTimeSeriesJsonExample() {
    JsonArray jsonArray = new JsonArray();
    JsonObject timeSeries1 = new JsonObject();

    timeSeries1.addProperty("timeSeries", "root.sg.d1.s1");
    timeSeries1.addProperty("alias", "temperature");
    timeSeries1.addProperty("dataType", "DOUBLE");
    timeSeries1.addProperty("encoding", "PLAIN");

    JsonArray props = new JsonArray();
    JsonObject keyValue = new JsonObject();
    keyValue.addProperty("key", "hello");
    keyValue.addProperty("value", "world");
    props.add(keyValue);

    // same props
    timeSeries1.add("properties", props);
    timeSeries1.add("tags", props);
    timeSeries1.add("attributes", props);

    JsonObject timeSeries2 = new JsonObject();
    timeSeries2.addProperty("timeSeries", "root.sg.d1.s2");
    timeSeries2.addProperty("dataType", "DOUBLE");
    timeSeries2.addProperty("encoding", "PLAIN");

    jsonArray.add(timeSeries1);
    jsonArray.add(timeSeries2);
    return jsonArray;
  }

  JsonArray deleteTimeSeriesJsonExample() {
    JsonArray timeSeries = new JsonArray();
    for (String measurement : measurements) {
      timeSeries.add(processorName + TsFileConstant.PATH_SEPARATOR + measurement);
    }
    return timeSeries;
  }

  void checkDataAfterDeletingTimeSeries() throws Exception {
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

  void buildMetaDataForGetTimeSeries() throws Exception {
    mmanager.setStorageGroup(new PartialPath("root.laptop"));
    CompressionType compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
    mmanager.createTimeseries(new PartialPath("root.laptop.d1.s1"), TSDataType.valueOf("INT32"),
        TSEncoding.valueOf("RLE"), compressionType, Collections.emptyMap());
    mmanager
        .createTimeseries(new PartialPath("root.laptop.d1.1_2"), TSDataType.INT32, TSEncoding.RLE,
            TSFileDescriptor.getInstance().getConfig().getCompressor(), new HashMap<>());
    mmanager.createTimeseries(new PartialPath("root.laptop.d1.\"1.2.3\""), TSDataType.INT32,
        TSEncoding.RLE,
        TSFileDescriptor.getInstance().getConfig().getCompressor(), new HashMap<>());
  }

  JsonArray insertJsonExample(int i) {
    JsonArray inserts = new JsonArray();
    for (int j = 0; j < 6; j++) {
      JsonObject row = new JsonObject();
      row.addProperty(HttpConstant.IS_NEED_INFER_TYPE, false);
      row.addProperty(HttpConstant.DEVICE_ID, "root.ln.wf01.wt0" + i);
      JsonArray measurements = new JsonArray();
      measurements.add("temperature");
      measurements.add("status");
      measurements.add("hardware");
      row.add(HttpConstant.MEASUREMENTS, measurements);
      row.addProperty(HttpConstant.TIMESTAMP, j);
      JsonArray values = new JsonArray();
      values.add(j);
      values.add(true);
      values.add(j);
      row.add(HttpConstant.VALUES, values);
      inserts.add(row);
    }
    return inserts;
  }

  void checkDataAfterInserting(int i) throws Exception {
    List<PartialPath> pathList = new ArrayList<>();
    pathList.add(new PartialPath("root.ln.wf01.wt0" + i + ".temperature"));
    pathList.add(new PartialPath("root.ln.wf01.wt0" + i + ".status"));
    pathList.add(new PartialPath("root.ln.wf01.wt0" + i + ".hardware"));
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

  JsonObject queryJsonExample() {
    JsonObject query = new JsonObject();

    //add timeSeries
    JsonArray timeSeries = new JsonArray();
    timeSeries.add(processorName + TsFileConstant.PATH_SEPARATOR + measurements[0]);
    timeSeries.add(processorName + TsFileConstant.PATH_SEPARATOR + measurements[9]);
    query.add(HttpConstant.TIME_SERIES, timeSeries);

    //set isAggregated
    query.addProperty(HttpConstant.IS_AGGREGATED, true);

    // add functions
    JsonArray aggregations = new JsonArray();
    aggregations.add("COUNT");
    aggregations.add("COUNT");
    query.add(HttpConstant.AGGREGATIONS, aggregations);

    //set time filter
    query.addProperty(HttpConstant.FROM, 1L);
    query.addProperty(HttpConstant.TO, 20L);

    //Sets the number of partition
    query.addProperty(HttpConstant.isPoint, true);

    //set Group by
    JsonObject groupBy = new JsonObject();
    groupBy.addProperty(HttpConstant.SAMPLING_POINTS, 19);
    query.add(HttpConstant.GROUP_BY, groupBy);
    return query;
  }


}
