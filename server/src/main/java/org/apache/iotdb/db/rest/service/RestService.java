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
package org.apache.iotdb.db.rest.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.rest.model.TimeValue;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestService {

  protected Planner planner = new Planner();
  private RestParser restParser = new RestParser();
  private static final Logger logger = LoggerFactory.getLogger(RestService.class);
  private IPlanExecutor executor;

  private RestService() {
    try {
      executor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(String.valueOf(e));
    }
  }

  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private String username;

  private List<TimeValue> querySeries(String s, Pair<String, String> timeRange)
      throws QueryProcessException, AuthException, IOException, MetadataException, QueryFilterOptimizationException, SQLException, StorageEngineException {
    String from = timeRange.left;
    String to = timeRange.right;
    String suffixPath = s.substring(s.lastIndexOf('.') + 1);
    String prefixPath = s.substring(0, s.lastIndexOf('.'));
    String sql = "SELECT " + suffixPath + " FROM"
        + prefixPath + " WHERE time > " + from + " and time < " + to;
    logger.info(sql);
    QueryOperator queryOperator = restParser.generateOperator(suffixPath, prefixPath, timeRange);
    QueryPlan plan = (QueryPlan) planner.logicalPlanToPhysicalPlan(queryOperator);
    List<Path> paths = plan.getPaths();
    if (checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
    }

    // check permissions
    if (!checkAuthorization(paths, plan)) {
      throw new AuthException("Don't have permissions");
    }

    QueryContext context = new QueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    QueryDataSet queryDataSet = executor.processQuery(plan, context);
    String[] args;
    List<TimeValue> list = new ArrayList<>();
    while(queryDataSet.hasNext()) {
      TimeValue timeValue = new TimeValue();
      args = queryDataSet.next().toString().split("\t");
      timeValue.setTime(Long.parseLong(args[0]));
      timeValue.setValue(args[1]);
      list.add(timeValue);
    }
    return list;
  }

  private boolean checkLogin() {
    return username == null;
  }

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan) throws AuthException {
    return AuthorityChecker.check(username, paths, plan.getOperatorType(), null);
  }


  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * get JSON type of input JSON object.
   *
   * @param jsonObject JSON Object
   * @return type (string)
   * @throws JSONException JSONException
   */
  public String getJsonType(JSONObject jsonObject){
    JSONArray array = (JSONArray) jsonObject.get("targets");
    JSONObject object = (JSONObject) array.get(0);
    return (String) object.get("type");
  }

  public void setJsonTable(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws StorageEngineException, QueryFilterOptimizationException,
      MetadataException, IOException, SQLException, QueryProcessException, AuthException {
    List<TimeValue> timeValue = querySeries(target, timeRange);
    JSONArray columns = new JSONArray();
    JSONObject column = new JSONObject();
    column.put("text", "Time");
    column.put("type", "time");
    columns.add(column);
    column = new JSONObject();
    column.put("text", "Number");
    column.put("type", "number");
    columns.add(column);
    obj.put("columns", columns);
    JSONArray values = new JSONArray();
    for (TimeValue tv : timeValue) {
      JSONArray value = new JSONArray();
      value.add(tv.getTime());
      value.add(tv.getValue());
      values.add(value);
    }
    obj.put("values", values);
  }

  public void setJsonTimeseries(JSONObject obj, String target,
      Pair<String, String> timeRange)
      throws StorageEngineException, QueryFilterOptimizationException,
      MetadataException, IOException, SQLException, QueryProcessException, AuthException {
    List<TimeValue> timeValue = querySeries(target, timeRange);
    logger.info("query size: {}", timeValue.size());
    JSONArray dataPoints = new JSONArray();
    for (TimeValue tv : timeValue) {
      long time = tv.getTime();
      String value = tv.getValue();
      JSONArray jsonArray = new JSONArray();
      jsonArray.add(time);
      jsonArray.add(value);
      dataPoints.add(jsonArray);
    }
    obj.put("datapoints", dataPoints);
  }

  public boolean setStorageGroup(String storageGroup) throws QueryProcessException {
    if(checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return false;
    }
    SetStorageGroupPlan plan = new SetStorageGroupPlan(new Path(storageGroup));
    return executeNonQuery(plan);
  }

  public boolean createTimeSeries(String path, String dataType,
      String encoding, String compressor) throws QueryProcessException {
    if(checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return false;
    }
    CreateTimeSeriesPlan plan =
        new CreateTimeSeriesPlan(
            new Path(path),
            TSDataType.valueOf(dataType.trim().toUpperCase()),
            TSEncoding.valueOf(encoding.trim().toUpperCase()),
            CompressionType.findByShortName(compressor.trim().toUpperCase()),
            new HashMap<>());
    return executeNonQuery(plan);
  }

  public boolean insert(String deviceId, long time, List<String> measurements,
      List<String> values) throws QueryProcessException {
    if(checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      return false;
    }
    InsertPlan plan = new InsertPlan();
    plan.setDeviceId(deviceId);
    plan.setTime(time);
    plan.setMeasurements(measurements.toArray(new String[0]));
    plan.setValues(values.toArray(new String[0]));
    return executeNonQuery(plan);
  }

  private boolean executeNonQuery(PhysicalPlan plan) throws QueryProcessException {

    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new QueryProcessException(
          "Current system mode is read-only, does not support non-query operation");
    }
    return executor.processNonQuery(plan);
  }

  public static RestService getInstance() {
    return RestServiceHolder.INSTANCE;
  }

  private static class RestServiceHolder {
    private static final RestService INSTANCE = new RestService();
  }
}
