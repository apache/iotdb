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

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_CHILD_PATHS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COLUMN;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_COUNT;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_DEVICES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ITEM;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PARAMETER;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_PRIVILEGE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_ROLE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_STORAGE_GROUP;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_COMPRESSION;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_DATATYPE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TIMESERIES_ENCODING;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TTL;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_USER;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VALUE;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_VERSION;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.auth.authorizer.IAuthorizer;
import org.apache.iotdb.db.auth.authorizer.LocalFileAuthorizer;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.constant.SQLConstant;
import org.apache.iotdb.db.qp.executor.IPlanExecutor;
import org.apache.iotdb.db.qp.executor.PlanExecutor;
import org.apache.iotdb.db.qp.logical.Operator.OperatorType;
import org.apache.iotdb.db.qp.logical.crud.QueryOperator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan;
import org.apache.iotdb.db.qp.physical.crud.AlignByDevicePlan.MeasurementType;
import org.apache.iotdb.db.qp.physical.crud.InsertPlan;
import org.apache.iotdb.db.qp.physical.crud.LastQueryPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.AuthorPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.NonAlignEngineDataSet;
import org.apache.iotdb.db.rest.model.TimeValue;
import org.apache.iotdb.db.tools.watermark.GroupedLSBWatermarkEncoder;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestService {

  protected Planner planner = new Planner();
  private RestParser restParser = new RestParser();
  private static final Logger logger = LoggerFactory.getLogger(RestService.class);
  private IPlanExecutor executor;
  protected Planner processor;
  private boolean ignoreTimeStamp = false;

  private RestService() {
    try {
      processor = new Planner();
      executor = new PlanExecutor();
    } catch (QueryProcessException e) {
      logger.error(e.getMessage());
    }
  }

  private static final String INFO_NOT_LOGIN = "{}: Not login.";
  private String username;

  private List<TimeValue> querySeries(String s, Pair<String, String> timeRange)
      throws QueryProcessException, AuthException, IOException,
      MetadataException, QueryFilterOptimizationException, SQLException,
      StorageEngineException {

    long queryId;
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
    if (checkAuthorization(paths, plan, username)) {
      throw new AuthException("Don't have permissions");
    }

    // generate the queryId for the operation
    queryId = generateQueryId();
    String[] args;

    QueryDataSet queryDataSet = createQueryDataSet(queryId, plan);
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

  private long generateQueryId() {
    return QueryResourceManager.getInstance().assignQueryId(true);
  }

  private QueryContext genQueryContext(long queryId) {
    return new QueryContext(queryId);
  }

  private QueryDataSet createQueryDataSet(long queryId, PhysicalPlan physicalPlan)
      throws QueryProcessException, QueryFilterOptimizationException, StorageEngineException,
      IOException, MetadataException, SQLException {

    QueryContext context = genQueryContext(queryId);
    return executor.processQuery(physicalPlan, context);
  }

  private boolean checkLogin() {
    return username == null;
  }

  private boolean checkAuthorization(List<Path> paths, PhysicalPlan plan, String username) throws AuthException {
    return !AuthorityChecker.check(username, paths, plan.getOperatorType(), username);
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
            new HashMap<>(), new HashMap<>(), new HashMap<>(), path);
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

  public JSONArray executeStatement(String statement, int fetchSize)
      throws QueryProcessException, MetadataException,
      QueryFilterOptimizationException, SQLException,
      StorageEngineException, IOException, AuthException {
    if(checkLogin()) {
      logger.info(INFO_NOT_LOGIN, IoTDBConstant.GLOBAL_DB_NAME);
      throw new AuthException("Can't log in");
    }
    long queryId;
    PhysicalPlan physicalPlan =
        processor.parseSQLToPhysicalPlan(statement, IoTDBDescriptor.getInstance().getConfig().getZoneID());
    JSONArray jsonArray = new JSONArray();
    if (physicalPlan.isQuery()) {
      if (physicalPlan instanceof QueryPlan && !((QueryPlan) physicalPlan).isAlignByTime()) {
        if (physicalPlan.getOperatorType() == OperatorType.AGGREGATION) {
          throw new QueryProcessException("Aggregation doesn't support disable align clause.");
        }
        if (physicalPlan.getOperatorType() == OperatorType.FILL) {
          throw new QueryProcessException("Fill doesn't support disable align clause.");
        }
        if (physicalPlan.getOperatorType() == OperatorType.GROUPBY) {
          throw new QueryProcessException("Group by doesn't support disable align clause.");
        }
      }
      if (physicalPlan.getOperatorType() == OperatorType.AGGREGATION) {
        this.ignoreTimeStamp = false;
      } // else default ignoreTimeStamp is false
      // generate the queryId for the operation
      queryId = generateQueryId();
      // put it into the corresponding Set

      // create and cache dataset
      QueryDataSet newDataSet = createQueryDataSet(queryId, physicalPlan);

      IAuthorizer authorizer = LocalFileAuthorizer.getInstance();
      List<String> header = getQueryHeaders(physicalPlan, username);
      JSONArray headerJson = new JSONArray();
      if(!ignoreTimeStamp) {
        header.add(0, COLUMN_TIME);
      }
      headerJson.addAll(header);
      jsonArray.add(headerJson);
      IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
      WatermarkEncoder encoder = null;
      if (config.isEnableWatermark() && authorizer.isUserUseWaterMark(username)) {
        if (config.getWatermarkMethodName().equals(IoTDBConfig.WATERMARK_GROUPED_LSB)) {
          encoder = new GroupedLSBWatermarkEncoder(config);
        } else {
          throw new UnSupportedDataTypeException(
              String.format(
                  "Watermark method is not supported yet: %s", config.getWatermarkMethodName()));
        }
      }
      if(newDataSet instanceof NonAlignEngineDataSet) {
        throw new QueryProcessException("SQL through HTTP doesn't support disable align");
      } else {
        for (int i = 0; i < fetchSize; i++) {
          JSONArray rowJson = new JSONArray();
          if (newDataSet.hasNext()) {
            RowRecord rowRecord = newDataSet.next();
            rowJson.add(parseLongToDateWithPrecision(
                rowRecord.getTimestamp(), config.getZoneID()));
            if (encoder != null) {
              rowRecord = encoder.encodeRecord(rowRecord);
            }
            List<Field> fields = rowRecord.getFields();
            for (Field field : fields) {
              rowJson.add(field.getStringValue());
            }
          } else {
            break;
          }
          jsonArray.add(rowJson);
        }
      }
    } else {
      if(!executeNonQuery(physicalPlan)) {
        throw new QueryProcessException("Non query return false");
      }
    }
    return jsonArray;
  }

  private List<String> getQueryHeaders(PhysicalPlan plan, String username)
      throws QueryProcessException, AuthException {
    if (plan instanceof AuthorPlan) {
      ignoreTimeStamp = true;
      return getAuthQueryColumnHeaders(plan);
    } else if (plan instanceof ShowPlan) {
      ignoreTimeStamp = true;
      return getShowQueryColumnHeaders((ShowPlan) plan);
    } else {
      return getQueryColumnHeaders(plan, username);
    }
  }

  private List<String> getShowQueryColumnHeaders(ShowPlan showPlan)
      throws QueryProcessException {
    switch (showPlan.getShowContentType()) {
      case TTL:
        return Arrays.asList(COLUMN_STORAGE_GROUP, COLUMN_TTL);
      case FLUSH_TASK_INFO:
        return Arrays.asList(COLUMN_ITEM, COLUMN_VALUE);
      case DYNAMIC_PARAMETER:
        return Arrays.asList(COLUMN_PARAMETER, COLUMN_VALUE);
      case VERSION:
        return Collections.singletonList(COLUMN_VERSION);
      case TIMESERIES:
        return Arrays.asList(COLUMN_TIMESERIES, COLUMN_STORAGE_GROUP, COLUMN_TIMESERIES_DATATYPE,
            COLUMN_TIMESERIES_ENCODING, COLUMN_TIMESERIES_COMPRESSION);
      case STORAGE_GROUP:
        return Collections.singletonList(COLUMN_STORAGE_GROUP);
      case CHILD_PATH:
        return Collections.singletonList(COLUMN_CHILD_PATHS);
      case DEVICES:
        return Collections.singletonList(COLUMN_DEVICES);
      case COUNT_NODE_TIMESERIES:
        return Arrays.asList(COLUMN_COLUMN, COLUMN_COUNT);
      case COUNT_NODES:
      case COUNT_TIMESERIES:
        return Collections.singletonList(COLUMN_COUNT);
      default:
        logger.error("Unsupported show content type: {}", showPlan.getShowContentType());
        throw new QueryProcessException(
            "Unsupported show content type:" + showPlan.getShowContentType());
    }
  }

  private List<String> getAuthQueryColumnHeaders(PhysicalPlan plan) throws AuthException {
    AuthorPlan authorPlan = (AuthorPlan) plan;
    switch (authorPlan.getAuthorType()) {
      case LIST_ROLE:
      case LIST_USER_ROLES:
        return Collections.singletonList(COLUMN_ROLE);
      case LIST_USER:
      case LIST_ROLE_USERS:
        return Collections.singletonList(COLUMN_USER);
      case LIST_ROLE_PRIVILEGE:
        return Collections.singletonList(COLUMN_PRIVILEGE);
      case LIST_USER_PRIVILEGE:
        return Arrays.asList(COLUMN_ROLE, COLUMN_PRIVILEGE);
      default:
        throw new AuthException(authorPlan.getAuthorType().toString() +
            " is not an auth query");
    }
  }

  /**
   * get ResultSet schema
   */
  private List<String> getQueryColumnHeaders(PhysicalPlan physicalPlan, String username)
      throws AuthException, QueryProcessException {

    List<String> columns = new ArrayList<>();

    // check permissions
    if (checkAuthorization(physicalPlan.getPaths(), physicalPlan, username)) {
      throw new AuthException("Don't have permissions");
    }

    // align by device query
    QueryPlan plan = (QueryPlan) physicalPlan;
    if (plan instanceof AlignByDevicePlan) {
      getAlignByDeviceQueryHeaders((AlignByDevicePlan) plan, columns);
    } else if (plan instanceof LastQueryPlan) {
      return Arrays.asList(COLUMN_TIMESERIES, COLUMN_VALUE);
    } else {
      getWideQueryHeaders(plan, columns);
    }
    return columns;
  }

  private void getAlignByDeviceQueryHeaders(
      AlignByDevicePlan plan, List<String> respColumns) {
    // set columns in TSExecuteStatementResp.
    respColumns.add(SQLConstant.ALIGNBY_DEVICE_COLUMN_NAME);

    // get column types and do deduplication
    List<TSDataType> deduplicatedColumnsType = new ArrayList<>();
    deduplicatedColumnsType.add(TSDataType.TEXT); // the DEVICE column of ALIGN_BY_DEVICE result

    Set<String> deduplicatedMeasurements = new LinkedHashSet<>();
    Map<String, TSDataType> checker = plan.getMeasurementDataTypeMap();

    // build column header with constant and non exist column and deduplication
    List<String> measurements = plan.getMeasurements();
    Map<String, MeasurementType> measurementTypeMap = plan.getMeasurementTypeMap();
    for (String measurement : measurements) {
      TSDataType type = null;
      switch (measurementTypeMap.get(measurement)) {
        case Exist:
          type = checker.get(measurement);
          break;
        case NonExist:
        case Constant:
          type = TSDataType.TEXT;
      }
      respColumns.add(measurement);

      if (!deduplicatedMeasurements.contains(measurement)) {
        deduplicatedMeasurements.add(measurement);
        deduplicatedColumnsType.add(type);
      }
    }

    // save deduplicated measurementColumn names and types in QueryPlan for the next stage to use.
    // i.e., used by AlignByDeviceDataSet constructor in `fetchResults` stage.
    plan.setMeasurements(new ArrayList<>(deduplicatedMeasurements));
    plan.setDataTypes(deduplicatedColumnsType);

    // set these null since they are never used henceforth in ALIGN_BY_DEVICE query processing.
    plan.setPaths(null);
  }

  // wide means not align by device
  private void getWideQueryHeaders(
      QueryPlan plan, List<String> columns) throws QueryProcessException {
    // Restore column header of aggregate to func(column_name), only
    // support single aggregate function for now
    List<Path> paths = plan.getPaths();
    switch (plan.getOperatorType()) {
      case QUERY:
      case FILL:
        for (Path p : paths) {
          columns.add(p.getFullPath());
        }
        break;
      case AGGREGATION:
      case GROUPBY:
      case GROUP_BY_FILL:
        List<String> aggregations = plan.getAggregations();
        if (aggregations.size() != paths.size()) {
          for (int i = 1; i < paths.size(); i++) {
            aggregations.add(aggregations.get(0));
          }
        }
        for (int i = 0; i < paths.size(); i++) {
          columns.add(aggregations.get(i) + "(" + paths.get(i).getFullPath() + ")");
        }
        break;
      default:
        throw new QueryProcessException("unsupported query type: " + plan.getOperatorType());
    }
  }

  private String parseLongToDateWithPrecision(long timestamp, ZoneId zoneid) {
    long integerofDate = timestamp / 1000;
    StringBuilder digits = new StringBuilder(Long.toString(timestamp % 1000));
    ZonedDateTime dateTime = ZonedDateTime
        .ofInstant(Instant.ofEpochSecond(integerofDate), zoneid);
    String datetime = dateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    int length = digits.length();
    if (length != 3) {
      for (int i = 0; i < 3 - length; i++) {
        digits.insert(0, "0");
      }
    }
    return datetime.substring(0, 19) + "." + digits + datetime.substring(19);
  }


  public static RestService getInstance() {
    return RestServiceHolder.INSTANCE;
  }

  private static class RestServiceHolder {
    private static final RestService INSTANCE = new RestService();
  }
}
