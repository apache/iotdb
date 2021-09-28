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

package org.apache.iotdb.influxdb;

import org.apache.iotdb.influxdb.qp.constant.FilterConstant;
import org.apache.iotdb.influxdb.qp.constant.SQLConstant;
import org.apache.iotdb.influxdb.qp.logical.Operator;
import org.apache.iotdb.influxdb.qp.logical.crud.*;
import org.apache.iotdb.influxdb.qp.logical.function.*;
import org.apache.iotdb.influxdb.qp.strategy.LogicalGenerator;
import org.apache.iotdb.influxdb.query.expression.Expression;
import org.apache.iotdb.influxdb.query.expression.ResultColumn;
import org.apache.iotdb.influxdb.query.expression.unary.FunctionExpression;
import org.apache.iotdb.influxdb.query.expression.unary.NodeExpression;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.*;

import java.lang.reflect.Field;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class IoTDBInfluxDB implements InfluxDB {

  private static Session session;
  // Database currently selected by influxdb
  private String database;
  // Measurement currently selected by influxdb
  private String measurement;
  // Tag list and order corresponding to all measurements in the current database
  // TODO At present, the distributed situation is not considered. It is assumed that all writes are
  // performed by the instance
  private Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();
  // Tag list and order under current measurement
  private Map<String, Integer> tagOrders;

  // The list of fields under the current measurement and the order of the specified rules
  private Map<String, Integer> fieldOrders;
  private Map<Integer, String> fieldOrdersReversed;

  private final String placeholder = "PH";

  /**
   * constructor function
   *
   * @param url contain host and port
   * @param userName username
   * @param password user password
   */
  public IoTDBInfluxDB(String url, String userName, String password) {
    try {
      URI uri = new URI(url);
      new IoTDBInfluxDB(uri.getHost(), uri.getPort(), userName, password);
    } catch (URISyntaxException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * constructor function
   *
   * @param host host
   * @param rpcPort port
   * @param userName username
   * @param password user password
   */
  public IoTDBInfluxDB(String host, int rpcPort, String userName, String password) {
    session = new Session(host, rpcPort, userName, password);
    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
      throw new IllegalArgumentException(e.getMessage());
    }
    session.setFetchSize(10000);
  }

  /**
   * write function compatible with influxdb
   *
   * @param point Data structure for inserting data
   */
  public void write(Point point) {
    String measurement = null;
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    Long time = null;
    Field[] reflectFields = point.getClass().getDeclaredFields();
    // Get the property of point in influxdb by reflection
    for (Field reflectField : reflectFields) {
      reflectField.setAccessible(true);
      try {
        if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
            && reflectField.getName().equalsIgnoreCase("fields")) {
          fields = (Map<String, Object>) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
            && reflectField.getName().equalsIgnoreCase("tags")) {
          tags = (Map<String, String>) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String")
            && reflectField.getName().equalsIgnoreCase("measurement")) {
          measurement = (String) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.Number")
            && reflectField.getName().equalsIgnoreCase("time")) {
          time = (Long) reflectField.get(point);
        }
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    // set current time
    if (time == null) {
      time = System.currentTimeMillis();
    }
    tagOrders = measurementTagOrder.get(measurement);
    if (tagOrders == null) {
      tagOrders = new HashMap<>();
    }
    int measurementTagNum = tagOrders.size();
    // The actual number of tags at the time of current insertion
    Map<Integer, String> realTagOrders = new HashMap<>();
    for (Map.Entry<String, String> entry : tags.entrySet()) {
      if (tagOrders.containsKey(entry.getKey())) {
        realTagOrders.put(tagOrders.get(entry.getKey()), entry.getKey());
      } else {
        measurementTagNum++;
        try {
          updateNewTagIntoDB(measurement, entry.getKey(), measurementTagNum);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
          e.printStackTrace();
        }
        realTagOrders.put(measurementTagNum, entry.getKey());
        tagOrders.put(entry.getKey(), measurementTagNum);
      }
    }
    // update tagOrder map in memory
    measurementTagOrder.put(measurement, tagOrders);
    StringBuilder path = new StringBuilder("root." + database + "." + measurement);
    for (int i = 1; i <= measurementTagNum; i++) {
      if (realTagOrders.containsKey(i)) {
        path.append(".").append(tags.get(realTagOrders.get(i)));
      } else {
        path.append("." + placeholder);
      }
    }

    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      measurements.add(entry.getKey());
      Object value = entry.getValue();
      if (value instanceof String) {
        types.add(TSDataType.TEXT);
      } else if (value instanceof Integer) {
        types.add(TSDataType.INT32);
      } else if (value instanceof Double) {
        types.add(TSDataType.DOUBLE);
      } else {
        System.err.printf("can't solve type:%s", entry.getValue().getClass());
      }
      values.add(value);
    }
    try {
      session.insertRecord(String.valueOf(path), time, measurements, types, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  /**
   * query function compatible with influxdb
   *
   * @param query query parameters of influxdb, including databasename and SQL statement
   * @return returns the query result of influxdb
   */
  public QueryResult query(Query query) {
    String sql = query.getCommand();
    String database = query.getDatabase();
    if (!this.database.equals(database)) {
      updateDatabase(database);
    }
    Operator operator = LogicalGenerator.generate(sql);
    IoTDBInfluxDBUtils.checkQueryOperator(operator);
    QueryOperator queryOperator = (QueryOperator) operator;
    // update relative data
    updateMeasurement(queryOperator.getFromComponent().getNodeName().get(0));
    QueryResult queryResult = null;
    try {
      updateFiledOrders();
      // contain filter condition or don't have function the result of the function is calculated by
      // traversal
      if (queryOperator.getWhereComponent() != null
          || !queryOperator.getSelectComponent().isHasFunction()) {
        // step1 : generate query results
        queryResult = queryExpr(queryOperator.getWhereComponent().getFilterOperator());
        // step2 : select filter
        ProcessSelectComponent(queryResult, queryOperator.getSelectComponent());
      }
      // don't contain filter condition and have function use iotdb function
      else {
        queryResult = queryFuncWithoutFilter(queryOperator.getSelectComponent());
      }
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new RuntimeException(e.getMessage());
    }
    return queryResult;
  }

  /**
   * create database,write to iotdb
   *
   * @param name database name
   */
  public void createDatabase(String name) {
    IoTDBInfluxDBUtils.checkNonEmptyString(name, "database name");
    try {
      session.setStorageGroup("root." + name);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      if (e instanceof StatementExecutionException
          && ((StatementExecutionException) e).getStatusCode() == 300) {
        // current database have been created
        System.out.println(e.getMessage());
      } else {
        e.printStackTrace();
      }
    }
  }

  /**
   * delete database
   *
   * @param name database name
   */
  public void deleteDatabase(String name) {
    try {
      session.deleteStorageGroup("root." + name);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      e.printStackTrace();
    }
  }

  /**
   * set database，and get the list and order of all tags corresponding to the database
   *
   * @param database database name
   */
  public InfluxDB setDatabase(String database) {
    if (!database.equals(this.database)) {
      updateDatabase(database);
      this.database = database;
    }
    return this;
  }

  @Override
  public void write(String s) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(List<String> list) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(String s, String s1, Point point) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(int i, Point point) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(BatchPoints batchPoints) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void writeWithRetry(BatchPoints batchPoints) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(String s, String s1, ConsistencyLevel consistencyLevel, String s2) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      String s, String s1, ConsistencyLevel consistencyLevel, TimeUnit timeUnit, String s2) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(String s, String s1, ConsistencyLevel consistencyLevel, List<String> list) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      String s,
      String s1,
      ConsistencyLevel consistencyLevel,
      TimeUnit timeUnit,
      List<String> list) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(int i, String s) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(int i, List<String> list) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, Consumer<QueryResult> consumer, Consumer<Throwable> consumer1) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int i, Consumer<QueryResult> consumer) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int i, BiConsumer<Cancellable, QueryResult> biConsumer) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int i, Consumer<QueryResult> consumer, Runnable runnable) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query, int i, BiConsumer<Cancellable, QueryResult> biConsumer, Runnable runnable) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query,
      int i,
      BiConsumer<Cancellable, QueryResult> biConsumer,
      Runnable runnable,
      Consumer<Throwable> consumer) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public QueryResult query(Query query, TimeUnit timeUnit) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public List<String> describeDatabases() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean databaseExists(String s) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void close() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setConsistency(ConsistencyLevel consistencyLevel) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setRetentionPolicy(String s) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(String s, String s1, String s2, String s3, int i, boolean b) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(String s, String s1, String s2, int i, boolean b) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(String s, String s1, String s2, String s3, int i) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void dropRetentionPolicy(String s, String s1) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setLogLevel(LogLevel logLevel) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableGzip() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB disableGzip() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isGzipEnabled() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(BatchOptions batchOptions) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(int i, int i1, TimeUnit timeUnit) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(int i, int i1, TimeUnit timeUnit, ThreadFactory threadFactory) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      int i,
      int i1,
      TimeUnit timeUnit,
      ThreadFactory threadFactory,
      BiConsumer<Iterable<Point>, Throwable> biConsumer,
      ConsistencyLevel consistencyLevel) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      int i,
      int i1,
      TimeUnit timeUnit,
      ThreadFactory threadFactory,
      BiConsumer<Iterable<Point>, Throwable> biConsumer) {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void disableBatch() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isBatchEnabled() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Pong ping() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String version() {
    throw new UnsupportedOperationException(Constant.METHOD_NOT_SUPPORTED);
  }

  /**
   * when the database changes, update the database related information, that is, obtain the list
   * and order of all tags corresponding to the database from iotdb
   *
   * @param database update database name
   */
  private void updateDatabase(String database) {
    try {
      SessionDataSet result =
          session.executeQueryStatement(
              "select * from root.TAG_INFO where database_name="
                  + String.format("\"%s\"", database));
      Map<String, Integer> tagOrder = new HashMap<>();
      String measurementName = null;
      while (result.hasNext()) {
        List<org.apache.iotdb.tsfile.read.common.Field> fields = result.next().getFields();
        String tmpMeasurementName = fields.get(1).getStringValue();
        if (measurementName == null) {
          // get measurement name for the first time
          measurementName = tmpMeasurementName;
        } else {
          // if it is not equal, a new measurement is encountered
          if (!tmpMeasurementName.equals(measurementName)) {
            // add the tags of the current measurement to it
            measurementTagOrder.put(measurementName, tagOrder);
            tagOrder = new HashMap<>();
          }
        }
        tagOrder.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
      }
      // the last measurement is to add the tags of the current measurement
      measurementTagOrder.put(measurementName, tagOrder);
    } catch (StatementExecutionException e) {
      // at first execution, tag_ If the info table is not created, intercept the error and print
      // the log
      if (e.getStatusCode() == 411) {
        System.out.println(e.getMessage());
      }
    } catch (IoTDBConnectionException e) {
      e.printStackTrace();
    }
  }

  /**
   * get query results in the format of influxdb through conditions
   *
   * @param conditions list of conditions, including tag and field condition
   * @return returns the results of the influxdb query
   */
  private QueryResult queryByConditions(List<Condition> conditions)
      throws IoTDBConnectionException, StatementExecutionException {
    // used to store the actual order according to the tag
    Map<Integer, Condition> realTagOrders = new HashMap<>();
    // stores a list of conditions belonging to the field
    List<Condition> fieldConditions = new ArrayList<>();
    // maximum number of tags in the current query criteria
    int currentQueryMaxTagNum = 0;
    for (Condition condition : conditions) {
      // the current condition is in tag
      if (tagOrders.containsKey(condition.getValue())) {
        int curOrder = tagOrders.get(condition.getValue());
        // put it into the map according to the tag
        realTagOrders.put(curOrder, condition);
        // update the maximum tag order of the current query criteria
        currentQueryMaxTagNum = Math.max(currentQueryMaxTagNum, curOrder);
      } else {
        fieldConditions.add(condition);
      }
    }
    // construct the actual query path
    StringBuilder curQueryPath = new StringBuilder("root." + database + "." + measurement);
    // the maximum number of traversals from 1 to the current query condition
    for (int i = 1; i <= currentQueryMaxTagNum; i++) {
      if (realTagOrders.containsKey(i)) {
        // since it is the value in the path, you need to remove the quotation marks at the
        // beginning and end
        curQueryPath
            .append(".")
            .append(IoTDBInfluxDBUtils.removeQuotation(realTagOrders.get(i).getLiteral()));
      } else {
        curQueryPath.append(".").append("*");
      }
    }
    // construct actual query condition
    StringBuilder realIotDBCondition = new StringBuilder();
    for (int i = 0; i < fieldConditions.size(); i++) {
      Condition condition = fieldConditions.get(i);
      if (i != 0) {
        realIotDBCondition.append(" and ");
      }
      realIotDBCondition
          .append(condition.getValue())
          .append(" ")
          .append(FilterConstant.filterSymbol.get(condition.getFilterType()))
          .append(" ")
          .append(condition.getLiteral());
    }
    // actual query SQL statement
    String realQuerySql;

    realQuerySql = "select * from " + curQueryPath;
    if (!(realIotDBCondition.length() == 0)) {
      realQuerySql += " where " + realIotDBCondition;
    }
    realQuerySql += " align by device";
    SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
    return iotdbAlignByDeviceResultCvtToInfluxdbResult(sessionDataSet);
    // The following comments refer to the scheme of non align by device
    //        if (realIotDBCondition.isEmpty()) {
    //            realQuerySql = ("select * from " + curQueryPath);
    //            SessionDataSet sessionDataSet = session.executeQueryStatement(realQuerySql);
    //            queryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
    //            System.out.println(sessionDataSet.toString());
    //        } else {
    //            //With filter conditions, we can only traverse multiple times
    //            QueryResult lastQueryResult = null;
    //            for (int i = currentQueryMaxTagNum; i <= measurementTagNum; i++) {
    //                if (i != currentQueryMaxTagNum) {
    //                    curQueryPath.append(".*");
    //                }
    //                realQuerySql = ("select * from " + curQueryPath + " where " +
    // realIotDBCondition + " align by device");
    //                SessionDataSet sessionDataSet = null;
    //                try {
    //                    sessionDataSet = session.executeQueryStatement(realQuerySql);
    //                } catch (StatementExecutionException e) {
    //                    if (e.getStatusCode() == 411) {
    //                        //If the timeseries of where do not match, an error of 411 will be
    // thrown and blocked for printing
    //                        System.out.println(e.getMessage());
    //                    } else {
    //                        throw e;
    //                    }
    //                }
    //                //Temporary conversion results
    //                QueryResult tmpQueryResult = iotdbResultCvtToInfluxdbResult(sessionDataSet);
    //                //If it is the first time, it is assigned directly without or operation
    //                if (i == currentQueryMaxTagNum) {
    //                    lastQueryResult = tmpQueryResult;
    //                } else {
    //                    //Perform the add operation
    //                    lastQueryResult =
    // IotDBInfluxDBUtils.addQueryResultProcess(lastQueryResult, tmpQueryResult);
    //                }
    //            }
    //            queryResult = lastQueryResult;
    //        }
  }

  /**
   * Convert align by device query result of iotdb to the query result of influxdb
   *
   * @param sessionDataSet iotdb query results to be converted
   * @return query results in influxdb format
   */
  private QueryResult iotdbAlignByDeviceResultCvtToInfluxdbResult(SessionDataSet sessionDataSet)
      throws IoTDBConnectionException, StatementExecutionException {
    if (sessionDataSet == null) {
      return IoTDBInfluxDBUtils.getNullQueryResult();
    }
    // generate series
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    // gets the reverse map of the tag
    Map<Integer, String> tagOrderReversed =
        tagOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    int tagSize = tagOrderReversed.size();
    ArrayList<String> tagList = new ArrayList<>();
    for (int i = 1; i <= tagSize; i++) {
      tagList.add(tagOrderReversed.get(i));
    }

    ArrayList<String> fieldList = new ArrayList<>();
    for (int i = 1 + tagSize; i < 1 + tagSize + fieldOrders.size(); i++) {
      fieldList.add(fieldOrdersReversed.get(i));
    }
    ArrayList<String> columns = new ArrayList<>();
    columns.add("time");
    columns.addAll(tagList);
    columns.addAll(fieldList);
    // insert columns into series
    series.setColumns(columns);

    List<List<Object>> values = new ArrayList<>();

    List<String> iotdbResultColumn = sessionDataSet.getColumnNames();
    while (sessionDataSet.hasNext()) {
      Object[] value = new Object[columns.size()];

      RowRecord record = sessionDataSet.next();
      List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();

      value[0] = record.getTimestamp();

      String deviceName = fields.get(0).getStringValue();
      String[] deviceNameList = deviceName.split("\\.");
      for (int i = 3; i < deviceNameList.length; i++) {
        if (!deviceNameList[i].equals(placeholder)) {
          value[i - 2] = deviceNameList[i];
        }
      }
      for (int i = 1; i < fields.size(); i++) {
        Object o = IoTDBInfluxDBUtils.iotdbFiledCvt(fields.get(i));
        if (o != null) {
          // insert the value of filed into it
          value[fieldOrders.get(iotdbResultColumn.get(i + 1))] = o;
        }
      }
      // insert actual value
      values.add(Arrays.asList(value));
    }
    series.setValues(values);

    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

    return queryResult;
  }

  /**
   * convert the query result of iotdb to the query result of influxdb
   *
   * @param sessionDataSet iotdb query results to be converted
   * @return query results in influxdb format
   */
  private QueryResult iotdbResultCvtToInfluxdbResult(SessionDataSet sessionDataSet)
      throws IoTDBConnectionException, StatementExecutionException {
    if (sessionDataSet == null) {
      return IoTDBInfluxDBUtils.getNullQueryResult();
    }
    QueryResult.Series series = new QueryResult.Series();
    series.setName(measurement);
    Map<Integer, String> tagOrderReversed =
        tagOrders.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    int tagSize = tagOrderReversed.size();
    ArrayList<String> tagList = new ArrayList<>();
    for (int i = 1; i <= tagSize; i++) {
      tagList.add(tagOrderReversed.get(i));
    }

    ArrayList<String> fieldList = new ArrayList<>();
    for (int i = 1 + tagSize; i < 1 + tagSize + fieldOrders.size(); i++) {
      fieldList.add(fieldOrdersReversed.get(i));
    }
    ArrayList<String> columns = new ArrayList<>();
    columns.add("time");
    columns.addAll(tagList);
    columns.addAll(fieldList);
    series.setColumns(columns);

    List<List<Object>> values = new ArrayList<>();

    List<String> iotdbResultColumn = sessionDataSet.getColumnNames();
    ArrayList<Integer> samePath =
        IoTDBInfluxDBUtils.getSamePathForList(
            iotdbResultColumn.subList(1, iotdbResultColumn.size()));
    while (sessionDataSet.hasNext()) {
      Object[] value = new Object[columns.size()];

      RowRecord record = sessionDataSet.next();
      List<org.apache.iotdb.tsfile.read.common.Field> fields = record.getFields();
      long timestamp = record.getTimestamp();
      // determine whether all values of the path are null
      boolean allNull = true;
      // record the current index of sameList
      int sameListIndex = 0;
      for (int i = 0; i < fields.size(); i++) {
        Object o = IoTDBInfluxDBUtils.iotdbFiledCvt(fields.get(i));
        if (o != null) {
          if (allNull) {
            allNull = false;
          }
          // insert the value of filed into it
          value[fieldOrders.get(IoTDBInfluxDBUtils.getFiledByPath(iotdbResultColumn.get(i + 1)))] =
              o;
        }
        // the same path has been traversed
        if (i == samePath.get(sameListIndex)) {
          // if there is non-null in the data, it will be inserted into the actual data, otherwise
          // it will be skipped directly
          if (!allNull) {
            // insert time into value
            value[0] = timestamp;
            // then insert the tag in the path into value
            // plus 1, the zeroth column is time
            String tmpPathName = iotdbResultColumn.get(i + 1);
            String[] tmpTags = tmpPathName.split("\\.");
            for (int j = 3; i < tmpTags.length - 1; i++) {
              if (!tmpTags[j].equals(placeholder)) {
                // put into the specified sequence
                value[j - 2] = tmpTags[j];
              }
            }
          }
          // insert actual value
          values.add(Arrays.asList(value));
          // init value
          value = new Object[columns.size()];
        }
      }
    }
    series.setValues(values);

    QueryResult queryResult = new QueryResult();
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));

    return queryResult;
  }

  /**
   * get query results through the syntax tree of influxdb
   *
   * @param operator query syntax tree to be processed
   * @return query results in influxdb format
   */
  private QueryResult queryExpr(FilterOperator operator)
      throws IoTDBConnectionException, StatementExecutionException {
    if (operator instanceof BasicFunctionOperator) {
      List<Condition> conditions = new ArrayList<>();
      conditions.add(
          IoTDBInfluxDBUtils.getConditionForBasicFunctionOperator(
              (BasicFunctionOperator) operator));
      return queryByConditions(conditions);
    } else {
      FilterOperator leftOperator = operator.getChildOperators().get(0);
      FilterOperator rightOperator = operator.getChildOperators().get(1);
      if (operator.getFilterType() == FilterConstant.FilterType.KW_OR) {
        return IoTDBInfluxDBUtils.orQueryResultProcess(
            queryExpr(leftOperator), queryExpr(rightOperator));
      } else if (operator.getFilterType() == FilterConstant.FilterType.KW_AND) {
        if (IoTDBInfluxDBUtils.canMergeOperator(leftOperator)
            && IoTDBInfluxDBUtils.canMergeOperator(rightOperator)) {
          List<Condition> conditions1 =
              IoTDBInfluxDBUtils.getConditionsByFilterOperatorOperator(leftOperator);
          List<Condition> conditions2 =
              IoTDBInfluxDBUtils.getConditionsByFilterOperatorOperator(rightOperator);
          conditions1.addAll(conditions2);
          return queryByConditions(conditions1);
        } else {
          return IoTDBInfluxDBUtils.andQueryResultProcess(
              queryExpr(leftOperator), queryExpr(rightOperator));
        }
      }
    }
    throw new IllegalArgumentException("unknown operator " + operator.toString());
  }

  /**
   * When a new tag appears, it is inserted into the database
   *
   * @param measurement inserted measurement
   * @param tag tag name
   * @param order tag order
   */
  private void updateNewTagIntoDB(String measurement, String tag, int order)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    measurements.add("database_name");
    measurements.add("measurement_name");
    measurements.add("tag_name");
    measurements.add("tag_order");
    types.add(TSDataType.TEXT);
    types.add(TSDataType.TEXT);
    types.add(TSDataType.TEXT);
    types.add(TSDataType.INT32);
    values.add(database);
    values.add(measurement);
    values.add(tag);
    values.add(order);
    session.insertRecord("root.TAG_INFO", System.currentTimeMillis(), measurements, types, values);
  }

  /**
   * Query the select result. By default, there are no filter conditions. The functions to be
   * queried use the built-in iotdb functions
   *
   * @param selectComponent select data to query
   * @return select query result
   */
  private QueryResult queryFuncWithoutFilter(SelectComponent selectComponent) {
    // columns
    List<String> columns = new ArrayList<>();
    columns.add(SQLConstant.RESERVED_TIME);

    List<Function> functions = new ArrayList<>();
    String path = "root." + this.database + "." + this.measurement;
    for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
      Expression expression = resultColumn.getExpression();
      if (expression instanceof FunctionExpression) {
        String functionName = ((FunctionExpression) expression).getFunctionName();
        functions.add(
            FunctionFactory.generateFunctionBySession(
                functionName, ((FunctionExpression) expression).getExpressions(), session, path));
        columns.add(functionName);
      }
    }

    List<Object> value = new ArrayList<>();
    List<List<Object>> values = new ArrayList<>();
    for (Function function : functions) {
      FunctionValue functionValue = function.calculateByIoTDBFunc();
      if (value.size() == 0) {
        value.add(functionValue.getTimestamp());
      } else {
        value.set(0, functionValue.getTimestamp());
      }
      value.add(functionValue.getValue());
    }
    if (selectComponent.isHasAggregationFunction() || selectComponent.isHasMoreFunction()) {
      value.set(0, 0);
    }
    values.add(value);

    // generate series
    QueryResult queryResult = new QueryResult();
    QueryResult.Series series = new QueryResult.Series();
    series.setColumns(columns);
    series.setValues(values);
    series.setName(measurement);
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(new ArrayList<>(Arrays.asList(series)));
    queryResult.setResults(new ArrayList<>(Arrays.asList(result)));
    return queryResult;
  }

  /**
   * further process the obtained query result through the query criteria of select
   *
   * @param queryResult query results to be processed
   * @param selectComponent select conditions to be filtered
   */
  private void ProcessSelectComponent(QueryResult queryResult, SelectComponent selectComponent) {
    // get the row order map of the current data result first
    List<String> columns = queryResult.getResults().get(0).getSeries().get(0).getColumns();
    Map<String, Integer> columnOrders = new HashMap<>();
    for (int i = 0; i < columns.size(); i++) {
      columnOrders.put(columns.get(i), i);
    }
    // get current values
    List<List<Object>> values = queryResult.getResults().get(0).getSeries().get(0).getValues();
    // new columns
    List<String> newColumns = new ArrayList<>();
    newColumns.add(SQLConstant.RESERVED_TIME);

    // when have function
    if (selectComponent.isHasFunction()) {
      List<Function> functions = new ArrayList<>();
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof FunctionExpression) {
          String functionName = ((FunctionExpression) expression).getFunctionName();
          functions.add(
              FunctionFactory.generateFunction(
                  functionName, ((FunctionExpression) expression).getExpressions()));
          newColumns.add(functionName);
        } else if (expression instanceof NodeExpression) {
          String columnName = ((NodeExpression) expression).getName();
          if (!columnName.equals(SQLConstant.STAR)) {
            newColumns.add(columnName);
          } else {
            newColumns.addAll(columns.subList(1, columns.size()));
          }
        }
      }
      for (List<Object> value : values) {
        for (Function function : functions) {
          List<Expression> expressions = function.getExpressions();
          if (expressions == null) {
            throw new IllegalArgumentException("not support param");
          }
          NodeExpression parmaExpression = (NodeExpression) expressions.get(0);
          String parmaName = parmaExpression.getName();
          if (columnOrders.containsKey(parmaName)) {
            Object selectedValue = value.get(columnOrders.get(parmaName));
            Long selectedTimestamp = (Long) value.get(0);
            if (selectedValue != null) {
              // selector function
              if (function instanceof Selector) {
                ((Selector) function)
                    .updateValueAndRelate(
                        new FunctionValue(selectedValue, selectedTimestamp), value);
              } else {
                // aggregate function
                ((Aggregate) function)
                    .updateValue(new FunctionValue(selectedValue, selectedTimestamp));
              }
            }
          }
        }
      }
      List<Object> value = new ArrayList<>();
      values = new ArrayList<>();
      // after the data is constructed, the final results are generated
      // First, judge whether there are common queries. If there are, a selector function is allowed
      // without aggregate functions
      if (selectComponent.isHasCommonQuery()) {
        Selector selector = (Selector) functions.get(0);
        List<Object> relatedValue = selector.getRelatedValues();
        for (String column : newColumns) {
          if (SQLConstant.getNativeSelectorFunctionNames().contains(column)) {
            value.add(selector.calculate().getValue());
          } else {
            if (relatedValue != null) {
              value.add(relatedValue.get(columnOrders.get(column)));
            }
          }
        }
      } else {
        // If there are no common queries, they are all function queries
        for (Function function : functions) {
          if (value.size() == 0) {
            value.add(function.calculate().getTimestamp());
          } else {
            value.set(0, function.calculate().getTimestamp());
          }
          value.add(function.calculate().getValue());
        }
        if (selectComponent.isHasAggregationFunction() || selectComponent.isHasMoreFunction()) {
          value.set(0, 0);
        }
      }
      values.add(value);
    }
    // if it is not a function query, it is only a common query
    else if (selectComponent.isHasCommonQuery()) {
      // start traversing the scope of the select
      for (ResultColumn resultColumn : selectComponent.getResultColumns()) {
        Expression expression = resultColumn.getExpression();
        if (expression instanceof NodeExpression) {
          // not star case
          if (!((NodeExpression) expression).getName().equals(SQLConstant.STAR)) {
            newColumns.add(((NodeExpression) expression).getName());
          } else {
            newColumns.addAll(columns.subList(1, columns.size()));
          }
        }
      }
      List<List<Object>> newValues = new ArrayList();
      for (List<Object> value : values) {
        List<Object> tmpValue = new ArrayList();
        for (String newColumn : newColumns) {
          tmpValue.add(value.get(columnOrders.get(newColumn)));
        }
        newValues.add(tmpValue);
      }
      values = newValues;
    }
    IoTDBInfluxDBUtils.updateQueryResultColumnValue(
        queryResult, IoTDBInfluxDBUtils.removeDuplicate(newColumns), values);
  }

  /**
   * before each query, first obtain all the field lists in the measurement, and update all the
   * field lists of the current measurement and the specified order
   */
  private void updateFiledOrders() throws IoTDBConnectionException, StatementExecutionException {
    // first init
    fieldOrders = new HashMap<>();
    fieldOrdersReversed = new HashMap<>();
    String showTimeseriesSql = "show timeseries root." + database + '.' + measurement;
    SessionDataSet result = session.executeQueryStatement(showTimeseriesSql);
    int fieldNums = 0;
    int tagOrderNums = tagOrders.size();
    while (result.hasNext()) {
      List<org.apache.iotdb.tsfile.read.common.Field> fields = result.next().getFields();
      String filed = IoTDBInfluxDBUtils.getFiledByPath(fields.get(0).getStringValue());
      if (!fieldOrders.containsKey(filed)) {
        // The corresponding order of fields is 1 + tagnum (the first is timestamp, then all tags,
        // and finally all fields)
        fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
        fieldOrdersReversed.put(tagOrderNums + fieldNums + 1, filed);
        fieldNums++;
      }
    }
  }

  /**
   * update current measurement
   *
   * @param measurement measurement to be changed
   */
  private void updateMeasurement(String measurement) {
    if (!measurement.equals(this.measurement)) {
      this.measurement = measurement;
      tagOrders = measurementTagOrder.get(measurement);
      if (tagOrders == null) {
        tagOrders = new HashMap<>();
      }
    }
  }
}
