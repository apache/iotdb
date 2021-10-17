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

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class IoTDBInfluxDB implements InfluxDB {

  private static final String METHOD_NOT_SUPPORTED = "Method not supported.";

  private final String placeholder = "PH";

  private final Session session;

  // Tag list and order under current measurement
  private Map<String, Integer> tagOrders = new HashMap<>();

  private final Map<String, Map<String, Integer>> measurementTagOrder = new HashMap<>();

  // Database currently selected by influxdb
  private String database;

  public IoTDBInfluxDB(String url, String userName, String password) {
    URI uri;
    try {
      uri = new URI(url);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to parse url: " + url, e);
    }
    session = new Session(uri.getHost(), uri.getPort(), userName, password);
    openSession();
  }

  public IoTDBInfluxDB(String host, int rpcPort, String userName, String password) {
    session = new Session(host, rpcPort, userName, password);
    openSession();
  }

  public IoTDBInfluxDB(Session session) {
    this.session = session;
    openSession();
  }

  public IoTDBInfluxDB(Session.Builder builder) {
    session = builder.build();
    openSession();
  }

  private void openSession() {
    try {
      session.open(false);
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public void write(final Point point) {
    String measurement = null;
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    Long time = null;
    java.lang.reflect.Field[] reflectFields = point.getClass().getDeclaredFields();
    // Get the property of point in influxdb by reflection
    for (java.lang.reflect.Field reflectField : reflectFields) {
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
    // Don't need to check field, tt has been checked during construction
    IoTDBInfluxDBUtils.checkNonEmptyString(measurement, "measurement name");
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
          throw new InfluxDBException(e.getMessage());
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
      types.add(IoTDBInfluxDBUtils.normalTypeToTSDataType(value));
      values.add(value);
    }
    try {
      session.insertRecord(String.valueOf(path), time, measurements, types, values);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public QueryResult query(final Query query) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createDatabase(final String name) {
    IoTDBInfluxDBUtils.checkNonEmptyString(name, "database name");
    try {
      session.setStorageGroup("root." + name);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      if (!(e instanceof StatementExecutionException)
          || ((StatementExecutionException) e).getStatusCode() != 300) {
        // current database have been created
        throw new InfluxDBException(e.getMessage());
      }
    }
  }

  @Override
  public void deleteDatabase(final String name) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setDatabase(final String database) {
    IoTDBInfluxDBUtils.checkNonEmptyString(database, "database name");
    if (!database.equals(this.database)) {
      updateDatabase(database);
      this.database = database;
    }
    return this;
  }

  @Override
  public void write(final String records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final List<String> records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final String database, final String retentionPolicy, final Point point) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final int udpPort, final Point point) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final BatchPoints batchPoints) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void writeWithRetry(final BatchPoints batchPoints) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final String records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final TimeUnit precision,
      final String records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final List<String> records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final TimeUnit precision,
      final List<String> records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final int udpPort, final String records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final int udpPort, final List<String> records) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      final Query query,
      final Consumer<QueryResult> onSuccess,
      final Consumer<Throwable> onFailure) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext, Runnable onComplete) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query,
      int chunkSize,
      BiConsumer<Cancellable, QueryResult> onNext,
      Runnable onComplete) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query,
      int chunkSize,
      BiConsumer<Cancellable, QueryResult> onNext,
      Runnable onComplete,
      Consumer<Throwable> onFailure) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public QueryResult query(Query query, TimeUnit timeUnit) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public List<String> describeDatabases() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean databaseExists(final String name) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void flush() {
    try {
      session.executeNonQueryStatement("flush");
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new InfluxDBException(e);
    }
  }

  @Override
  public void close() {
    try {
      session.close();
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public InfluxDB setConsistency(final ConsistencyLevel consistencyLevel) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setRetentionPolicy(final String retentionPolicy) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final String shardDuration,
      final int replicationFactor,
      final boolean isDefault) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final int replicationFactor,
      final boolean isDefault) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final String shardDuration,
      final int replicationFactor) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void dropRetentionPolicy(final String rpName, final String database) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setLogLevel(LogLevel logLevel) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableGzip() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB disableGzip() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isGzipEnabled() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(BatchOptions batchOptions) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions,
      final int flushDuration,
      final TimeUnit flushDurationTimeUnit,
      final ThreadFactory threadFactory) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      int actions,
      int flushDuration,
      TimeUnit flushDurationTimeUnit,
      ThreadFactory threadFactory,
      BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
      ConsistencyLevel consistency) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions,
      final int flushDuration,
      final TimeUnit flushDurationTimeUnit,
      final ThreadFactory threadFactory,
      final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public void disableBatch() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isBatchEnabled() {
    throw new UnsupportedOperationException(METHOD_NOT_SUPPORTED);
  }

  @Override
  public Pong ping() {
    final long started = System.currentTimeMillis();
    String version = version();
    Pong pong = new Pong();
    pong.setVersion(version);
    pong.setResponseTime(System.currentTimeMillis() - started);
    return pong;
  }

  @Override
  public String version() {
    try {
      SessionDataSet sessionDataSet = session.executeQueryStatement("show version");
      String version = null;
      while (sessionDataSet.hasNext()) {
        RowRecord record = sessionDataSet.next();
        List<Field> fields = record.getFields();
        version = fields.get(0).getStringValue();
      }
      return version;
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
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
      // at first execution, tag_INFO table is not created, intercept the error
      if (e.getStatusCode() != 411) {
        throw new InfluxDBException(e.getMessage());
      }
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }
}
