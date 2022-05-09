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

import org.apache.iotdb.influxdb.protocol.constant.InfluxDBConstant;
import org.apache.iotdb.influxdb.protocol.dto.SessionPoint;
import org.apache.iotdb.influxdb.protocol.impl.IoTDBInfluxDBService;
import org.apache.iotdb.influxdb.protocol.util.ParameterUtils;
import org.apache.iotdb.session.Session;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Pong;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.impl.TimeUtil;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class IoTDBInfluxDB implements InfluxDB {

  private final IoTDBInfluxDBService influxDBService;

  public IoTDBInfluxDB(String url, String userName, String password) {
    URI uri;
    try {
      uri = new URI(url);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unable to parse url: " + url, e);
    }
    influxDBService = new IoTDBInfluxDBService(uri.getHost(), uri.getPort(), userName, password);
  }

  public IoTDBInfluxDB(String host, int rpcPort, String userName, String password) {
    influxDBService = new IoTDBInfluxDBService(host, rpcPort, userName, password);
  }

  public IoTDBInfluxDB(Session.Builder builder) {
    this(builder.build());
  }

  public IoTDBInfluxDB(Session session) {
    SessionPoint sessionPoint = new SessionPoint(session);
    influxDBService =
        new IoTDBInfluxDBService(
            sessionPoint.getHost(),
            sessionPoint.getRpcPort(),
            sessionPoint.getUsername(),
            sessionPoint.getPassword());
  }

  @Override
  public void write(final Point point) {
    write(null, null, point);
  }

  @Override
  public void write(final String database, final String retentionPolicy, final Point point) {
    TimeUnit precision = TimeUnit.NANOSECONDS;
    // Get the precision of point in influxdb by reflection
    for (java.lang.reflect.Field reflectField : point.getClass().getDeclaredFields()) {
      reflectField.setAccessible(true);
      try {
        if (reflectField.getType().getName().equalsIgnoreCase("java.util.concurrent.TimeUnit")
            && reflectField.getName().equalsIgnoreCase("precision")) {
          precision = (TimeUnit) reflectField.get(point);
          break;
        }
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }
    BatchPoints batchPoints =
        BatchPoints.database(database)
            .retentionPolicy(retentionPolicy)
            .precision(precision)
            .build();
    batchPoints.point(point);
    write(batchPoints);
  }

  @Override
  public void write(final int udpPort, final Point point) {
    write(null, null, point);
  }

  @Override
  public void write(final BatchPoints batchPoints) {
    influxDBService.writePoints(
        batchPoints.getDatabase(),
        batchPoints.getRetentionPolicy(),
        TimeUtil.toTimePrecision(batchPoints.getPrecision()),
        batchPoints.getConsistency().value(),
        batchPoints.lineProtocol());
  }

  @Override
  public void writeWithRetry(final BatchPoints batchPoints) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void write(final String records) {
    write(null, null, null, null, records);
  }

  @Override
  public void write(final List<String> records) {
    write(String.join("\n", records));
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final String records) {
    write(database, retentionPolicy, consistency, null, records);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final TimeUnit precision,
      final String records) {
    influxDBService.writePoints(
        database,
        retentionPolicy,
        consistency.value(),
        precision == null ? "" : TimeUtil.toTimePrecision(precision),
        records);
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final List<String> records) {
    write(database, retentionPolicy, consistency, null, String.join("\n", records));
  }

  @Override
  public void write(
      final String database,
      final String retentionPolicy,
      final ConsistencyLevel consistency,
      final TimeUnit precision,
      final List<String> records) {
    write(database, retentionPolicy, consistency, precision, String.join("\n", records));
  }

  @Override
  public void write(final int udpPort, final String records) {
    write(records);
  }

  @Override
  public void write(final int udpPort, final List<String> records) {
    write(String.join("\n", records));
  }

  @Override
  public QueryResult query(final Query queryReq) {
    return influxDBService.query(queryReq);
  }

  @Override
  public void createDatabase(final String name) {
    ParameterUtils.checkNonEmptyString(name, "database name");
    influxDBService.createDatabase(name);
  }

  @Override
  public void deleteDatabase(final String name) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setDatabase(final String database) {
    ParameterUtils.checkNonEmptyString(database, "database name");
    influxDBService.setDatabase(database);
    return this;
  }

  @Override
  public void query(
      final Query query,
      final Consumer<QueryResult> onSuccess,
      final Consumer<Throwable> onFailure) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, BiConsumer<Cancellable, QueryResult> onNext) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(Query query, int chunkSize, Consumer<QueryResult> onNext, Runnable onComplete) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query,
      int chunkSize,
      BiConsumer<Cancellable, QueryResult> onNext,
      Runnable onComplete) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void query(
      Query query,
      int chunkSize,
      BiConsumer<Cancellable, QueryResult> onNext,
      Runnable onComplete,
      Consumer<Throwable> onFailure) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public QueryResult query(Query query, TimeUnit timeUnit) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public List<String> describeDatabases() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean databaseExists(final String name) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void flush() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void close() {
    influxDBService.close();
  }

  @Override
  public InfluxDB setConsistency(final ConsistencyLevel consistencyLevel) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setRetentionPolicy(final String retentionPolicy) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final String shardDuration,
      final int replicationFactor,
      final boolean isDefault) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final int replicationFactor,
      final boolean isDefault) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void createRetentionPolicy(
      final String rpName,
      final String database,
      final String duration,
      final String shardDuration,
      final int replicationFactor) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void dropRetentionPolicy(final String rpName, final String database) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB setLogLevel(LogLevel logLevel) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableGzip() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB disableGzip() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isGzipEnabled() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(BatchOptions batchOptions) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions, final int flushDuration, final TimeUnit flushDurationTimeUnit) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions,
      final int flushDuration,
      final TimeUnit flushDurationTimeUnit,
      final ThreadFactory threadFactory) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      int actions,
      int flushDuration,
      TimeUnit flushDurationTimeUnit,
      ThreadFactory threadFactory,
      BiConsumer<Iterable<Point>, Throwable> exceptionHandler,
      ConsistencyLevel consistency) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public InfluxDB enableBatch(
      final int actions,
      final int flushDuration,
      final TimeUnit flushDurationTimeUnit,
      final ThreadFactory threadFactory,
      final BiConsumer<Iterable<Point>, Throwable> exceptionHandler) {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public void disableBatch() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public boolean isBatchEnabled() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public Pong ping() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }

  @Override
  public String version() {
    throw new UnsupportedOperationException(InfluxDBConstant.METHOD_NOT_SUPPORTED);
  }
}
