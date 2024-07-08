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

package org.apache.iotdb.session.pool;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.session.Session;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * used for SessionPool.getSession need to do some other things like calling
 * cleanSessionAndMayThrowConnectionException in SessionPool while encountering connection exception
 * only need to putBack to SessionPool while closing
 */
public class SessionWrapper implements ISession {

  private static final Logger LOGGER = LoggerFactory.getLogger(SessionWrapper.class);

  private Session session;

  private final SessionPool sessionPool;

  private final AtomicBoolean closed;

  public SessionWrapper(Session session, SessionPool sessionPool) {
    this.session = session;
    this.sessionPool = sessionPool;
    this.closed = new AtomicBoolean(false);
  }

  @Override
  public Version getVersion() {
    return session.getVersion();
  }

  @Override
  public void setVersion(Version version) {
    session.setVersion(version);
  }

  @Override
  public int getFetchSize() {
    return session.getFetchSize();
  }

  @Override
  public void setFetchSize(int fetchSize) {
    session.setFetchSize(fetchSize);
  }

  @Override
  public void open() throws IoTDBConnectionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open(boolean enableRPCCompression) throws IoTDBConnectionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void open(
      boolean enableRPCCompression,
      int connectionTimeoutInMs,
      Map<String, TEndPoint> deviceIdToEndpoint,
      INodeSupplier nodeSupplier)
      throws IoTDBConnectionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IoTDBConnectionException {
    if (closed.compareAndSet(false, true)) {
      if (!Objects.equals(session.getDatabase(), sessionPool.database)
          && sessionPool.database != null) {
        try {
          session.executeNonQueryStatement("use " + sessionPool.database);
        } catch (StatementExecutionException e) {
          LOGGER.warn(
              "Failed to change back database by executing: use " + sessionPool.database, e);
        }
      }
      sessionPool.putBack(session);
      session = null;
    }
  }

  @Override
  public String getTimeZone() {
    return session.getTimeZone();
  }

  @Override
  public void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.setTimeZone(zoneId);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void setTimeZoneOfSession(String zoneId) {
    session.setTimeZoneOfSession(zoneId);
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.setStorageGroup(storageGroup);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteStorageGroup(storageGroup);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteStorageGroups(storageGroups);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createDatabase(database);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteDatabase(database);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteDatabases(List<String> databases)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteDatabases(databases);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createTimeseries(path, dataType, encoding, compressor);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createTimeseries(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createTimeseries(
          path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createAlignedTimeseries(
          deviceId, measurements, dataTypes, encodings, compressors, measurementAliasList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createAlignedTimeseries(
          deviceId,
          measurements,
          dataTypes,
          encodings,
          compressors,
          measurementAliasList,
          tagsList,
          attributesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createMultiTimeseries(
          paths,
          dataTypes,
          encodings,
          compressors,
          propsList,
          tagsList,
          attributesList,
          measurementAliasList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      return session.checkTimeseriesExists(path);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeQueryStatement(sql, timeoutInMs);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.executeNonQueryStatement(sql);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeRawDataQuery(
      List<String> paths, long startTime, long endTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeRawDataQuery(paths, startTime, endTime, timeOut);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeRawDataQuery(paths, startTime, endTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths, long lastTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeLastDataQuery(paths, lastTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeLastDataQuery(paths, lastTime, timeOut);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeLastDataQuery(paths);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeLastDataQueryForOneDevice(db, device, sensors, isLegalPathNodes);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeAggregationQuery(paths, aggregations);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeAggregationQuery(paths, aggregations, startTime, endTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeAggregationQuery(paths, aggregations, startTime, endTime, interval);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval,
      long slidingStep)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.executeAggregationQuery(
          paths, aggregations, startTime, endTime, interval, slidingStep);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecord(deviceId, time, measurements, types, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecord(deviceId, time, measurements, types, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecord(deviceId, time, measurements, types, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecord(deviceId, time, measurements, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public String getTimestampPrecision() throws TException {
    try {
      return session.getTimestampPrecision();
    } catch (TException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecord(deviceId, time, measurements, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecords(deviceIds, times, measurementsList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecords(deviceIds, times, measurementsList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, haveSorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertStringRecordsOfOneDevice(
          deviceId, times, measurementsList, valuesList, haveSorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedRecordsOfOneDevice(
          deviceId, times, measurementsList, typesList, valuesList, haveSorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedStringRecordsOfOneDevice(
          deviceId, times, measurementsList, valuesList, haveSorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.insertTablet(tablet);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertTablet(tablet, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      session.insertAlignedTablet(tablet);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedTablet(tablet, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertTablets(tablets);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertTablets(tablets, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedTablets(tablets);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.insertAlignedTablets(tablets, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertTablet(tablet);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertTablet(tablet, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertTablets(tablets);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertTablets(tablets, sorted);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertRecords(deviceIds, times, measurementsList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertRecord(deviceId, time, measurements, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.testInsertRecord(deviceId, time, measurements, types, values);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteTimeseries(path);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteTimeseries(paths);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteData(String path, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteData(path, endTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteData(List<String> paths, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteData(paths, endTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteData(paths, startTime, endTime);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.setSchemaTemplate(templateName, prefixPath);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createSchemaTemplate(Template template)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.createSchemaTemplate(template);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createSchemaTemplate(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.createSchemaTemplate(
          templateName, measurements, dataTypes, encodings, compressors, isAligned);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createSchemaTemplate(
          name, schemaNames, measurements, dataTypes, encodings, compressors);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void addAlignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.addAlignedMeasurementsInTemplate(
          templateName, measurementsPath, dataTypes, encodings, compressors);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void addAlignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.addAlignedMeasurementInTemplate(
          templateName, measurementPath, dataType, encoding, compressor);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void addUnalignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.addUnalignedMeasurementsInTemplate(
          templateName, measurementsPath, dataTypes, encodings, compressors);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void addUnalignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.addUnalignedMeasurementInTemplate(
          templateName, measurementPath, dataType, encoding, compressor);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void deleteNodeInTemplate(String templateName, String path)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    try {
      session.deleteNodeInTemplate(templateName, path);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public int countMeasurementsInTemplate(String name)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.countMeasurementsInTemplate(name);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.isMeasurementInTemplate(templateName, path);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public boolean isPathExistInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.isPathExistInTemplate(templateName, path);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public List<String> showMeasurementsInTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.showMeasurementsInTemplate(templateName);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public List<String> showMeasurementsInTemplate(String templateName, String pattern)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.showMeasurementsInTemplate(templateName, pattern);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public List<String> showAllTemplates()
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.showAllTemplates();
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public List<String> showPathsTemplateSetOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.showPathsTemplateSetOn(templateName);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public List<String> showPathsTemplateUsingOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return session.showPathsTemplateUsingOn(templateName);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void unsetSchemaTemplate(String prefixPath, String templateName)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.unsetSchemaTemplate(prefixPath, templateName);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void dropSchemaTemplate(String templateName)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.dropSchemaTemplate(templateName);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      session.createTimeseriesUsingSchemaTemplate(devicePathList);
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public boolean isEnableQueryRedirection() {
    return session.isEnableQueryRedirection();
  }

  @Override
  public void setEnableQueryRedirection(boolean enableQueryRedirection) {
    session.setEnableQueryRedirection(enableQueryRedirection);
  }

  @Override
  public boolean isEnableRedirection() {
    return session.isEnableRedirection();
  }

  @Override
  public void setEnableRedirection(boolean enableRedirection) {
    session.setEnableRedirection(enableRedirection);
  }

  @Override
  public void sortTablet(Tablet tablet) {
    session.sortTablet(tablet);
  }

  @Override
  public TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      return session.getBackupConfiguration();
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {
    try {
      return session.fetchAllConnections();
    } catch (IoTDBConnectionException e) {
      sessionPool.cleanSessionAndMayThrowConnectionException(session);
      closed.set(true);
      session = null;
      throw e;
    }
  }

  @Override
  public void setQueryTimeout(long timeoutInMs) {
    session.setQueryTimeout(timeoutInMs);
  }

  @Override
  public long getQueryTimeout() {
    return session.getQueryTimeout();
  }
}
