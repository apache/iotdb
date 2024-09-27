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

package org.apache.iotdb.isession.pool;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.isession.IPooledSession;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.SystemStatus;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

public interface ISessionPool {

  int currentAvailableSize();

  int currentOccupiedSize();

  void close();

  void closeResultSet(SessionDataSetWrapper wrapper);

  void insertTablet(Tablet tablet) throws IoTDBConnectionException, StatementExecutionException;

  void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void insertOneDeviceRecords(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void insertOneDeviceRecords(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecord(
      String multiSeriesId,
      long time,
      List<String> multiMeasurementComponents,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  String getTimestampPrecision() throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecord(
      String multiSeriesId, long time, List<String> multiMeasurementComponents, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertTablet(Tablet tablet) throws IoTDBConnectionException, StatementExecutionException;

  void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertRecord(String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteTimeseries(String path) throws IoTDBConnectionException, StatementExecutionException;

  void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteData(String path, long time)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteData(List<String> paths, long time)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void deleteStorageGroups(List<String> storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  void createDatabase(String database) throws IoTDBConnectionException, StatementExecutionException;

  void deleteDatabase(String database) throws IoTDBConnectionException, StatementExecutionException;

  void deleteDatabases(List<String> databases)
      throws IoTDBConnectionException, StatementExecutionException;

  void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException;

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
  void createTimeseries(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException;

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
  void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
  void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList)
      throws IoTDBConnectionException, StatementExecutionException;

  @SuppressWarnings("squid:S107") // ignore Methods should not have too many parameters
  void createMultiTimeseries(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException;

  boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException;

  void createSchemaTemplate(Template template)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  void createSchemaTemplate(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated
   */
  @Deprecated
  void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException;

  void addAlignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  void addAlignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  void addUnalignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  void addUnalignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  void deleteNodeInTemplate(String templateName, String path)
      throws IOException, IoTDBConnectionException, StatementExecutionException;

  int countMeasurementsInTemplate(String name)
      throws StatementExecutionException, IoTDBConnectionException;

  boolean isMeasurementInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException;

  boolean isPathExistInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException;

  List<String> showMeasurementsInTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException;

  List<String> showMeasurementsInTemplate(String templateName, String pattern)
      throws StatementExecutionException, IoTDBConnectionException;

  List<String> showAllTemplates() throws StatementExecutionException, IoTDBConnectionException;

  List<String> showPathsTemplateSetOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException;

  List<String> showPathsTemplateUsingOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException;

  void setSchemaTemplate(String templateName, String prefixPath)
      throws StatementExecutionException, IoTDBConnectionException;

  void unsetSchemaTemplate(String prefixPath, String templateName)
      throws StatementExecutionException, IoTDBConnectionException;

  void dropSchemaTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException;

  void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  SessionDataSetWrapper executeQueryStatement(String sql, long timeoutInMs)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  void executeNonQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeRawDataQuery(
      List<String> paths, long startTime, long endTime, long timeOut)
      throws IoTDBConnectionException, StatementExecutionException;

  SessionDataSetWrapper executeLastDataQuery(List<String> paths, long lastTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSetWrapper executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval,
      long slidingStep)
      throws StatementExecutionException, IoTDBConnectionException;

  int getMaxSize();

  String getHost();

  int getPort();

  String getUser();

  String getPassword();

  void setFetchSize(int fetchSize);

  int getFetchSize();

  void setTimeZone(String zoneId) throws StatementExecutionException, IoTDBConnectionException;

  ZoneId getZoneId();

  long getWaitToGetSessionTimeoutInMs();

  boolean isEnableCompression();

  void setEnableRedirection(boolean enableRedirection);

  boolean isEnableRedirection();

  void setEnableQueryRedirection(boolean enableQueryRedirection);

  boolean isEnableQueryRedirection();

  int getConnectionTimeoutInMs();

  void sortTablet(Tablet tablet) throws IoTDBConnectionException;

  TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException;

  TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException;

  void setVersion(Version version);

  Version getVersion();

  void setQueryTimeout(long timeoutInMs);

  long getQueryTimeout();

  IPooledSession getPooledSession() throws IoTDBConnectionException;

  /**
   * @deprecated
   */
  @Deprecated
  default void createTimeseriesOfTemplateOnPath(String path)
      throws IoTDBConnectionException, StatementExecutionException {}

  /**
   * @deprecated
   */
  @Deprecated
  default void deactivateTempalte(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {}

  /**
   * @deprecated
   */
  @Deprecated
  default SessionDataSetWrapper executeRawDataQuery(
      List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    return null;
  }

  /**
   * @deprecated
   */
  @Deprecated
  default boolean operationSyncTransmit(ByteBuffer buffer)
      throws IoTDBConnectionException, StatementExecutionException {
    return false;
  }

  /**
   * @deprecated
   */
  @Deprecated
  default SystemStatus getSystemStatus() throws IoTDBConnectionException {
    return SystemStatus.NORMAL;
  }

  /**
   * @deprecated
   */
  @Deprecated
  default boolean isEnableCacheLeader() {
    return false;
  }
}
