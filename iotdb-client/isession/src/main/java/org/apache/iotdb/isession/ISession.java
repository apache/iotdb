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

package org.apache.iotdb.isession;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.SystemStatus;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface ISession extends AutoCloseable {

  Version getVersion();

  void setVersion(Version version);

  int getFetchSize();

  void setFetchSize(int fetchSize);

  void open() throws IoTDBConnectionException;

  void open(boolean enableRPCCompression) throws IoTDBConnectionException;

  void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException;

  void open(
      boolean enableRPCCompression,
      int connectionTimeoutInMs,
      Map<String, TEndPoint> deviceIdToEndpoint,
      INodeSupplier nodeSupplier)
      throws IoTDBConnectionException;

  void open(
      boolean enableRPCCompression,
      int connectionTimeoutInMs,
      Map<String, TEndPoint> deviceIdToEndpoint,
      Map<IDeviceID, TEndPoint> tabletModelDeviceIdToEndpoint,
      INodeSupplier nodeSupplier)
      throws IoTDBConnectionException;

  void close() throws IoTDBConnectionException;

  String getTimeZone();

  void setTimeZone(String zoneId) throws StatementExecutionException, IoTDBConnectionException;

  void setTimeZoneOfSession(String zoneId);

  /**
   * @deprecated Use {@link #createDatabase(String)} instead.
   */
  @Deprecated
  void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated Use {@link #deleteDatabase(String)} instead.
   */
  @Deprecated
  void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  /**
   * @deprecated Use {@link #deleteDatabases(List)} instead.
   */
  @Deprecated
  void deleteStorageGroups(List<String> storageGroups)
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

  void createAlignedTimeseries(
      String deviceId,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException;

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

  SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException;

  void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQuery(List<String> paths, long lastTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeAggregationQuery(List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeAggregationQuery(
      List<String> paths,
      List<TAggregationType> aggregations,
      long startTime,
      long endTime,
      long interval,
      long slidingStep)
      throws StatementExecutionException, IoTDBConnectionException;

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
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecord(String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  String getTimestampPrecision() throws TException;

  void insertAlignedRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
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

  void insertRecordsOfOneDevice(
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

  void insertStringRecordsOfOneDevice(
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
      List<List<Object>> valuesList)
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

  void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablet(Tablet tablet) throws StatementExecutionException, IoTDBConnectionException;

  void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException;

  void insertAlignedTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException;

  void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
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

  void deleteData(String path, long endTime)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteData(List<String> paths, long endTime)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException;

  void setSchemaTemplate(String templateName, String prefixPath)
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

  void unsetSchemaTemplate(String prefixPath, String templateName)
      throws IoTDBConnectionException, StatementExecutionException;

  void dropSchemaTemplate(String templateName)
      throws IoTDBConnectionException, StatementExecutionException;

  void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException;

  boolean isEnableQueryRedirection();

  void setEnableQueryRedirection(boolean enableQueryRedirection);

  boolean isEnableRedirection();

  void setEnableRedirection(boolean enableRedirection);

  void sortTablet(Tablet tablet);

  TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException;

  TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException;

  void setQueryTimeout(long timeoutInMs);

  long getQueryTimeout();

  /**
   * @deprecated
   */
  @Deprecated
  default SystemStatus getSystemStatus() {
    return SystemStatus.NORMAL;
  }

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
  default void deactivateTemplateOn(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {}

  /**
   * @deprecated
   */
  @Deprecated
  default void operationSyncTransmit(ByteBuffer buffer)
      throws IoTDBConnectionException, StatementExecutionException {}

  /**
   * @deprecated
   */
  @Deprecated
  default boolean isEnableCacheLeader() {
    return true;
  }

  /**
   * @deprecated
   */
  @Deprecated
  default void setEnableCacheLeader(boolean enableCacheLeader) {}
}
