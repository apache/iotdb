/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.session.template.Template;
import org.apache.iotdb.session.util.SystemStatus;
import org.apache.iotdb.session.util.Version;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;

import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public interface ISession {

  void setFetchSize(int fetchSize);

  int getFetchSize();

  Version getVersion();

  void setVersion(Version version);

  void open() throws IoTDBConnectionException;

  void open(boolean enableRPCCompression) throws IoTDBConnectionException;

  void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException;

  void close() throws IoTDBConnectionException;

  SystemStatus getSystemStatus() throws IoTDBConnectionException;

  String getTimeZone();

  void setTimeZone(String zoneId) throws StatementExecutionException, IoTDBConnectionException;

  void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException;

  void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException;

  void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException;

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

  void setQueryTimeout(long timeoutInMs);

  long getQueryTimeout();

  SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException;

  void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException;

  SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQuery(List<String> paths, long LastTime)
      throws StatementExecutionException, IoTDBConnectionException;

  SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException;

  void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException;

  String getTimestampPrecision() throws TException;

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

  void unsetSchemaTemplate(String prefixPath, String templateName)
      throws IoTDBConnectionException, StatementExecutionException;

  void createTimeseriesOfTemplateOnPath(String path)
      throws IoTDBConnectionException, StatementExecutionException;

  void deactivateTemplateOn(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException;

  void dropSchemaTemplate(String templateName)
      throws IoTDBConnectionException, StatementExecutionException;

  void operationSyncTransmit(ByteBuffer buffer)
      throws IoTDBConnectionException, StatementExecutionException;

  boolean isEnableQueryRedirection();

  void setEnableQueryRedirection(boolean enableQueryRedirection);

  boolean isEnableCacheLeader();

  void setEnableCacheLeader(boolean enableCacheLeader);

  TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException;

  TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException;
}
