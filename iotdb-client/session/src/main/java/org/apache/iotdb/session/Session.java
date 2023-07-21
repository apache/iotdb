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

package org.apache.iotdb.session;

import org.apache.iotdb.common.rpc.thrift.TAggregationType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.isession.template.Template;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.NoValidValueException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TCreateTimeseriesUsingSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSAppendSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSBackupConfigurationResp;
import org.apache.iotdb.service.rpc.thrift.TSConnectionInfoResp;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSDropSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSPruneSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSQueryTemplateResp;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.session.template.MeasurementNode;
import org.apache.iotdb.session.template.TemplateQueryType;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.session.util.ThreadUtils;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@SuppressWarnings({"java:S107", "java:S1135"}) // need enough parameters, ignore todos
public class Session implements ISession {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  protected static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  public static final String MSG_UNSUPPORTED_DATA_TYPE = "Unsupported data type:";
  public static final String MSG_DONOT_ENABLE_REDIRECT =
      "Query do not enable redirect," + " please confirm the session and server conf.";
  private static final ThreadPoolExecutor OPERATION_EXECUTOR =
      new ThreadPoolExecutor(
          SessionConfig.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          SessionConfig.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          0,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(SessionConfig.DEFAULT_SESSION_EXECUTOR_TASK_NUM),
          ThreadUtils.createThreadFactory("SessionExecutor", true));
  protected List<String> nodeUrls;
  protected String username;
  protected String password;
  protected int fetchSize;
  /**
   * Timeout of query can be set by users. A negative number means using the default configuration
   * of server. And value 0 will disable the function of query timeout.
   */
  private long queryTimeoutInMs = -1;

  protected boolean enableRPCCompression;
  protected int connectionTimeoutInMs;
  protected ZoneId zoneId;

  protected int thriftDefaultBufferSize;
  protected int thriftMaxFrameSize;

  protected TEndPoint defaultEndPoint;
  protected SessionConnection defaultSessionConnection;
  private boolean isClosed = true;

  // Cluster version cache
  protected boolean enableRedirection;

  @SuppressWarnings("squid:S3077") // Non-primitive fields should not be "volatile"
  protected volatile Map<String, TEndPoint> deviceIdToEndpoint;

  @SuppressWarnings("squid:S3077") // Non-primitive fields should not be "volatile"
  protected volatile Map<TEndPoint, SessionConnection> endPointToSessionConnection;

  protected boolean enableQueryRedirection = false;

  // The version number of the client which used for compatibility in the server
  protected Version version;

  private static final String REDIRECT_TWICE = "redirect twice";

  private static final String REDIRECT_TWICE_RETRY = "redirect twice, please try again.";

  private static final String VALUES_SIZE_SHOULD_BE_EQUAL =
      "times, measurementsList and valuesList's size should be equal";

  private static final String SESSION_CANNOT_CONNECT = "Session can not connect to {}";

  private static final String ALL_VALUES_ARE_NULL =
      "All values are null and this submission is ignored,deviceId is [{}],time is [{}],measurements is [{}]";

  private static final String ALL_VALUES_ARE_NULL_WITH_TIME =
      "All values are null and this submission is ignored,deviceId is [{}],times are [{}],measurements are [{}]";
  private static final String ALL_VALUES_ARE_NULL_MULTI_DEVICES =
      "All values are null and this submission is ignored,deviceIds are [{}],times are [{}],measurements are [{}]";
  private static final String ALL_INSERT_DATA_IS_NULL = "All inserted data is null.";

  public Session(String host, int rpcPort) {
    this(
        host,
        rpcPort,
        SessionConfig.DEFAULT_USER,
        SessionConfig.DEFAULT_PASSWORD,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(String host, String rpcPort, String username, String password) {
    this(
        host,
        Integer.parseInt(rpcPort),
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(String host, int rpcPort, String username, String password) {
    this(
        host,
        rpcPort,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(String host, int rpcPort, String username, String password, int fetchSize) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      long queryTimeoutInMs) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
    this.queryTimeoutInMs = queryTimeoutInMs;
  }

  public Session(String host, int rpcPort, String username, String password, ZoneId zoneId) {
    this(
        host,
        rpcPort,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        zoneId,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(
      String host, int rpcPort, String username, String password, boolean enableRedirection) {
    this(
        host,
        rpcPort,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        enableRedirection,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      boolean enableRedirection) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        zoneId,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        enableRedirection,
        SessionConfig.DEFAULT_VERSION);
  }

  @SuppressWarnings("squid:S107")
  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    this.defaultEndPoint = new TEndPoint(host, rpcPort);
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.enableRedirection = enableRedirection;
    this.version = version;
  }

  public Session(List<String> nodeUrls, String username, String password) {
    this(
        nodeUrls,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  /**
   * Multiple nodeUrl,If one node down, connect to the next one
   *
   * @param nodeUrls List<String> Multiple ip:rpcPort eg.127.0.0.1:9001
   */
  public Session(List<String> nodeUrls, String username, String password, int fetchSize) {
    this(
        nodeUrls,
        username,
        password,
        fetchSize,
        null,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(List<String> nodeUrls, String username, String password, ZoneId zoneId) {
    this(
        nodeUrls,
        username,
        password,
        SessionConfig.DEFAULT_FETCH_SIZE,
        zoneId,
        SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY,
        SessionConfig.DEFAULT_MAX_FRAME_SIZE,
        SessionConfig.DEFAULT_REDIRECTION_MODE,
        SessionConfig.DEFAULT_VERSION);
  }

  public Session(
      List<String> nodeUrls,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableRedirection,
      Version version) {
    this.nodeUrls = nodeUrls;
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.enableRedirection = enableRedirection;
    this.version = version;
  }

  @Override
  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  @Override
  public int getFetchSize() {
    return this.fetchSize;
  }

  @Override
  public Version getVersion() {
    return version;
  }

  @Override
  public void setVersion(Version version) {
    this.version = version;
  }

  @Override
  public synchronized void open() throws IoTDBConnectionException {
    open(false, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  @Override
  public synchronized void open(boolean enableRPCCompression) throws IoTDBConnectionException {
    open(enableRPCCompression, SessionConfig.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  @Override
  public synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }

    this.enableRPCCompression = enableRPCCompression;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    defaultSessionConnection = constructSessionConnection(this, defaultEndPoint, zoneId);
    defaultSessionConnection.setEnableRedirect(enableQueryRedirection);
    isClosed = false;
    if (enableRedirection || enableQueryRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
      endPointToSessionConnection = new ConcurrentHashMap<>();
      endPointToSessionConnection.put(defaultEndPoint, defaultSessionConnection);
    }
  }

  @Override
  public synchronized void open(
      boolean enableRPCCompression,
      int connectionTimeoutInMs,
      Map<String, TEndPoint> deviceIdToEndpoint)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }

    this.enableRPCCompression = enableRPCCompression;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    defaultSessionConnection = constructSessionConnection(this, defaultEndPoint, zoneId);
    defaultSessionConnection.setEnableRedirect(enableQueryRedirection);
    isClosed = false;
    if (enableRedirection || enableQueryRedirection) {
      this.deviceIdToEndpoint = deviceIdToEndpoint;
      endPointToSessionConnection = new ConcurrentHashMap<>();
      endPointToSessionConnection.put(defaultEndPoint, defaultSessionConnection);
    }
  }

  @Override
  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }
    try {
      if (enableRedirection) {
        for (SessionConnection sessionConnection : endPointToSessionConnection.values()) {
          sessionConnection.close();
        }
      } else {
        defaultSessionConnection.close();
      }
    } finally {
      isClosed = true;
    }
  }

  public SessionConnection constructSessionConnection(
      Session session, TEndPoint endpoint, ZoneId zoneId) throws IoTDBConnectionException {
    if (endpoint == null) {
      return new SessionConnection(session, zoneId);
    }
    return new SessionConnection(session, endpoint, zoneId);
  }

  @Override
  public synchronized String getTimeZone() {
    return defaultSessionConnection.getTimeZone();
  }

  @Override
  public synchronized void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    defaultSessionConnection.setTimeZone(zoneId);
    this.zoneId = ZoneId.of(zoneId);
  }

  /** Only changes the member variable of the Session object without sending it to server. */
  @Override
  public void setTimeZoneOfSession(String zoneId) {
    defaultSessionConnection.setTimeZoneOfSession(zoneId);
    this.zoneId = ZoneId.of(zoneId);
  }

  @Override
  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.setStorageGroup(storageGroup);
  }

  @Override
  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteStorageGroups(Collections.singletonList(storageGroup));
  }

  @Override
  public void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteStorageGroups(storageGroups);
  }

  @Override
  public void createDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.setStorageGroup(database);
  }

  @Override
  public void deleteDatabase(String database)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteStorageGroups(Collections.singletonList(database));
  }

  @Override
  public void deleteDatabases(List<String> databases)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteStorageGroups(databases);
  }

  @Override
  public void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateTimeseriesReq request =
        genTSCreateTimeseriesReq(path, dataType, encoding, compressor, null, null, null, null);
    defaultSessionConnection.createTimeseries(request);
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
    TSCreateTimeseriesReq request =
        genTSCreateTimeseriesReq(
            path, dataType, encoding, compressor, props, tags, attributes, measurementAlias);
    defaultSessionConnection.createTimeseries(request);
  }

  private TSCreateTimeseriesReq genTSCreateTimeseriesReq(
      String path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String measurementAlias) {
    TSCreateTimeseriesReq request = new TSCreateTimeseriesReq();
    request.setPath(path);
    request.setDataType(dataType.ordinal());
    request.setEncoding(encoding.ordinal());
    request.setCompressor(compressor.serialize());
    request.setProps(props);
    request.setTags(tags);
    request.setAttributes(attributes);
    request.setMeasurementAlias(measurementAlias);
    return request;
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
    TSCreateAlignedTimeseriesReq request =
        getTSCreateAlignedTimeseriesReq(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            null,
            null);
    defaultSessionConnection.createAlignedTimeseries(request);
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
    TSCreateAlignedTimeseriesReq request =
        getTSCreateAlignedTimeseriesReq(
            deviceId,
            measurements,
            dataTypes,
            encodings,
            compressors,
            measurementAliasList,
            tagsList,
            attributesList);
    defaultSessionConnection.createAlignedTimeseries(request);
  }

  private TSCreateAlignedTimeseriesReq getTSCreateAlignedTimeseriesReq(
      String prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<String> measurementAliasList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList) {
    TSCreateAlignedTimeseriesReq request = new TSCreateAlignedTimeseriesReq();
    request.setPrefixPath(prefixPath);
    request.setMeasurements(measurements);
    request.setDataTypes(dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    request.setEncodings(encodings.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    request.setCompressors(
        compressors.stream().map(i -> (int) i.serialize()).collect(Collectors.toList()));
    request.setMeasurementAlias(measurementAliasList);
    request.setTagsList(tagsList);
    request.setAttributesList(attributesList);
    return request;
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
    TSCreateMultiTimeseriesReq request =
        genTSCreateMultiTimeseriesReq(
            paths,
            dataTypes,
            encodings,
            compressors,
            propsList,
            tagsList,
            attributesList,
            measurementAliasList);
    defaultSessionConnection.createMultiTimeseries(request);
  }

  private TSCreateMultiTimeseriesReq genTSCreateMultiTimeseriesReq(
      List<String> paths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> propsList,
      List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList,
      List<String> measurementAliasList) {
    TSCreateMultiTimeseriesReq request = new TSCreateMultiTimeseriesReq();

    request.setPaths(paths);

    List<Integer> dataTypeOrdinals = new ArrayList<>(dataTypes.size());
    for (TSDataType dataType : dataTypes) {
      dataTypeOrdinals.add(dataType.ordinal());
    }
    request.setDataTypes(dataTypeOrdinals);

    List<Integer> encodingOrdinals = new ArrayList<>(dataTypes.size());
    for (TSEncoding encoding : encodings) {
      encodingOrdinals.add(encoding.ordinal());
    }
    request.setEncodings(encodingOrdinals);

    List<Integer> compressionOrdinals = new ArrayList<>(paths.size());
    for (CompressionType compression : compressors) {
      compressionOrdinals.add((int) compression.serialize());
    }
    request.setCompressors(compressionOrdinals);

    request.setPropsList(propsList);
    request.setTagsList(tagsList);
    request.setAttributesList(attributesList);
    request.setMeasurementAliasList(measurementAliasList);

    return request;
  }

  @Override
  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    return defaultSessionConnection.checkTimeseriesExists(path, queryTimeoutInMs);
  }

  @Override
  public void setQueryTimeout(long timeoutInMs) {
    this.queryTimeoutInMs = timeoutInMs;
  }

  @Override
  public long getQueryTimeout() {
    return queryTimeoutInMs;
  }

  /**
   * execute query sql
   *
   * @param sql query statement
   * @return result set
   */
  @Override
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    return executeStatementMayRedirect(sql, queryTimeoutInMs);
  }

  /**
   * execute query sql with explicit timeout
   *
   * @param sql query statement
   * @param timeoutInMs the timeout of this query, in milliseconds
   * @return result set
   */
  @Override
  public SessionDataSet executeQueryStatement(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    return executeStatementMayRedirect(sql, timeoutInMs);
  }

  /**
   * execute the query, may redirect query to other node.
   *
   * @param sql the query statement
   * @param timeoutInMs time in ms
   * @return data set
   * @throws StatementExecutionException statement is not right
   * @throws IoTDBConnectionException the network is not good
   */
  private SessionDataSet executeStatementMayRedirect(String sql, long timeoutInMs)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeQueryStatement(sql, timeoutInMs);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeQueryStatement(sql, queryTimeoutInMs);
        } catch (RedirectException redirectException) {
          logger.error("{} redirect twice", sql, redirectException);
          throw new StatementExecutionException(sql + " redirect twice, please try again.");
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  @Override
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.executeNonQueryStatement(sql);
  }

  /**
   * query eg. select * from paths where time >= startTime and time < endTime time interval include
   * startTime and exclude endTime
   *
   * @param paths series path
   * @param startTime included
   * @param endTime excluded
   * @return data set
   * @throws StatementExecutionException statement is not right
   * @throws IoTDBConnectionException the network is not good
   */
  @Override
  public SessionDataSet executeRawDataQuery(
      List<String> paths, long startTime, long endTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime, timeOut);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime, timeOut);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  @Override
  public SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    return executeRawDataQuery(paths, startTime, endTime, queryTimeoutInMs);
  }

  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths, long lastTime)
      throws StatementExecutionException, IoTDBConnectionException {
    return executeLastDataQuery(paths, lastTime, queryTimeoutInMs);
  }

  /**
   * query e.g. select last data from paths where time >= lastTime
   *
   * @param paths timeSeries eg. root.ln.d1.s1,root.ln.d1.s2
   * @param lastTime get the last data, whose timestamp is greater than or equal lastTime e.g.
   *     1621326244168
   */
  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths, long lastTime, long timeOut)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeLastDataQuery(paths, lastTime, timeOut);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeLastDataQuery(paths, lastTime, timeOut);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * query eg. select last status from root.ln.wf01.wt01; <PrefixPath> + <suffixPath> = <TimeSeries>
   *
   * @param paths timeSeries. eg.root.ln.d1.s1,root.ln.d1.s2
   */
  @Override
  public SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException {
    long time = 0L;
    return executeLastDataQuery(paths, time, queryTimeoutInMs);
  }

  @Override
  public SessionDataSet executeLastDataQueryForOneDevice(
      String db, String device, List<String> sensors, boolean isLegalPathNodes)
      throws StatementExecutionException, IoTDBConnectionException {
    Pair<SessionDataSet, TEndPoint> pair;
    try {
      pair =
          getSessionConnection(device)
              .executeLastDataQueryForOneDevice(
                  db, device, sensors, isLegalPathNodes, queryTimeoutInMs);
      if (pair.right != null) {
        handleRedirection(device, pair.right);
      }
      return pair.left;
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(device) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(device));
        deviceIdToEndpoint.remove(device);

        // reconnect with default connection
        return defaultSessionConnection.executeLastDataQueryForOneDevice(
                db, device, sensors, isLegalPathNodes, queryTimeoutInMs)
            .left;
      } else {
        throw e;
      }
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeAggregationQuery(paths, aggregations);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeAggregationQuery(paths, aggregations);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  @Override
  public SessionDataSet executeAggregationQuery(
      List<String> paths, List<TAggregationType> aggregations, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeAggregationQuery(
          paths, aggregations, startTime, endTime);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeAggregationQuery(
              paths, aggregations, startTime, endTime);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
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
      return defaultSessionConnection.executeAggregationQuery(
          paths, aggregations, startTime, endTime, interval);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeAggregationQuery(
              paths, aggregations, startTime, endTime, interval);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
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
      return defaultSessionConnection.executeAggregationQuery(
          paths, aggregations, startTime, endTime, interval, slidingStep);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeAggregationQuery(
              paths, aggregations, startTime, endTime, interval, slidingStep);
        } catch (RedirectException redirectException) {
          logger.error(REDIRECT_TWICE, redirectException);
          throw new StatementExecutionException(REDIRECT_TWICE_RETRY);
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * insert data in one row, if you want to improve your performance, please use insertRecords
   * method or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request;
    try {
      request =
          filterAndGenTSInsertRecordReq(
              deviceId, time, measurements, types, Arrays.asList(values), false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }

    insertRecord(deviceId, request);
  }

  private void insertRecord(String prefixPath, TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(prefixPath).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(prefixPath, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(prefixPath) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(prefixPath));
        deviceIdToEndpoint.remove(prefixPath);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertRecord(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertRecord fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  private void insertRecord(String deviceId, TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(deviceId));
        deviceIdToEndpoint.remove(deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertRecord(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertRecord fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  private SessionConnection getSessionConnection(String deviceId) {
    TEndPoint endPoint;
    if (enableRedirection
        && !deviceIdToEndpoint.isEmpty()
        && (endPoint = deviceIdToEndpoint.get(deviceId)) != null
        && endPointToSessionConnection.containsKey(endPoint)) {
      return endPointToSessionConnection.get(endPoint);
    } else {
      return defaultSessionConnection;
    }
  }

  @Override
  public String getTimestampPrecision() throws TException {
    return defaultSessionConnection.getClient().getProperties().getTimestampPrecision();
  }

  // TODO https://issues.apache.org/jira/browse/IOTDB-1399
  private void removeBrokenSessionConnection(SessionConnection sessionConnection) {
    // remove the cached broken leader session
    if (enableRedirection) {
      TEndPoint endPoint = null;
      for (Iterator<Entry<TEndPoint, SessionConnection>> it =
              endPointToSessionConnection.entrySet().iterator();
          it.hasNext(); ) {
        Entry<TEndPoint, SessionConnection> entry = it.next();
        if (entry.getValue().equals(sessionConnection)) {
          endPoint = entry.getKey();
          it.remove();
          break;
        }
      }

      for (Iterator<Entry<String, TEndPoint>> it = deviceIdToEndpoint.entrySet().iterator();
          it.hasNext(); ) {
        Entry<String, TEndPoint> entry = it.next();
        if (entry.getValue().equals(endPoint)) {
          it.remove();
        }
      }
    }
  }

  private void handleRedirection(String deviceId, TEndPoint endpoint) {
    if (enableRedirection) {
      // no need to redirection
      if (endpoint.ip.equals("0.0.0.0")) {
        return;
      }
      AtomicReference<IoTDBConnectionException> exceptionReference = new AtomicReference<>();
      if (!deviceIdToEndpoint.containsKey(deviceId)
          || !deviceIdToEndpoint.get(deviceId).equals(endpoint)) {
        deviceIdToEndpoint.put(deviceId, endpoint);
      }
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              endpoint,
              k -> {
                try {
                  return constructSessionConnection(this, endpoint, zoneId);
                } catch (IoTDBConnectionException ex) {
                  exceptionReference.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        deviceIdToEndpoint.remove(deviceId);
        logger.warn("Can not redirect to {}, because session can not connect to it.", endpoint);
      }
    }
  }

  private void handleQueryRedirection(TEndPoint endPoint) throws IoTDBConnectionException {
    if (enableQueryRedirection) {
      AtomicReference<IoTDBConnectionException> exceptionReference = new AtomicReference<>();
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              endPoint,
              k -> {
                try {
                  SessionConnection sessionConnection =
                      constructSessionConnection(this, endPoint, zoneId);
                  sessionConnection.setEnableRedirect(enableQueryRedirection);
                  return sessionConnection;
                } catch (IoTDBConnectionException ex) {
                  exceptionReference.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        throw new IoTDBConnectionException(exceptionReference.get());
      }
      defaultSessionConnection = connection;
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    // not vector by default
    TSInsertRecordReq request;
    try {
      request = filterAndGenTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }

    insertRecord(deviceId, request);
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request;
    try {
      request = filterAndGenTSInsertRecordReq(deviceId, time, measurements, types, values, true);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }
    insertRecord(deviceId, request);
  }

  private TSInsertRecordReq filterAndGenTSInsertRecordReq(
      String prefixPath,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values,
      boolean isAligned)
      throws IoTDBConnectionException {
    if (hasNull(values)) {
      measurements = new ArrayList<>(measurements);
      values = new ArrayList<>(values);
      types = new ArrayList<>(types);
      boolean isAllValuesNull =
          filterNullValueAndMeasurement(prefixPath, measurements, types, values);
      if (isAllValuesNull) {
        throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
      }
    }
    return genTSInsertRecordReq(prefixPath, time, measurements, types, values, isAligned);
  }

  private TSInsertRecordReq genTSInsertRecordReq(
      String prefixPath,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values,
      boolean isAligned)
      throws IoTDBConnectionException {
    TSInsertRecordReq request = new TSInsertRecordReq();
    request.setPrefixPath(prefixPath);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    ByteBuffer buffer = SessionUtils.getValueBuffer(types, values);
    request.setValues(buffer);
    request.setIsAligned(isAligned);
    return request;
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertRecords method
   * or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request;
    try {
      request = filterAndGenTSInsertStringRecordReq(deviceId, time, measurements, values, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }
    insertRecord(deviceId, request);
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request;
    try {
      request = filterAndGenTSInsertStringRecordReq(deviceId, time, measurements, values, true);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }
    insertRecord(deviceId, request);
  }

  private TSInsertStringRecordReq filterAndGenTSInsertStringRecordReq(
      String prefixPath,
      long time,
      List<String> measurements,
      List<String> values,
      boolean isAligned) {
    if (hasNull(values)) {
      measurements = new ArrayList<>(measurements);
      values = new ArrayList<>(values);
      boolean isAllValueNull =
          filterNullValueAndMeasurementWithStringType(values, prefixPath, measurements);
      if (isAllValueNull) {
        throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
      }
    }
    return genTSInsertStringRecordReq(prefixPath, time, measurements, values, isAligned);
  }

  private TSInsertStringRecordReq genTSInsertStringRecordReq(
      String prefixPath,
      long time,
      List<String> measurements,
      List<String> values,
      boolean isAligned) {
    TSInsertStringRecordReq request = new TSInsertStringRecordReq();
    request.setPrefixPath(prefixPath);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);
    request.setIsAligned(isAligned);
    return request;
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }
    if (enableRedirection) {
      insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, false);
    } else {
      TSInsertStringRecordsReq request;
      try {
        request =
            filterAndGenTSInsertStringRecordsReq(
                deviceIds, times, measurementsList, valuesList, false);
      } catch (NoValidValueException e) {
        logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
        return;
      }
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertRecords fail:{}", ignored.getMessage());
      }
    }
  }

  /**
   * When the value is null,filter this,don't use this measurement.
   *
   * @param times
   * @param measurementsList
   * @param valuesList
   * @param typesList
   */
  private void filterNullValueAndMeasurement(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<Object>> valuesList,
      List<List<TSDataType>> typesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      List<Object> values = valuesList.get(i);
      List<String> measurements = measurementsList.get(i);
      List<TSDataType> types = typesList.get(i);
      boolean isAllValuesNull =
          filterNullValueAndMeasurement(deviceIds.get(i), measurements, types, values);
      if (isAllValuesNull) {
        valuesList.remove(i);
        measurementsList.remove(i);
        deviceIds.remove(i);
        times.remove(i);
        typesList.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
    }
  }

  /**
   * Filter the null value of list。
   *
   * @param deviceId
   * @param times
   * @param measurementsList
   * @param typesList
   * @param valuesList
   */
  private void filterNullValueAndMeasurementOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      List<Object> values = valuesList.get(i);
      List<String> measurements = measurementsList.get(i);
      List<TSDataType> types = typesList.get(i);
      boolean isAllValuesNull =
          filterNullValueAndMeasurement(deviceId, measurements, types, values);
      if (isAllValuesNull) {
        valuesList.remove(i);
        measurementsList.remove(i);
        typesList.remove(i);
        times.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
    }
  }

  /**
   * Filter the null value of list。
   *
   * @param times
   * @param deviceId
   * @param measurementsList
   * @param valuesList
   */
  private void filterNullValueAndMeasurementWithStringTypeOfOneDevice(
      List<Long> times,
      String deviceId,
      List<List<String>> measurementsList,
      List<List<String>> valuesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      List<String> values = valuesList.get(i);
      List<String> measurements = measurementsList.get(i);
      boolean isAllValuesNull =
          filterNullValueAndMeasurementWithStringType(values, deviceId, measurements);
      if (isAllValuesNull) {
        valuesList.remove(i);
        measurementsList.remove(i);
        times.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
    }
  }

  /**
   * Filter the null object of list。
   *
   * @param deviceId
   * @param measurementsList
   * @param types
   * @param valuesList
   * @return true:all value is null;false:not all null value is null.
   */
  private boolean filterNullValueAndMeasurement(
      String deviceId,
      List<String> measurementsList,
      List<TSDataType> types,
      List<Object> valuesList) {
    Map<String, Object> nullMap = new HashMap<>();
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      if (valuesList.get(i) == null) {
        nullMap.put(measurementsList.get(i), valuesList.get(i));
        valuesList.remove(i);
        measurementsList.remove(i);
        types.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      logger.info("All values of the {} are null,null values are {}", deviceId, nullMap);
      return true;
    } else {
      logger.info("Some values of {} are null,null values are {}", deviceId, nullMap);
    }
    return false;
  }

  /**
   * Filter the null object of list。
   *
   * @param prefixPaths devices path。
   * @param times
   * @param measurementsList
   * @param valuesList
   * @return true:all values of valuesList are null;false:Not all values of valuesList are null.
   */
  private void filterNullValueAndMeasurementWithStringType(
      List<String> prefixPaths,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList) {
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      List<String> values = valuesList.get(i);
      List<String> measurements = measurementsList.get(i);
      boolean isAllValueNull =
          filterNullValueAndMeasurementWithStringType(values, prefixPaths.get(i), measurements);
      if (isAllValueNull) {
        valuesList.remove(i);
        measurementsList.remove(i);
        times.remove(i);
        prefixPaths.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
    }
  }

  /**
   * When the value is null,filter this,don't use this measurement.
   *
   * @param valuesList
   * @param measurementsList
   * @return true:all value is null;false:not all null value is null.
   */
  private boolean filterNullValueAndMeasurementWithStringType(
      List<String> valuesList, String deviceId, List<String> measurementsList) {
    Map<String, Object> nullMap = new HashMap<>();
    for (int i = valuesList.size() - 1; i >= 0; i--) {
      if (valuesList.get(i) == null) {
        nullMap.put(measurementsList.get(i), valuesList.get(i));
        valuesList.remove(i);
        measurementsList.remove(i);
      }
    }
    if (valuesList.isEmpty()) {
      logger.info("All values of the {} are null,null values are {}", deviceId, nullMap);
      return true;
    } else {
      logger.info("Some values of {} are null,null values are {}", deviceId, nullMap);
    }
    return false;
  }

  private boolean hasNull(List valuesList) {
    boolean haveNull = false;
    for (int i1 = 0; i1 < valuesList.size(); i1++) {
      Object o = valuesList.get(i1);
      if (o instanceof List) {
        List o1 = (List) o;
        if (hasNull(o1)) {
          haveNull = true;
          break;
        }
      } else {
        if (o == null) {
          haveNull = true;
          break;
        }
      }
    }
    return haveNull;
  }

  /**
   * Insert aligned multiple rows, which can reduce the overhead of network. This method is just
   * like jdbc executeBatch, we pack some insert request in batch and send them to server. If you
   * want improve your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different prefixPath, time, number of
   * subMeasurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "prefixPaths, times, subMeasurementsList and valuesList's size should be equal");
    }
    if (enableRedirection) {
      insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, true);
    } else {
      TSInsertStringRecordsReq request;
      try {
        request =
            filterAndGenTSInsertStringRecordsReq(
                deviceIds, times, measurementsList, valuesList, true);
      } catch (NoValidValueException e) {
        logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
        return;
      }

      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertRecords fail:{}", ignored.getMessage());
      }
    }
  }

  private void insertStringRecordsWithLeaderCache(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<SessionConnection, TSInsertStringRecordsReq> recordsGroup = new HashMap<>();
    for (int i = 0; i < deviceIds.size(); i++) {
      final SessionConnection connection = getSessionConnection(deviceIds.get(i));
      TSInsertStringRecordsReq request =
          recordsGroup.getOrDefault(connection, new TSInsertStringRecordsReq());
      request.setIsAligned(isAligned);
      try {
        filterAndUpdateTSInsertStringRecordsReq(
            request, deviceIds.get(i), times.get(i), measurementsList.get(i), valuesList.get(i));
        recordsGroup.putIfAbsent(connection, request);
      } catch (NoValidValueException e) {
        logger.warn(
            ALL_VALUES_ARE_NULL,
            deviceIds.get(i),
            times.get(i),
            measurementsList.get(i).toString());
      }
    }

    insertByGroup(recordsGroup, SessionConnection::insertRecords);
  }

  private TSInsertStringRecordsReq filterAndGenTSInsertStringRecordsReq(
      List<String> prefixPaths,
      List<Long> time,
      List<List<String>> measurements,
      List<List<String>> values,
      boolean isAligned) {
    if (hasNull(values)) {
      values = changeToArrayListWithStringType(values);
      measurements = changeToArrayListWithStringType(measurements);
      prefixPaths = new ArrayList<>(prefixPaths);
      time = new ArrayList<>(time);
      filterNullValueAndMeasurementWithStringType(prefixPaths, time, measurements, values);
    }
    return genTSInsertStringRecordsReq(prefixPaths, time, measurements, values, isAligned);
  }

  private List<List<String>> changeToArrayListWithStringType(List<List<String>> values) {
    if (!(values instanceof ArrayList)) {
      values = new ArrayList<>(values);
    }
    for (int i = 0; i < values.size(); i++) {
      List<String> currentValue = values.get(i);
      if (!(currentValue instanceof ArrayList)) {
        values.set(i, new ArrayList<>(currentValue));
      }
    }
    return values;
  }

  private List<List<Object>> changeToArrayList(List<List<Object>> values) {
    if (!(values instanceof ArrayList)) {
      values = new ArrayList<>(values);
    }
    for (int i = 0; i < values.size(); i++) {
      List<Object> currentValue = values.get(i);
      if (!(currentValue instanceof ArrayList)) {
        values.set(i, new ArrayList<>(currentValue));
      }
    }
    return values;
  }

  private List<List<TSDataType>> changeToArrayListWithTSDataType(List<List<TSDataType>> values) {
    if (!(values instanceof ArrayList)) {
      values = new ArrayList<>(values);
    }
    for (int i = 0; i < values.size(); i++) {
      List<TSDataType> currentValue = values.get(i);
      if (!(currentValue instanceof ArrayList)) {
        values.set(i, new ArrayList<>(currentValue));
      }
    }
    return values;
  }

  private TSInsertStringRecordsReq genTSInsertStringRecordsReq(
      List<String> prefixPaths,
      List<Long> time,
      List<List<String>> measurements,
      List<List<String>> values,
      boolean isAligned) {
    TSInsertStringRecordsReq request = new TSInsertStringRecordsReq();

    request.setPrefixPaths(prefixPaths);
    request.setTimestamps(time);
    request.setMeasurementsList(measurements);
    request.setValuesList(values);
    request.setIsAligned(isAligned);
    return request;
  }

  private void filterAndUpdateTSInsertStringRecordsReq(
      TSInsertStringRecordsReq request,
      String deviceId,
      long time,
      List<String> measurements,
      List<String> values) {
    if (hasNull(values)) {
      measurements = new ArrayList<>(measurements);
      values = new ArrayList<>(values);
      boolean isAllValueNull =
          filterNullValueAndMeasurementWithStringType(values, deviceId, measurements);
      if (isAllValueNull) {
        throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
      }
    }
    updateTSInsertStringRecordsReq(request, deviceId, time, measurements, values);
  }

  private void updateTSInsertStringRecordsReq(
      TSInsertStringRecordsReq request,
      String deviceId,
      long time,
      List<String> measurements,
      List<String> values) {
    request.addToPrefixPaths(deviceId);
    request.addToTimestamps(time);
    request.addToMeasurementsList(measurements);
    request.addToValuesList(values);
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }
    if (enableRedirection) {
      insertRecordsWithLeaderCache(
          deviceIds, times, measurementsList, typesList, valuesList, false);
    } else {
      TSInsertRecordsReq request;
      try {
        request =
            filterAndGenTSInsertRecordsReq(
                deviceIds, times, measurementsList, typesList, valuesList, false);
      } catch (NoValidValueException e) {
        logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
        return;
      }
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertRecords fail:{}", ignored.getMessage());
      }
    }
  }

  /**
   * Insert aligned multiple rows, which can reduce the overhead of network. This method is just
   * like jdbc executeBatch, we pack some insert request in batch and send them to server. If you
   * want improve your performance, please see insertTablet method
   *
   * <p>Each row is independent, which could have different prefixPath, time, number of
   * subMeasurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "prefixPaths, times, subMeasurementsList and valuesList's size should be equal");
    }
    if (enableRedirection) {
      insertRecordsWithLeaderCache(deviceIds, times, measurementsList, typesList, valuesList, true);
    } else {
      TSInsertRecordsReq request;
      try {
        request =
            filterAndGenTSInsertRecordsReq(
                deviceIds, times, measurementsList, typesList, valuesList, true);
      } catch (NoValidValueException e) {
        logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
        return;
      }
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertRecords fail:{}", ignored.getMessage());
      }
    }
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row could have same deviceId but different time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertRecordsOfOneDevice(deviceId, times, measurementsList, typesList, valuesList, false);
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   *
   * <p>Each row could have same deviceId but different time, number of measurements
   *
   * @param haveSorted deprecated, whether the times have been sorted
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(VALUES_SIZE_SHOULD_BE_EQUAL);
    }
    TSInsertRecordsOfOneDeviceReq request;
    try {
      request =
          filterAndGenTSInsertRecordsOfOneDeviceReq(
              deviceId, times, measurementsList, typesList, valuesList, haveSorted, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_WITH_TIME, deviceId, times, measurementsList);
      return;
    }
    try {
      getSessionConnection(deviceId).insertRecordsOfOneDevice(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(deviceId));
        deviceIdToEndpoint.remove(deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertRecordsOfOneDevice(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertRecordsOfOneDevice fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Insert multiple rows with String format data, which can reduce the overhead of network. This
   * method is just like jdbc executeBatch, we pack some insert request in batch and send them to
   * server. If you want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same deviceId but different time, number of measurements, number of
   * values as String
   *
   * @param haveSorted deprecated, whether the times have been sorted
   */
  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(VALUES_SIZE_SHOULD_BE_EQUAL);
    }
    TSInsertStringRecordsOfOneDeviceReq req;
    try {
      req =
          filterAndGenTSInsertStringRecordsOfOneDeviceReq(
              deviceId, times, measurementsList, valuesList, haveSorted, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_WITH_TIME, deviceId, times, measurementsList);
      return;
    }
    try {
      getSessionConnection(deviceId).insertStringRecordsOfOneDevice(req);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(deviceId));
        deviceIdToEndpoint.remove(deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertStringRecordsOfOneDevice(req);
        } catch (RedirectException ignored) {
          logger.warn("session insertStringRecordsOfOneDevice fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Insert multiple rows with String format data, which can reduce the overhead of network. This
   * method is just like jdbc executeBatch, we pack some insert request in batch and send them to
   * server. If you want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same deviceId but different time, number of measurements, number of
   * values as String
   */
  @Override
  public void insertStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList, false);
  }

  /**
   * Insert aligned multiple rows, which can reduce the overhead of network. This method is just
   * like jdbc executeBatch, we pack some insert request in batch and send them to server. If you
   * want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same prefixPath but different time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedRecordsOfOneDevice(
        deviceId, times, measurementsList, typesList, valuesList, false);
  }

  /**
   * Insert aligned multiple rows, which can reduce the overhead of network. This method is just
   * like jdbc executeBatch, we pack some insert request in batch and send them to server. If you
   * want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same prefixPath but different time, number of measurements
   *
   * @param haveSorted deprecated, whether the times have been sorted
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "times, subMeasurementsList and valuesList's size should be equal");
    }
    TSInsertRecordsOfOneDeviceReq request;
    try {
      request =
          filterAndGenTSInsertRecordsOfOneDeviceReq(
              deviceId, times, measurementsList, typesList, valuesList, haveSorted, true);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_WITH_TIME, deviceId, times, measurementsList);
      return;
    }
    try {
      getSessionConnection(deviceId).insertRecordsOfOneDevice(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(deviceId));
        deviceIdToEndpoint.remove(deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertRecordsOfOneDevice(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertRecordsOfOneDevice fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Insert multiple rows with String format data, which can reduce the overhead of network. This
   * method is just like jdbc executeBatch, we pack some insert request in batch and send them to
   * server. If you want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same deviceId but different time, number of measurements, number of
   * values as String
   *
   * @param haveSorted deprecated, whether the times have been sorted
   */
  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(VALUES_SIZE_SHOULD_BE_EQUAL);
    }
    TSInsertStringRecordsOfOneDeviceReq req;
    try {
      req =
          filterAndGenTSInsertStringRecordsOfOneDeviceReq(
              deviceId, times, measurementsList, valuesList, haveSorted, true);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_WITH_TIME, deviceId, times, measurementsList);
      return;
    }
    try {
      getSessionConnection(deviceId).insertStringRecordsOfOneDevice(req);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(deviceId));
        deviceIdToEndpoint.remove(deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertStringRecordsOfOneDevice(req);
        } catch (RedirectException ignored) {
          logger.warn("session insertStringRecordsOfOneDevice fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * Insert aligned multiple rows with String format data, which can reduce the overhead of network.
   * This method is just like jdbc executeBatch, we pack some insert request in batch and send them
   * to server. If you want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same prefixPath but different time, number of measurements, number of
   * values as String
   *
   * @see Session#insertTablet(Tablet)
   */
  @Override
  public void insertAlignedStringRecordsOfOneDevice(
      String deviceId,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedStringRecordsOfOneDevice(deviceId, times, measurementsList, valuesList, false);
  }

  private TSInsertRecordsOfOneDeviceReq filterAndGenTSInsertRecordsOfOneDeviceReq(
      String prefixPath,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted,
      boolean isAligned)
      throws IoTDBConnectionException {
    if (hasNull(valuesList)) {
      measurementsList = changeToArrayListWithStringType(measurementsList);
      valuesList = changeToArrayList(valuesList);
      typesList = changeToArrayListWithTSDataType(typesList);
      times = new ArrayList<>(times);
      filterNullValueAndMeasurementOfOneDevice(
          prefixPath, times, measurementsList, typesList, valuesList);
    }
    return genTSInsertRecordsOfOneDeviceReq(
        prefixPath, times, measurementsList, typesList, valuesList, haveSorted, isAligned);
  }

  private TSInsertRecordsOfOneDeviceReq genTSInsertRecordsOfOneDeviceReq(
      String prefixPath,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted,
      boolean isAligned)
      throws IoTDBConnectionException {
    // check params size
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(VALUES_SIZE_SHOULD_BE_EQUAL);
    }

    if (!checkSorted(times)) {
      // sort
      Integer[] index = new Integer[times.size()];
      for (int i = 0; i < times.size(); i++) {
        index[i] = i;
      }
      Arrays.sort(index, Comparator.comparingLong(times::get));
      times.sort(Long::compareTo);
      // sort measurementList
      measurementsList = sortList(measurementsList, index);
      // sort typesList
      typesList = sortList(typesList, index);
      // sort values
      valuesList = sortList(valuesList, index);
    }

    TSInsertRecordsOfOneDeviceReq request = new TSInsertRecordsOfOneDeviceReq();
    request.setPrefixPath(prefixPath);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    List<ByteBuffer> buffersList = objectValuesListToByteBufferList(valuesList, typesList);
    request.setValuesList(buffersList);
    request.setIsAligned(isAligned);
    return request;
  }

  private TSInsertStringRecordsOfOneDeviceReq filterAndGenTSInsertStringRecordsOfOneDeviceReq(
      String prefixPath,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted,
      boolean isAligned) {
    if (hasNull(valuesList)) {
      measurementsList = changeToArrayListWithStringType(measurementsList);
      valuesList = changeToArrayListWithStringType(valuesList);
      times = new ArrayList<>(times);
      filterNullValueAndMeasurementWithStringTypeOfOneDevice(
          times, prefixPath, measurementsList, valuesList);
    }
    return genTSInsertStringRecordsOfOneDeviceReq(
        prefixPath, times, measurementsList, valuesList, haveSorted, isAligned);
  }

  private TSInsertStringRecordsOfOneDeviceReq genTSInsertStringRecordsOfOneDeviceReq(
      String prefixPath,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList,
      boolean haveSorted,
      boolean isAligned) {
    // check params size
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(VALUES_SIZE_SHOULD_BE_EQUAL);
    }

    if (!checkSorted(times)) {
      Integer[] index = new Integer[times.size()];
      for (int i = 0; i < index.length; i++) {
        index[i] = i;
      }
      Arrays.sort(index, Comparator.comparingLong(times::get));
      times.sort(Long::compareTo);
      // sort measurementsList
      measurementsList = sortList(measurementsList, index);
      // sort valuesList
      valuesList = sortList(valuesList, index);
    }

    TSInsertStringRecordsOfOneDeviceReq req = new TSInsertStringRecordsOfOneDeviceReq();
    req.setPrefixPath(prefixPath);
    req.setTimestamps(times);
    req.setMeasurementsList(measurementsList);
    req.setValuesList(valuesList);
    req.setIsAligned(isAligned);
    return req;
  }

  /**
   * Sort the input source list.
   *
   * <p>e.g. source: [1,2,3,4,5], index:[1,0,3,2,4], return : [2,1,4,3,5]
   *
   * @param source Input list
   * @param index retuen order
   * @param <T> Input type
   * @return ordered list
   */
  private static <T> List<T> sortList(List<T> source, Integer[] index) {
    return Arrays.stream(index).map(source::get).collect(Collectors.toList());
  }

  private List<ByteBuffer> objectValuesListToByteBufferList(
      List<List<Object>> valuesList, List<List<TSDataType>> typesList)
      throws IoTDBConnectionException {
    List<ByteBuffer> buffersList = new ArrayList<>();
    for (int i = 0; i < valuesList.size(); i++) {
      ByteBuffer buffer = SessionUtils.getValueBuffer(typesList.get(i), valuesList.get(i));
      buffersList.add(buffer);
    }
    return buffersList;
  }

  private void insertRecordsWithLeaderCache(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<SessionConnection, TSInsertRecordsReq> recordsGroup = new HashMap<>();
    for (int i = 0; i < deviceIds.size(); i++) {
      final SessionConnection connection = getSessionConnection(deviceIds.get(i));
      TSInsertRecordsReq request = recordsGroup.getOrDefault(connection, new TSInsertRecordsReq());
      request.setIsAligned(isAligned);
      try {
        filterAndUpdateTSInsertRecordsReq(
            request,
            deviceIds.get(i),
            times.get(i),
            measurementsList.get(i),
            typesList.get(i),
            valuesList.get(i));
        recordsGroup.putIfAbsent(connection, request);
      } catch (NoValidValueException e) {
        logger.warn(
            "All values are null and this submission is ignored,deviceId is [{}],time is [{}],measurements are [{}]",
            deviceIds.get(i),
            times.get(i),
            measurementsList.get(i));
      }
    }
    insertByGroup(recordsGroup, SessionConnection::insertRecords);
  }

  private TSInsertRecordsReq filterAndGenTSInsertRecordsReq(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean isAligned)
      throws IoTDBConnectionException {
    if (hasNull(valuesList)) {
      measurementsList = changeToArrayListWithStringType(measurementsList);
      valuesList = changeToArrayList(valuesList);
      deviceIds = new ArrayList<>(deviceIds);
      times = new ArrayList<>(times);
      typesList = changeToArrayListWithTSDataType(typesList);
      filterNullValueAndMeasurement(deviceIds, times, measurementsList, valuesList, typesList);
    }
    return genTSInsertRecordsReq(
        deviceIds, times, measurementsList, typesList, valuesList, isAligned);
  }

  private TSInsertRecordsReq genTSInsertRecordsReq(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean isAligned)
      throws IoTDBConnectionException {
    TSInsertRecordsReq request = new TSInsertRecordsReq();
    request.setPrefixPaths(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    request.setIsAligned(isAligned);
    List<ByteBuffer> buffersList = objectValuesListToByteBufferList(valuesList, typesList);
    request.setValuesList(buffersList);
    return request;
  }

  private void filterAndUpdateTSInsertRecordsReq(
      TSInsertRecordsReq request,
      String deviceId,
      Long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException {
    if (hasNull(values)) {
      measurements = new ArrayList<>(measurements);
      types = new ArrayList<>(types);
      values = new ArrayList<>(values);
      boolean isAllValuesNull =
          filterNullValueAndMeasurement(deviceId, measurements, types, values);
      if (isAllValuesNull) {
        throw new NoValidValueException(ALL_INSERT_DATA_IS_NULL);
      }
    }
    updateTSInsertRecordsReq(request, deviceId, time, measurements, types, values);
  }

  private void updateTSInsertRecordsReq(
      TSInsertRecordsReq request,
      String deviceId,
      Long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException {
    request.addToPrefixPaths(deviceId);
    request.addToTimestamps(time);
    request.addToMeasurementsList(measurements);
    ByteBuffer buffer = SessionUtils.getValueBuffer(types, values);
    request.addToValuesList(buffer);
  }

  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   *
   * <p>a Tablet example: device1 time s1, s2, s3 1, 1, 1, 1 2, 2, 2, 2 3, 3, 3, 3
   *
   * <p>times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  @Override
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    insertTablet(tablet, false);
  }

  /**
   * insert a Tablet
   *
   * @param tablet data batch
   * @param sorted deprecated, whether times in Tablet are in ascending order
   */
  @Override
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted, false);
    try {
      getSessionConnection(tablet.deviceId).insertTablet(request);
    } catch (RedirectException e) {
      handleRedirection(tablet.deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(tablet.deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(tablet.deviceId));
        deviceIdToEndpoint.remove(tablet.deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertTablet(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertTablet fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  /**
   * insert the aligned timeseries data of a device. For each timestamp, the number of measurements
   * is the same.
   *
   * <p>a Tablet example: device1 time s1, s2, s3 1, 1, 1, 1 2, 2, 2, 2 3, 3, 3, 3
   *
   * <p>times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  @Override
  public void insertAlignedTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    insertAlignedTablet(tablet, false);
  }

  /**
   * insert the aligned timeseries data of a device.
   *
   * @param tablet data batch
   * @param sorted deprecated, whether times in Tablet are in ascending order
   */
  @Override
  public void insertAlignedTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted, true);
    try {
      getSessionConnection(tablet.deviceId).insertTablet(request);
    } catch (RedirectException e) {
      handleRedirection(tablet.deviceId, e.getEndPoint());
    } catch (IoTDBConnectionException e) {
      if (enableRedirection
          && !deviceIdToEndpoint.isEmpty()
          && deviceIdToEndpoint.get(tablet.deviceId) != null) {
        logger.warn(SESSION_CANNOT_CONNECT, deviceIdToEndpoint.get(tablet.deviceId));
        deviceIdToEndpoint.remove(tablet.deviceId);

        // reconnect with default connection
        try {
          defaultSessionConnection.insertTablet(request);
        } catch (RedirectException ignored) {
          logger.warn("session insertTablet fail:{}", ignored.getMessage());
        }
      } else {
        throw e;
      }
    }
  }

  private TSInsertTabletReq genTSInsertTabletReq(Tablet tablet, boolean sorted, boolean isAligned) {
    if (!checkSorted(tablet)) {
      sortTablet(tablet);
    }

    TSInsertTabletReq request = new TSInsertTabletReq();

    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      request.addToMeasurements(measurementSchema.getMeasurementId());
      request.addToTypes(measurementSchema.getType().ordinal());
    }

    request.setPrefixPath(tablet.deviceId);
    request.setIsAligned(isAligned);
    request.setTimestamps(SessionUtils.getTimeBuffer(tablet));
    request.setValues(SessionUtils.getValueBuffer(tablet));
    request.setSize(tablet.rowSize);
    return request;
  }

  /**
   * insert the data of several deivces. Given a deivce, for each timestamp, the number of
   * measurements is the same.
   *
   * <p>Times in each Tablet may not be in ascending order
   *
   * @param tablets data batch in multiple device
   */
  @Override
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * insert the data of several devices. Given a device, for each timestamp, the number of
   * measurements is the same.
   *
   * @param tablets data batch in multiple device
   * @param sorted deprecated, whether times in each Tablet are in ascending order
   */
  @Override
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    if (enableRedirection) {
      insertTabletsWithLeaderCache(tablets, sorted, false);
    } else {
      TSInsertTabletsReq request =
          genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted, false);
      try {
        defaultSessionConnection.insertTablets(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertTablets fail:{}", ignored.getMessage());
      }
    }
  }

  /**
   * insert aligned data of several deivces. Given a deivce, for each timestamp, the number of
   * measurements is the same.
   *
   * <p>Times in each Tablet may not be in ascending order
   *
   * @param tablets data batch in multiple device
   */
  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedTablets(tablets, false);
  }

  /**
   * insert aligned data of several devices. Given a device, for each timestamp, the number of
   * measurements is the same.
   *
   * @param tablets data batch in multiple device
   * @param sorted deprecated, whether times in each Tablet are in ascending order
   */
  @Override
  public void insertAlignedTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    if (enableRedirection) {
      insertTabletsWithLeaderCache(tablets, sorted, true);
    } else {
      TSInsertTabletsReq request =
          genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted, true);
      try {
        defaultSessionConnection.insertTablets(request);
      } catch (RedirectException ignored) {
        logger.warn("session insertTablets fail:{}", ignored.getMessage());
      }
    }
  }

  private void insertTabletsWithLeaderCache(
      Map<String, Tablet> tablets, boolean sorted, boolean isAligned)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<SessionConnection, TSInsertTabletsReq> tabletGroup = new HashMap<>();
    for (Entry<String, Tablet> entry : tablets.entrySet()) {
      final SessionConnection connection = getSessionConnection(entry.getKey());
      TSInsertTabletsReq request =
          tabletGroup.computeIfAbsent(connection, k -> new TSInsertTabletsReq());
      updateTSInsertTabletsReq(request, entry.getValue(), sorted, isAligned);
    }

    insertByGroup(tabletGroup, SessionConnection::insertTablets);
  }

  private TSInsertTabletsReq genTSInsertTabletsReq(
      List<Tablet> tablets, boolean sorted, boolean isAligned) throws BatchExecutionException {
    TSInsertTabletsReq request = new TSInsertTabletsReq();
    if (tablets.isEmpty()) {
      throw new BatchExecutionException("No tablet is inserting!");
    }
    for (Tablet tablet : tablets) {
      updateTSInsertTabletsReq(request, tablet, sorted, isAligned);
    }
    return request;
  }

  private void updateTSInsertTabletsReq(
      TSInsertTabletsReq request, Tablet tablet, boolean sorted, boolean isAligned) {
    if (!checkSorted(tablet)) {
      sortTablet(tablet);
    }
    request.addToPrefixPaths(tablet.deviceId);
    List<String> measurements = new ArrayList<>();
    List<Integer> dataTypes = new ArrayList<>();
    request.setIsAligned(isAligned);
    for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
      measurements.add(measurementSchema.getMeasurementId());
      dataTypes.add(measurementSchema.getType().ordinal());
    }
    request.addToMeasurementsList(measurements);
    request.addToTypesList(dataTypes);
    request.addToTimestampsList(SessionUtils.getTimeBuffer(tablet));
    request.addToValuesList(SessionUtils.getValueBuffer(tablet));
    request.addToSizeList(tablet.rowSize);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablet(tablet, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted, false);
    defaultSessionConnection.testInsertTablet(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablets(tablets, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletsReq request =
        genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted, false);
    defaultSessionConnection.testInsertTablets(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordsReq request;
    try {
      request =
          filterAndGenTSInsertStringRecordsReq(
              deviceIds, times, measurementsList, valuesList, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
      return;
    }

    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordsReq request;
    try {
      request =
          filterAndGenTSInsertRecordsReq(
              deviceIds, times, measurementsList, typesList, valuesList, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL_MULTI_DEVICES, deviceIds, times, measurementsList);
      return;
    }

    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request;
    try {
      request = filterAndGenTSInsertStringRecordReq(deviceId, time, measurements, values, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  @Override
  public void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request;
    try {
      request = filterAndGenTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    } catch (NoValidValueException e) {
      logger.warn(ALL_VALUES_ARE_NULL, deviceId, time, measurements);
      return;
    }
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  @Override
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteTimeseries(Collections.singletonList(path));
  }

  /**
   * delete some timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  @Override
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteTimeseries(paths);
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param endTime data with time stamp less than or equal to time will be deleted
   */
  @Override
  public void deleteData(String path, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    deleteData(Collections.singletonList(path), Long.MIN_VALUE, endTime);
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param endTime data with time stamp less than or equal to time will be deleted
   */
  @Override
  public void deleteData(List<String> paths, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    deleteData(paths, Long.MIN_VALUE, endTime);
  }

  /**
   * delete data >= startTime and data <= endTime in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param startTime delete range start time
   * @param endTime delete range end time
   */
  @Override
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    TSDeleteDataReq request = genTSDeleteDataReq(paths, startTime, endTime);
    defaultSessionConnection.deleteData(request);
  }

  private TSDeleteDataReq genTSDeleteDataReq(List<String> paths, long startTime, long endTime) {
    TSDeleteDataReq request = new TSDeleteDataReq();
    request.setPaths(paths);
    request.setStartTime(startTime);
    request.setEndTime(endTime);
    return request;
  }

  /**
   * check whether the batch has been sorted
   *
   * @return whether the batch has been sorted
   */
  private boolean checkSorted(Tablet tablet) {
    for (int i = 1; i < tablet.rowSize; i++) {
      if (tablet.timestamps[i] < tablet.timestamps[i - 1]) {
        return false;
      }
    }
    return true;
  }

  private boolean checkSorted(List<Long> times) {
    for (int i = 1; i < times.size(); i++) {
      if (times.get(i) < times.get(i - 1)) {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings({
    "squid:S3776"
  }) // ignore Cognitive Complexity of methods should not be too high
  public void sortTablet(Tablet tablet) {
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    Integer[] index = new Integer[tablet.rowSize];
    for (int i = 0; i < tablet.rowSize; i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(o -> tablet.timestamps[o]));
    Arrays.sort(tablet.timestamps, 0, tablet.rowSize);
    int columnIndex = 0;
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      if (schema instanceof MeasurementSchema) {
        tablet.values[columnIndex] = sortList(tablet.values[columnIndex], schema.getType(), index);
        if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
          tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
        }
        columnIndex++;
      } else {
        int measurementSize = schema.getSubMeasurementsList().size();
        for (int j = 0; j < measurementSize; j++) {
          tablet.values[columnIndex] =
              sortList(
                  tablet.values[columnIndex],
                  schema.getSubMeasurementsTSDataTypeList().get(j),
                  index);
          if (tablet.bitMaps != null && tablet.bitMaps[columnIndex] != null) {
            tablet.bitMaps[columnIndex] = sortBitMap(tablet.bitMaps[columnIndex], index);
          }
          columnIndex++;
        }
      }
    }
  }

  /**
   * sort value list by index
   *
   * @param valueList value list
   * @param dataType data type
   * @param index index
   * @return sorted list
   */
  private Object sortList(Object valueList, TSDataType dataType, Integer[] index) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        boolean[] sortedValues = new boolean[boolValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedValues[i] = boolValues[index[i]];
        }
        return sortedValues;
      case INT32:
        int[] intValues = (int[]) valueList;
        int[] sortedIntValues = new int[intValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedIntValues[i] = intValues[index[i]];
        }
        return sortedIntValues;
      case INT64:
        long[] longValues = (long[]) valueList;
        long[] sortedLongValues = new long[longValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedLongValues[i] = longValues[index[i]];
        }
        return sortedLongValues;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        float[] sortedFloatValues = new float[floatValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedFloatValues[i] = floatValues[index[i]];
        }
        return sortedFloatValues;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        double[] sortedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedDoubleValues[i] = doubleValues[index[i]];
        }
        return sortedDoubleValues;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        Binary[] sortedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedBinaryValues[i] = binaryValues[index[i]];
        }
        return sortedBinaryValues;
      default:
        throw new UnSupportedDataTypeException(MSG_UNSUPPORTED_DATA_TYPE + dataType);
    }
  }

  /**
   * sort BitMap by index
   *
   * @param bitMap BitMap to be sorted
   * @param index index
   * @return sorted bitMap
   */
  private BitMap sortBitMap(BitMap bitMap, Integer[] index) {
    BitMap sortedBitMap = new BitMap(bitMap.getSize());
    for (int i = 0; i < index.length; i++) {
      if (bitMap.isMarked(index[i])) {
        sortedBitMap.mark(i);
      }
    }
    return sortedBitMap;
  }

  @Override
  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    TSSetSchemaTemplateReq request = getTSSetSchemaTemplateReq(templateName, prefixPath);
    defaultSessionConnection.setSchemaTemplate(request);
  }

  /**
   * Construct Template at session and create it at server.
   *
   * <p>The template instance constructed within session is SUGGESTED to be a flat measurement
   * template, which has no internal nodes inside a template.
   *
   * <p>For example, template(s1, s2, s3) is a flat measurement template, while template2(GPS.x,
   * GPS.y, s1) is not.
   *
   * <p>Tree-structured template, which is contrary to flat measurement template, may not be
   * supported in further version of IoTDB
   *
   * @see Template
   */
  @Override
  public void createSchemaTemplate(Template template)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSCreateSchemaTemplateReq req = new TSCreateSchemaTemplateReq();
    req.setName(template.getName());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    template.serialize(baos);
    req.setSerializedTemplate(baos.toByteArray());
    baos.close();
    defaultSessionConnection.createSchemaTemplate(req);
  }

  /**
   * Create a template with flat measurements, not tree structured. Need to specify datatype,
   * encoding and compressor of each measurement, and alignment of these measurements at once.
   *
   * @param measurements flat measurements of the template, cannot contain character dot
   * @param dataTypes datatype of each measurement in the template
   * @param encodings encodings of each measurement in the template
   * @param compressors compression type of each measurement in the template
   * @param isAligned specify whether these flat measurements are aligned
   * @oaram templateName name of template to create
   */
  @Override
  public void createSchemaTemplate(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      boolean isAligned)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    Template temp = new Template(templateName, isAligned);
    int len = measurements.size();
    if (len != dataTypes.size() || len != encodings.size() || len != compressors.size()) {
      throw new StatementExecutionException(
          "Different length of measurements, datatypes, encodings "
              + "or compressors when create schema tempalte.");
    }
    for (int idx = 0; idx < measurements.size(); idx++) {
      MeasurementNode mNode =
          new MeasurementNode(
              measurements.get(idx), dataTypes.get(idx), encodings.get(idx), compressors.get(idx));
      temp.addToTemplate(mNode);
    }
    createSchemaTemplate(temp);
  }

  /**
   * Compatible for rel/0.12, this method will create an unaligned flat template as a result. Notice
   * that there is no aligned concept in 0.12, so only the first measurement in each nested list
   * matters.
   *
   * @param name name of the template
   * @param schemaNames it works as a virtual layer inside template in 0.12, and makes no difference
   *     after 0.13
   * @param measurements the first measurement in each nested list will constitute the final flat
   *     template
   * @param dataTypes the data type of each measurement, only the first one in each nested list
   *     matters as above
   * @param encodings the encoding of each measurement, only the first one in each nested list
   *     matters as above
   * @param compressors the compressor of each measurement
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   * @deprecated
   */
  @Override
  @Deprecated
  public void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> flatMeasurements = new ArrayList<>();
    List<TSDataType> flatDataTypes = new ArrayList<>();
    List<TSEncoding> flatEncodings = new ArrayList<>();
    for (int idx = 0; idx < measurements.size(); idx++) {
      flatMeasurements.add(measurements.get(idx).get(0));
      flatDataTypes.add(dataTypes.get(idx).get(0));
      flatEncodings.add(encodings.get(idx).get(0));
    }
    try {
      createSchemaTemplate(
          name, flatMeasurements, flatDataTypes, flatEncodings, compressors, false);
    } catch (IOException e) {
      throw new StatementExecutionException(e.getMessage());
    }
  }

  /**
   * @param templateName Template to add aligned measurements.
   * @param measurementsPath If measurements get different prefix, or the prefix already exists in
   *     template but not aligned, throw exception.
   * @param dataTypes Data type of these measurements.
   * @param encodings Encoding of these measurements.
   * @param compressors CompressionType of these measurements.
   */
  @Override
  public void addAlignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSAppendSchemaTemplateReq req = new TSAppendSchemaTemplateReq();
    req.setName(templateName);
    req.setMeasurements(measurementsPath);
    req.setDataTypes(dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    req.setEncodings(encodings.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    req.setCompressors(
        compressors.stream().map(i -> (int) i.serialize()).collect(Collectors.toList()));
    req.setIsAligned(true);
    defaultSessionConnection.appendSchemaTemplate(req);
  }

  /**
   * @param templateName Template to add a single aligned measurement.
   * @param measurementPath If prefix of the path exists in template and not aligned, throw
   *     exception.
   */
  @Override
  public void addAlignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSAppendSchemaTemplateReq req = new TSAppendSchemaTemplateReq();
    req.setName(templateName);
    req.setMeasurements(Collections.singletonList(measurementPath));
    req.setDataTypes(Collections.singletonList(dataType.ordinal()));
    req.setEncodings(Collections.singletonList(encoding.ordinal()));
    req.setCompressors(Collections.singletonList((int) compressor.serialize()));
    req.setIsAligned(true);
    defaultSessionConnection.appendSchemaTemplate(req);
  }

  /**
   * @param templateName Template to add unaligned measurements.
   * @param measurementsPath If prefix of any path exist in template but aligned, throw exception.
   */
  @Override
  public void addUnalignedMeasurementsInTemplate(
      String templateName,
      List<String> measurementsPath,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSAppendSchemaTemplateReq req = new TSAppendSchemaTemplateReq();
    req.setName(templateName);
    req.setMeasurements(measurementsPath);
    req.setDataTypes(dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    req.setEncodings(encodings.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    req.setCompressors(
        compressors.stream().map(i -> (int) i.serialize()).collect(Collectors.toList()));
    req.setIsAligned(false);
    defaultSessionConnection.appendSchemaTemplate(req);
  }

  /**
   * @param templateName Template to add a single unaligned measurement.
   * @param measurementPath If prefix of path exists in template but aligned, throw exception.
   */
  @Override
  public void addUnalignedMeasurementInTemplate(
      String templateName,
      String measurementPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSAppendSchemaTemplateReq req = new TSAppendSchemaTemplateReq();
    req.setName(templateName);
    req.setMeasurements(Collections.singletonList(measurementPath));
    req.setDataTypes(Collections.singletonList(dataType.ordinal()));
    req.setEncodings(Collections.singletonList(encoding.ordinal()));
    req.setCompressors(Collections.singletonList((int) compressor.serialize()));
    req.setIsAligned(false);
    defaultSessionConnection.appendSchemaTemplate(req);
  }

  /**
   * @param templateName Template to prune.
   * @param path Remove node from template specified by the path, including its children nodes.
   */
  @Override
  public void deleteNodeInTemplate(String templateName, String path)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    TSPruneSchemaTemplateReq req = new TSPruneSchemaTemplateReq();
    req.setName(templateName);
    req.setPath(path);
    defaultSessionConnection.pruneSchemaTemplate(req);
  }

  /** @return Amount of measurements in the template */
  @Override
  public int countMeasurementsInTemplate(String name)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(name);
    req.setQueryType(TemplateQueryType.COUNT_MEASUREMENTS.ordinal());
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getCount();
  }

  /** @return If the node specified by the path is a measurement. */
  @Override
  public boolean isMeasurementInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.IS_MEASUREMENT.ordinal());
    req.setMeasurement(path);
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.result;
  }

  /** @return if there is a node correspond to the path in the template. */
  @Override
  public boolean isPathExistInTemplate(String templateName, String path)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.PATH_EXIST.ordinal());
    req.setMeasurement(path);
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.result;
  }

  /** @return All paths of measurements in the template. */
  @Override
  public List<String> showMeasurementsInTemplate(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.SHOW_MEASUREMENTS.ordinal());
    req.setMeasurement("");
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getMeasurements();
  }

  /** @return All paths of measurements under the pattern in the template. */
  @Override
  public List<String> showMeasurementsInTemplate(String templateName, String pattern)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.SHOW_MEASUREMENTS.ordinal());
    req.setMeasurement(pattern);
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getMeasurements();
  }

  /** @return All template names. */
  @Override
  public List<String> showAllTemplates()
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName("");
    req.setQueryType(TemplateQueryType.SHOW_TEMPLATES.ordinal());
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getMeasurements();
  }

  /** @return All paths have been set to designated template. */
  @Override
  public List<String> showPathsTemplateSetOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.SHOW_SET_TEMPLATES.ordinal());
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getMeasurements();
  }

  /** @return All paths are using designated template. */
  @Override
  public List<String> showPathsTemplateUsingOn(String templateName)
      throws StatementExecutionException, IoTDBConnectionException {
    TSQueryTemplateReq req = new TSQueryTemplateReq();
    req.setName(templateName);
    req.setQueryType(TemplateQueryType.SHOW_USING_TEMPLATES.ordinal());
    TSQueryTemplateResp resp = defaultSessionConnection.querySchemaTemplate(req);
    return resp.getMeasurements();
  }

  @Override
  public void unsetSchemaTemplate(String prefixPath, String templateName)
      throws IoTDBConnectionException, StatementExecutionException {
    TSUnsetSchemaTemplateReq request = getTSUnsetSchemaTemplateReq(prefixPath, templateName);
    defaultSessionConnection.unsetSchemaTemplate(request);
  }

  @Override
  public void dropSchemaTemplate(String templateName)
      throws IoTDBConnectionException, StatementExecutionException {
    TSDropSchemaTemplateReq request = getTSDropSchemaTemplateReq(templateName);
    defaultSessionConnection.dropSchemaTemplate(request);
  }

  private TSSetSchemaTemplateReq getTSSetSchemaTemplateReq(String templateName, String prefixPath) {
    TSSetSchemaTemplateReq request = new TSSetSchemaTemplateReq();
    request.setTemplateName(templateName);
    request.setPrefixPath(prefixPath);
    return request;
  }

  private TSUnsetSchemaTemplateReq getTSUnsetSchemaTemplateReq(
      String prefixPath, String templateName) {
    TSUnsetSchemaTemplateReq request = new TSUnsetSchemaTemplateReq();
    request.setPrefixPath(prefixPath);
    request.setTemplateName(templateName);
    return request;
  }

  private TSDropSchemaTemplateReq getTSDropSchemaTemplateReq(String templateName) {
    TSDropSchemaTemplateReq request = new TSDropSchemaTemplateReq();
    request.setTemplateName(templateName);
    return request;
  }

  /**
   * Create timeseries represented by schema template under given device paths.
   *
   * @param devicePathList the target device paths used for timeseries creation
   */
  @Override
  public void createTimeseriesUsingSchemaTemplate(List<String> devicePathList)
      throws IoTDBConnectionException, StatementExecutionException {
    if (devicePathList == null || devicePathList.contains(null)) {
      throw new StatementExecutionException(
          "Given device path list should not be  or contains null.");
    }
    TCreateTimeseriesUsingSchemaTemplateReq request = new TCreateTimeseriesUsingSchemaTemplateReq();
    request.setDevicePathList(devicePathList);
    defaultSessionConnection.createTimeseriesUsingSchemaTemplate(request);
  }

  /**
   * @param recordsGroup connection to record map
   * @param insertConsumer insert function
   * @param <T>
   *     <ul>
   *       <li>{@link TSInsertRecordsReq}
   *       <li>{@link TSInsertStringRecordsReq}
   *       <li>{@link TSInsertTabletsReq}
   *     </ul>
   *
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  @SuppressWarnings({
    "squid:S3776"
  }) // ignore Cognitive Complexity of methods should not be too high
  private <T> void insertByGroup(
      Map<SessionConnection, T> recordsGroup, InsertConsumer<T> insertConsumer)
      throws IoTDBConnectionException, StatementExecutionException {
    List<CompletableFuture<Void>> completableFutures =
        recordsGroup.entrySet().stream()
            .map(
                entry -> {
                  SessionConnection connection = entry.getKey();
                  T recordsReq = entry.getValue();
                  return CompletableFuture.runAsync(
                      () -> {
                        try {
                          insertConsumer.insert(connection, recordsReq);
                        } catch (RedirectException e) {
                          e.getDeviceEndPointMap().forEach(this::handleRedirection);
                        } catch (StatementExecutionException e) {
                          throw new CompletionException(e);
                        } catch (IoTDBConnectionException e) {
                          // remove the broken session
                          removeBrokenSessionConnection(connection);
                          try {
                            insertConsumer.insert(defaultSessionConnection, recordsReq);
                          } catch (IoTDBConnectionException | StatementExecutionException ex) {
                            throw new CompletionException(ex);
                          } catch (RedirectException ignored) {
                            logger.info("insert by group has been redirect");
                          }
                        }
                      },
                      OPERATION_EXECUTOR);
                })
            .collect(Collectors.toList());

    StringBuilder errMsgBuilder = new StringBuilder();
    for (CompletableFuture<Void> completableFuture : completableFutures) {
      try {
        completableFuture.join();
      } catch (CompletionException completionException) {
        Throwable cause = completionException.getCause();
        logger.error("Meet error when async insert!", cause);
        if (cause instanceof IoTDBConnectionException) {
          throw (IoTDBConnectionException) cause;
        } else {
          errMsgBuilder.append(cause.getMessage());
        }
      }
    }
    if (errMsgBuilder.length() > 0) {
      throw new StatementExecutionException(errMsgBuilder.toString());
    }
  }

  @Override
  public boolean isEnableQueryRedirection() {
    return enableQueryRedirection;
  }

  @Override
  public void setEnableQueryRedirection(boolean enableQueryRedirection) {
    this.enableQueryRedirection = enableQueryRedirection;
  }

  @Override
  public boolean isEnableRedirection() {
    return enableRedirection;
  }

  @Override
  public void setEnableRedirection(boolean enableRedirection) {
    this.enableRedirection = enableRedirection;
  }

  @Override
  public TSBackupConfigurationResp getBackupConfiguration()
      throws IoTDBConnectionException, StatementExecutionException {
    return defaultSessionConnection.getBackupConfiguration();
  }

  @Override
  public TSConnectionInfoResp fetchAllConnections() throws IoTDBConnectionException {
    return defaultSessionConnection.fetchAllConnections();
  }

  public static class Builder {
    private String host = SessionConfig.DEFAULT_HOST;
    private int rpcPort = SessionConfig.DEFAULT_PORT;
    private String username = SessionConfig.DEFAULT_USER;
    private String pw = SessionConfig.DEFAULT_PASSWORD;
    private int fetchSize = SessionConfig.DEFAULT_FETCH_SIZE;
    private ZoneId zoneId = null;
    private int thriftDefaultBufferSize = SessionConfig.DEFAULT_INITIAL_BUFFER_CAPACITY;
    private int thriftMaxFrameSize = SessionConfig.DEFAULT_MAX_FRAME_SIZE;
    private boolean enableRedirection = SessionConfig.DEFAULT_REDIRECTION_MODE;
    private Version version = SessionConfig.DEFAULT_VERSION;
    private long timeOut = SessionConfig.DEFAULT_QUERY_TIME_OUT;

    private List<String> nodeUrls = null;

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder port(int port) {
      this.rpcPort = port;
      return this;
    }

    public Builder username(String username) {
      this.username = username;
      return this;
    }

    public Builder password(String password) {
      this.pw = password;
      return this;
    }

    public Builder fetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
      return this;
    }

    public Builder zoneId(ZoneId zoneId) {
      this.zoneId = zoneId;
      return this;
    }

    public Builder thriftDefaultBufferSize(int thriftDefaultBufferSize) {
      this.thriftDefaultBufferSize = thriftDefaultBufferSize;
      return this;
    }

    public Builder thriftMaxFrameSize(int thriftMaxFrameSize) {
      this.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public Builder enableRedirection(boolean enableRedirection) {
      this.enableRedirection = enableRedirection;
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Builder version(Version version) {
      this.version = version;
      return this;
    }

    public Builder timeOut(long timeOut) {
      this.timeOut = timeOut;
      return this;
    }

    public Session build() {
      if (nodeUrls != null
          && (!SessionConfig.DEFAULT_HOST.equals(host) || rpcPort != SessionConfig.DEFAULT_PORT)) {
        throw new IllegalArgumentException(
            "You should specify either nodeUrls or (host + rpcPort), but not both");
      }

      if (nodeUrls != null) {
        Session newSession =
            new Session(
                nodeUrls,
                username,
                pw,
                fetchSize,
                zoneId,
                thriftDefaultBufferSize,
                thriftMaxFrameSize,
                enableRedirection,
                version);
        newSession.setEnableQueryRedirection(true);
        return newSession;
      }

      return new Session(
          host,
          rpcPort,
          username,
          pw,
          fetchSize,
          zoneId,
          thriftDefaultBufferSize,
          thriftMaxFrameSize,
          enableRedirection,
          version);
    }
  }
}
