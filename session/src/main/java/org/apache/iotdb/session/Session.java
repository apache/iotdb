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

import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RedirectException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.EndPoint;
import org.apache.iotdb.service.rpc.thrift.TSCreateAlignedTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsOfOneDeviceReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetSchemaTemplateReq;
import org.apache.iotdb.service.rpc.thrift.TSUnsetSchemaTemplateReq;
import org.apache.iotdb.session.util.SessionUtils;
import org.apache.iotdb.session.util.ThreadUtils;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.UnaryMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class Session {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  protected static final TSProtocolVersion protocolVersion =
      TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  public static final String MSG_UNSUPPORTED_DATA_TYPE = "Unsupported data type:";
  public static final String MSG_DONOT_ENABLE_REDIRECT =
      "Query do not enable redirect," + " please confirm the session and server conf.";
  private static final ThreadPoolExecutor OPERATION_EXECUTOR =
      new ThreadPoolExecutor(
          Config.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          Config.DEFAULT_SESSION_EXECUTOR_THREAD_NUM,
          0,
          TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<>(Config.DEFAULT_SESSION_EXECUTOR_TASK_NUM),
          ThreadUtils.createThreadFactory("SessionExecutor", true));
  protected List<String> nodeUrls;
  protected String username;
  protected String password;
  protected int fetchSize;
  private static final byte TYPE_NULL = -2;
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

  protected EndPoint defaultEndPoint;
  protected SessionConnection defaultSessionConnection;
  private boolean isClosed = true;

  // Cluster version cache
  protected boolean enableCacheLeader;
  protected SessionConnection metaSessionConnection;
  protected volatile Map<String, EndPoint> deviceIdToEndpoint;
  protected volatile Map<EndPoint, SessionConnection> endPointToSessionConnection;

  protected boolean enableQueryRedirection = false;

  public Session(String host, int rpcPort) {
    this(
        host,
        rpcPort,
        Config.DEFAULT_USER,
        Config.DEFAULT_PASSWORD,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, String rpcPort, String username, String password) {
    this(
        host,
        Integer.parseInt(rpcPort),
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, int rpcPort, String username, String password) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(String host, int rpcPort, String username, String password, int fetchSize) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
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
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
    this.queryTimeoutInMs = queryTimeoutInMs;
  }

  public Session(String host, int rpcPort, String username, String password, ZoneId zoneId) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        zoneId,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(
      String host, int rpcPort, String username, String password, boolean enableCacheLeader) {
    this(
        host,
        rpcPort,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        enableCacheLeader);
  }

  public Session(
      String host,
      int rpcPort,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      boolean enableCacheLeader) {
    this(
        host,
        rpcPort,
        username,
        password,
        fetchSize,
        zoneId,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        enableCacheLeader);
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
      boolean enableCacheLeader) {
    this.defaultEndPoint = new EndPoint(host, rpcPort);
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.enableCacheLeader = enableCacheLeader;
  }

  public Session(List<String> nodeUrls, String username, String password) {
    this(
        nodeUrls,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        null,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
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
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(List<String> nodeUrls, String username, String password, ZoneId zoneId) {
    this(
        nodeUrls,
        username,
        password,
        Config.DEFAULT_FETCH_SIZE,
        zoneId,
        Config.DEFAULT_INITIAL_BUFFER_CAPACITY,
        Config.DEFAULT_MAX_FRAME_SIZE,
        Config.DEFAULT_CACHE_LEADER_MODE);
  }

  public Session(
      List<String> nodeUrls,
      String username,
      String password,
      int fetchSize,
      ZoneId zoneId,
      int thriftDefaultBufferSize,
      int thriftMaxFrameSize,
      boolean enableCacheLeader) {
    this.nodeUrls = nodeUrls;
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
    this.zoneId = zoneId;
    this.thriftDefaultBufferSize = thriftDefaultBufferSize;
    this.thriftMaxFrameSize = thriftMaxFrameSize;
    this.enableCacheLeader = enableCacheLeader;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public int getFetchSize() {
    return this.fetchSize;
  }

  public synchronized void open() throws IoTDBConnectionException {
    open(false, Config.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public synchronized void open(boolean enableRPCCompression) throws IoTDBConnectionException {
    open(enableRPCCompression, Config.DEFAULT_CONNECTION_TIMEOUT_MS);
  }

  public synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }

    this.enableRPCCompression = enableRPCCompression;
    this.connectionTimeoutInMs = connectionTimeoutInMs;
    defaultSessionConnection = constructSessionConnection(this, defaultEndPoint, zoneId);
    defaultSessionConnection.setEnableRedirect(enableQueryRedirection);
    metaSessionConnection = defaultSessionConnection;
    isClosed = false;
    if (enableCacheLeader || enableQueryRedirection) {
      deviceIdToEndpoint = new ConcurrentHashMap<>();
      endPointToSessionConnection = new ConcurrentHashMap<>();
      endPointToSessionConnection.put(defaultEndPoint, defaultSessionConnection);
    }
  }

  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }
    try {
      if (enableCacheLeader) {
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
      Session session, EndPoint endpoint, ZoneId zoneId) throws IoTDBConnectionException {
    if (endpoint == null) {
      return new SessionConnection(session, zoneId);
    }
    return new SessionConnection(session, endpoint, zoneId);
  }

  public synchronized String getTimeZone() {
    return defaultSessionConnection.getTimeZone();
  }

  public synchronized void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    defaultSessionConnection.setTimeZone(zoneId);
  }

  public void setStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.setStorageGroup(storageGroup);
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroup, e);
    }
  }

  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.deleteStorageGroups(Collections.singletonList(storageGroup));
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroup, e);
    }
  }

  public void deleteStorageGroups(List<String> storageGroups)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      metaSessionConnection.deleteStorageGroups(storageGroups);
    } catch (RedirectException e) {
      handleMetaRedirection(storageGroups.toString(), e);
    }
  }

  public void createTimeseries(
      String path, TSDataType dataType, TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateTimeseriesReq request =
        genTSCreateTimeseriesReq(path, dataType, encoding, compressor, null, null, null, null);
    defaultSessionConnection.createTimeseries(request);
  }

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
    request.setCompressor(compressor.ordinal());
    request.setProps(props);
    request.setTags(tags);
    request.setAttributes(attributes);
    request.setMeasurementAlias(measurementAlias);
    return request;
  }

  public void createAlignedTimeseries(
      String multiSeriesId,
      List<String> multiMeasurementComponents,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor,
      List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateAlignedTimeseriesReq request =
        getTSCreateAlignedTimeseriesReq(
            multiSeriesId,
            multiMeasurementComponents,
            dataTypes,
            encodings,
            compressor,
            measurementAliasList);
    defaultSessionConnection.createAlignedTimeseries(request);
  }

  private TSCreateAlignedTimeseriesReq getTSCreateAlignedTimeseriesReq(
      String prefixPath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      CompressionType compressor,
      List<String> measurementAliasList) {
    TSCreateAlignedTimeseriesReq request = new TSCreateAlignedTimeseriesReq();
    request.setPrefixPath(prefixPath);
    request.setMeasurements(measurements);
    request.setDataTypes(dataTypes.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    request.setEncodings(encodings.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    request.setCompressor(compressor.ordinal());
    request.setMeasurementAlias(measurementAliasList);
    return request;
  }

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
      compressionOrdinals.add(compression.ordinal());
    }
    request.setCompressors(compressionOrdinals);

    request.setPropsList(propsList);
    request.setTagsList(tagsList);
    request.setAttributesList(attributesList);
    request.setMeasurementAliasList(measurementAliasList);

    return request;
  }

  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    return defaultSessionConnection.checkTimeseriesExists(path, queryTimeoutInMs);
  }

  public void setQueryTimeout(long timeoutInMs) {
    this.queryTimeoutInMs = timeoutInMs;
  }

  public long getQueryTimeout() {
    return queryTimeoutInMs;
  }

  /**
   * execute query sql
   *
   * @param sql query statement
   * @return result set
   */
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
      logger.debug("{} execute sql {}", defaultSessionConnection.getEndPoint(), sql);
      return defaultSessionConnection.executeQueryStatement(sql, timeoutInMs);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        logger.debug(
            "{} redirect query {} to {}",
            defaultSessionConnection.getEndPoint(),
            sql,
            e.getEndPoint());
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
  public SessionDataSet executeRawDataQuery(List<String> paths, long startTime, long endTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        logger.debug("redirect query {} to {}", paths, e.getEndPoint());
        // retry
        try {
          return defaultSessionConnection.executeRawDataQuery(paths, startTime, endTime);
        } catch (RedirectException redirectException) {
          logger.error("Redirect twice", redirectException);
          throw new StatementExecutionException("Redirect twice, please try again.");
        }
      } else {
        throw new StatementExecutionException(MSG_DONOT_ENABLE_REDIRECT);
      }
    }
  }

  /**
   * only: select last status from root.ln.d1.s1 where time >= 1621326244168;
   *
   * @param paths timeSeries eg. root.ln.d1.s1,root.ln.d1.s2
   * @param LastTime get the last data, whose timestamp greater than or equal LastTime eg.
   *     1621326244168
   */
  public SessionDataSet executeLastDataQuery(List<String> paths, long LastTime)
      throws StatementExecutionException, IoTDBConnectionException {
    try {
      return defaultSessionConnection.executeLastDataQuery(paths, LastTime);
    } catch (RedirectException e) {
      handleQueryRedirection(e.getEndPoint());
      if (enableQueryRedirection) {
        // retry
        try {
          return defaultSessionConnection.executeLastDataQuery(paths, LastTime);
        } catch (RedirectException redirectException) {
          logger.error("redirect twice", redirectException);
          throw new StatementExecutionException("redirect twice, please try again.");
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
  public SessionDataSet executeLastDataQuery(List<String> paths)
      throws StatementExecutionException, IoTDBConnectionException {
    long time = 0L;
    return executeLastDataQuery(paths, time);
  }

  /**
   * insert data in one row, if you want to improve your performance, please use insertRecords
   * method or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      Object... values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, Arrays.asList(values), false);
    insertRecord(deviceId, request);
  }

  private void insertRecord(String prefixPath, TSInsertRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(prefixPath).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(prefixPath, e.getEndPoint());
    }
  }

  private void insertRecord(String deviceId, TSInsertStringRecordReq request)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      getSessionConnection(deviceId).insertRecord(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
  }

  private SessionConnection getSessionConnection(String deviceId) {
    EndPoint endPoint;
    if (enableCacheLeader
        && !deviceIdToEndpoint.isEmpty()
        && (endPoint = deviceIdToEndpoint.get(deviceId)) != null) {
      return endPointToSessionConnection.get(endPoint);
    } else {
      return defaultSessionConnection;
    }
  }

  // TODO https://issues.apache.org/jira/browse/IOTDB-1399
  private void removeBrokenSessionConnection(SessionConnection sessionConnection) {
    // remove the cached broken leader session
    if (enableCacheLeader) {
      EndPoint endPoint = null;
      for (Iterator<Entry<EndPoint, SessionConnection>> it =
              endPointToSessionConnection.entrySet().iterator();
          it.hasNext(); ) {
        Map.Entry<EndPoint, SessionConnection> entry = it.next();
        if (entry.getValue().equals(sessionConnection)) {
          endPoint = entry.getKey();
          it.remove();
          break;
        }
      }

      for (Iterator<Entry<String, EndPoint>> it = deviceIdToEndpoint.entrySet().iterator();
          it.hasNext(); ) {
        Map.Entry<String, EndPoint> entry = it.next();
        if (entry.getValue().equals(endPoint)) {
          it.remove();
        }
      }
    }
  }

  private void handleMetaRedirection(String storageGroup, RedirectException e)
      throws IoTDBConnectionException {
    if (enableCacheLeader) {
      logger.debug("storageGroup[{}]:{}", storageGroup, e.getMessage());
      AtomicReference<IoTDBConnectionException> exceptionReference = new AtomicReference<>();
      SessionConnection connection =
          endPointToSessionConnection.computeIfAbsent(
              e.getEndPoint(),
              k -> {
                try {
                  return constructSessionConnection(this, e.getEndPoint(), zoneId);
                } catch (IoTDBConnectionException ex) {
                  exceptionReference.set(ex);
                  return null;
                }
              });
      if (connection == null) {
        throw new IoTDBConnectionException(exceptionReference.get());
      }
      metaSessionConnection = connection;
    }
  }

  private void handleRedirection(String deviceId, EndPoint endpoint)
      throws IoTDBConnectionException {
    if (enableCacheLeader) {
      AtomicReference<IoTDBConnectionException> exceptionReference = new AtomicReference<>();
      deviceIdToEndpoint.put(deviceId, endpoint);
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
        throw new IoTDBConnectionException(exceptionReference.get());
      }
    }
  }

  private void handleQueryRedirection(EndPoint endPoint) throws IoTDBConnectionException {
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
  public void insertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    // not vector by default
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    insertRecord(deviceId, request);
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertAlignedRecord(
      String multiSeriesId,
      long time,
      List<String> multiMeasurementComponents,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(multiSeriesId, time, multiMeasurementComponents, types, values, true);
    insertRecord(multiSeriesId, request);
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
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
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
  public void insertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request =
        genTSInsertStringRecordReq(deviceId, time, measurements, values, false);
    insertRecord(deviceId, request);
  }

  /**
   * insert aligned data in one row, if you want improve your performance, please use
   * insertAlignedRecords method or insertTablet method.
   *
   * @see Session#insertAlignedRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertAlignedRecord(
      String multiSeriesId, long time, List<String> multiMeasurementComponents, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request =
        genTSInsertStringRecordReq(multiSeriesId, time, multiMeasurementComponents, values, true);
    insertRecord(multiSeriesId, request);
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
    if (enableCacheLeader) {
      insertStringRecordsWithLeaderCache(deviceIds, times, measurementsList, valuesList, false);
    } else {
      TSInsertStringRecordsReq request =
          genTSInsertStringRecordsReq(deviceIds, times, measurementsList, valuesList, false);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
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
  public void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = multiSeriesIds.size();
    if (len != times.size()
        || len != multiMeasurementComponentsList.size()
        || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "prefixPaths, times, subMeasurementsList and valuesList's size should be equal");
    }
    if (enableCacheLeader) {
      insertStringRecordsWithLeaderCache(
          multiSeriesIds, times, multiMeasurementComponentsList, valuesList, true);
    } else {
      TSInsertStringRecordsReq request =
          genTSInsertStringRecordsReq(
              multiSeriesIds, times, multiMeasurementComponentsList, valuesList, true);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
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
          recordsGroup.computeIfAbsent(connection, k -> new TSInsertStringRecordsReq());
      request.setIsAligned(isAligned);
      updateTSInsertStringRecordsReq(
          request, deviceIds.get(i), times.get(i), measurementsList.get(i), valuesList.get(i));
    }

    insertByGroup(recordsGroup, SessionConnection::insertRecords);
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
    if (enableCacheLeader) {
      insertRecordsWithLeaderCache(
          deviceIds, times, measurementsList, typesList, valuesList, false);
    } else {
      TSInsertRecordsReq request =
          genTSInsertRecordsReq(deviceIds, times, measurementsList, typesList, valuesList, false);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
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
  public void insertAlignedRecords(
      List<String> multiSeriesIds,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = multiSeriesIds.size();
    if (len != times.size()
        || len != multiMeasurementComponentsList.size()
        || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "prefixPaths, times, subMeasurementsList and valuesList's size should be equal");
    }
    if (enableCacheLeader) {
      insertRecordsWithLeaderCache(
          multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList, true);
    } else {
      TSInsertRecordsReq request =
          genTSInsertRecordsReq(
              multiSeriesIds, times, multiMeasurementComponentsList, typesList, valuesList, true);
      try {
        defaultSessionConnection.insertRecords(request);
      } catch (RedirectException ignored) {
        // ignore
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
   * @param haveSorted whether the times have been sorted
   * @see Session#insertTablet(Tablet)
   */
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
      throw new IllegalArgumentException(
          "times, measurementsList and valuesList's size should be equal");
    }
    TSInsertRecordsOfOneDeviceReq request =
        genTSInsertRecordsOfOneDeviceReq(
            deviceId, times, measurementsList, typesList, valuesList, haveSorted, false);
    try {
      getSessionConnection(deviceId).insertRecordsOfOneDevice(request);
    } catch (RedirectException e) {
      handleRedirection(deviceId, e.getEndPoint());
    }
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
  public void insertAlignedRecordsOfOneDevice(
      String multiSeriesId,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    insertAlignedRecordsOfOneDevice(
        multiSeriesId, times, multiMeasurementComponentsList, typesList, valuesList, false);
  }

  /**
   * Insert aligned multiple rows, which can reduce the overhead of network. This method is just
   * like jdbc executeBatch, we pack some insert request in batch and send them to server. If you
   * want improve your performance, please see insertTablet method
   *
   * <p>Each row could have same prefixPath but different time, number of measurements
   *
   * @param haveSorted whether the times have been sorted
   * @see Session#insertTablet(Tablet)
   */
  public void insertAlignedRecordsOfOneDevice(
      String multiSeriesId,
      List<Long> times,
      List<List<String>> multiMeasurementComponentsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted)
      throws IoTDBConnectionException, StatementExecutionException {
    int len = times.size();
    if (len != multiMeasurementComponentsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "times, subMeasurementsList and valuesList's size should be equal");
    }
    TSInsertRecordsOfOneDeviceReq request =
        genTSInsertRecordsOfOneDeviceReq(
            multiSeriesId,
            times,
            multiMeasurementComponentsList,
            typesList,
            valuesList,
            haveSorted,
            true);
    try {
      getSessionConnection(multiSeriesId).insertRecordsOfOneDevice(request);
    } catch (RedirectException e) {
      handleRedirection(multiSeriesId, e.getEndPoint());
    }
  }

  private TSInsertRecordsOfOneDeviceReq genTSInsertRecordsOfOneDeviceReq(
      String prefixPath,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList,
      boolean haveSorted,
      boolean isAligned)
      throws IoTDBConnectionException, BatchExecutionException {
    // check params size
    int len = times.size();
    if (len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "times, measurementsList and valuesList's size should be equal");
    }

    if (haveSorted) {
      if (!checkSorted(times)) {
        throw new BatchExecutionException(
            "Times in InsertOneDeviceRecords are not in ascending order");
      }
    } else {
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
      ByteBuffer buffer = ByteBuffer.allocate(calculateLength(typesList.get(i), valuesList.get(i)));
      putValues(typesList.get(i), valuesList.get(i), buffer);
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
      TSInsertRecordsReq request =
          recordsGroup.computeIfAbsent(connection, k -> new TSInsertRecordsReq());
      request.setIsAligned(isAligned);
      updateTSInsertRecordsReq(
          request,
          deviceIds.get(i),
          times.get(i),
          measurementsList.get(i),
          typesList.get(i),
          valuesList.get(i));
    }
    insertByGroup(recordsGroup, SessionConnection::insertRecords);
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
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
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
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, false);
    try {
      getSessionConnection(tablet.prefixPath).insertTablet(request);
    } catch (RedirectException e) {
      handleRedirection(tablet.prefixPath, e.getEndPoint());
    }
  }

  /**
   * insert a Tablet
   *
   * @param tablet data batch
   * @param sorted whether times in Tablet are in ascending order
   */
  public void insertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);
    try {
      getSessionConnection(tablet.prefixPath).insertTablet(request);
    } catch (RedirectException e) {
      handleRedirection(tablet.prefixPath, e.getEndPoint());
    }
  }

  private TSInsertTabletReq genTSInsertTabletReq(Tablet tablet, boolean sorted)
      throws BatchExecutionException {
    if (sorted) {
      checkSortedThrowable(tablet);
    } else {
      sortTablet(tablet);
    }

    TSInsertTabletReq request = new TSInsertTabletReq();

    if (tablet.isAligned()) {
      if (tablet.getSchemas().size() > 1) {
        throw new BatchExecutionException("One tablet should only contain one aligned timeseries!");
      }
      request.setIsAligned(true);
      IMeasurementSchema measurementSchema = tablet.getSchemas().get(0);
      request.setPrefixPath(tablet.prefixPath);
      int measurementsSize = measurementSchema.getSubMeasurementsList().size();
      for (int i = 0; i < measurementsSize; i++) {
        request.addToMeasurements(measurementSchema.getSubMeasurementsList().get(i));
        request.addToTypes(measurementSchema.getSubMeasurementsTSDataTypeList().get(i).ordinal());
      }
      request.setIsAligned(true);
    } else {
      for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
        request.setPrefixPath(tablet.prefixPath);
        request.addToMeasurements(measurementSchema.getMeasurementId());
        request.addToTypes(measurementSchema.getType().ordinal());
        request.setIsAligned(tablet.isAligned());
      }
    }
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
  public void insertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    insertTablets(tablets, false);
  }

  /**
   * insert the data of several devices. Given a device, for each timestamp, the number of
   * measurements is the same.
   *
   * @param tablets data batch in multiple device
   * @param sorted whether times in each Tablet are in ascending order
   */
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    if (enableCacheLeader) {
      insertTabletsWithLeaderCache(tablets, sorted);
    } else {
      TSInsertTabletsReq request = genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted);
      try {
        defaultSessionConnection.insertTablets(request);
      } catch (RedirectException ignored) {
        // ignored
      }
    }
  }

  private void insertTabletsWithLeaderCache(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    Map<SessionConnection, TSInsertTabletsReq> tabletGroup = new HashMap<>();
    for (Entry<String, Tablet> entry : tablets.entrySet()) {
      final SessionConnection connection = getSessionConnection(entry.getKey());
      TSInsertTabletsReq request =
          tabletGroup.computeIfAbsent(connection, k -> new TSInsertTabletsReq());
      updateTSInsertTabletsReq(request, entry.getValue(), sorted);
    }

    insertByGroup(tabletGroup, SessionConnection::insertTablets);
  }

  private TSInsertTabletsReq genTSInsertTabletsReq(List<Tablet> tablets, boolean sorted)
      throws BatchExecutionException {
    TSInsertTabletsReq request = new TSInsertTabletsReq();
    if (tablets.isEmpty()) {
      throw new BatchExecutionException("No tablet is inserting!");
    }
    boolean isFirstTabletAligned = tablets.get(0).isAligned();
    for (Tablet tablet : tablets) {
      if (isFirstTabletAligned != tablet.isAligned()) {
        throw new BatchExecutionException("The tablets should be all aligned or non-aligned!");
      }
      updateTSInsertTabletsReq(request, tablet, sorted);
    }
    return request;
  }

  private void updateTSInsertTabletsReq(TSInsertTabletsReq request, Tablet tablet, boolean sorted)
      throws BatchExecutionException {
    if (sorted) {
      checkSortedThrowable(tablet);
    } else {
      sortTablet(tablet);
    }

    if (tablet.isAligned()) {
      if (tablet.getSchemas().size() > 1) {
        throw new BatchExecutionException("One tablet should only contain one aligned timeseries!");
      }
      request.setIsAligned(true);
      IMeasurementSchema measurementSchema = tablet.getSchemas().get(0);
      request.addToPrefixPaths(tablet.prefixPath);
      int measurementsSize = measurementSchema.getSubMeasurementsList().size();
      List<String> measurements = new ArrayList<>();
      List<Integer> dataTypes = new ArrayList<>();
      for (int i = 0; i < measurementsSize; i++) {
        measurements.add(measurementSchema.getSubMeasurementsList().get(i));
        dataTypes.add(measurementSchema.getSubMeasurementsTSDataTypeList().get(i).ordinal());
      }
      request.addToMeasurementsList(measurements);
      request.addToTypesList(dataTypes);
      request.setIsAligned(true);
    } else {
      request.addToPrefixPaths(tablet.prefixPath);
      List<String> measurements = new ArrayList<>();
      List<Integer> dataTypes = new ArrayList<>();
      for (IMeasurementSchema measurementSchema : tablet.getSchemas()) {
        measurements.add(measurementSchema.getMeasurementId());
        dataTypes.add(measurementSchema.getType().ordinal());
      }
      request.addToMeasurementsList(measurements);
      request.addToTypesList(dataTypes);
    }
    request.addToTimestampsList(SessionUtils.getTimeBuffer(tablet));
    request.addToValuesList(SessionUtils.getValueBuffer(tablet));
    request.addToSizeList(tablet.rowSize);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablet(tablet, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);
    defaultSessionConnection.testInsertTablet(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    testInsertTablets(tablets, false);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletsReq request = genTSInsertTabletsReq(new ArrayList<>(tablets.values()), sorted);
    defaultSessionConnection.testInsertTablets(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordsReq request =
        genTSInsertStringRecordsReq(deviceIds, times, measurementsList, valuesList, false);
    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(
      List<String> deviceIds,
      List<Long> times,
      List<List<String>> measurementsList,
      List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordsReq request =
        genTSInsertRecordsReq(deviceIds, times, measurementsList, typesList, valuesList, false);
    defaultSessionConnection.testInsertRecords(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(
      String deviceId, long time, List<String> measurements, List<String> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request =
        genTSInsertStringRecordReq(deviceId, time, measurements, values, false);
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(
      String deviceId,
      long time,
      List<String> measurements,
      List<TSDataType> types,
      List<Object> values)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request =
        genTSInsertRecordReq(deviceId, time, measurements, types, values, false);
    defaultSessionConnection.testInsertRecord(request);
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    defaultSessionConnection.deleteTimeseries(Collections.singletonList(path));
  }

  /**
   * delete some timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
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

  private int calculateLength(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Byte.BYTES;
      switch (types.get(i)) {
        case BOOLEAN:
          res += 1;
          break;
        case INT32:
          res += Integer.BYTES;
          break;
        case INT64:
          res += Long.BYTES;
          break;
        case FLOAT:
          res += Float.BYTES;
          break;
        case DOUBLE:
          res += Double.BYTES;
          break;
        case TEXT:
          res += Integer.BYTES;
          res += ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET).length;
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    return res;
  }

  /**
   * put value in buffer
   *
   * @param types types list
   * @param values values list
   * @param buffer buffer to insert
   * @throws IoTDBConnectionException
   */
  private void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
      if (values.get(i) == null) {
        ReadWriteIOUtils.write(TYPE_NULL, buffer);
        continue;
      }
      ReadWriteIOUtils.write(types.get(i), buffer);
      switch (types.get(i)) {
        case BOOLEAN:
          ReadWriteIOUtils.write((Boolean) values.get(i), buffer);
          break;
        case INT32:
          ReadWriteIOUtils.write((Integer) values.get(i), buffer);
          break;
        case INT64:
          ReadWriteIOUtils.write((Long) values.get(i), buffer);
          break;
        case FLOAT:
          ReadWriteIOUtils.write((Float) values.get(i), buffer);
          break;
        case DOUBLE:
          ReadWriteIOUtils.write((Double) values.get(i), buffer);
          break;
        case TEXT:
          byte[] bytes = ((String) values.get(i)).getBytes(TSFileConfig.STRING_CHARSET);
          ReadWriteIOUtils.write(bytes.length, buffer);
          buffer.put(bytes);
          break;
        default:
          throw new IoTDBConnectionException(MSG_UNSUPPORTED_DATA_TYPE + types.get(i));
      }
    }
    buffer.flip();
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

  private void checkSortedThrowable(Tablet tablet) throws BatchExecutionException {
    if (!checkSorted(tablet)) {
      throw new BatchExecutionException("Times in Tablet are not in ascending order");
    }
  }

  protected void sortTablet(Tablet tablet) {
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
      if (schema instanceof UnaryMeasurementSchema) {
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

  public void setSchemaTemplate(String templateName, String prefixPath)
      throws IoTDBConnectionException, StatementExecutionException {
    TSSetSchemaTemplateReq request = getTSSetSchemaTemplateReq(templateName, prefixPath);
    defaultSessionConnection.setSchemaTemplate(request);
  }

  /**
   * @param name template name
   * @param schemaNames list of schema names, if this measurement is vector, name it. if this
   *     measurement is not vector, keep this name as same as measurement's name
   * @param measurements List of measurements, if it is a single measurement, just put it's name
   *     into a list and add to measurements if it is a vector measurement, put all measurements of
   *     the vector into a list and add to measurements
   * @param dataTypes List of datatypes, if it is a single measurement, just put it's type into a
   *     list and add to dataTypes if it is a vector measurement, put all types of the vector into a
   *     list and add to dataTypes
   * @param encodings List of encodings, if it is a single measurement, just put it's encoding into
   *     a list and add to encodings if it is a vector measurement, put all encodings of the vector
   *     into a list and add to encodings
   * @param compressors List of compressors
   * @throws IoTDBConnectionException
   * @throws StatementExecutionException
   */
  public void createSchemaTemplate(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateSchemaTemplateReq request =
        getTSCreateSchemaTemplateReq(
            name, schemaNames, measurements, dataTypes, encodings, compressors);
    defaultSessionConnection.createSchemaTemplate(request);
  }

  public void unsetSchemaTemplate(String prefixPath, String templateName)
      throws IoTDBConnectionException, StatementExecutionException {
    TSUnsetSchemaTemplateReq request = getTSUnsetSchemaTemplateReq(prefixPath, templateName);
    defaultSessionConnection.unsetSchemaTemplate(request);
  }

  private TSSetSchemaTemplateReq getTSSetSchemaTemplateReq(String templateName, String prefixPath) {
    TSSetSchemaTemplateReq request = new TSSetSchemaTemplateReq();
    request.setTemplateName(templateName);
    request.setPrefixPath(prefixPath);
    return request;
  }

  private TSCreateSchemaTemplateReq getTSCreateSchemaTemplateReq(
      String name,
      List<String> schemaNames,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<CompressionType> compressors) {
    TSCreateSchemaTemplateReq request = new TSCreateSchemaTemplateReq();
    request.setName(name);
    request.setSchemaNames(schemaNames);
    request.setMeasurements(measurements);

    List<List<Integer>> requestType = new ArrayList<>();
    for (List<TSDataType> typesList : dataTypes) {
      requestType.add(typesList.stream().map(TSDataType::ordinal).collect(Collectors.toList()));
    }
    request.setDataTypes(requestType);

    List<List<Integer>> requestEncoding = new ArrayList<>();
    for (List<TSEncoding> encodingList : encodings) {
      requestEncoding.add(
          encodingList.stream().map(TSEncoding::ordinal).collect(Collectors.toList()));
    }
    request.setEncodings(requestEncoding);
    request.setCompressors(
        compressors.stream().map(CompressionType::ordinal).collect(Collectors.toList()));
    return request;
  }

  private TSUnsetSchemaTemplateReq getTSUnsetSchemaTemplateReq(
      String prefixPath, String templateName) {
    TSUnsetSchemaTemplateReq request = new TSUnsetSchemaTemplateReq();
    request.setPrefixPath(prefixPath);
    request.setTemplateName(templateName);
    return request;
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
                          e.getDeviceEndPointMap()
                              .forEach(
                                  (deviceId, endpoint) -> {
                                    try {
                                      handleRedirection(deviceId, endpoint);
                                    } catch (IoTDBConnectionException ioTDBConnectionException) {
                                      throw new CompletionException(ioTDBConnectionException);
                                    }
                                  });
                        } catch (StatementExecutionException e) {
                          throw new CompletionException(e);
                        } catch (IoTDBConnectionException e) {
                          // remove the broken session
                          removeBrokenSessionConnection(connection);
                          throw new CompletionException(e);
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

  public boolean isEnableQueryRedirection() {
    return enableQueryRedirection;
  }

  public void setEnableQueryRedirection(boolean enableQueryRedirection) {
    this.enableQueryRedirection = enableQueryRedirection;
  }

  public boolean isEnableCacheLeader() {
    return enableCacheLeader;
  }

  public void setEnableCacheLeader(boolean enableCacheLeader) {
    this.enableCacheLeader = enableCacheLeader;
  }

  public static class Builder {
    private String host = Config.DEFAULT_HOST;
    private int rpcPort = Config.DEFAULT_PORT;
    private String username = Config.DEFAULT_USER;
    private String password = Config.DEFAULT_PASSWORD;
    private int fetchSize = Config.DEFAULT_FETCH_SIZE;
    private ZoneId zoneId = null;
    private int thriftDefaultBufferSize = Config.DEFAULT_INITIAL_BUFFER_CAPACITY;
    private int thriftMaxFrameSize = Config.DEFAULT_MAX_FRAME_SIZE;
    private boolean enableCacheLeader = Config.DEFAULT_CACHE_LEADER_MODE;
    List<String> nodeUrls = null;

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
      this.password = password;
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

    public Builder enableCacheLeader(boolean enableCacheLeader) {
      this.enableCacheLeader = enableCacheLeader;
      return this;
    }

    public Builder nodeUrls(List<String> nodeUrls) {
      this.nodeUrls = nodeUrls;
      return this;
    }

    public Session build() {
      if (nodeUrls != null
          && (!Config.DEFAULT_HOST.equals(host) || rpcPort != Config.DEFAULT_PORT)) {
        throw new IllegalArgumentException(
            "You should specify either nodeUrls or (host + rpcPort), but not both");
      }

      if (nodeUrls != null) {
        return new Session(
            nodeUrls,
            username,
            password,
            fetchSize,
            zoneId,
            thriftDefaultBufferSize,
            thriftMaxFrameSize,
            enableCacheLeader);
      }

      return new Session(
          host,
          rpcPort,
          username,
          password,
          fetchSize,
          zoneId,
          thriftDefaultBufferSize,
          thriftMaxFrameSize,
          enableCacheLeader);
    }
  }
}
