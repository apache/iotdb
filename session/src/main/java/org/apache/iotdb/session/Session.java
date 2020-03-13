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

import static org.apache.iotdb.session.Config.PATH_PATTERN;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSBatchInsertionReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertInBatchReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.record.RowBatch;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  private final TSProtocolVersion protocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V2;
  private String host;
  private int port;
  private String username;
  private String password;
  private TSIService.Iface client = null;
  private long sessionId;
  private TSocket transport;
  private boolean isClosed = true;
  private ZoneId zoneId;
  private long statementId;
  private int fetchSize;

  public Session(String host, int port) {
    this(host, port, Config.DEFAULT_USER, Config.DEFAULT_PASSWORD);
  }

  public Session(String host, String port, String username, String password) {
    this(host, Integer.parseInt(port), username, password);
  }

  public Session(String host, int port, String username, String password) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.fetchSize = Config.DEFAULT_FETCH_SIZE;
  }

  public Session(String host, int port, String username, String password, int fetchSize) {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.fetchSize = fetchSize;
  }

  public synchronized void open() throws IoTDBConnectionException {
    open(false, Config.DEFAULT_TIMEOUT_MS);
  }

  private synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }
    transport = new TSocket(host, port, connectionTimeoutInMs);
    if (!transport.isOpen()) {
      try {
        transport.open();
      } catch (TTransportException e) {
        throw new IoTDBConnectionException(e);
      }
    }

    if (enableRPCCompression) {
      client = new TSIService.Client(new TCompactProtocol(transport));
    } else {
      client = new TSIService.Client(new TBinaryProtocol(transport));
    }

    TSOpenSessionReq openReq = new TSOpenSessionReq();
    openReq.setUsername(username);
    openReq.setPassword(password);

    try {
      TSOpenSessionResp openResp = client.openSession(openReq);

      RpcUtils.verifySuccess(openResp.getStatus());

      if (protocolVersion.getValue() != openResp.getServerProtocolVersion().getValue()) {
        logger.warn("Protocol differ, Client version is {}}, but Server version is {}",
            protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue());
        if (openResp.getServerProtocolVersion().getValue() == 0) {// less than 0.10
          throw new TException(String
              .format("Protocol not supported, Client version is %s, but Server version is %s",
                  protocolVersion.getValue(), openResp.getServerProtocolVersion().getValue()));
        }
      }

      sessionId = openResp.getSessionId();

      statementId = client.requestStatementId(sessionId);

      if (zoneId != null) {
        setTimeZone(zoneId.toString());
      } else {
        zoneId = ZoneId.of(getTimeZone());
      }

    } catch (Exception e) {
      transport.close();
      throw new IoTDBConnectionException(e);
    }
    isClosed = false;

    client = RpcUtils.newSynchronizedClient(client);

  }

  public synchronized void close() throws IoTDBConnectionException {
    if (isClosed) {
      return;
    }
    TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
    try {
      client.closeSession(req);
    } catch (TException e) {
      throw new IoTDBConnectionException(
          "Error occurs when closing session at server. Maybe server is down.", e);
    } finally {
      isClosed = true;
      if (transport != null) {
        transport.close();
      }
    }
  }

  /**
   * check whether the batch has been sorted
   * @return whether the batch has been sorted
   */
  private boolean checkSorted(RowBatch rowBatch){
    for (int i = 1; i < rowBatch.batchSize; i++) {
      if(rowBatch.timestamps[i] < rowBatch.timestamps[i - 1]){
        return false;
      }
    }

    return true;
  }

  /**
   * use batch interface to insert sorted data
   *
   * @param rowBatch data batch
   */
  private void insertSortedBatchIntern(RowBatch rowBatch)
      throws IoTDBConnectionException, BatchExecutionException {
    TSBatchInsertionReq request = new TSBatchInsertionReq();
    request.setSessionId(sessionId);
    request.deviceId = rowBatch.deviceId;
    for (MeasurementSchema measurementSchema : rowBatch.measurements) {
      request.addToMeasurements(measurementSchema.getMeasurementId());
      request.addToTypes(measurementSchema.getType().ordinal());
    }
    request.setTimestamps(SessionUtils.getTimeBuffer(rowBatch));
    request.setValues(SessionUtils.getValueBuffer(rowBatch));
    request.setSize(rowBatch.batchSize);

    try {
      RpcUtils.verifySuccess(client.insertBatch(request).statusList);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * use batch interface to insert sorted data
   * times in row batch must be sorted before!
   *
   * @param rowBatch data batch
   */
  public void insertSortedBatch(RowBatch rowBatch)
      throws BatchExecutionException, IoTDBConnectionException {
    if(!checkSorted(rowBatch)){
      throw new BatchExecutionException("Row batch has't been sorted when calling insertSortedBatch");
    }
    insertSortedBatchIntern(rowBatch);
  }

  /**
   * use batch interface to insert data in multiple device
   *
   * @param rowBatchMap data batch in multiple device
   */
  public void insertMultipleDeviceBatch
      (Map<String, RowBatch> rowBatchMap) throws IoTDBConnectionException, BatchExecutionException {
    for(Map.Entry<String, RowBatch> dataInOneDevice : rowBatchMap.entrySet()){
      sortRowBatch(dataInOneDevice.getValue());
      insertBatch(dataInOneDevice.getValue());
    }
  }

  /**
   * use batch interface to insert sorted data in multiple device
   * times in row batch must be sorted before!
   *
   * @param rowBatchMap data batch in multiple device
   */
  public void insertMultipleDeviceSortedBatch
  (Map<String, RowBatch> rowBatchMap) throws IoTDBConnectionException, BatchExecutionException {
    for(Map.Entry<String, RowBatch> dataInOneDevice : rowBatchMap.entrySet()){
      checkSorted(dataInOneDevice.getValue());
      insertSortedBatchIntern(dataInOneDevice.getValue());
    }
  }

  /**
   * use batch interface to insert data
   *
   * @param rowBatch data batch
   */
  public void insertBatch(RowBatch rowBatch)
      throws IoTDBConnectionException, BatchExecutionException {

    sortRowBatch(rowBatch);

    insertSortedBatchIntern(rowBatch);
  }

  private void sortRowBatch(RowBatch rowBatch){
    /*
     * following part of code sort the batch data by time,
     * so we can insert continuous data in value list to get a better performance
     */
    // sort to get index, and use index to sort value list
    Integer[] index = new Integer[rowBatch.batchSize];
    for (int i = 0; i < rowBatch.batchSize; i++) {
      index[i] = i;
    }
    Arrays.sort(index, Comparator.comparingLong(o -> rowBatch.timestamps[o]));
    Arrays.sort(rowBatch.timestamps, 0, rowBatch.batchSize);
    for (int i = 0; i < rowBatch.measurements.size(); i++) {
      rowBatch.values[i] =
          sortList(rowBatch.values[i], rowBatch.measurements.get(i).getType(), index);
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
          sortedValues[index[i]] = boolValues[i];
        }
        return sortedValues;
      case INT32:
        int[] intValues = (int[]) valueList;
        int[] sortedIntValues = new int[intValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedIntValues[index[i]] = intValues[i];
        }
        return sortedIntValues;
      case INT64:
        long[] longValues = (long[]) valueList;
        long[] sortedLongValues = new long[longValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedLongValues[index[i]] = longValues[i];
        }
        return sortedLongValues;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        float[] sortedFloatValues = new float[floatValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedFloatValues[index[i]] = floatValues[i];
        }
        return sortedFloatValues;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        double[] sortedDoubleValues = new double[doubleValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedDoubleValues[index[i]] = doubleValues[i];
        }
        return sortedDoubleValues;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        Binary[] sortedBinaryValues = new Binary[binaryValues.length];
        for (int i = 0; i < index.length; i++) {
          sortedBinaryValues[index[i]] = binaryValues[i];
        }
        return sortedBinaryValues;
      default:
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

  /**
   * Insert data in batch format, which can reduce the overhead of network. This method is just like
   * jdbc batch insert, we pack some insert request in batch and send them to server If you want
   * improve your performance, please see insertBatch method
   *
   * @see Session#insertBatch(RowBatch)
   */
  public void insertInBatch(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, BatchExecutionException {
    // check params size
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }

    TSInsertInBatchReq request = new TSInsertInBatchReq();
    request.setSessionId(sessionId);
    request.setDeviceIds(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    request.setValuesList(valuesList);

    try {
      RpcUtils.verifySuccess(client.insertRowInBatch(request).statusList);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertInBatch(List, List, List, List)
   * @see Session#insertBatch(RowBatch)
   */
  public TSStatus insert(String deviceId, long time, List<String> measurements,
      Object... values) throws IoTDBConnectionException, StatementExecutionException {
    List<String> stringValues = new ArrayList<>();
    for (Object o : values) {
      stringValues.add(o.toString());
    }

    return insert(deviceId, time, measurements, stringValues);
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertInBatch(List, List, List, List)
   * @see Session#insertBatch(RowBatch)
   */
  public TSStatus insert(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {
    TSInsertReq request = new TSInsertReq();
    request.setSessionId(sessionId);
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);

    TSStatus result;
    try {
      result = client.insert(request);
      RpcUtils.verifySuccess(result);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }

    return result;
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertBatch(RowBatch rowBatch)
      throws IoTDBConnectionException, BatchExecutionException {
    TSBatchInsertionReq request = new TSBatchInsertionReq();
    request.setSessionId(sessionId);
    request.deviceId = rowBatch.deviceId;
    for (MeasurementSchema measurementSchema : rowBatch.measurements) {
      request.addToMeasurements(measurementSchema.getMeasurementId());
      request.addToTypes(measurementSchema.getType().ordinal());
    }
    request.setTimestamps(SessionUtils.getTimeBuffer(rowBatch));
    request.setValues(SessionUtils.getValueBuffer(rowBatch));
    request.setSize(rowBatch.batchSize);

    try {
      RpcUtils.verifySuccess(client.testInsertBatch(request).statusList);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertInBatch(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, BatchExecutionException {
    // check params size
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }

    TSInsertInBatchReq request = new TSInsertInBatchReq();
    request.setSessionId(sessionId);
    request.setDeviceIds(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    request.setValuesList(valuesList);

    try {
      RpcUtils.verifySuccess(client.testInsertRowInBatch(request).statusList);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsert(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {
    TSInsertReq request = new TSInsertReq();
    request.setSessionId(sessionId);
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);

    try {
      RpcUtils.verifySuccess(client.testInsertRow(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param path timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(path);
    deleteTimeseries(paths);
  }

  /**
   * delete a timeseries, including data and schema
   *
   * @param paths timeseries to delete, should be a whole path
   */
  public void deleteTimeseries(List<String> paths)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.deleteTimeseries(sessionId, paths));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * delete data <= time in one timeseries
   *
   * @param path data in which time series to delete
   * @param time data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(String path, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> paths = new ArrayList<>();
    paths.add(path);
    deleteData(paths, time);
  }

  /**
   * delete data <= time in multiple timeseries
   *
   * @param paths data in which time series to delete
   * @param time  data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(List<String> paths, long time)
      throws IoTDBConnectionException, StatementExecutionException {
    TSDeleteDataReq request = new TSDeleteDataReq();
    request.setSessionId(sessionId);
    request.setPaths(paths);
    request.setTimestamp(time);

    try {
      RpcUtils.verifySuccess(client.deleteData(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException {
    checkPathValidity(storageGroupId);
    try {
      RpcUtils.verifySuccess(client.setStorageGroup(sessionId, storageGroupId));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }


  public void deleteStorageGroup(String storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    List<String> groups = new ArrayList<>();
    groups.add(storageGroup);
    deleteStorageGroups(groups);
  }

  public void deleteStorageGroups(List<String> storageGroup)
      throws IoTDBConnectionException, StatementExecutionException {
    try {
      RpcUtils.verifySuccess(client.deleteStorageGroups(sessionId, storageGroup));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor)
      throws IoTDBConnectionException, StatementExecutionException {
    checkPathValidity(path);
    TSCreateTimeseriesReq request = new TSCreateTimeseriesReq();
    request.setSessionId(sessionId);
    request.setPath(path);
    request.setDataType(dataType.ordinal());
    request.setEncoding(encoding.ordinal());
    request.setCompressor(compressor.ordinal());

    try {
      RpcUtils.verifySuccess(client.createTimeseries(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public boolean checkTimeseriesExists(String path)
      throws StatementExecutionException, IoTDBConnectionException {
    checkPathValidity(path);
    try {
      return executeQueryStatement(String.format("SHOW TIMESERIES %s", path)).hasNext();
    } catch (Exception e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private synchronized String getTimeZone()
      throws StatementExecutionException, IoTDBConnectionException {
    if (zoneId != null) {
      return zoneId.toString();
    }

    TSGetTimeZoneResp resp;
    try {
      resp = client.getTimeZone(sessionId);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
    RpcUtils.verifySuccess(resp.getStatus());
    return resp.getTimeZone();
  }

  private synchronized void setTimeZone(String zoneId)
      throws StatementExecutionException, IoTDBConnectionException {
    TSSetTimeZoneReq req = new TSSetTimeZoneReq(sessionId, zoneId);
    TSStatus resp;
    try {
      resp = client.setTimeZone(req);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
    RpcUtils.verifySuccess(resp);
    this.zoneId = ZoneId.of(zoneId);
  }

  /**
   * check whether this sql is for query
   *
   * @param sql sql
   * @return whether this sql is for query
   */
  private boolean checkIsQuery(String sql) {
    sql = sql.trim().toLowerCase();
    return sql.startsWith("select") || sql.startsWith("show") || sql.startsWith("list");
  }

  /**
   * execure query sql
   *
   * @param sql query statement
   * @return result set
   */
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    if (!checkIsQuery(sql)) {
      throw new IllegalArgumentException("your sql \"" + sql
          + "\" is not a query statement, you should use executeNonQueryStatement method instead.");
    }

    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    execReq.setFetchSize(fetchSize);
    TSExecuteStatementResp execResp;
    try {
      execResp = client.executeQueryStatement(execReq);
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }

    RpcUtils.verifySuccess(execResp.getStatus());
    return new SessionDataSet(sql, execResp.getColumns(), execResp.getDataTypeList(),
        execResp.getQueryId(), client, sessionId, execResp.queryDataSet);
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    if (checkIsQuery(sql)) {
      throw new IllegalArgumentException("your sql \"" + sql
          + "\" is a query statement, you should use executeQueryStatement method instead.");
    }

    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private void checkPathValidity(String path) throws StatementExecutionException {
    if (!PATH_PATTERN.matcher(path).matches()) {
      throw new StatementExecutionException(String.format("Path [%s] is invalid", path));
    }
  }
}
