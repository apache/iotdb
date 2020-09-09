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

import java.nio.ByteBuffer;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.rpc.BatchExecutionException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertStringRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletReq;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateMultiTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSCreateTimeseriesReq;
import org.apache.iotdb.service.rpc.thrift.TSDeleteDataReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSGetTimeZoneResp;
import org.apache.iotdb.service.rpc.thrift.TSIService;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordsReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSInsertTabletsReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.service.rpc.thrift.TSSetTimeZoneReq;
import org.apache.iotdb.service.rpc.thrift.TSStatus;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Session {

  private static final Logger logger = LoggerFactory.getLogger(Session.class);
  private final TSProtocolVersion protocolVersion = TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3;
  private String host;
  private int port;
  private String username;
  private String password;
  private TSIService.Iface client = null;
  private long sessionId;
  private TTransport transport;
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

  public synchronized void open(boolean enableRPCCompression) throws IoTDBConnectionException {
    open(enableRPCCompression, Config.DEFAULT_TIMEOUT_MS);
  }

  private synchronized void open(boolean enableRPCCompression, int connectionTimeoutInMs)
      throws IoTDBConnectionException {
    if (!isClosed) {
      return;
    }

    transport = new TFastFramedTransport(new TSocket(host, port, connectionTimeoutInMs));

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
   * insert data in one row, if you want to improve your performance, please use insertRecords
   * method or insertTablet method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types,
      Object... values) throws IoTDBConnectionException, StatementExecutionException {
    List<Object> valuesList = new ArrayList<>(Arrays.asList(values));

    insertRecord(deviceId, time, measurements, types, valuesList);
  }


  /**
   * insert the data of a device. For each timestamp, the number of measurements is the same.
   * <p>
   * a Tablet example:
   * <p>
   * device1 time s1, s2, s3 1,   1,  1,  1 2,   2,  2,  2 3,   3,  3,  3
   * <p>
   * times in Tablet may be not in ascending order
   *
   * @param tablet data batch
   */
  public void insertTablet(Tablet tablet)
      throws StatementExecutionException, IoTDBConnectionException {
    insertTablet(tablet, false);
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
      RpcUtils.verifySuccess(client.insertTablet(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertTabletReq genTSInsertTabletReq(Tablet tablet, boolean sorted)
      throws BatchExecutionException {
    if (sorted) {
      if (!checkSorted(tablet)) {
        throw new BatchExecutionException("Times in Tablet are not in ascending order");
      }
    } else {
      sortTablet(tablet);
    }

    TSInsertTabletReq request = new TSInsertTabletReq();
    request.setSessionId(sessionId);
    request.deviceId = tablet.deviceId;
    for (MeasurementSchema measurementSchema : tablet.getSchemas()) {
      request.addToMeasurements(measurementSchema.getMeasurementId());
      request.addToTypes(measurementSchema.getType().ordinal());
    }
    request.setTimestamps(SessionUtils.getTimeBuffer(tablet));
    request.setValues(SessionUtils.getValueBuffer(tablet));
    request.setSize(tablet.rowSize);
    return request;
  }

  /**
   * insert the data of several deivces. Given a deivce, for each timestamp, the number of
   * measurements is the same.
   * <p>
   * Times in each Tablet may not be in ascending order
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
   * @param sorted  whether times in each Tablet are in ascending order
   */
  public void insertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {

    TSInsertTabletsReq request = genTSInsertTabletsReq(tablets, sorted);
    try {
      RpcUtils.verifySuccess(client.insertTablets(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertTabletsReq genTSInsertTabletsReq(Map<String, Tablet> tablets, boolean sorted)
      throws BatchExecutionException {
    TSInsertTabletsReq request = new TSInsertTabletsReq();
    request.setSessionId(sessionId);

    for (Tablet tablet : tablets.values()) {
      if (sorted) {
        if (!checkSorted(tablet)) {
          throw new BatchExecutionException("Times in Tablet are not in ascending order");
        }
      } else {
        sortTablet(tablet);
      }

      request.addToDeviceIds(tablet.deviceId);
      List<String> measurements = new ArrayList<>();
      List<Integer> dataTypes = new ArrayList<>();
      for (MeasurementSchema measurementSchema : tablet.getSchemas()) {
        measurements.add(measurementSchema.getMeasurementId());
        dataTypes.add(measurementSchema.getType().ordinal());
      }
      request.addToMeasurementsList(measurements);
      request.addToTypesList(dataTypes);
      request.addToTimestampsList(SessionUtils.getTimeBuffer(tablet));
      request.addToValuesList(SessionUtils.getValueBuffer(tablet));
      request.addToSizeList(tablet.rowSize);
    }
    return request;
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordsReq request = genTSInsertRecordsReq(deviceIds, times, measurementsList,
        typesList, valuesList);
    try {
      RpcUtils.verifySuccess(client.insertRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertRecordsReq genTSInsertRecordsReq(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList) throws IoTDBConnectionException {
    // check params size
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }

    TSInsertRecordsReq request = new TSInsertRecordsReq();
    request.setSessionId(sessionId);
    request.setDeviceIds(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    List<ByteBuffer> buffersList = new ArrayList<>();
    for (int i = 0; i < measurementsList.size(); i++) {
      ByteBuffer buffer = ByteBuffer.allocate(calculateLength(typesList.get(i), valuesList.get(i)));
      putValues(typesList.get(i), valuesList.get(i), buffer);
      buffer.flip();
      buffersList.add(buffer);
    }
    request.setValuesList(buffersList);
    return  request;
  }

  /**
   * Insert multiple rows, which can reduce the overhead of network. This method is just like jdbc
   * executeBatch, we pack some insert request in batch and send them to server. If you want improve
   * your performance, please see insertTablet method
   * <p>
   * Each row is independent, which could have different deviceId, time, number of measurements
   *
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {

    TSInsertStringRecordsReq request = genTSInsertStringRecordsReq(deviceIds, times,
        measurementsList, valuesList);
    try {
      RpcUtils.verifySuccess(client.insertStringRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertStringRecordsReq genTSInsertStringRecordsReq(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList) {
    // check params size
    int len = deviceIds.size();
    if (len != times.size() || len != measurementsList.size() || len != valuesList.size()) {
      throw new IllegalArgumentException(
          "deviceIds, times, measurementsList and valuesList's size should be equal");
    }

    TSInsertStringRecordsReq request = new TSInsertStringRecordsReq();
    request.setSessionId(sessionId);
    request.setDeviceIds(deviceIds);
    request.setTimestamps(times);
    request.setMeasurementsList(measurementsList);
    request.setValuesList(valuesList);
    return request;
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types,
      List<Object> values) throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request = genTSInsertRecordReq(deviceId, time, measurements, types, values);
    try {
      RpcUtils.verifySuccess(client.insertRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertRecordReq genTSInsertRecordReq(String deviceId, long time, List<String> measurements,
      List<TSDataType> types,
      List<Object> values) throws IoTDBConnectionException {
    TSInsertRecordReq request = new TSInsertRecordReq();
    request.setSessionId(sessionId);
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    ByteBuffer buffer = ByteBuffer.allocate(calculateLength(types, values));
    putValues(types, values, buffer);
    buffer.flip();
    request.setValues(buffer);
    return  request;
  }

  /**
   * insert data in one row, if you want improve your performance, please use insertInBatch method
   * or insertBatch method
   *
   * @see Session#insertRecords(List, List, List, List, List)
   * @see Session#insertTablet(Tablet)
   */
  public void insertRecord(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {

    TSInsertStringRecordReq request = genTSInsertStringRecordReq(deviceId, time, measurements, values);
    try {
      RpcUtils.verifySuccess(client.insertStringRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  private TSInsertStringRecordReq genTSInsertStringRecordReq(String deviceId, long time,
      List<String> measurements, List<String> values) {
    TSInsertStringRecordReq request = new TSInsertStringRecordReq();
    request.setSessionId(sessionId);
    request.setDeviceId(deviceId);
    request.setTimestamp(time);
    request.setMeasurements(measurements);
    request.setValues(values);
    return request;
  }

  /**
   * put value in buffer
   *
   * @param types  types list
   * @param values values list
   * @param buffer buffer to insert
   * @throws IoTDBConnectionException
   */
  private void putValues(List<TSDataType> types, List<Object> values, ByteBuffer buffer)
      throws IoTDBConnectionException {
    for (int i = 0; i < values.size(); i++) {
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
          throw new IoTDBConnectionException("Unsupported data type:" + types.get(i));
      }
    }
  }

  private int calculateStrLength(List<String> values) {
    int res = 0;

    for (int i = 0; i < values.size(); i++) {
      // types
      res += Short.BYTES;
      res += Integer.BYTES;
      res += values.get(i).getBytes(TSFileConfig.STRING_CHARSET).length;
    }

    return res;
  }

  private int calculateLength(List<TSDataType> types, List<Object> values)
      throws IoTDBConnectionException {
    int res = 0;
    for (int i = 0; i < types.size(); i++) {
      // types
      res += Short.BYTES;
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
          throw new IoTDBConnectionException("Unsupported data type:" + types.get(i));
      }
    }

    return res;
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, false);

    try {
      RpcUtils.verifySuccess(client.testInsertTablet(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablet(Tablet tablet, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletReq request = genTSInsertTabletReq(tablet, sorted);

    try {
      RpcUtils.verifySuccess(client.testInsertTablet(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletsReq request = genTSInsertTabletsReq(tablets, false);

    try {
      RpcUtils.verifySuccess(client.testInsertTablets(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertTablets(Map<String, Tablet> tablets, boolean sorted)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertTabletsReq request = genTSInsertTabletsReq(tablets, sorted);

    try {
      RpcUtils.verifySuccess(client.testInsertTablets(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<String>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordsReq request = genTSInsertStringRecordsReq(deviceIds, times,
        measurementsList, valuesList);

    try {
      RpcUtils.verifySuccess(client.testInsertStringRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void testInsertRecords(List<String> deviceIds, List<Long> times,
      List<List<String>> measurementsList, List<List<TSDataType>> typesList,
      List<List<Object>> valuesList)
      throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordsReq request = genTSInsertRecordsReq(deviceIds, times, measurementsList,
        typesList, valuesList);
    try {
      RpcUtils.verifySuccess(client.testInsertRecords(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }


  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<String> values) throws IoTDBConnectionException, StatementExecutionException {
    TSInsertStringRecordReq request = genTSInsertStringRecordReq(deviceId, time, measurements, values);

    try {
      RpcUtils.verifySuccess(client.testInsertStringRecord(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  /**
   * This method NOT insert data into database and the server just return after accept the request,
   * this method should be used to test other time cost in client
   */
  public void testInsertRecord(String deviceId, long time, List<String> measurements,
      List<TSDataType> types, List<Object> values) throws IoTDBConnectionException, StatementExecutionException {
    TSInsertRecordReq request = genTSInsertRecordReq(deviceId, time, measurements, types, values);

    try {
      RpcUtils.verifySuccess(client.testInsertRecord(request));
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
   * @param endTime data with time stamp less than or equal to time will be deleted
   */
  public void deleteData(List<String> paths, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    deleteData(paths, Long.MIN_VALUE, endTime);
  }

  /**
   * delete data >= startTime and data <= endTime in multiple timeseries
   *
   * @param paths   data in which time series to delete
   * @param startTime delete range start time
   * @param endTime delete range end time
   */
  public void deleteData(List<String> paths, long startTime, long endTime)
      throws IoTDBConnectionException, StatementExecutionException {
    TSDeleteDataReq request = new TSDeleteDataReq();
    request.setSessionId(sessionId);
    request.setPaths(paths);
    request.setStartTime(startTime);
    request.setEndTime(endTime);

    try {
      RpcUtils.verifySuccess(client.deleteData(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void setStorageGroup(String storageGroupId)
      throws IoTDBConnectionException, StatementExecutionException {
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
    createTimeseries(path, dataType, encoding, compressor, null, null, null, null);
  }

  public void createTimeseries(String path, TSDataType dataType,
      TSEncoding encoding, CompressionType compressor, Map<String, String> props,
      Map<String, String> tags, Map<String, String> attributes, String measurementAlias)
      throws IoTDBConnectionException, StatementExecutionException {
    TSCreateTimeseriesReq request = new TSCreateTimeseriesReq();
    request.setSessionId(sessionId);
    request.setPath(path);
    request.setDataType(dataType.ordinal());
    request.setEncoding(encoding.ordinal());
    request.setCompressor(compressor.ordinal());
    request.setProps(props);
    request.setTags(tags);
    request.setAttributes(attributes);
    request.setMeasurementAlias(measurementAlias);

    try {
      RpcUtils.verifySuccess(client.createTimeseries(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public void createMultiTimeseries(List<String> paths, List<TSDataType> dataTypes,
      List<TSEncoding> encodings, List<CompressionType> compressors,
      List<Map<String, String>> propsList, List<Map<String, String>> tagsList,
      List<Map<String, String>> attributesList, List<String> measurementAliasList)
      throws IoTDBConnectionException, StatementExecutionException {

    TSCreateMultiTimeseriesReq request = new TSCreateMultiTimeseriesReq();
    request.setSessionId(sessionId);
    request.setPaths(paths);

    List<Integer> dataTypeOrdinals = new ArrayList<>(paths.size());
    for (TSDataType dataType : dataTypes) {
      dataTypeOrdinals.add(dataType.ordinal());
    }
    request.setDataTypes(dataTypeOrdinals);

    List<Integer> encodingOrdinals = new ArrayList<>(paths.size());
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

    try {
      RpcUtils.verifySuccess(client.createMultiTimeseries(request));
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
  }

  public boolean checkTimeseriesExists(String path)
      throws IoTDBConnectionException, StatementExecutionException {
    SessionDataSet dataSet = executeQueryStatement(String.format("SHOW TIMESERIES %s", path));
    boolean result = dataSet.hasNext();
    dataSet.closeOperationHandle();
    return result;
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
   * execure query sql
   *
   * @param sql query statement
   * @return result set
   */
  public SessionDataSet executeQueryStatement(String sql)
      throws StatementExecutionException, IoTDBConnectionException {

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
        execResp.columnNameIndexMap,
        execResp.getQueryId(), client, sessionId, execResp.queryDataSet,
        execResp.isIgnoreTimeStamp());
  }

  /**
   * execute non query statement
   *
   * @param sql non query statement
   */
  public void executeNonQueryStatement(String sql)
      throws IoTDBConnectionException, StatementExecutionException {
    TSExecuteStatementReq execReq = new TSExecuteStatementReq(sessionId, sql, statementId);
    try {
      TSExecuteStatementResp execResp = client.executeUpdateStatement(execReq);
      RpcUtils.verifySuccess(execResp.getStatus());
    } catch (TException e) {
      throw new IoTDBConnectionException(e);
    }
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
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      tablet.values[i] =
          sortList(tablet.values[i], tablet.getSchemas().get(i).getType(), index);
    }
  }

  /**
   * sort value list by index
   *
   * @param valueList value list
   * @param dataType  data type
   * @param index     index
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
        throw new UnSupportedDataTypeException("Unsupported data type:" + dataType);
    }
  }

}
