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
package org.apache.iotdb.db.protocol.influxdb.meta;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.query.control.SessionManager;
import org.apache.iotdb.db.query.control.clientsession.IClientSession;
import org.apache.iotdb.db.query.control.clientsession.InternalClientSession;
import org.apache.iotdb.db.service.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.IoTDBJDBCDataSet;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSCloseSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;
import org.apache.iotdb.service.rpc.thrift.TSInsertRecordReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionReq;
import org.apache.iotdb.service.rpc.thrift.TSOpenSessionResp;

import org.influxdb.InfluxDBException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** InfluxDBMetaManager for NewIoTDB When schema region is memory or schema file */
public class NewInfluxDBMetaManager extends AbstractInfluxDBMetaManager {

  // NewIoTDB uses ClientRPCServiceImpl to handle the request
  private final ClientRPCServiceImpl clientRPCService;

  private NewInfluxDBMetaManager() {
    clientRPCService = NewInfluxDBServiceImpl.getClientRPCService();
  }

  public static NewInfluxDBMetaManager getInstance() {
    return InfluxDBMetaManagerHolder.INSTANCE;
  }

  /** recover the influxdb metadata */
  @Override
  public void recover() {
    long sessionID = 0;
    try {
      IClientSession clientSession = new InternalClientSession("InfluxDB-Recovery");
      SessionManager.getInstance().registerSession(clientSession);
      TSOpenSessionResp tsOpenSessionResp =
          clientRPCService.openSession(
              new TSOpenSessionReq()
                  .setUsername("root")
                  .setPassword("root")
                  .setZoneId("Asia/Shanghai"));
      sessionID = tsOpenSessionResp.getSessionId();
      TSExecuteStatementResp resp =
          NewInfluxDBServiceImpl.executeStatement(SELECT_TAG_INFO_SQL, sessionID);
      IoTDBJDBCDataSet dataSet = QueryResultUtils.creatIoTJDBCDataset(resp);
      try {
        Map<String, Map<String, Integer>> measurement2TagOrders;
        Map<String, Integer> tagOrders;
        while (dataSet.hasCachedResults()) {
          dataSet.constructOneRow();
          String database = dataSet.getString("root.TAG_INFO.database_name");
          String measurement = dataSet.getString("root.TAG_INFO.measurement_name");
          String tag = dataSet.getString("root.TAG_INFO.tag_name");
          Integer tagOrder = dataSet.getInt("root.TAG_INFO.tag_order");
          if (database2Measurement2TagOrders.containsKey(database)) {
            measurement2TagOrders = database2Measurement2TagOrders.get(database);
            if (measurement2TagOrders.containsKey(measurement)) {
              tagOrders = measurement2TagOrders.get(measurement);
            } else {
              tagOrders = new HashMap<>();
            }
          } else {
            measurement2TagOrders = new HashMap<>();
            tagOrders = new HashMap<>();
          }
          tagOrders.put(tag, tagOrder);
          measurement2TagOrders.put(measurement, tagOrders);
          database2Measurement2TagOrders.put(database, measurement2TagOrders);
        }
      } catch (StatementExecutionException e) {
        throw new InfluxDBException(e.getMessage());
      }
    } catch (Exception e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      clientRPCService.closeSession(new TSCloseSessionReq().setSessionId(sessionID));
      SessionManager.getInstance().removeCurrSession();
    }
  }

  /**
   * create database
   *
   * @param database database of influxdb
   * @param sessionID session id
   */
  @Override
  public void setStorageGroup(String database, long sessionID) {
    TSStatus status = clientRPCService.setStorageGroup(sessionID, "root." + database);
    if (status.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()
        || status.getCode() == TSStatusCode.DATABASE_ALREADY_EXISTS.getStatusCode()) {
      return;
    }
    throw new InfluxDBException(status.getMessage());
  }

  /**
   * update tag info
   *
   * @param tagInfoRecords tagInfoRecords
   * @param sessionID session id
   */
  @Override
  public void updateTagInfoRecords(TagInfoRecords tagInfoRecords, long sessionID) {
    try {
      List<TSInsertRecordReq> reqs = tagInfoRecords.convertToInsertRecordsReq(sessionID);
      for (TSInsertRecordReq tsInsertRecordReq : reqs) {
        TSStatus tsStatus = clientRPCService.insertRecord(tsInsertRecordReq);
        if (tsStatus.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          throw new InfluxDBException(tsStatus.getMessage());
        }
      }
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  /**
   * get field orders
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param sessionID session id
   * @return a map of field orders
   */
  @Override
  public Map<String, Integer> getFieldOrders(String database, String measurement, long sessionID) {
    Map<String, Integer> fieldOrders = new HashMap<>();
    String showTimeseriesSql = "show timeseries root." + database + '.' + measurement + ".**";
    TSExecuteStatementResp executeStatementResp =
        NewInfluxDBServiceImpl.executeStatement(showTimeseriesSql, sessionID);
    List<String> paths = QueryResultUtils.getFullPaths(executeStatementResp);
    Map<String, Integer> tagOrders =
        InfluxDBMetaManagerFactory.getInstance().getTagOrders(database, measurement, sessionID);
    int tagOrderNums = tagOrders.size();
    int fieldNums = 0;
    for (String path : paths) {
      String filed = StringUtils.getFieldByPath(path);
      if (!fieldOrders.containsKey(filed)) {
        // The corresponding order of fields is 1 + tagNum (the first is timestamp, then all tags,
        // and finally all fields)
        fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
        fieldNums++;
      }
    }
    return fieldOrders;
  }

  private static class InfluxDBMetaManagerHolder {
    private static final NewInfluxDBMetaManager INSTANCE = new NewInfluxDBMetaManager();

    private InfluxDBMetaManagerHolder() {}
  }
}
