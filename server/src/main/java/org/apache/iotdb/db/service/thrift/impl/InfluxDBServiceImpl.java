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

package org.apache.iotdb.db.service.thrift.impl;

import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.protocol.influxdb.dto.IoTDBPoint;
import org.apache.iotdb.db.protocol.influxdb.input.InfluxLineParser;
import org.apache.iotdb.db.protocol.influxdb.meta.MetaManager;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.service.basic.BasicOpenSessionResp;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.db.utils.DataTypeUtils;
import org.apache.iotdb.db.utils.ParameterUtils;
import org.apache.iotdb.protocol.influxdb.rpc.thrift.*;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.service.rpc.thrift.TSProtocolVersion;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InfluxDBServiceImpl implements InfluxDBService.Iface {

  private final BasicServiceProvider basicServiceProvider;

  private Map<Long, String> sessionIdToDatabase;
  private MetaManager metaManager;

  public InfluxDBServiceImpl() throws QueryProcessException {
    basicServiceProvider = new BasicServiceProvider();
    sessionIdToDatabase = new HashMap<>();
    metaManager = MetaManager.getInstance();
  }

  @Override
  public TSOpenSessionResp openSession(TSOpenSessionReq req) throws TException {
    BasicOpenSessionResp basicOpenSessionResp =
        basicServiceProvider.openSession(
            req.username, req.password, req.zoneId, TSProtocolVersion.IOTDB_SERVICE_PROTOCOL_V3);
    TSStatus tsStatus =
        RpcUtils.getInfluxDBStatus(
            basicOpenSessionResp.getCode(), basicOpenSessionResp.getMessage());
    TSOpenSessionResp resp = new TSOpenSessionResp();
    resp.setStatus(tsStatus);
    return resp.setSessionId(basicOpenSessionResp.getSessionId());
  }

  @Override
  public TSStatus closeSession(TSCloseSessionReq req) {
    return new TSStatus(
        !basicServiceProvider.closeSession(req.sessionId)
            ? RpcUtils.getInfluxDBStatus(TSStatusCode.NOT_LOGIN_ERROR)
            : RpcUtils.getInfluxDBStatus(TSStatusCode.SUCCESS_STATUS));
  }

  @Override
  public TSStatus writePoints(TSWritePointsReq req) throws TException {
    if (!basicServiceProvider.checkLogin(req.sessionId)) {
      return getNotLoggedInStatus();
    }

    ArrayList<Point> points =
        (ArrayList<Point>) InfluxLineParser.parserRecordsToPoints(req.lineProtocol);
    for (Point point : points) {
      IoTDBPoint iotdbPoint = convertToIoTDBPoint(req.sessionId, req.database, point);
      try {
        InsertRowPlan plan = iotdbPoint.cvtToInsertRowPlan();
        return executeNonQueryPlan(plan, req.sessionId);
      } catch (StorageGroupNotSetException
          | StorageEngineException
          | IllegalPathException
          | IoTDBConnectionException
          | QueryProcessException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
    return null;
  }

  @Override
  public TSStatus createDatabase(TSCreateDatabaseReq req) throws TException {
    if (!basicServiceProvider.checkLogin(req.sessionId)) {
      return getNotLoggedInStatus();
    }

    try {
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new PartialPath("root." + req.getDatabase()));
      return executeNonQueryPlan(setStorageGroupPlan, req.getSessionId());
    } catch (IllegalPathException
        | QueryProcessException
        | StorageGroupNotSetException
        | StorageEngineException e) {
      if (e instanceof QueryProcessException && e.getErrorCode() == 300) {
        return RpcUtils.getInfluxDBStatus(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(), "Execute successfully");
      }
      throw new InfluxDBException(e.getMessage());
    }
  }

  private IoTDBPoint convertToIoTDBPoint(long sessionId, String database, Point point) {
    String measurement = null;
    Map<String, String> tags = new HashMap<>();
    Map<String, Object> fields = new HashMap<>();
    Long time = null;

    // Get the property of point in influxdb by reflection
    for (java.lang.reflect.Field reflectField : point.getClass().getDeclaredFields()) {
      reflectField.setAccessible(true);
      try {
        if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
            && reflectField.getName().equalsIgnoreCase("fields")) {
          fields = (Map<String, Object>) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.util.Map")
            && reflectField.getName().equalsIgnoreCase("tags")) {
          tags = (Map<String, String>) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.String")
            && reflectField.getName().equalsIgnoreCase("measurement")) {
          measurement = (String) reflectField.get(point);
        }
        if (reflectField.getType().getName().equalsIgnoreCase("java.lang.Number")
            && reflectField.getName().equalsIgnoreCase("time")) {
          time = (Long) reflectField.get(point);
        }
        // set current time
        if (time == null) {
          time = System.currentTimeMillis();
        }
      } catch (IllegalAccessException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    }

    database = database == null ? sessionIdToDatabase.get(sessionId) : database;
    ParameterUtils.checkNonEmptyString(database, "database");
    ParameterUtils.checkNonEmptyString(measurement, "measurement name");
    String path = metaManager.generatePath(database, measurement, tags);
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    for (Map.Entry<String, Object> entry : fields.entrySet()) {
      measurements.add(entry.getKey());
      Object value = entry.getValue();
      types.add(DataTypeUtils.normalTypeToTSDataType(value));
      values.add(value);
    }
    return new IoTDBPoint(path, time, measurements, types, values);
  }

  public void handleClientExit() {
    Long sessionId = BasicServiceProvider.sessionManager.getCurrSessionId();
    if (sessionId != null) {
      TSCloseSessionReq req = new TSCloseSessionReq(sessionId);
      closeSession(req);
    }
  }

  private TSStatus getNotLoggedInStatus() {
    return RpcUtils.getInfluxDBStatus(
        TSStatusCode.NOT_LOGIN_ERROR.getStatusCode(),
        "Log in failed. Either you are not authorized or the session has timed out.");
  }

  private TSStatus executeNonQueryPlan(PhysicalPlan plan, long sessionId)
      throws QueryProcessException, StorageGroupNotSetException, StorageEngineException {
    org.apache.iotdb.service.rpc.thrift.TSStatus status =
        basicServiceProvider.checkAuthority(plan, sessionId);
    if (status == null) {
      status =
          basicServiceProvider.executeNonQuery(plan)
              ? RpcUtils.getStatus(TSStatusCode.SUCCESS_STATUS, "Execute successfully")
              : RpcUtils.getStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR);
    }
    return DataTypeUtils.RPCStatusToInfluxDBTSStatus(status);
  }
}
