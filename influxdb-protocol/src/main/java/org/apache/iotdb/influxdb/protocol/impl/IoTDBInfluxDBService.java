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

package org.apache.iotdb.influxdb.protocol.impl;

import org.apache.iotdb.influxdb.protocol.dto.IoTDBPoint;
import org.apache.iotdb.influxdb.protocol.meta.MetaManager;
import org.apache.iotdb.influxdb.protocol.meta.MetaManagerHolder;
import org.apache.iotdb.influxdb.protocol.util.DataTypeUtils;
import org.apache.iotdb.influxdb.protocol.util.ParameterUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.influxdb.InfluxDBException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class IoTDBInfluxDBService {

  private final Session session;
  private final MetaManager metaManager;
  private String currentDatabase;

  public IoTDBInfluxDBService(Session session) {
    this.session = session;
    metaManager = MetaManagerHolder.getInstance(DataTypeUtils.sessionToSessionPoint(session));
    currentDatabase = null;
  }

  public void setDatabase(String database) {
    currentDatabase = database;
  }

  public void writePoints(
      String database,
      String retentionPolicy,
      String precision,
      String consistency,
      BatchPoints batchPoints) {
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();

    for (Point point : batchPoints.getPoints()) {
      IoTDBPoint iotdbPoint = convertToIoTDBPoint(database, point);
      deviceIds.add(iotdbPoint.getDeviceId());
      times.add(iotdbPoint.getTime());
      measurementsList.add(iotdbPoint.getMeasurements());
      typesList.add(iotdbPoint.getTypes());
      valuesList.add(iotdbPoint.getValues());
    }

    try {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  private IoTDBPoint convertToIoTDBPoint(String database, Point point) {
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

    database = database == null ? currentDatabase : database;
    ParameterUtils.checkNonEmptyString(database, "database");
    ParameterUtils.checkNonEmptyString(measurement, "measurement name");
    String path = metaManager.generatePath(database, measurement, tags);
    List<String> measurements = new ArrayList<>();
    List<TSDataType> types = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    for (Entry<String, Object> entry : fields.entrySet()) {
      measurements.add(entry.getKey());
      Object value = entry.getValue();
      types.add(DataTypeUtils.normalTypeToTSDataType(value));
      values.add(value);
    }
    return new IoTDBPoint(path, time, measurements, types, values);
  }

  public void createDatabase(String name) {
    metaManager.createDatabase(name);
  }

  public void close() {
    try {
      MetaManagerHolder.close(DataTypeUtils.sessionToSessionPoint(session).ipAndPortToString());
    } catch (IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }
}
