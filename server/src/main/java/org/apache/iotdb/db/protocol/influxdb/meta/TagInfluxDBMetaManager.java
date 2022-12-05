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

import org.apache.iotdb.db.protocol.influxdb.util.QueryResultUtils;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.service.thrift.impl.NewInfluxDBServiceImpl;
import org.apache.iotdb.service.rpc.thrift.TSExecuteStatementResp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** InfluxDBMetaManager for NewIoTDB When schema region is tag schema region */
public class TagInfluxDBMetaManager implements IInfluxDBMetaManager {

  // The database used to save influxdb metadata in IoTDB
  private static final String STORAGE_GROUP_PATH = "root.influxdbmeta";

  private static final String TAGS_SET = "set.tags";

  private static final String FIELDS_SET = "set.fields";

  private TagInfluxDBMetaManager() {}

  public static TagInfluxDBMetaManager getInstance() {
    return TagInfluxDBMetaManagerHolder.INSTANCE;
  }

  /** use tag schema region to save state information, no need to recover here */
  @Override
  public void recover() {}

  /**
   * get the fields information of influxdb corresponding database and measurement through tag
   * schema region
   *
   * @param database influxdb database
   * @param measurement influxdb measurement
   * @param sessionId session id
   * @return field information
   */
  @Override
  public Map<String, Integer> getFieldOrders(String database, String measurement, long sessionId) {
    return getTimeseriesFieldOrders(database, measurement, FIELDS_SET, sessionId);
  }

  /**
   * convert the database,measurement,and tags of influxdb to device path of IoTDB,and save the tags
   * and fields information of the database and measurement to the tag schema region
   *
   * @param database influxdb database
   * @param measurement influxdb measurement
   * @param tags influxdb tags
   * @param fields influxdb fields
   * @param sessionID session id
   * @return device path
   */
  @Override
  public String generatePath(
      String database,
      String measurement,
      Map<String, String> tags,
      Set<String> fields,
      long sessionID) {
    createInfluxDBMetaTimeseries(database, measurement, tags, fields, sessionID);
    return generateDevicesPath(database, measurement, tags);
  }

  private void createInfluxDBMetaTimeseries(
      String database,
      String measurement,
      Map<String, String> tags,
      Set<String> fields,
      long sessionID) {
    List<String> fieldsList = new ArrayList<>(tags.keySet());
    createInfluxDBMetaTimeseries(database, measurement, TAGS_SET, fieldsList, sessionID);
    fieldsList.clear();
    fieldsList.addAll(fields);
    createInfluxDBMetaTimeseries(database, measurement, FIELDS_SET, fieldsList, sessionID);
  }

  private void createInfluxDBMetaTimeseries(
      String database, String measurement, String device, List<String> fields, long sessionID) {
    String statement = generateTimeseriesStatement(database, measurement, device, fields);
    NewInfluxDBServiceImpl.executeStatement(statement, sessionID);
  }

  private String generateTimeseriesStatement(
      String database, String measurement, String device, List<String> fields) {
    StringBuilder timeseriesStatement =
        new StringBuilder(
            "create aligned timeseries "
                + STORAGE_GROUP_PATH
                + ".database."
                + database
                + ".measurement."
                + measurement
                + "."
                + device
                + "(");
    for (int i = 0; i < fields.size() - 1; i++) {
      String field = fields.get(i);
      timeseriesStatement.append(field).append(" BOOLEAN, ");
    }
    timeseriesStatement.append(fields.get(fields.size() - 1)).append(" BOOLEAN)");
    return timeseriesStatement.toString();
  }

  /**
   * get the tags information of influxdb corresponding database and measurement through tag schema
   * region
   *
   * @param database influxdb database
   * @param measurement influxdb measurement
   * @param sessionID session id
   * @return tags information
   */
  @Override
  public Map<String, Integer> getTagOrders(String database, String measurement, long sessionID) {
    return getTimeseriesFieldOrders(database, measurement, TAGS_SET, sessionID);
  }

  private Map<String, Integer> getTimeseriesFieldOrders(
      String database, String measurement, String device, long sessionID) {
    TSExecuteStatementResp statementResp =
        NewInfluxDBServiceImpl.executeStatement(
            "show timeseries "
                + STORAGE_GROUP_PATH
                + ".database."
                + database
                + ".measurement."
                + measurement
                + "."
                + device,
            sessionID);
    List<String> timeseriesPaths = QueryResultUtils.getFullPaths(statementResp);
    Map<String, Integer> fieldOrders = new HashMap<>();
    for (String timeseriesPath : timeseriesPaths) {
      String field = StringUtils.getFieldByPath(timeseriesPath);
      fieldOrders.put(field, fieldOrders.size());
    }
    return fieldOrders;
  }

  /**
   * convert the database,measurement,and tags of influxdb to device path of IoTDB,ensure that
   * influxdb records with the same semantics generate the same device path, so the device path is
   * generated in order after sorting the tags
   *
   * @param database influxdb database
   * @param measurement influxdb measurement
   * @param tags influxdb tags
   * @return device path
   */
  private String generateDevicesPath(
      String database, String measurement, Map<String, String> tags) {
    TreeMap<String, String> tagsMap = new TreeMap<>(tags);
    tagsMap.put("measurement", measurement);
    StringBuilder devicePath = new StringBuilder("root." + database);
    for (String tagKey : tagsMap.keySet()) {
      devicePath.append(".").append(tagKey).append(".").append(tagsMap.get(tagKey));
    }
    return devicePath.toString();
  }

  private static class TagInfluxDBMetaManagerHolder {
    private static final TagInfluxDBMetaManager INSTANCE = new TagInfluxDBMetaManager();

    private TagInfluxDBMetaManagerHolder() {}
  }
}
