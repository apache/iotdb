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

package org.apache.iotdb.influxdb.protocol.meta;

import org.apache.iotdb.influxdb.protocol.constant.InfluxDBConstant;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.Field;
import org.influxdb.InfluxDBException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MetaManager {

  private final Session session;

  // TODO avoid OOM
  private final Map<String, Map<String, Map<String, Integer>>> database2Measurement2TagOrders;


  public MetaManager(Session session) {
    this.session = session;
    database2Measurement2TagOrders = new HashMap<>();
    recover();
  }

  private void recover() {
    try {
      SessionDataSet result = session.executeQueryStatement(
              "select database_name,measurement_name,tag_name,tag_order from root.TAG_INFO ");
      Map<String, Map<String, Integer>> measurement2TagOrders=new HashMap<>();
       Map<String, Integer> tagOrders=new HashMap<>();
      while (result.hasNext()) {
        List<Field> fields = result.next().getFields();
        String databaseName=fields.get(0).getStringValue();
        String measurementName = fields.get(1).getStringValue();
        if (database2Measurement2TagOrders.containsKey(databaseName)) {
          measurement2TagOrders= database2Measurement2TagOrders.get(measurementName);
          if (measurement2TagOrders.containsKey(measurementName)){
            tagOrders=measurement2TagOrders.get(measurementName);
          }
        }
        tagOrders.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
        measurement2TagOrders.put(measurementName, tagOrders);
        database2Measurement2TagOrders.put(databaseName,measurement2TagOrders);
      }
    } catch (StatementExecutionException | IoTDBConnectionException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  public Map<String, Map<String, Integer>> createDatabase(String database) {
    Map<String, Map<String, Integer>> measurement2TagOrders =
        database2Measurement2TagOrders.get(database);
    if (measurement2TagOrders != null) {
      return measurement2TagOrders;
    }

    try {
      session.setStorageGroup("root." + database);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }

    measurement2TagOrders = new HashMap<>();
    database2Measurement2TagOrders.put(database, measurement2TagOrders);
    return measurement2TagOrders;
  }

  public Map<String, Integer> getTagOrdersWithAutoCreatingSchema(
      String database, String measurement) {
    return createDatabase(database).computeIfAbsent(measurement, m -> new HashMap<>());
  }

  public String generatePath(String database, String measurement, Map<String, String> tags) {
    Map<String, Integer> tagKeyToLayerOrders =
        getTagOrdersWithAutoCreatingSchema(database, measurement);
    // to support rollback if fails to persisting new tag info
    Map<String, Integer> newTagKeyToLayerOrders = new HashMap<>(tagKeyToLayerOrders);
    // record the layer orders of tag keys that the path contains
    Map<Integer, String> layerOrderToTagKeysInPath = new HashMap<>();

    int tagNumber = tagKeyToLayerOrders.size();

    TagInfoRecords newTagInfoRecords = null;
    for (Entry<String, String> tag : tags.entrySet()) {
      final String tagKey = tag.getKey();
      if (!newTagKeyToLayerOrders.containsKey(tagKey)) {
        if (newTagInfoRecords == null) {
          newTagInfoRecords = new TagInfoRecords();
        }
        ++tagNumber;
        newTagInfoRecords.add(database, measurement, tagKey, tagNumber);
        newTagKeyToLayerOrders.put(tagKey, tagNumber);
      }

      layerOrderToTagKeysInPath.put(newTagKeyToLayerOrders.get(tagKey), tagKey);
    }

    if (newTagInfoRecords != null) {
      newTagInfoRecords.persist(session);
      database2Measurement2TagOrders.get(database).put(measurement, newTagKeyToLayerOrders);
    }

    StringBuilder path =
        new StringBuilder("root.").append(database).append(".").append(measurement);
    for (int i = 1; i <= tagNumber; ++i) {
      path.append(".")
          .append(
              layerOrderToTagKeysInPath.containsKey(i)
                  ? tags.get(layerOrderToTagKeysInPath.get(i))
                  : InfluxDBConstant.PLACE_HOLDER);
    }
    return path.toString();
  }
}
