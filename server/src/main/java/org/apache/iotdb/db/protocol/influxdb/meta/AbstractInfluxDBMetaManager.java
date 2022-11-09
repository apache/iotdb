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

import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/** InfluxDBMetaManager used in schema region is memory or schema file */
public abstract class AbstractInfluxDBMetaManager implements IInfluxDBMetaManager {

  protected static final String SELECT_TAG_INFO_SQL =
      "select database_name,measurement_name,tag_name,tag_order from root.TAG_INFO ";

  // TODO avoid OOM
  protected static Map<String, Map<String, Map<String, Integer>>> database2Measurement2TagOrders =
      new HashMap<>();

  /**
   * get tag orders
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param sessionID session id
   * @return a map of tag orders
   */
  @Override
  public Map<String, Integer> getTagOrders(String database, String measurement, long sessionID) {
    Map<String, Integer> tagOrders = new HashMap<>();
    Map<String, Map<String, Integer>> measurement2TagOrders =
        database2Measurement2TagOrders.get(database);
    if (measurement2TagOrders != null) {
      tagOrders = measurement2TagOrders.get(measurement);
    }
    if (tagOrders == null) {
      tagOrders = new HashMap<>();
    }
    return tagOrders;
  }

  /**
   * create database
   *
   * @param database database of influxdb
   * @param sessionID session id
   */
  abstract void setStorageGroup(String database, long sessionID);

  /**
   * update tag info
   *
   * @param tagInfoRecords tagInfoRecords
   * @param sessionID session id
   */
  abstract void updateTagInfoRecords(TagInfoRecords tagInfoRecords, long sessionID);

  public final synchronized Map<String, Map<String, Integer>> createDatabase(
      String database, long sessionID) {
    Map<String, Map<String, Integer>> measurement2TagOrders =
        database2Measurement2TagOrders.get(database);
    if (measurement2TagOrders != null) {
      return measurement2TagOrders;
    }
    setStorageGroup(database, sessionID);
    measurement2TagOrders = new HashMap<>();
    database2Measurement2TagOrders.put(database, measurement2TagOrders);
    return measurement2TagOrders;
  }

  public final synchronized Map<String, Integer> getTagOrdersWithAutoCreatingSchema(
      String database, String measurement, long sessionID) {
    return createDatabase(database, sessionID).computeIfAbsent(measurement, m -> new HashMap<>());
  }

  /**
   * generate time series path for insertion
   *
   * @param database database of influxdb
   * @param measurement measurement of influxdb
   * @param tags influxdb tags
   * @param fields influxdb fields
   * @param sessionID session id
   * @return series path
   */
  @Override
  public final synchronized String generatePath(
      String database,
      String measurement,
      Map<String, String> tags,
      Set<String> fields,
      long sessionID) {
    Map<String, Integer> tagKeyToLayerOrders =
        getTagOrdersWithAutoCreatingSchema(database, measurement, sessionID);
    // to support rollback if fails to persisting new tag info
    Map<String, Integer> newTagKeyToLayerOrders = new HashMap<>(tagKeyToLayerOrders);
    // record the layer orders of tag keys that the path contains
    Map<Integer, String> layerOrderToTagKeysInPath = new HashMap<>();

    int tagNumber = tagKeyToLayerOrders.size();

    TagInfoRecords newTagInfoRecords = null;
    for (Map.Entry<String, String> tag : tags.entrySet()) {
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
      updateTagInfoRecords(newTagInfoRecords, sessionID);
      database2Measurement2TagOrders.get(database).put(measurement, newTagKeyToLayerOrders);
    }

    StringBuilder path =
        new StringBuilder("root.").append(database).append(".").append(measurement);
    for (int i = 1; i <= tagNumber; ++i) {
      path.append(".")
          .append(
              layerOrderToTagKeysInPath.containsKey(i)
                  ? tags.get(layerOrderToTagKeysInPath.get(i))
                  : InfluxConstant.PLACE_HOLDER);
    }
    return path.toString();
  }
}
