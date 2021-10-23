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

import org.influxdb.InfluxDBException;

import java.util.HashMap;
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
    // TODO: recover metadata from db
  }

  public boolean isDatabaseExisted(String database) {
    return database2Measurement2TagOrders.containsKey(database);
  }

  public void createDatabase(String database) {
    if (isDatabaseExisted(database)) {
      return;
    }

    try {
      session.setStorageGroup("root." + database);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      throw new InfluxDBException(e.getMessage());
    }

    database2Measurement2TagOrders.put(database, new HashMap<>());
  }

  public Map<String, Integer> getTagOrdersWithAutoCreatingSchema(
      String database, String measurement) {
    createDatabase(database);
    database2Measurement2TagOrders.get(database).computeIfAbsent(measurement, k -> new HashMap<>());
    return database2Measurement2TagOrders.get(database).get(measurement);
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
      if (!newTagKeyToLayerOrders.containsKey(tag.getKey())) {
        if (newTagInfoRecords == null) {
          newTagInfoRecords = new TagInfoRecords();
        }

        ++tagNumber;
        newTagInfoRecords.add(database, measurement, tag.getKey(), tagNumber);
        newTagKeyToLayerOrders.put(tag.getKey(), tagNumber);
      }
      layerOrderToTagKeysInPath.put(newTagKeyToLayerOrders.get(tag.getKey()), tag.getKey());
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
