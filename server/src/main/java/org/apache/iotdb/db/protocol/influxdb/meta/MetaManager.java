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

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxDBConstant;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.service.basic.BasicServiceProvider;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.thrift.TException;
import org.influxdb.InfluxDBException;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaManager {

  protected final Planner planner = new Planner();

  private final BasicServiceProvider basicServiceProvider;

  private static String SELECT_TAG_INFO_SQL =
      "select database_name,measurement_name,tag_name,tag_order from root.TAG_INFO ";

  public static MetaManager getInstance() {
    return MetaManagerHolder.INSTANCE;
  }

  // TODO avoid OOM
  private static Map<String, Map<String, Map<String, Integer>>> database2Measurement2TagOrders =
      new HashMap<>();

  private MetaManager() {
    try {
      basicServiceProvider = new BasicServiceProvider();
    } catch (QueryProcessException e) {
      throw new InfluxDBException(e.getMessage());
    }
    database2Measurement2TagOrders = new HashMap<>();
    recover();
  }

  private void recover() {
    long queryId = QueryResourceManager.getInstance().assignQueryId(true);
    try {
      QueryPlan queryPlan = (QueryPlan) planner.parseSQLToPhysicalPlan(SELECT_TAG_INFO_SQL);
      QueryContext queryContext =
          basicServiceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              SELECT_TAG_INFO_SQL,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          basicServiceProvider.createQueryDataSet(
              queryContext, queryPlan, IoTDBConstant.DEFAULT_FETCH_SIZE);
      while (queryDataSet.hasNext()) {
        List<Field> fields = queryDataSet.next().getFields();
        String databaseName = fields.get(0).getStringValue();
        String measurementName = fields.get(1).getStringValue();

        Map<String, Map<String, Integer>> measurement2TagOrders;
        Map<String, Integer> tagOrders;
        if (database2Measurement2TagOrders.containsKey(databaseName)) {
          measurement2TagOrders = database2Measurement2TagOrders.get(databaseName);
          if (measurement2TagOrders.containsKey(measurementName)) {
            tagOrders = measurement2TagOrders.get(measurementName);
          } else {
            tagOrders = new HashMap<>();
          }
        } else {
          measurement2TagOrders = new HashMap<>();
          tagOrders = new HashMap<>();
        }

        tagOrders.put(fields.get(2).getStringValue(), fields.get(3).getIntV());
        measurement2TagOrders.put(measurementName, tagOrders);
        database2Measurement2TagOrders.put(databaseName, measurement2TagOrders);
      }
    } catch (QueryProcessException
        | TException
        | StorageEngineException
        | SQLException
        | IOException
        | InterruptedException
        | QueryFilterOptimizationException
        | MetadataException e) {
      throw new InfluxDBException(e.getMessage());
    } finally {
      BasicServiceProvider.sessionManager.releaseQueryResourceNoExceptions(queryId);
    }
  }

  public synchronized Map<String, Map<String, Integer>> createDatabase(String database) {
    Map<String, Map<String, Integer>> measurement2TagOrders =
        database2Measurement2TagOrders.get(database);
    if (measurement2TagOrders != null) {
      return measurement2TagOrders;
    }

    try {
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new PartialPath("root." + database));
      basicServiceProvider.executeNonQuery(setStorageGroupPlan);
    } catch (QueryProcessException e) {
      // errCode = 300 means sg has already set
      if (e.getErrorCode() != 300) {
        throw new InfluxDBException(e.getMessage());
      }
    } catch (IllegalPathException | StorageGroupNotSetException | StorageEngineException e) {
      throw new InfluxDBException(e.getMessage());
    }

    measurement2TagOrders = new HashMap<>();
    database2Measurement2TagOrders.put(database, measurement2TagOrders);
    return measurement2TagOrders;
  }

  public synchronized Map<String, Integer> getTagOrdersWithAutoCreatingSchema(
      String database, String measurement) {
    return createDatabase(database).computeIfAbsent(measurement, m -> new HashMap<>());
  }

  public synchronized String generatePath(
      String database, String measurement, Map<String, String> tags) {
    Map<String, Integer> tagKeyToLayerOrders =
        getTagOrdersWithAutoCreatingSchema(database, measurement);
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
      updateTagInfoRecords(newTagInfoRecords);
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

  private void updateTagInfoRecords(TagInfoRecords tagInfoRecords) {
    List<InsertRowPlan> plans = tagInfoRecords.convertToInsertRowPlans();
    for (InsertRowPlan plan : plans) {
      try {
        basicServiceProvider.executeNonQuery(plan);
      } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
  }

  private static class MetaManagerHolder {
    private static final MetaManager INSTANCE = new MetaManager();

    private MetaManagerHolder() {}
  }
}
