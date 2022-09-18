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

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.StorageGroupNotSetException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.protocol.influxdb.constant.InfluxConstant;
import org.apache.iotdb.db.protocol.influxdb.util.StringUtils;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.db.service.basic.ServiceProvider;
import org.apache.iotdb.rpc.TSStatusCode;
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

public class InfluxDBMetaManager extends AbstractInfluxDBMetaManager {

  protected final Planner planner;

  private final ServiceProvider serviceProvider;

  private InfluxDBMetaManager() {
    serviceProvider = IoTDB.serviceProvider;
    database2Measurement2TagOrders = new HashMap<>();
    planner = serviceProvider.getPlanner();
  }

  public static InfluxDBMetaManager getInstance() {
    return InfluxDBMetaManagerHolder.INSTANCE;
  }

  @Override
  public void recover() {
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      QueryPlan queryPlan = (QueryPlan) planner.parseSQLToPhysicalPlan(SELECT_TAG_INFO_SQL);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              SELECT_TAG_INFO_SQL,
              IoTDBConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
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
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
  }

  @Override
  public void setStorageGroup(String database, long sessionID) {
    try {
      SetStorageGroupPlan setStorageGroupPlan =
          new SetStorageGroupPlan(new PartialPath("root." + database));
      serviceProvider.executeNonQuery(setStorageGroupPlan);
    } catch (QueryProcessException e) {
      // errCode = 300 means sg has already set
      if (e.getErrorCode() != TSStatusCode.STORAGE_GROUP_ALREADY_EXISTS.getStatusCode()) {
        throw new InfluxDBException(e.getMessage());
      }
    } catch (IllegalPathException | StorageGroupNotSetException | StorageEngineException e) {
      throw new InfluxDBException(e.getMessage());
    }
  }

  @Override
  public void updateTagInfoRecords(TagInfoRecords tagInfoRecords, long sessionID) {
    List<InsertRowPlan> plans = tagInfoRecords.convertToInsertRowPlans();
    for (InsertRowPlan plan : plans) {
      try {
        serviceProvider.executeNonQuery(plan);
      } catch (QueryProcessException | StorageGroupNotSetException | StorageEngineException e) {
        throw new InfluxDBException(e.getMessage());
      }
    }
  }

  @Override
  public Map<String, Integer> getFieldOrders(String database, String measurement, long sessionID) {
    Map<String, Integer> fieldOrders = new HashMap<>();
    long queryId = ServiceProvider.SESSION_MANAGER.requestQueryId(true);
    try {
      String showTimeseriesSql = "show timeseries root." + database + '.' + measurement + ".**";
      PhysicalPlan physicalPlan =
          serviceProvider.getPlanner().parseSQLToPhysicalPlan(showTimeseriesSql);
      QueryContext queryContext =
          serviceProvider.genQueryContext(
              queryId,
              true,
              System.currentTimeMillis(),
              showTimeseriesSql,
              InfluxConstant.DEFAULT_CONNECTION_TIMEOUT_MS);
      QueryDataSet queryDataSet =
          serviceProvider.createQueryDataSet(
              queryContext, physicalPlan, InfluxConstant.DEFAULT_FETCH_SIZE);
      int fieldNums = 0;
      Map<String, Integer> tagOrders =
          InfluxDBMetaManagerFactory.getInstance().getTagOrders(database, measurement, sessionID);
      int tagOrderNums = tagOrders.size();
      while (queryDataSet.hasNext()) {
        List<Field> fields = queryDataSet.next().getFields();
        String filed = StringUtils.getFieldByPath(fields.get(0).getStringValue());
        if (!fieldOrders.containsKey(filed)) {
          // The corresponding order of fields is 1 + tagNum (the first is timestamp, then all tags,
          // and finally all fields)
          fieldOrders.put(filed, tagOrderNums + fieldNums + 1);
          fieldNums++;
        }
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
      ServiceProvider.SESSION_MANAGER.releaseQueryResourceNoExceptions(queryId);
    }
    return fieldOrders;
  }

  private static class InfluxDBMetaManagerHolder {
    private static final InfluxDBMetaManager INSTANCE = new InfluxDBMetaManager();

    private InfluxDBMetaManagerHolder() {}
  }
}
