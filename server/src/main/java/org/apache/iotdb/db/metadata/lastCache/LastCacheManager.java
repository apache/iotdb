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

package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.VectorPartialPath;
import org.apache.iotdb.db.metadata.lastCache.container.ILastCacheContainer;
import org.apache.iotdb.db.metadata.mnode.IEntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

// this class provides all the operations on last cache
public class LastCacheManager {

  private static final Logger logger = LoggerFactory.getLogger(LastCacheManager.class);

  /**
   * get the last cache value of time series of given seriesPath
   *
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
   * @param node the measurementMNode holding the lastCache When invoker only has the target
   *     seriesPath, the node could be null and MManager will search the node
   * @return the last cache value
   */
  public static TimeValuePair getLastCache(PartialPath seriesPath, IMeasurementMNode node) {
    if (node == null) {
      return null;
    }

    checkIsTemplateLastCacheAndSetIfAbsent(node);

    ILastCacheContainer lastCacheContainer = node.getLastCacheContainer();
    if (seriesPath == null) {
      return lastCacheContainer.getCachedLast();
    } else if (seriesPath instanceof VectorPartialPath) {
      IMeasurementSchema schema = node.getSchema();
      if (schema instanceof VectorMeasurementSchema) {
        return lastCacheContainer.getCachedLast(
            node.getSchema()
                .getSubMeasurementIndex(
                    ((VectorPartialPath) seriesPath).getSubSensorsList().get(0)));
      }
      return null;
    } else {
      return lastCacheContainer.getCachedLast();
    }
  }

  /**
   * update the last cache value of time series of given seriesPath
   *
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
   * @param timeValuePair the latest point value
   * @param highPriorityUpdate the last value from insertPlan is high priority
   * @param latestFlushedTime latest flushed time
   * @param node the measurementMNode holding the lastCache When invoker only has the target
   *     seriesPath, the node could be null and MManager will search the node
   */
  public static void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime,
      IMeasurementMNode node) {
    if (node == null) {
      return;
    }

    checkIsTemplateLastCacheAndSetIfAbsent(node);

    ILastCacheContainer lastCacheContainer = node.getLastCacheContainer();
    if (seriesPath == null) {
      lastCacheContainer.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    } else if (seriesPath instanceof VectorPartialPath) {
      IMeasurementSchema schema = node.getSchema();
      if (schema instanceof VectorMeasurementSchema) {
        if (lastCacheContainer.isEmpty()) {
          lastCacheContainer.init(schema.getSubMeasurementsCount());
        }
        lastCacheContainer.updateCachedLast(
            schema.getSubMeasurementIndex(
                ((VectorPartialPath) seriesPath).getSubSensorsList().get(0)),
            timeValuePair,
            highPriorityUpdate,
            latestFlushedTime);
      }
    } else {
      lastCacheContainer.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    }
  }

  /**
   * reset the last cache value of time series of given seriesPath
   *
   * @param seriesPath the path of timeseries or subMeasurement of aligned timeseries
   * @param node the measurementMNode holding the lastCache When invoker only has the target
   *     seriesPath, the node could be null and MManager will search the node
   */
  public static void resetLastCache(PartialPath seriesPath, IMeasurementMNode node) {
    if (node == null) {
      return;
    }

    checkIsTemplateLastCacheAndSetIfAbsent(node);

    ILastCacheContainer lastCacheContainer = node.getLastCacheContainer();
    if (seriesPath == null) {
      lastCacheContainer.resetLastCache();
    } else if (seriesPath instanceof VectorPartialPath) {
      IMeasurementSchema schema = node.getSchema();
      if (schema instanceof VectorMeasurementSchema) {
        if (lastCacheContainer.isEmpty()) {
          lastCacheContainer.init(schema.getSubMeasurementsCount());
        }
        lastCacheContainer.resetLastCache(
            schema.getSubMeasurementIndex(
                ((VectorPartialPath) seriesPath).getSubSensorsList().get(0)));
      }
    } else {
      lastCacheContainer.resetLastCache();
    }
  }

  private static void checkIsTemplateLastCacheAndSetIfAbsent(IMeasurementMNode node) {
    IEntityMNode entityMNode = node.getParent();
    if (entityMNode == null) {
      // cluster cached remote measurementMNode doesn't have parent
      return;
    }
    String measurement = node.getName();

    // if entityMNode doesn't have this child, the child is derived from template
    if (!entityMNode.hasChild(measurement)) {
      ILastCacheContainer lastCacheContainer = entityMNode.getLastCacheContainer(measurement);
      IMeasurementSchema schema = node.getSchema();
      if (lastCacheContainer.isEmpty() && (schema instanceof VectorMeasurementSchema)) {
        lastCacheContainer.init(schema.getSubMeasurementsCount());
      }
      node.setLastCacheContainer(lastCacheContainer);
    }
  }

  /**
   * delete all the last cache value of any timeseries or aligned timeseries under the entity
   *
   * @param node entity node
   */
  public static void deleteLastCacheByDevice(IEntityMNode node) {
    // process lastCache of timeseries represented by measurementNode
    for (IMNode child : node.getChildren().values()) {
      if (child.isMeasurement()) {
        child.getAsMeasurementMNode().getLastCacheContainer().resetLastCache();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[tryToDeleteLastCacheByDevice] Last cache for path: {} is set to null",
              child.getFullPath());
        }
      }
    }
    // process lastCache of timeseries represented by template
    for (Map.Entry<String, ILastCacheContainer> entry : node.getTemplateLastCaches().entrySet()) {
      entry.getValue().resetLastCache();
      if (logger.isDebugEnabled()) {
        logger.debug(
            "[tryToDeleteLastCacheByDevice] Last cache for path: {} is set to null",
            node.getPartialPath().concatNode(entry.getKey()).getFullPath());
      }
    }
  }

  /**
   * delete the last cache value of timeseries or subMeasurement of some aligned timeseries, which
   * is under the entity and matching the originalPath
   *
   * @param node entity node
   * @param originalPath origin timeseries path
   * @param startTime startTime
   * @param endTime endTime
   */
  public static void deleteLastCacheByDevice(
      IEntityMNode node, PartialPath originalPath, long startTime, long endTime) {
    PartialPath path;
    IMeasurementSchema schema;
    ILastCacheContainer lastCacheContainer;

    // process lastCache of timeseries represented by measurementNode
    IMeasurementMNode measurementMNode;
    for (IMNode child : node.getChildren().values()) {
      if (child == null || !child.isMeasurement()) {
        continue;
      }
      path = child.getPartialPath();
      measurementMNode = child.getAsMeasurementMNode();
      if (originalPath.matchFullPath(path)) {
        lastCacheContainer = measurementMNode.getLastCacheContainer();
        if (lastCacheContainer == null) {
          continue;
        }
        schema = measurementMNode.getSchema();
        deleteLastCache(path, schema, lastCacheContainer, startTime, endTime);
      }
    }

    // process lastCache of timeseries represented by template
    Template template = node.getUpperTemplate();
    for (Map.Entry<String, ILastCacheContainer> entry : node.getTemplateLastCaches().entrySet()) {
      path = node.getPartialPath().concatNode(entry.getKey());
      if (originalPath.matchFullPath(path)) {
        lastCacheContainer = entry.getValue();
        if (lastCacheContainer == null) {
          continue;
        }
        schema = template.getSchemaMap().get(entry.getKey());
        deleteLastCache(path, schema, lastCacheContainer, startTime, endTime);
      }
    }
  }

  private static void deleteLastCache(
      PartialPath path,
      IMeasurementSchema schema,
      ILastCacheContainer lastCacheContainer,
      long startTime,
      long endTime) {
    TimeValuePair lastPair;
    if (schema instanceof VectorMeasurementSchema) {
      int index;
      for (String measurement : schema.getSubMeasurementsList()) {
        index = schema.getSubMeasurementIndex(measurement);
        lastPair = lastCacheContainer.getCachedLast(index);
        if (lastPair != null
            && startTime <= lastPair.getTimestamp()
            && lastPair.getTimestamp() <= endTime) {
          lastCacheContainer.resetLastCache(index);
          if (logger.isDebugEnabled()) {
            logger.debug(
                "[tryToDeleteLastCache] Last cache for path: {} is set to null",
                path.concatNode(measurement).getFullPath());
          }
        }
      }
    } else {
      lastPair = lastCacheContainer.getCachedLast();
      if (lastPair != null
          && startTime <= lastPair.getTimestamp()
          && lastPair.getTimestamp() <= endTime) {
        lastCacheContainer.resetLastCache();
        if (logger.isDebugEnabled()) {
          logger.debug(
              "[tryToDeleteLastCache] Last cache for path: {} is set to null", path.getFullPath());
        }
      }
    }
  }

  /**
   * get the last value of timeseries represented by given measurementMNode get last value from
   * cache in measurementMNode if absent, get last value from file
   *
   * @param node measurementMNode representing the target timeseries
   * @param queryContext query context
   * @return the last value
   */
  public static long getLastTimeStamp(IMeasurementMNode node, QueryContext queryContext) {
    TimeValuePair last = getLastCache(null, node);
    if (last != null) {
      return getLastCache(null, node).getTimestamp();
    } else {
      try {
        QueryDataSource dataSource =
            QueryResourceManager.getInstance()
                .getQueryDataSource(node.getPartialPath(), queryContext, null);
        Set<String> measurementSet = new HashSet<>();
        measurementSet.add(node.getPartialPath().getFullPath());
        LastPointReader lastReader =
            new LastPointReader(
                node.getPartialPath(),
                node.getSchema().getType(),
                measurementSet,
                queryContext,
                dataSource,
                Long.MAX_VALUE,
                null);
        last = lastReader.readLastPoint();
        return (last != null ? last.getTimestamp() : Long.MIN_VALUE);
      } catch (Exception e) {
        logger.error(
            "Something wrong happened while trying to get last time value pair of {}",
            node.getFullPath(),
            e);
        return Long.MIN_VALUE;
      }
    }
  }
}
