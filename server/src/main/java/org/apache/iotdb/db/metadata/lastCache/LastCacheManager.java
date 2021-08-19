package org.apache.iotdb.db.metadata.lastCache;

import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.lastCache.entry.ILastCacheEntry;
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

public class LastCacheManager {

  private static final Logger logger = LoggerFactory.getLogger(LastCacheManager.class);

  public static TimeValuePair getLastCache(PartialPath seriesPath, IMeasurementMNode node) {
    if (node == null) {
      return null;
    }

    checkIsEntityLastCache(node);

    ILastCacheEntry lastCacheEntry = node.getLastCacheEntry();
    if (seriesPath == null) {
      return lastCacheEntry.getCachedLast();
    } else {
      String measurementId = seriesPath.getMeasurement();
      if (measurementId.equals(node.getName()) || measurementId.equals(node.getAlias())) {
        return lastCacheEntry.getCachedLast();
      } else {
        IMeasurementSchema schema = node.getSchema();
        if (schema instanceof VectorMeasurementSchema) {
          return lastCacheEntry.getCachedLast(
              schema.getMeasurementIdColumnIndex(seriesPath.getMeasurement()));
        }
        return null;
      }
    }
  }

  public static void updateLastCache(
      PartialPath seriesPath,
      TimeValuePair timeValuePair,
      boolean highPriorityUpdate,
      Long latestFlushedTime,
      IMeasurementMNode node) {
    if (node == null) {
      return;
    }

    checkIsEntityLastCache(node);

    ILastCacheEntry lastCacheEntry = node.getLastCacheEntry();
    if (seriesPath == null) {
      lastCacheEntry.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
    } else {
      String measurementId = seriesPath.getMeasurement();
      if (measurementId.equals(node.getName()) || measurementId.equals(node.getAlias())) {
        lastCacheEntry.updateCachedLast(timeValuePair, highPriorityUpdate, latestFlushedTime);
      } else {
        IMeasurementSchema schema = node.getSchema();
        if (schema instanceof VectorMeasurementSchema) {
          if (lastCacheEntry.isEmpty()) {
            lastCacheEntry.init(schema.getMeasurementCount());
          }
          lastCacheEntry.updateCachedLast(
              schema.getMeasurementIdColumnIndex(seriesPath.getMeasurement()),
              timeValuePair,
              highPriorityUpdate,
              latestFlushedTime);
        }
      }
    }
  }

  public static void resetLastCache(PartialPath seriesPath, IMeasurementMNode node) {
    if (node == null) {
      return;
    }

    checkIsEntityLastCache(node);

    ILastCacheEntry lastCacheEntry = node.getLastCacheEntry();
    if (seriesPath == null) {
      lastCacheEntry.resetLastCache();
    } else {
      String measurementId = seriesPath.getMeasurement();
      if (measurementId.equals(node.getName()) || measurementId.equals(node.getAlias())) {
        lastCacheEntry.resetLastCache();
      } else {
        IMeasurementSchema schema = node.getSchema();
        if (schema instanceof VectorMeasurementSchema) {
          if (lastCacheEntry.isEmpty()) {
            lastCacheEntry.init(schema.getMeasurementCount());
          }
          lastCacheEntry.resetLastCache(
              schema.getMeasurementIdColumnIndex(seriesPath.getMeasurement()));
        }
      }
    }
  }

  private static void checkIsEntityLastCache(IMeasurementMNode node) {
    IEntityMNode entityMNode = node.getParent();
    String measurement = node.getName();
    if (!entityMNode.hasChild(measurement)) {
      ILastCacheEntry lastCacheEntry = entityMNode.getLastCacheEntry(measurement);
      IMeasurementSchema schema = node.getSchema();
      if (lastCacheEntry.isEmpty() && (schema instanceof VectorMeasurementSchema)) {
        lastCacheEntry.init(schema.getMeasurementCount());
      }
      node.setLastCacheEntry(lastCacheEntry);
    }
  }

  public static void deleteLastCacheByDevice(IEntityMNode node) {
    // process lastCache of timeseries represented by measurementNode
    for (IMNode measurementNode : node.getChildren().values()) {
      if (measurementNode != null) {
        ((IMeasurementMNode) measurementNode).getLastCacheEntry().resetLastCache();
        logger.debug(
            "[tryToDeleteLastCacheByDevice] Last cache for path: {} is set to null",
            measurementNode.getFullPath());
      }
    }
    // process lastCache of timeseries represented by template
    for (Map.Entry<String, ILastCacheEntry> entry : node.getTemplateLastCaches().entrySet()) {
      entry.getValue().resetLastCache();
      logger.debug(
          "[tryToDeleteLastCacheByDevice] Last cache for path: {} is set to null",
          node.getPartialPath().concatNode(entry.getKey()).getFullPath());
    }
  }

  public static void deleteLastCacheByDevice(
      IEntityMNode node, PartialPath originalPath, long startTime, long endTime) {
    PartialPath path;
    IMeasurementSchema schema;
    ILastCacheEntry lastCacheEntry;

    // process lastCache of timeseries represented by measurementNode
    IMeasurementMNode measurementMNode;
    for (IMNode child : node.getChildren().values()) {
      if (child == null || !child.isMeasurement()) {
        continue;
      }
      path = child.getPartialPath();
      measurementMNode = (IMeasurementMNode) child;
      if (originalPath.matchFullPath(path)) {
        lastCacheEntry = measurementMNode.getLastCacheEntry();
        if (lastCacheEntry == null) {
          continue;
        }
        schema = measurementMNode.getSchema();
        deleteLastCache(path, schema, lastCacheEntry, startTime, endTime);
      }
    }

    // process lastCache of timeseries represented by template
    Template template = node.getUpperTemplate();
    for (Map.Entry<String, ILastCacheEntry> entry : node.getTemplateLastCaches().entrySet()) {
      path = node.getPartialPath().concatNode(entry.getKey());
      if (originalPath.matchFullPath(path)) {
        lastCacheEntry = entry.getValue();
        if (lastCacheEntry == null) {
          continue;
        }
        schema = template.getSchemaMap().get(entry.getKey());
        deleteLastCache(path, schema, lastCacheEntry, startTime, endTime);
      }
    }
  }

  private static void deleteLastCache(
      PartialPath path,
      IMeasurementSchema schema,
      ILastCacheEntry lastCacheEntry,
      long startTime,
      long endTime) {
    TimeValuePair lastPair;
    if (schema instanceof VectorMeasurementSchema) {
      int index;
      for (String measurement : schema.getValueMeasurementIdList()) {
        index = schema.getMeasurementIdColumnIndex(measurement);
        lastPair = lastCacheEntry.getCachedLast(index);
        if (lastPair != null
            && startTime <= lastPair.getTimestamp()
            && lastPair.getTimestamp() <= endTime) {
          lastCacheEntry.resetLastCache(index);
          logger.info(
              "[tryToDeleteLastCache] Last cache for path: {} is set to null",
              path.concatNode(measurement).getFullPath());
        }
      }
    } else {
      lastPair = lastCacheEntry.getCachedLast();
      if (lastPair != null
          && startTime <= lastPair.getTimestamp()
          && lastPair.getTimestamp() <= endTime) {
        lastCacheEntry.resetLastCache();
        logger.info(
            "[tryToDeleteLastCache] Last cache for path: {} is set to null", path.getFullPath());
      }
    }
  }

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
