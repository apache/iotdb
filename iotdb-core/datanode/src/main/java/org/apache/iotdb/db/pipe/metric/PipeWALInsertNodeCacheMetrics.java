package org.apache.iotdb.db.pipe.metric;

import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALInsertNodeCache;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PipeWALInsertNodeCacheMetrics implements IMetricSet {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeWALInsertNodeCacheMetrics.class);

  private AbstractMetricService metricService;

  private final Map<Integer, WALInsertNodeCache> cacheMap = new HashMap<>();

  //////////////////////////// bindTo & unbindFrom (metric framework) ////////////////////////////

  @Override
  public void bindTo(AbstractMetricService metricService) {
    this.metricService = metricService;
    synchronized (this) {
      for (Integer dataRegionId : cacheMap.keySet()) {
        createMetrics(dataRegionId);
      }
    }
  }

  private void createMetrics(Integer dataRegionId) {
    createAutoGauge(dataRegionId);
  }

  private void createAutoGauge(Integer dataRegionId) {
    metricService.createAutoGauge(
        Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_RATE.toString(),
        MetricLevel.IMPORTANT,
        cacheMap.get(dataRegionId),
        WALInsertNodeCache::getCacheHitRate,
        Tag.REGION.toString(),
        String.valueOf(dataRegionId));
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    ImmutableSet<Integer> dataRegionIds = ImmutableSet.copyOf(cacheMap.keySet());
    for (Integer dataRegionId : dataRegionIds) {
      deregister(dataRegionId);
    }
    if (!cacheMap.isEmpty()) {
      LOGGER.warn(
          "Failed to unbind from wal insert node cache hit rate metrics, cache map not empty");
    }
  }

  private void removeMetrics(Integer dataRegionId) {
    removeAutoGauge(dataRegionId);
  }

  private void removeAutoGauge(Integer dataRegionId) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.PIPE_WAL_INSERT_NODE_CACHE_HIT_RATE.toString(),
        Tag.REGION.toString(),
        String.valueOf(dataRegionId));
  }

  //////////////////////////// register & deregister (pipe integration) ////////////////////////////

  public void register(@NonNull WALInsertNodeCache walInsertNodeCache, Integer dataRegionId) {
    synchronized (this) {
      cacheMap.putIfAbsent(dataRegionId, walInsertNodeCache);
      if (Objects.nonNull(metricService)) {
        createMetrics(dataRegionId);
      }
    }
  }

  public void deregister(Integer dataRegionId) {
    // TODO: waiting called by WALInsertNodeCache
    synchronized (this) {
      if (!cacheMap.containsKey(dataRegionId)) {
        LOGGER.warn(
            "Failed to deregister wal insert node cache hit rate metrics, WALInsertNodeCache({}) does not exist",
            dataRegionId);
        return;
      }
      if (Objects.nonNull(metricService)) {
        removeMetrics(dataRegionId);
      }
      cacheMap.remove(dataRegionId);
    }
  }

  //////////////////////////// singleton ////////////////////////////

  private static class PipeWALInsertNodeCacheMetricsHolder {

    private static final PipeWALInsertNodeCacheMetrics INSTANCE =
        new PipeWALInsertNodeCacheMetrics();

    private PipeWALInsertNodeCacheMetricsHolder() {
      // empty constructor
    }
  }

  public static PipeWALInsertNodeCacheMetrics getInstance() {
    return PipeWALInsertNodeCacheMetricsHolder.INSTANCE;
  }

  private PipeWALInsertNodeCacheMetrics() {
    // empty constructor
  }
}
