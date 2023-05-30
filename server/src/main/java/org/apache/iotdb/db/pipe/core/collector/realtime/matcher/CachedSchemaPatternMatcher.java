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

package org.apache.iotdb.db.pipe.core.collector.realtime.matcher;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeDataRegionCollector;
import org.apache.iotdb.db.pipe.core.event.realtime.PipeRealtimeCollectEvent;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedSchemaPatternMatcher implements PipeDataRegionMatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(CachedSchemaPatternMatcher.class);

  private final ReentrantReadWriteLock lock;

  private final Set<PipeRealtimeDataRegionCollector> collectors;
  private final Cache<String, Set<PipeRealtimeDataRegionCollector>> deviceToCollectorsCache;

  public CachedSchemaPatternMatcher() {
    this.lock = new ReentrantReadWriteLock();
    this.collectors = new HashSet<>();
    this.deviceToCollectorsCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeCollectorMatcherCacheSize())
            .build();
  }

  @Override
  public void register(PipeRealtimeDataRegionCollector collector) {
    lock.writeLock().lock();
    try {
      collectors.add(collector);
      deviceToCollectorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(PipeRealtimeDataRegionCollector collector) {
    lock.writeLock().lock();
    try {
      collectors.remove(collector);
      deviceToCollectorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public int getRegisterCount() {
    lock.readLock().lock();
    try {
      return collectors.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  // TODO: maximum the efficiency of matching when pattern is root
  // TODO: memory control
  @Override
  public Set<PipeRealtimeDataRegionCollector> match(PipeRealtimeCollectEvent event) {
    final Set<PipeRealtimeDataRegionCollector> matchedCollectors = new HashSet<>();

    lock.readLock().lock();
    try {
      if (collectors.isEmpty()) {
        return matchedCollectors;
      }

      for (final Map.Entry<String, String[]> entry : event.getSchemaInfo().entrySet()) {
        final String device = entry.getKey();
        final String[] measurements = entry.getValue();

        // 1. try to get matched collectors from cache, if not success, match them by device
        final Set<PipeRealtimeDataRegionCollector> collectorsFilteredByDevice =
            deviceToCollectorsCache.get(device, this::filterCollectorsByDevice);
        // this would not happen
        if (collectorsFilteredByDevice == null) {
          LOGGER.warn(String.format("Match result NPE when handle device %s", device));
          continue;
        }

        // 2. filter matched candidate collectors by measurements
        if (measurements.length == 0) {
          // `measurements` is empty (only in case of tsfile event). match all collectors.
          //
          // case 1: for example, pattern is root.a.b, device is root.a.b.c, measurement can be any.
          // in this case, the collector can be matched without checking the measurements.
          //
          // case 2: for example, pattern is root.a.b.c, device is root.a.b.
          // in this situation, the collector can not be matched in some cases, but we can not know
          // all the measurements of the device in an efficient way, so we ASSUME that the collector
          // can be matched. this is a trade-off between efficiency and accuracy. for most user's
          // usage, this is acceptable, which may result in some unnecessary data processing and
          // transmission, but will not result in data loss.
          matchedCollectors.addAll(collectorsFilteredByDevice);
        } else {
          // `measurements` is not empty (only in case of tablet event). match collectors by
          // measurements.
          collectorsFilteredByDevice.forEach(
              collector -> {
                final String pattern = collector.getPattern();

                // case 1: for example, pattern is root.a.b and device is root.a.b.c
                // in this case, the collector can be matched without checking the measurements
                if (pattern.length() <= device.length()) {
                  matchedCollectors.add(collector);
                }
                // case 2: for example, pattern is root.a.b.c and device is root.a.b
                // in this case, we need to check the full path
                else {
                  for (String measurement : measurements) {
                    // for example, pattern is root.a.b.c, device is root.a.b and measurement is c
                    // in this case, the collector can be matched. other cases are not matched.
                    // please note that there should be a . between device and measurement.
                    if (
                    // low cost check comes first
                    pattern.length() == device.length() + measurement.length() + 1
                        // high cost check comes later
                        && pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)) {
                      matchedCollectors.add(collector);
                      // there would be no more matched collectors because the measurements are
                      // unique
                      break;
                    }
                  }
                }
              });
        }

        if (matchedCollectors.size() == collectors.size()) {
          break;
        }
      }
    } finally {
      lock.readLock().unlock();
    }

    return matchedCollectors;
  }

  private Set<PipeRealtimeDataRegionCollector> filterCollectorsByDevice(String device) {
    final Set<PipeRealtimeDataRegionCollector> filteredCollectors = new HashSet<>();

    for (PipeRealtimeDataRegionCollector collector : collectors) {
      String pattern = collector.getPattern();
      if (
      // for example, pattern is root.a.b and device is root.a.b.c
      // in this case, the collector can be matched without checking the measurements
      (pattern.length() <= device.length() && device.startsWith(pattern))
          // for example, pattern is root.a.b.c and device is root.a.b
          // in this case, the collector can be selected as candidate, but the measurements should
          // be checked further
          || (pattern.length() > device.length() && pattern.startsWith(device))) {
        filteredCollectors.add(collector);
      }
    }

    return filteredCollectors;
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      collectors.clear();
      deviceToCollectorsCache.invalidateAll();
      deviceToCollectorsCache.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
