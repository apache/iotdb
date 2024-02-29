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

package org.apache.iotdb.db.pipe.pattern.matcher;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.db.pipe.pattern.PipePattern;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedSchemaPatternMatcher implements PipeDataRegionMatcher {

  protected static final Logger LOGGER = LoggerFactory.getLogger(CachedSchemaPatternMatcher.class);

  protected final ReentrantReadWriteLock lock;

  protected final Set<PipeRealtimeDataRegionExtractor> extractors;
  protected final Cache<String, Set<PipeRealtimeDataRegionExtractor>> deviceToExtractorsCache;

  public CachedSchemaPatternMatcher() {
    this.lock = new ReentrantReadWriteLock();
    // Should be thread-safe because the extractors will be returned by {@link #match} and
    // iterated by {@link #assignToExtractor}, at the same time the extractors may be added or
    // removed by {@link #register} and {@link #deregister}.
    this.extractors = new CopyOnWriteArraySet<>();
    this.deviceToExtractorsCache =
        Caffeine.newBuilder()
            .maximumSize(PipeConfig.getInstance().getPipeExtractorMatcherCacheSize())
            .build();
  }

  @Override
  public void register(PipeRealtimeDataRegionExtractor extractor) {
    lock.writeLock().lock();
    try {
      extractors.add(extractor);
      deviceToExtractorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(PipeRealtimeDataRegionExtractor extractor) {
    lock.writeLock().lock();
    try {
      extractors.remove(extractor);
      deviceToExtractorsCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Set<PipeRealtimeDataRegionExtractor> match(PipeRealtimeEvent event) {
    final Set<PipeRealtimeDataRegionExtractor> matchedExtractors = new HashSet<>();

    lock.readLock().lock();
    try {
      if (extractors.isEmpty()) {
        return matchedExtractors;
      }

      // HeartbeatEvent will be assigned to all extractors
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        return extractors;
      }

      for (final Map.Entry<String, String[]> entry : event.getSchemaInfo().entrySet()) {
        final String device = entry.getKey();
        final String[] measurements = entry.getValue();

        // 1. try to get matched extractors from cache, if not success, match them by device
        final Set<PipeRealtimeDataRegionExtractor> extractorsFilteredByDevice =
            deviceToExtractorsCache.get(device, this::filterExtractorsByDevice);
        // this would not happen
        if (extractorsFilteredByDevice == null) {
          LOGGER.warn("Match result NPE when handle device {}", device);
          continue;
        }

        // 2. filter matched candidate extractors by measurements
        if (measurements.length == 0) {
          // We can't get all measurements efficiently here,
          // so we just ASSUME the extractor matches and do more checks later.
          matchedExtractors.addAll(extractorsFilteredByDevice);
        } else {
          // `measurements` is not empty (only in case of tablet event). match extractors by
          // measurements.
          extractorsFilteredByDevice.forEach(
              extractor -> {
                final PipePattern pattern = extractor.getPipePattern();
                if (Objects.isNull(pattern) || pattern.coversDevice(device)) {
                  matchedExtractors.add(extractor);
                } else {
                  for (final String measurement : measurements) {
                    // Ignore null measurement for partial insert
                    if (measurement == null) {
                      continue;
                    }

                    if (pattern.matchesMeasurement(device, measurement)) {
                      matchedExtractors.add(extractor);
                      // There would be no more matched extractors because the measurements are
                      // unique
                      break;
                    }
                  }
                }
              });
        }

        if (matchedExtractors.size() == extractors.size()) {
          break;
        }
      }
    } finally {
      lock.readLock().unlock();
    }

    return matchedExtractors;
  }

  @Override
  public int getRegisterCount() {
    lock.readLock().lock();
    try {
      return extractors.size();
    } finally {
      lock.readLock().unlock();
    }
  }

  protected Set<PipeRealtimeDataRegionExtractor> filterExtractorsByDevice(String device) {
    final Set<PipeRealtimeDataRegionExtractor> filteredExtractors = new HashSet<>();

    for (PipeRealtimeDataRegionExtractor extractor : extractors) {
      final PipePattern pipePattern = extractor.getPipePattern();
      if (Objects.isNull(pipePattern) || pipePattern.mayOverlapWithDevice(device)) {
        filteredExtractors.add(extractor);
      }
    }

    return filteredExtractors;
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      extractors.clear();
      deviceToExtractorsCache.invalidateAll();
      deviceToExtractorsCache.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
