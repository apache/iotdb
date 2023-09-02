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

package org.apache.iotdb.db.pipe.extractor.realtime.matcher;

import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.extractor.realtime.PipeRealtimeDataRegionExtractor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PrefixPatternMatcher implements PipeDataRegionMatcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(PrefixPatternMatcher.class);

  private final ReentrantReadWriteLock lock;

  private final Set<PipeRealtimeDataRegionExtractor> extractors;
  private final Cache<String, Set<PipeRealtimeDataRegionExtractor>> deviceToExtractorsCache;

  public PrefixPatternMatcher() {
    this.lock = new ReentrantReadWriteLock();
    this.extractors = new HashSet<>();
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
  public int getRegisterCount() {
    lock.readLock().lock();
    try {
      return extractors.size();
    } finally {
      lock.readLock().unlock();
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
          // `measurements` is empty (only in case of tsFile event). match all extractors.
          //
          // case 1: for example, pattern is root.a.b, device is root.a.b.c, measurement can be any.
          // in this case, the extractor can be matched without checking the measurements.
          //
          // case 2: for example, pattern is root.a.b.c, device is root.a.b.
          // in this situation, the extractor can not be matched in some cases, but we can not know
          // all the measurements of the device in an efficient way, so we ASSUME that the extractor
          // can be matched. this is a trade-off between efficiency and accuracy. for most user's
          // usage, this is acceptable, which may result in some unnecessary data processing and
          // transmission, but will not result in data loss.
          matchedExtractors.addAll(extractorsFilteredByDevice);
        } else {
          // `measurements` is not empty (only in case of tablet event). match extractors by
          // measurements.
          extractorsFilteredByDevice.forEach(
              extractor -> {
                final String pattern = extractor.getPattern();

                // case 1: for example, pattern is root.a.b and device is root.a.b.c
                // in this case, the extractor can be matched without checking the measurements
                if (pattern.length() <= device.length()) {
                  matchedExtractors.add(extractor);
                }
                // case 2: for example, pattern is root.a.b.c and device is root.a.b
                // in this case, we need to check the full path
                else {
                  final String measurementPattern =
                      pattern.replace(device + TsFileConstant.PATH_SEPARATOR, "");

                  for (final String measurement : measurements) {
                    // ignore null measurement for partial insert
                    if (measurement == null) {
                      continue;
                    }

                    // for example, pattern is root.a.b.c, device is root.a.b and measurement is c1
                    // in this case, the extractor can be matched.
                    // please note that there should be a . between device and measurement.
                    if (measurement.startsWith(measurementPattern)) {
                      matchedExtractors.add(extractor);
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

  private Set<PipeRealtimeDataRegionExtractor> filterExtractorsByDevice(String device) {
    final Set<PipeRealtimeDataRegionExtractor> filteredExtractors = new HashSet<>();

    for (PipeRealtimeDataRegionExtractor extractor : extractors) {
      String pattern = extractor.getPattern();
      // If device is root.test.a1 and pattern is root.test.a, match.
      // else if device is root.test and pattern is root.test.b, match first and investigate the
      // tsFile later.
      if (device.startsWith(pattern)
          || pattern.startsWith(device + TsFileConstant.PATH_SEPARATOR)) {
        filteredExtractors.add(extractor);
        extractor.updatePatternGranularity(device);
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
