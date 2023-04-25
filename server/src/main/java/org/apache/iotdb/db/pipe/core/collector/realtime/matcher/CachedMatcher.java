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

import org.apache.iotdb.db.pipe.config.PipeConfig;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedMatcher implements PipePatternMatcher {
  private static final Logger logger = LoggerFactory.getLogger(CachedMatcher.class);
  private final ReentrantReadWriteLock lock;
  private final Set<PipeRealtimeCollector> collectors;
  private final Cache<String, Set<PipeRealtimeCollector>> deviceCache;

  public CachedMatcher() {
    this.lock = new ReentrantReadWriteLock();
    this.collectors = new HashSet<>();
    this.deviceCache =
        Caffeine.newBuilder().maximumSize(PipeConfig.getInstance().getMatcherCacheSize()).build();
  }

  @Override
  public void register(PipeRealtimeCollector collector) {
    lock.writeLock().lock();
    try {
      collectors.add(collector);
      deviceCache.invalidateAll();
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(PipeRealtimeCollector collector) {
    lock.writeLock().lock();
    try {
      collectors.remove(collector);
      deviceCache.invalidateAll();
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

  @Override
  public Set<PipeRealtimeCollector> match(Map<String, String[]> device2Measurements) {
    lock.readLock().lock();
    try {
      if (collectors.isEmpty()) {
        return new HashSet<>();
      }

      Set<PipeRealtimeCollector> matchCollectors = new HashSet<>();
      for (Map.Entry<String, String[]> entry : device2Measurements.entrySet()) {
        final String device = entry.getKey();
        final String[] measurements = entry.getValue();
        final Set<PipeRealtimeCollector> deviceMatchCollectors =
            deviceCache.get(device, this::matchDevice);
        if (deviceMatchCollectors == null) {
          logger.warn(String.format("Match result NPE when handle device %s", device));
          continue;
        }

        if (measurements.length == 0) { // match all measurements
          matchCollectors.addAll(deviceMatchCollectors);
        } else {
          deviceMatchCollectors.forEach(
              collector -> {
                String pattern = collector.getPattern();
                if (pattern.length() <= device.length()) {
                  matchCollectors.add(collector);
                } else {
                  for (String measurement : measurements) {
                    if (pattern.endsWith(TsFileConstant.PATH_SEPARATOR + measurement)
                        && pattern.length() == device.length() + measurement.length() + 1) {
                      matchCollectors.add(collector);
                      break;
                    }
                  }
                }
              });
        }
        if (matchCollectors.size() == collectors.size()) {
          break;
        }
      }
      return matchCollectors;
    } finally {
      lock.readLock().unlock();
    }
  }

  private Set<PipeRealtimeCollector> matchDevice(String device) {
    Set<PipeRealtimeCollector> matchCollectors = new HashSet<>();
    for (PipeRealtimeCollector collector : collectors) {
      String pattern = collector.getPattern();
      if ((pattern.length() <= device.length() && device.startsWith(pattern))
          || (pattern.length() > device.length() && pattern.startsWith(device))) {
        matchCollectors.add(collector);
      }
    }
    return matchCollectors;
  }

  @Override
  public void clear() {
    lock.writeLock().lock();
    try {
      collectors.clear();
      deviceCache.invalidateAll();
      deviceCache.cleanUp();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
