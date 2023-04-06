package org.apache.iotdb.db.pipe.core.collector.realtime.matcher;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.iotdb.db.pipe.PipeConfig;
import org.apache.iotdb.db.pipe.core.collector.realtime.PipeRealtimeCollector;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MapMatcher implements PipePatternMatcher {
  private final ReentrantReadWriteLock lock;
  private final Set<PipeRealtimeCollector> collectors;
  private final Cache<String, Set<PipeRealtimeCollector>> deviceCache;

  public MapMatcher() {
    this.lock = new ReentrantReadWriteLock();
    this.collectors = new HashSet<>();
    this.deviceCache =
        Caffeine.newBuilder().maximumSize(PipeConfig.getInstance().getMatcherCacheSize()).build();
  }

  @Override
  public void register(PipeRealtimeCollector collector, String[] nodes) {
    lock.writeLock().lock();
    try {
      collectors.add(collector);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public void deregister(PipeRealtimeCollector collector, String[] nodes) {
    lock.writeLock().lock();
    try {
      collectors.remove(collector);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public Set<PipeRealtimeCollector> matchSeries(String device, String[] measurements) {
    lock.readLock().lock();
    try {
      Set<PipeRealtimeCollector> matchCollectors = new HashSet<>();

      for (PipeRealtimeCollector collector : deviceCache.get(device, this::matchDevice)) {
        String pattern = collector.getPattern();
        if (pattern.length() <= device.length()) {
          matchCollectors.add(collector);
        } else {
          for (String measurement : measurements) {
            if (pattern.endsWith(measurement)
                && pattern.length() == device.length() + measurement.length() + 1) {
              matchCollectors.add(collector);
              break;
            }
          }
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
  public Set<PipeRealtimeCollector> matchDevices(Set<String> devices) {
    lock.readLock().lock();
    try {
      Set<PipeRealtimeCollector> matchCollectors = new HashSet<>();
      for (Map.Entry<String, Set<PipeRealtimeCollector>> entry : pattern2Collectors.entrySet()) {
        if (checkIfDevicesMatch(entry.getKey(), devices)) {
          matchCollectors.addAll(entry.getValue());
        }
      }
      return matchCollectors;
    } finally {
      lock.readLock().unlock();
    }
  }

  private boolean checkIfDevicesMatch(String pattern, Set<String> devices) {
    return true;
  }
}
