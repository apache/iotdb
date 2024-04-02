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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memory;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.schemaengine.metric.SchemaEngineCachedMetric;
import org.apache.iotdb.db.schemaengine.rescon.CachedSchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.rescon.ISchemaEngineStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.CachedMTreeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush.Scheduler;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.ReleaseFlushStrategyNumBasedImpl;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.ReleaseFlushStrategySizeBasedImpl;
import org.apache.iotdb.db.utils.concurrent.FiniteSemaphore;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * All instances of CachedMTreeStoreCacheMemoryManager shall be registered in ReleaseFlushMonitor.
 * ReleaseFlushMonitor provides the {@link ReleaseFlushMonitor#ensureMemoryStatus} interface, which
 * starts asynchronous threads to free and flush the disk when memory usage exceeds a threshold.
 */
public class ReleaseFlushMonitor {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseFlushMonitor.class);

  /** configuration */
  private static final double FREE_FLUSH_PROPORTION = 0.2;

  private static final int MONITOR_INETRVAL_MILLISECONDS = 5000;
  private static final int MAX_WAITING_TIME_WHEN_RELEASING = 3_000;

  /** data structure */
  private final Map<Integer, RecordList> regionToTraverserTime = new ConcurrentHashMap<>();

  private final Map<Integer, CachedMTreeStore> regionToStoreMap = new ConcurrentHashMap<>();
  private final Set<Integer> flushingRegionSet = new CopyOnWriteArraySet<>();
  private CachedSchemaEngineStatistics engineStatistics;
  private SchemaEngineCachedMetric engineMetric;
  private IReleaseFlushStrategy releaseFlushStrategy;

  /** thread and lock */
  private final Object blockObject = new Object();

  private ScheduledExecutorService flushMonitor;
  private ExecutorService releaseMonitor;
  private FiniteSemaphore releaseSemaphore;
  private Scheduler scheduler;

  public void registerCachedMTreeStore(CachedMTreeStore store) {
    regionToStoreMap.put(store.getRegionStatistics().getSchemaRegionId(), store);
    regionToTraverserTime.put(store.getRegionStatistics().getSchemaRegionId(), new RecordList());
  }

  public void clearCachedMTreeStore(CachedMTreeStore store) {
    regionToStoreMap.remove(store.getRegionStatistics().getSchemaRegionId());
    regionToTraverserTime.remove(store.getRegionStatistics().getSchemaRegionId());
  }

  public void init(ISchemaEngineStatistics engineStatistics) {
    releaseSemaphore = new FiniteSemaphore(2, 0);
    this.engineStatistics = engineStatistics.getAsCachedSchemaEngineStatistics();
    if (IoTDBDescriptor.getInstance().getConfig().getCachedMNodeSizeInPBTreeMode() >= 0) {
      releaseFlushStrategy = new ReleaseFlushStrategyNumBasedImpl(this.engineStatistics);
    } else {
      releaseFlushStrategy = new ReleaseFlushStrategySizeBasedImpl(this.engineStatistics);
    }
    scheduler = new Scheduler(regionToStoreMap, flushingRegionSet, releaseFlushStrategy);
    releaseMonitor =
        IoTDBThreadPoolFactory.newSingleThreadExecutor(ThreadName.PBTREE_RELEASE_MONITOR.getName());
    flushMonitor =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.PBTREE_FLUSH_MONITOR.getName());
    releaseMonitor.submit(
        () -> {
          try {
            while (!Thread.currentThread().isInterrupted()) {
              releaseSemaphore.acquire();
              // 1. first, it will try to release node cache
              if (releaseFlushStrategy.isExceedReleaseThreshold()) {
                scheduler.scheduleRelease(false);
                // 2. if it still exceeds release threshold, it will try to flush node buffer, then
                // release node cache again
                if (releaseFlushStrategy.isExceedReleaseThreshold()) {
                  scheduler.scheduleFlushAll();
                  regionToTraverserTime.values().forEach(RecordList::clear);
                }
                synchronized (blockObject) {
                  // invoke the notifyAll() method to wake up the thread waiting for the release
                  blockObject.notifyAll();
                }
              }
            }
          } catch (InterruptedException e) {
            logger.info("ReleaseTaskMonitor thread is interrupted.");
            Thread.currentThread().interrupt();
          }
        });
    ScheduledExecutorUtil.safelyScheduleAtFixedRate(
        flushMonitor,
        () -> {
          if (releaseFlushStrategy.isExceedReleaseThreshold()) {
            releaseSemaphore.release();
          } else {
            scheduler.scheduleFlush(getRegionsToFlush(System.currentTimeMillis()));
          }
        },
        MONITOR_INETRVAL_MILLISECONDS,
        MONITOR_INETRVAL_MILLISECONDS,
        TimeUnit.MILLISECONDS);
  }

  public void setEngineMetric(SchemaEngineCachedMetric engineMetric) {
    this.engineMetric = engineMetric;
  }

  /**
   * Check the current memory usage. If the release threshold is exceeded, trigger the task to
   * perform an internal and external memory swap to release the memory.
   */
  public void ensureMemoryStatus() {
    if (releaseFlushStrategy.isExceedReleaseThreshold()) {
      releaseSemaphore.release();
    }
  }

  /**
   * If there is a ReleaseTask or FlushTask, block the current thread to wait up to
   * MAX_WAITING_TIME_WHEN_RELEASING. The thread will be woken up if the ReleaseTask or FlushTask
   * ends or the wait time exceeds MAX_WAITING_TIME_WHEN_RELEASING.
   */
  public void waitIfReleasing() {
    synchronized (blockObject) {
      try {
        blockObject.wait(MAX_WAITING_TIME_WHEN_RELEASING);
      } catch (InterruptedException e) {
        logger.warn(
            "Interrupt because the release task and flush task did not finish within {} milliseconds.",
            MAX_WAITING_TIME_WHEN_RELEASING);
        Thread.currentThread().interrupt();
      }
    }
  }

  public RecordNode recordTraverserTime(int regionId) {
    return regionToTraverserTime.get(regionId).createAndAddToTail();
  }

  @TestOnly
  public void initRecordList(int regionId) {
    regionToTraverserTime.computeIfAbsent(regionId, k -> new RecordList());
  }

  public List<Integer> getRegionsToFlush(long windowsEndTime) {
    long windowsStartTime = windowsEndTime - MONITOR_INETRVAL_MILLISECONDS;
    List<Pair<Integer, Long>> regionAndFreeTimeList = new ArrayList<>();
    for (Map.Entry<Integer, RecordList> entry : regionToTraverserTime.entrySet()) {
      int regionId = entry.getKey();

      long traverserEndTime = windowsStartTime;
      long traverserFreeTime = 0;
      RecordList recordList = entry.getValue();
      Iterator<RecordNode> iterator = recordList.iterator();
      while (iterator.hasNext()) {
        RecordNode recordNode = iterator.next();
        if (recordNode.startTime > windowsEndTime) {
          break;
        }
        if (recordNode.startTime > traverserEndTime) {
          traverserFreeTime += (recordNode.startTime - traverserEndTime);
          traverserEndTime = recordNode.endTime;
        } else if (recordNode.endTime > traverserEndTime) {
          traverserEndTime = recordNode.endTime;
        }
        if (recordNode.endTime < windowsStartTime) {
          iterator.remove();
        } else if (recordNode.endTime >= windowsEndTime) {
          break;
        }
      }
      if (traverserEndTime < windowsEndTime) {
        traverserFreeTime += (windowsEndTime - traverserEndTime);
      }
      if (traverserFreeTime > FREE_FLUSH_PROPORTION * MONITOR_INETRVAL_MILLISECONDS) {
        regionAndFreeTimeList.add(new Pair<>(regionId, traverserFreeTime));
      }
    }
    regionAndFreeTimeList.sort(Comparator.comparing((Pair<Integer, Long> o) -> o.right).reversed());
    return regionAndFreeTimeList.stream().map(Pair::getLeft).collect(Collectors.toList());
  }

  @TestOnly
  public void forceFlushAndRelease() {
    boolean needFlush;
    while (true) {
      needFlush = false;
      for (CachedMTreeStore store : regionToStoreMap.values()) {
        if (store.getMemoryManager().getBufferNodeNum() > 0) {
          needFlush = true;
          break;
        }
      }
      if (needFlush) {
        scheduler.scheduleFlushAll().join();
        scheduler.scheduleRelease(true);
      } else {
        break;
      }
    }
  }

  public void clear() {
    if (releaseMonitor != null) {
      releaseMonitor.shutdownNow();
      while (true) {
        if (releaseMonitor.isTerminated()) break;
      }
      releaseMonitor = null;
    }
    if (flushMonitor != null) {
      flushMonitor.shutdownNow();
      while (true) {
        if (flushMonitor.isTerminated()) break;
      }
      flushMonitor = null;
    }
    if (scheduler != null) {
      scheduler.clear();
      while (true) {
        if (scheduler.isTerminated()) break;
      }
      scheduler = null;
    }
    regionToStoreMap.clear();
    flushingRegionSet.clear();
    regionToTraverserTime.clear();
    releaseFlushStrategy = null;
    engineStatistics = null;
    releaseSemaphore = null;
    engineMetric = null;
  }

  public int getActiveWorkerNum() {
    return scheduler.getActiveWorkerNum();
  }

  private ReleaseFlushMonitor() {}

  private static class ReleaseFlushMonitorHolder {
    private static final ReleaseFlushMonitor INSTANCE = new ReleaseFlushMonitor();

    private ReleaseFlushMonitorHolder() {}
  }

  public static ReleaseFlushMonitor getInstance() {
    return ReleaseFlushMonitor.ReleaseFlushMonitorHolder.INSTANCE;
  }

  @NotThreadSafe
  private static class RecordList {
    // The start time of RecordNode is incremental from head to tail
    private final RecordNode head = new RecordNode();
    private final RecordNode tail = new RecordNode();

    private RecordList() {
      head.next = tail;
      tail.prev = head;
    }

    private synchronized RecordNode createAndAddToTail() {
      RecordNode recordNode = new RecordNode();
      recordNode.prev = tail.prev;
      recordNode.next = tail;
      tail.prev.next = recordNode;
      tail.prev = recordNode;
      return recordNode;
    }

    private synchronized void remove(RecordNode recordNode) {
      recordNode.prev.next = recordNode.next;
      recordNode.next.prev = recordNode.prev;
      recordNode.prev = null;
      recordNode.next = null;
    }

    private synchronized void clear() {
      head.next = tail;
      tail.prev = head;
    }

    private Iterator<RecordNode> iterator() {
      return new Iterator<RecordNode>() {
        private RecordNode next = null;
        private RecordNode cur = head;

        @Override
        public boolean hasNext() {
          if (next == null && cur.next != tail) {
            next = cur.next;
          }
          return next != null;
        }

        @Override
        public RecordNode next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          cur = next;
          next = null;
          return cur;
        }

        @Override
        public void remove() {
          if (next == null && cur.next != tail) {
            next = cur.next;
          }
          RecordList.this.remove(cur);
        }
      };
    }
  }

  public static class RecordNode {
    private RecordNode prev = null;
    private RecordNode next = null;
    private Long startTime = System.currentTimeMillis();
    private Long endTime = Long.MAX_VALUE;

    @TestOnly
    public void setStartTime(Long startTime) {
      this.startTime = startTime;
    }

    public void setEndTime(Long endTime) {
      this.endTime = endTime;
    }
  }
}
