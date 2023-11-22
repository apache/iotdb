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

package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.flush;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.pbtree.memcontrol.IReleaseFlushStrategy;
import org.apache.iotdb.tsfile.utils.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Monitor {
  /** configuration */
  private double FORCE_FLUSH_THREASHODL = 0.5;

  private double FREE_FLUSH_THREASHODL = 0.3;
  private int MONITOR_INETRVAL_MILLISECONDS = 5000;

  /** data structure */
  private final Map<Integer, RecordList> regionToTraverserTime = new ConcurrentHashMap<>();

  private final ScheduledExecutorService executorService =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.PBTREE_FLUSH_MONITOR.getName());
  private IReleaseFlushStrategy releaseFlushStrategy;
  private Scheduler scheduler;

  public void init(IReleaseFlushStrategy releaseFlushStrategy, Scheduler scheduler) {
    this.releaseFlushStrategy = releaseFlushStrategy;
    this.scheduler = scheduler;
    executorService.scheduleAtFixedRate(
        () -> {
          // TODO: update to use the new flush strategy
          if (this.releaseFlushStrategy.isExceedFlushThreshold()) {
            regionToTraverserTime.clear();
            scheduler.forceFlushAll();
          } else {
            scheduler.scheduleFlush(getRegionsToFlush(System.currentTimeMillis()));
          }
        },
        MONITOR_INETRVAL_MILLISECONDS,
        MONITOR_INETRVAL_MILLISECONDS,
        TimeUnit.MILLISECONDS);
  }

  public RecordNode recordTraverserTime(int regionId) {
    return regionToTraverserTime
        .computeIfAbsent(regionId, k -> new RecordList())
        .createAndAddToTail();
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
      if (traverserFreeTime > FREE_FLUSH_THREASHODL * MONITOR_INETRVAL_MILLISECONDS) {
        regionAndFreeTimeList.add(new Pair<>(regionId, traverserFreeTime));
      }
    }
    regionAndFreeTimeList.sort(Comparator.comparing((Pair<Integer, Long> o) -> o.right).reversed());
    return regionAndFreeTimeList.stream().map(Pair::getLeft).collect(Collectors.toList());
  }

  public int getFlushThreadNum() {
    return scheduler.getFlushThreadNum();
  }

  @TestOnly
  public void forceFlushAndRelease() {
    scheduler.forceFlushAll();
  }

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

    private Iterator<RecordNode> iterator() {
      return new Iterator<RecordNode>() {
        private RecordNode cur = head;

        @Override
        public boolean hasNext() {
          return cur.next != tail;
        }

        @Override
        public RecordNode next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          cur = cur.next;
          return cur;
        }

        @Override
        public void remove() {
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
