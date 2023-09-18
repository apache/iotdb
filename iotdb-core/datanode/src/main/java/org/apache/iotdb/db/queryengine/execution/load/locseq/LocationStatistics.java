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

package org.apache.iotdb.db.queryengine.execution.load.locseq;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocationStatistics {

  private static final Logger logger = LoggerFactory.getLogger(LocationStatistics.class);
  private Map<TDataNodeLocation, Statistic> locationStatisticMap = new ConcurrentHashMap<>();

  public Statistic getStatistic(TDataNodeLocation location) {
    return locationStatisticMap.computeIfAbsent(location,
        l -> new Statistic());
  }

  public void updateThroughput(TDataNodeLocation location, double sum, long cnt) {
    Statistic statistic = locationStatisticMap.computeIfAbsent(location,
        l -> new Statistic());
    statistic.updateThroughput(sum, cnt);
    statistic.increaseHit();
  }

  public static class Statistic {

    private double sum = 0.0;
    private long cnt = 0;
    private int hit;
    private long lastHitTime;

    public synchronized void updateThroughput(double sum, long cnt) {
      this.sum += sum;
      this.cnt += cnt;
    }

    public void increaseHit() {
      lastHitTime = System.currentTimeMillis();
      hit++;
    }

    public double getThroughput() {
      if (cnt == 0) {
        return Float.MAX_VALUE;
      }
      return sum / cnt;
    }

    @Override
    public String toString() {
      return "{" +
          "throughput=" + getThroughput() +
          ", hit=" + hit +
          ", lastHitTime=" + lastHitTime +
          '}';
    }

    public int getHit() {
      return hit;
    }

    public long getLastHitTime() {
      return lastHitTime;
    }
  }

  public void logLocationStatistics() {
    if (logger.isInfoEnabled()) {
      logger.info("Location throughput: {}", locationStatisticMap.entrySet().stream()
          .map(e -> new Pair<>(e.getKey().getDataNodeId(), e.getValue()))
          .collect(Collectors.toList()));
    }
  }
}
