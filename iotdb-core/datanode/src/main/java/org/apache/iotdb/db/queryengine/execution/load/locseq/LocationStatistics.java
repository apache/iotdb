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

  public double getThroughput(TDataNodeLocation location) {
    return locationStatisticMap.computeIfAbsent(location,
        l -> new Statistic()).throughput;
  }

  public void updateThroughput(TDataNodeLocation location, double throughput) {
    locationStatisticMap.computeIfAbsent(location,
        l -> new Statistic()).setThroughput(throughput);
  }

  public void increaseHit(TDataNodeLocation location) {
    locationStatisticMap.computeIfAbsent(location,
        l -> new Statistic()).increaseHit();
  }

  public static class Statistic {

    private double throughput = Float.MAX_VALUE;
    private int hit;

    public void setThroughput(double throughput) {
      this.throughput = throughput;
    }

    public void increaseHit() {
      hit++;
    }

    @Override
    public String toString() {
      return "{" +
          "throughput=" + throughput +
          ", hit=" + hit +
          '}';
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
