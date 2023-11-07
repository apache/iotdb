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
package org.apache.iotdb.db.pipe.extractor.historical;

import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class ThroughputMonitor {

  private static final Logger logger = LoggerFactory.getLogger(ThroughputMonitor.class);
  private int maxRecordNum = 10000;
  private int minRecordNumToAnalyze = 5;
  /** <timestamp, throughput> */
  private final Queue<Pair<Long, Double>> history = new ArrayDeque<>(maxRecordNum);

  public void record(long timestamp, double throughput) {
    synchronized (history) {
      if (history.size() >= maxRecordNum) {
        history.remove();
      }
      history.add(new Pair<>(timestamp, throughput));
    }
  }

  public double calculateAverage() {
    if (history.size() < minRecordNumToAnalyze) {
      logger.info("Only {} histories, do not update", history.size());
      return Double.NaN;
    }
    List<Pair<Long, Double>> historyCopy;
    synchronized (history) {
      historyCopy = new ArrayList<>(history);
    }
    logger.info("{} histories before purging", historyCopy.size());
    historyCopy = purge(historyCopy);
    logger.info("{} histories after purging", historyCopy.size());
    if (historyCopy.size() < minRecordNumToAnalyze) {
      return Double.NaN;
    }
    return historyCopy.stream().mapToDouble(p -> p.right).average().orElse(Double.NaN);
  }

  private List<Pair<Long, Double>> purge(List<Pair<Long, Double>> history) {
    history = purgeByTimeSpan(history);
    if (history.size() < minRecordNumToAnalyze) {
      return history;
    }
    return purgeByStderr(history);
  }

  private List<Pair<Long, Double>> purgeByStderr(List<Pair<Long, Double>> history) {
    double sum = 0.0;
    for (Pair<Long, Double> longDoublePair : history) {
      sum += longDoublePair.right;
    }

    while (true) {
      double average = sum / history.size();
      double stdErrSum = 0.0;
      int largestStdErrIdx = 0;
      double largestStdErr = 0.0;
      for (int i = 0; i < history.size(); i++) {
        Pair<Long, Double> longDoublePair = history.get(i);
        Double throughput = longDoublePair.right;
        double stdErr = Math.sqrt((throughput - average) * (throughput - average));
        stdErrSum += stdErr;
        if (stdErr > largestStdErr) {
          largestStdErrIdx = i;
          largestStdErr = stdErr;
        }
      }
      double stdErrAvg = stdErrSum / history.size();

      if (largestStdErr > stdErrAvg * 3.0) {
        history.remove(largestStdErrIdx);
        if (history.size() < minRecordNumToAnalyze) {
          return history;
        }
      } else {
        break;
      }
    }

    return history;
  }

  private List<Pair<Long, Double>> purgeByTimeSpan(List<Pair<Long, Double>> history) {
    long maxTimeSpan = 3600 * 1000L;
    long currTime = System.currentTimeMillis();
    long timeThreshold = currTime - maxTimeSpan;
    int i = 0;
    for (; i < history.size(); i++) {
      if (history.get(i).left >= timeThreshold) {
        break;
      }
    }
    if (i > 0) {
      history = history.subList(i, history.size());
    }
    return history;
  }
}
