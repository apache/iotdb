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
package org.apache.iotdb.tsfile.write.monitor;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class WriteStatistics {
  public static final WriteStatistics INSTANCE = new WriteStatistics();
  public static final String LABEL_TIME = "time";
  public static final String LABEL_VALUE = "value";

  private final Map<String, Statistic> statisticMap = new ConcurrentHashMap<>();

  private WriteStatistics() {}

  public void dump(String filePath) throws IOException {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath))) {
      writer.write("measurementId, rawSize, encodedSize, compressedSize");
      writer.newLine();
      for (Entry<String, Statistic> entry : statisticMap.entrySet()) {
        String label = entry.getKey();
        Statistic statistic = entry.getValue();
        writer.write(
            String.format(
                "%s,%d,%d,%d",
                label,
                statistic.rawSize.get(),
                statistic.encodedSize.get(),
                statistic.compressedSize.get()));
        writer.newLine();
      }
    }
  }

  public void update(String measurementId, long timeSize, long valueSize, StatisticType type) {
    WriteStatistics.INSTANCE.update(WriteStatistics.LABEL_TIME, timeSize, type);
    WriteStatistics.INSTANCE.update(WriteStatistics.LABEL_VALUE, valueSize, type);
    WriteStatistics.INSTANCE.update(
        measurementId + "-" + WriteStatistics.LABEL_TIME, timeSize, type);
    WriteStatistics.INSTANCE.update(
        measurementId + "-" + WriteStatistics.LABEL_VALUE, valueSize, type);
  }

  public void update(String label, long delta, StatisticType type) {
    statisticMap.computeIfAbsent(label, l -> new Statistic()).update(delta, type);
  }

  private static class Statistic {
    private final AtomicLong rawSize = new AtomicLong();
    private final AtomicLong encodedSize = new AtomicLong();
    private final AtomicLong compressedSize = new AtomicLong();

    private void update(long delta, StatisticType type) {
      switch (type) {
        case rawSize:
          rawSize.addAndGet(delta);
          break;
        case encodedSize:
          encodedSize.addAndGet(delta);
          break;
        case compressedSize:
          compressedSize.addAndGet(delta);
          break;
      }
    }
  }

  public enum StatisticType {
    rawSize,
    encodedSize,
    compressedSize;
  }
}
