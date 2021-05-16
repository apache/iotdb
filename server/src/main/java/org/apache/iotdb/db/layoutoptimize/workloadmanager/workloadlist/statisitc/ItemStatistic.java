package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ItemStatistic implements Serializable {
  long averageSpan;
  long spanNum;
  long maxSpan;
  long minSpan;
  BigInteger totalSpan;
  Map<String, Map<String, Long>> measurementVisitCount = new HashMap<>();

  public ItemStatistic() {
    averageSpan = 0;
    spanNum = 0;
    totalSpan = BigInteger.valueOf(0);
    maxSpan = 0;
    minSpan = 0;
  }

  public void addSpan(long span) {
    totalSpan = totalSpan.add(BigInteger.valueOf(span));
    spanNum++;
    averageSpan = totalSpan.divide(BigInteger.valueOf(spanNum)).longValue();
    if (span > maxSpan) {
      maxSpan = span;
    }
    if (span < minSpan) {
      minSpan = span;
    }
  }

  public void addVisitedMeasurement(String deviceId, List<String> measurements) {
    if (!measurementVisitCount.containsKey(deviceId)) {
      measurementVisitCount.put(deviceId, new HashMap<>());
    }
    Map<String, Long> countMap = measurementVisitCount.get(deviceId);
    for (String measurement : measurements) {
      if (!countMap.containsKey(measurement)) {
        countMap.put(measurement, 0L);
      }
      countMap.replace(measurement, countMap.get(measurement) + 1L);
    }
  }
}
