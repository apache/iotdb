package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

public class ListStatistic {
  long averageSpan;
  long spanNum;
  long maxSpan;
  long minSpan;
  BigInteger totalSpan;
  Map<String, Map<String, Long>> measurementVisitCount;

  public ListStatistic() {
    averageSpan = 0;
    spanNum = 0;
    maxSpan = Long.MIN_VALUE;
    minSpan = Long.MAX_VALUE;
    totalSpan = BigInteger.valueOf(0);
    measurementVisitCount = new HashMap<>();
  }

  public void addItemStatistic(ItemStatistic itemStatistic) {
    totalSpan = totalSpan.add(itemStatistic.totalSpan);
    spanNum += itemStatistic.spanNum;
    averageSpan = totalSpan.divide(BigInteger.valueOf(spanNum)).longValueExact();
    if (minSpan > itemStatistic.minSpan) {
      minSpan = itemStatistic.minSpan;
    }
    if (maxSpan < itemStatistic.maxSpan) {
      maxSpan = itemStatistic.maxSpan;
    }

    for (Map.Entry<String, Map<String, Long>> deviceEntry :
        itemStatistic.measurementVisitCount.entrySet()) {
      if (!measurementVisitCount.containsKey(deviceEntry.getKey())) {
        measurementVisitCount.put(deviceEntry.getKey(), new HashMap<>(deviceEntry.getValue()));
      } else {
        Map<String, Long> measurementMap = measurementVisitCount.get(deviceEntry.getKey());
        for (Map.Entry<String, Long> measurementEntry : deviceEntry.getValue().entrySet()) {
          if (!measurementMap.containsKey(measurementEntry.getKey())) {
            measurementMap.put(measurementEntry.getKey(), measurementEntry.getValue());
          } else {
            measurementMap.replace(
                measurementEntry.getKey(),
                measurementMap.get(measurementEntry.getKey()) + measurementEntry.getValue());
          }
        }
      }
    }
  }

  // TODO: return if the this statistic is the same as other statistic
  public boolean isTheSame(ListStatistic other) {
    return false;
  }
}
