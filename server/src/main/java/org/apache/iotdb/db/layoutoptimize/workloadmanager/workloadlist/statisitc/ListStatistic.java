package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist.statisitc;

import java.math.BigDecimal;
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
  private static double visitCountDiffRateThreshold = 0.2d;
  private static double visitMeasurementDiffRateThreshold = 0.2d;
  private static double spanChangeRateThreshold = 0.5d;

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

  public boolean isTheSame(ListStatistic other) {
    double countDiffRate = getVisitCountDiff(measurementVisitCount, other.measurementVisitCount);
    double measurementDiffRate =
        getVisitMeasurementDiff(measurementVisitCount, other.measurementVisitCount);
    double spanDiffRate = 0;
    if (averageSpan * other.averageSpan != 0) {
      spanDiffRate = (double) Math.abs((averageSpan - other.averageSpan)) / (double) averageSpan;
    } else if (averageSpan == other.averageSpan) {
      spanDiffRate = 0;
    } else {
      spanDiffRate = 1;
    }
    return countDiffRate < visitCountDiffRateThreshold
        && measurementDiffRate < visitMeasurementDiffRateThreshold
        && spanDiffRate < spanChangeRateThreshold;
  }

  private double getVisitCountDiff(
      Map<String, Map<String, Long>> map1, Map<String, Map<String, Long>> map2) {
    if (map1.size() * map2.size() == 0 && map1.size() + map2.size() != 0) {
      return 0;
    }
    BigDecimal totalDiff = BigDecimal.valueOf(0);
    BigDecimal totalVisit = BigDecimal.valueOf(0);
    for (String device : map1.keySet()) {
      if (map2.containsKey(device)) {
        Map<String, Long> measurementMap1 = map1.get(device);
        Map<String, Long> measurementMap2 = map2.get(device);
        for (String measurement : measurementMap1.keySet()) {
          totalVisit = totalVisit.add(BigDecimal.valueOf(measurementMap1.get(device)));
          if (measurementMap2.containsKey(measurement)) {
            BigDecimal diff =
                BigDecimal.valueOf(
                    measurementMap1.get(measurement) - measurementMap2.get(measurement));
            totalDiff = totalDiff.add(diff.multiply(diff));
          }
        }
      }
    }
    return totalDiff.divide(totalVisit.multiply(totalVisit)).doubleValue();
  }

  private double getVisitMeasurementDiff(
      Map<String, Map<String, Long>> map1, Map<String, Map<String, Long>> map2) {
    double diffCount = 0;
    double totalNum = 0;
    for (String deviceInMap1 : map1.keySet()) {
      totalNum += map1.get(deviceInMap1).keySet().size();
      if (!map2.containsKey(deviceInMap1)) {
        diffCount += map1.get(deviceInMap1).keySet().size();
      } else {
        Map<String, Long> measurementMap1 = map1.get(deviceInMap1);
        Map<String, Long> measurementMap2 = map2.get(deviceInMap1);
        for (String measurement : measurementMap1.keySet()) {
          if (!measurementMap2.containsKey(measurement)) {
            diffCount += 1;
          }
        }
      }
    }

    for (String deviceInMap2 : map2.keySet()) {
      if (!map1.containsKey(deviceInMap2)) {
        diffCount += map2.get(deviceInMap2).keySet().size();
        totalNum += map2.get(deviceInMap2).keySet().size();
      } else {
        Map<String, Long> measurementMap1 = map1.get(deviceInMap2);
        Map<String, Long> measurementMap2 = map2.get(deviceInMap2);
        for (String measurement : measurementMap2.keySet()) {
          if (!measurementMap1.containsKey(measurement)) {
            diffCount += 1;
            totalNum += 1;
          }
        }
      }
    }
    return diffCount / totalNum;
  }
}
