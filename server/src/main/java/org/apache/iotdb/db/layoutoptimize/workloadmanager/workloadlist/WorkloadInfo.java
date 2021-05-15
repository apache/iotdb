package org.apache.iotdb.db.layoutoptimize.workloadmanager.workloadlist;

import org.apache.iotdb.db.layoutoptimize.workloadmanager.queryrecord.QueryRecord;
import org.apache.iotdb.db.metadata.PartialPath;

import java.util.*;

public class WorkloadInfo {
  String deviceId;
  Map<String, Long> measurementVisitCount;
  Map<Long, Long> spanVisitCount;
  private long measurementVisitSum;
  private long spanVisitSum;

  public WorkloadInfo(String deviceId) {
    this.deviceId = deviceId;
    measurementVisitSum = 0L;
    spanVisitSum = 0L;
    measurementVisitCount = new HashMap<>();
    spanVisitCount = new HashMap<>();
  }

  public void addVisitedMeasurement(String measurement) {
    measurementVisitSum += 1;
    if (!measurementVisitCount.containsKey(measurement)) {
      measurementVisitCount.put(measurement, 1L);
    } else {
      measurementVisitCount.replace(measurement, measurementVisitCount.get(measurement) + 1L);
    }
  }

  public void addVisitedSpan(long span, long visitCount) {
    spanVisitSum += 1;
    if (!spanVisitCount.containsKey(span)) {
      spanVisitCount.put(span, visitCount);
    } else {
      spanVisitCount.replace(span, spanVisitCount.get(span) + visitCount);
    }
  }

  /**
   * Sample a query record instance according to the workload info
   *
   * @return a instance of QueryRecord
   */
  public QueryRecord sample() {
    Set<String> visitMeasurement = new HashSet<>();
    Random random = new Random();
    int visitSize = random.nextInt(measurementVisitCount.size()) + 1;
    if (visitSize > measurementVisitCount.size()) {
      visitSize -= 1;
    }
    long randNum = 0L;
    long span = 0L;
    for (int i = 0; i < visitSize; i++) {
      randNum = Math.abs(random.nextLong()) % measurementVisitSum;
      for (Map.Entry<String, Long> measurementEntry : measurementVisitCount.entrySet()) {
        if (randNum - measurementEntry.getValue() <= 0L) {
          visitMeasurement.add(measurementEntry.getKey());
          break;
        } else {
          randNum -= measurementEntry.getValue();
        }
      }
    }

    randNum = Math.abs(random.nextLong()) % spanVisitSum;
    for (Map.Entry<Long, Long> spanEntry : spanVisitCount.entrySet()) {
      if (randNum - spanEntry.getValue() <= 0L) {
        span = spanEntry.getKey();
        break;
      } else {
        randNum -= spanEntry.getValue();
      }
    }
    return new QueryRecord(
        PartialPath.fromStringList(Arrays.asList(new String[] {deviceId})).get(0),
        new ArrayList<>(visitMeasurement),
        span);
  }
}
