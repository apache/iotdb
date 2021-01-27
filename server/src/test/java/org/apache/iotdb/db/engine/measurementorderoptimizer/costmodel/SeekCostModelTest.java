package org.apache.iotdb.db.engine.measurementorderoptimizer.costmodel;

import org.apache.iotdb.db.engine.measurementorderoptimizer.SeekCostModel;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.AggregationQueryRecord;
import org.apache.iotdb.db.query.workloadmanager.queryrecord.QueryRecord;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class SeekCostModelTest {
  @Test
  public void seekCostTest() {
    List<String> measurements = new ArrayList<>();
    List<Long> chunkSizes = new ArrayList<>();
    List<String> operations = new ArrayList<>(Arrays.asList(new String[] {"AVG", "SUM", "COUNT", "LAST"}));
    for(int i = 0; i < 20; i++) {
      measurements.add("s" + i);
      chunkSizes.add((long)(i * 1024));
    }
    List<QueryRecord> queryRecords = new ArrayList<>();
    for(int i = 0; i < 5; i++) {
      queryRecords.add(new AggregationQueryRecord("D" + i, randomSample(measurements, i * 2 + 1), randomSample(operations, i * 2 + 1)));
    }
    float cost = SeekCostModel.approximate(queryRecords, measurements, chunkSizes);
    System.out.println(cost);
  }

  private List<String> randomSample(List<String> options, int num) {
    Random r = new Random();
    List<String> result = new ArrayList<>();
    for(int i = 0; i < num; ++i) {
      int idx = r.nextInt();
      idx = idx < 0 ? -idx : idx;
      idx %= options.size();
      result.add(options.get(idx));
    }
    return result;
  }
}
