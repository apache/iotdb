package org.apache.iotdb.db.queryengine.execution.operator.process.rowpattern;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class PatternAggregators {
  private PatternAggregator[][] values;
  // values[matchId][aggregationIndex] represents the `aggregationIndex` aggregate function in the
  // `matchId` match
  private final List<PatternAggregator> patternAggregators;

  public PatternAggregators(int capacity, List<PatternAggregator> patternAggregators) {
    this.values = new PatternAggregator[capacity][];
    this.patternAggregators = patternAggregators;
  }

  public PatternAggregator[] get(int key) {
    if (values[key] == null) {
      PatternAggregator[] aggregations = new PatternAggregator[patternAggregators.size()];
      for (int i = 0; i < patternAggregators.size(); i++) {
        aggregations[i] = patternAggregators.get(i);
        // no need to reset() when creating new MatchAggregation
        values[key] = aggregations;
      }
    }
    return values[key];
  }

  public void release(int key) {
    if (values[key] != null) {
      values[key] = null;
    }
  }

  public void copy(int parent, int child) {
    ensureCapacity(child);
    checkState(values[child] == null, "overriding aggregations for child thread");

    if (values[parent] != null) {
      PatternAggregator[] aggregations = new PatternAggregator[patternAggregators.size()];
      for (int i = 0; i < patternAggregators.size(); i++) {
        aggregations[i] = values[parent][i].copy();
        values[child] = aggregations;
      }
    }
  }

  private void ensureCapacity(int key) {
    if (key >= values.length) {
      values = Arrays.copyOf(values, Math.max(values.length * 2, key + 1));
    }
  }
}
