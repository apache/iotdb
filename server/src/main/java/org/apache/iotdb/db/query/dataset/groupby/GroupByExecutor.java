package org.apache.iotdb.db.query.dataset.groupby;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.tsfile.utils.Pair;

public interface GroupByExecutor {
  void addAggregateResult(AggregateResult aggrResult, int index);

  void resetAggregateResults();

  List<Pair<AggregateResult, Integer>> calcResult(long curStartTime, long curEndTime) throws IOException, QueryProcessException;
}
