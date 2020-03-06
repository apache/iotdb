package org.apache.iotdb.cluster.query.groupby;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Test;

public class RemoteGroupByExecutorTest extends BaseQueryTest {

  @Test
  public void testNoTimeFilter() throws QueryProcessException, IOException, StorageEngineException {
    Path path = new Path(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    Filter timeFilter = null;
    List<Integer> aggregationTypes = new ArrayList<>();
    for (int i = 0; i < AggregationType.values().length; i++) {
      aggregationTypes.add(i);
    }

    List<GroupByExecutor> groupByExecutors = testMetaMember
        .getGroupByExecutors(path, dataType, context, timeFilter, aggregationTypes);

    for (int i = 0; i < groupByExecutors.size(); i++) {
      GroupByExecutor groupByExecutor = groupByExecutors.get(i);
      Object[] answers;
      if (i == 1) {
        // a series is only managed by one group
        List<Pair<AggregateResult, Integer>> aggregateResults;
        answers = new Object[]{5.0, 2.0, 10.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0};
        aggregateResults = groupByExecutor.calcResult(0, 5);
        checkAggregations(aggregateResults, answers);

        groupByExecutor.resetAggregateResults();
        answers = new Object[]{5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0};
        aggregateResults = groupByExecutor.calcResult(5, 10);
        checkAggregations(aggregateResults, answers);
      } else {
        List<Pair<AggregateResult, Integer>> aggregateResults;
        answers = new Object[]{0.0, null, 0.0, null, null, null, null, null, null};
        aggregateResults = groupByExecutor.calcResult(0, 5);
        checkAggregations(aggregateResults, answers);

        groupByExecutor.resetAggregateResults();
        answers = new Object[]{0.0, null, 0.0, null, null, null, null, null, null};
        aggregateResults = groupByExecutor.calcResult(5, 10);
        checkAggregations(aggregateResults, answers);
      }
    }
  }

  @Test
  public void testTimeFilter() throws QueryProcessException, IOException, StorageEngineException {
    Path path = new Path(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    Filter timeFilter = TimeFilter.gtEq(3);
    List<Integer> aggregationTypes = new ArrayList<>();
    for (int i = 0; i < AggregationType.values().length; i++) {
      aggregationTypes.add(i);
    }

    List<GroupByExecutor> groupByExecutors = testMetaMember
        .getGroupByExecutors(path, dataType, context, timeFilter, aggregationTypes);

    for (int i = 0; i < groupByExecutors.size(); i++) {
      GroupByExecutor groupByExecutor = groupByExecutors.get(i);
      Object[] answers;
      if (i == 1) {
        // a series is only managed by one group
        List<Pair<AggregateResult, Integer>> aggregateResults;
        answers = new Object[]{2.0, 3.5, 7.0, 3.0, 4.0, 4.0, 3.0, 4.0, 3.0};
        aggregateResults = groupByExecutor.calcResult(0, 5);
        checkAggregations(aggregateResults, answers);

        groupByExecutor.resetAggregateResults();
        answers = new Object[]{5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0};
        aggregateResults = groupByExecutor.calcResult(5, 10);
        checkAggregations(aggregateResults, answers);
      } else {
        List<Pair<AggregateResult, Integer>> aggregateResults;
        answers = new Object[]{0.0, null, 0.0, null, null, null, null, null, null};
        aggregateResults = groupByExecutor.calcResult(0, 5);
        checkAggregations(aggregateResults, answers);

        groupByExecutor.resetAggregateResults();
        answers = new Object[]{0.0, null, 0.0, null, null, null, null, null, null};
        aggregateResults = groupByExecutor.calcResult(5, 10);
        checkAggregations(aggregateResults, answers);
      }
    }
  }
}
