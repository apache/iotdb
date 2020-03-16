package org.apache.iotdb.cluster.query.groupby;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.cluster.common.TestUtils;
import org.apache.iotdb.cluster.query.BaseQueryTest;
import org.apache.iotdb.cluster.query.RemoteQueryContext;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.control.QueryResourceManager;
import org.apache.iotdb.db.query.factory.AggregateResultFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.junit.Test;

public class MergeGroupByExecutorTest extends BaseQueryTest {

  @Test
  public void testNoTimeFilter() throws QueryProcessException, IOException {
    Path path = new Path(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    Filter timeFilter = null;

    MergeGroupByExecutor groupByExecutor = new MergeGroupByExecutor(path, dataType, context,
        timeFilter, testMetaMember);
    AggregationType[] types = AggregationType.values();
    for (AggregationType type : types) {
      groupByExecutor.addAggregateResult(AggregateResultFactory.getAggrResultByType(type,
          TSDataType.DOUBLE));
    }

    Object[] answers;
    List<AggregateResult> aggregateResults;
    answers = new Object[] {5.0, 2.0, 10.0, 0.0, 4.0, 4.0, 0.0, 4.0, 0.0};
    aggregateResults = groupByExecutor.calcResult(0, 5);
    checkAggregations(aggregateResults, answers);

    answers = new Object[] {5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0};
    aggregateResults = groupByExecutor.calcResult(5, 10);
    checkAggregations(aggregateResults, answers);
  }

  @Test
  public void testTimeFilter() throws QueryProcessException, IOException {
    Path path = new Path(TestUtils.getTestSeries(0, 0));
    TSDataType dataType = TSDataType.DOUBLE;
    QueryContext context =
        new RemoteQueryContext(QueryResourceManager.getInstance().assignQueryId(true));
    Filter timeFilter = TimeFilter.gtEq(3);

    MergeGroupByExecutor groupByExecutor = new MergeGroupByExecutor(path, dataType, context,
        timeFilter, testMetaMember);
    AggregationType[] types = AggregationType.values();
    for (AggregationType type : types) {
      groupByExecutor.addAggregateResult(AggregateResultFactory.getAggrResultByType(type,
          TSDataType.DOUBLE));
    }

    Object[] answers;
    List<AggregateResult> aggregateResults;
    answers = new Object[] {2.0, 3.5, 7.0, 3.0, 4.0, 4.0, 3.0, 4.0, 3.0};
    aggregateResults = groupByExecutor.calcResult(0, 5);
    checkAggregations(aggregateResults, answers);

    answers = new Object[] {5.0, 7.0, 35.0, 5.0, 9.0, 9.0, 5.0, 9.0, 5.0};
    aggregateResults = groupByExecutor.calcResult(5, 10);
    checkAggregations(aggregateResults, answers);
  }
}
