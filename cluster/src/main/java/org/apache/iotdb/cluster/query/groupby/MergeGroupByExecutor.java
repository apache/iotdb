package org.apache.iotdb.cluster.query.groupby;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.groupby.GroupByExecutor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;

public class MergeGroupByExecutor implements GroupByExecutor {

  private List<Pair<AggregateResult, Integer>> results = new ArrayList<>();
  private List<Integer> aggregationTypes = new ArrayList<>();
  private Path path;
  private TSDataType dataType;
  private QueryContext context;
  private Filter timeFilter;
  private MetaGroupMember metaGroupMember;

  private List<GroupByExecutor> groupByExecutors;

  MergeGroupByExecutor(Path path, TSDataType dataType, QueryContext context, Filter timeFilter,
      MetaGroupMember metaGroupMember) {
    this.path = path;
    this.dataType = dataType;
    this.context = context;
    this.timeFilter = timeFilter;
    this.metaGroupMember = metaGroupMember;
  }

  @Override
  public void addAggregateResult(AggregateResult aggrResult, int index) {
    results.add(new Pair<>(aggrResult, index));
    aggregationTypes.add(aggrResult.getAggregationType().ordinal());
  }

  @Override
  public void resetAggregateResults() {
    for (Pair<AggregateResult, Integer> result : results) {
      result.left.reset();
    }
  }

  @Override
  public List<Pair<AggregateResult, Integer>> calcResult(long curStartTime, long curEndTime)
      throws QueryProcessException, IOException {
    if (groupByExecutors == null) {
      initExecutors();
    }
    for (GroupByExecutor groupByExecutor : groupByExecutors) {
      groupByExecutor.resetAggregateResults();
      List<Pair<AggregateResult, Integer>> pairs = groupByExecutor
          .calcResult(curStartTime, curEndTime);
      for (int i = 0; i < pairs.size(); i++) {
        results.get(i).left.merge(pairs.get(i).left);
      }
    }
    return results;
  }

  private void initExecutors() throws QueryProcessException {
    try {
      groupByExecutors = metaGroupMember.getGroupByExecutors(path, dataType, context, timeFilter,
          aggregationTypes);
    } catch (StorageEngineException e) {
      throw new QueryProcessException(e);
    }
  }
}
