package org.apache.iotdb.cluster.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateResult;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AggregationExecutor;
import org.apache.iotdb.db.query.reader.series.IReaderByTimestamp;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;

public class ClusterAggregateExecutor extends AggregationExecutor {

  private MetaGroupMember metaMember;

  /**
   * constructor.
   *
   * @param aggregationPlan
   */
  public ClusterAggregateExecutor(AggregationPlan aggregationPlan, MetaGroupMember metaMember) {
    super(aggregationPlan);
    this.metaMember = metaMember;
  }

  @Override
  protected List<AggregateResult> aggregateOneSeries(Entry<Path, List<Integer>> pathToAggrIndexes,
      Filter timeFilter, QueryContext context) throws StorageEngineException {
    Path seriesPath = pathToAggrIndexes.getKey();
    TSDataType tsDataType = dataTypes.get(pathToAggrIndexes.getValue().get(0));
    List<String> aggregationNames = new ArrayList<>();

    for (int i : pathToAggrIndexes.getValue()) {
      aggregationNames.add(aggregations.get(i));
    }
    return metaMember.getAggregateResult(seriesPath, aggregationNames, tsDataType, timeFilter, context);
  }

  @Override
  protected TimeGenerator getTimeGenerator(QueryContext context)
      throws StorageEngineException {
    return new ClusterTimeGenerator(expression, context, metaMember);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(Path path, TSDataType dataType,
      QueryContext context)
      throws StorageEngineException {
    return metaMember.getReaderByTimestamp(path, dataType, context);
  }
}
