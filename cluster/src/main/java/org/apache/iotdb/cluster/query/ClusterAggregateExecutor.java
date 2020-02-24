package org.apache.iotdb.cluster.query;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.cluster.query.reader.ClusterTimeGenerator;
import org.apache.iotdb.cluster.server.member.MetaGroupMember;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.aggregation.AggregateFunction;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.IReaderByTimestamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.read.query.timegenerator.TimeGenerator;
import org.apache.iotdb.tsfile.read.reader.IAggregateReader;

public class ClusterAggregateExecutor extends AggregateEngineExecutor {

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
  protected void initReaders(List<AggregateFunction> aggregateFunctions,
      List<IAggregateReader> readersOfSequenceData, List<IPointReader> readersOfUnSequenceData,
      Filter timeFilter, QueryContext context) throws IOException, StorageEngineException {
    metaMember.getAggregateReader(selectedSeries, timeFilter, context, readersOfSequenceData,
        readersOfUnSequenceData, aggregateFunctions, dataTypes);
  }

  @Override
  protected TimeGenerator getTimeGenerator(IExpression expression, QueryContext context)
      throws StorageEngineException {
    return new ClusterTimeGenerator(expression, context, metaMember);
  }

  @Override
  protected IReaderByTimestamp getReaderByTime(Path path, QueryContext context)
      throws IOException, StorageEngineException {
    return metaMember.getReaderByTimestamp(path, context);
  }
}
