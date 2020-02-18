package org.apache.iotdb.cluster.query;

import java.io.IOException;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.physical.crud.AggregationPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.AggregateEngineExecutor;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ClusterAggregateExecutor extends AggregateEngineExecutor {

  /**
   * constructor.
   *
   * @param aggregationPlan
   */
  public ClusterAggregateExecutor(AggregationPlan aggregationPlan) {
    super(aggregationPlan);
  }

  @Override
  public QueryDataSet executeWithoutValueFilter(QueryContext context)
      throws MetadataException, IOException, QueryProcessException, StorageEngineException {
    return super.executeWithoutValueFilter(context);
  }

  @Override
  public QueryDataSet executeWithValueFilter(QueryContext context)
      throws StorageEngineException, MetadataException, IOException {
    return super.executeWithValueFilter(context);
  }
}
