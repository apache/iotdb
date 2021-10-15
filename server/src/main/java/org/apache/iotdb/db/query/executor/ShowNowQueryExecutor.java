package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.sys.ShowNowPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.dataset.ShowNowDataSet;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

public class ShowNowQueryExecutor {

  public ShowNowQueryExecutor() {}

  public ShowNowQueryExecutor(ShowNowPlan showNowPlan) {}

  public QueryDataSet execute(QueryContext context, ShowNowPlan showNowPlan)
      throws MetadataException {
    return new ShowNowDataSet(showNowPlan, context);
  }
}
