package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class AlterTimeSeriesTask implements IConfigTask {
  private final String queryId;

  private final AlterTimeSeriesStatement alterTimeSeriesStatement;

  public AlterTimeSeriesTask(
      final String queryId, final AlterTimeSeriesStatement alterTimeSeriesStatement) {
    this.queryId = queryId;
    this.alterTimeSeriesStatement = alterTimeSeriesStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    if (alterTimeSeriesStatement.getDataType() != null) {
      return configTaskExecutor.alterTimeSeriesDataType(queryId, alterTimeSeriesStatement);
    } else {
      // not support
      throw new InterruptedException("AlterTimeSeriesTask is not support");
    }
  }
}
