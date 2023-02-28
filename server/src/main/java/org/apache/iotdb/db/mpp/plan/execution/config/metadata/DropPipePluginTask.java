package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.DropPipePluginStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class DropPipePluginTask implements IConfigTask {

  private final String pluginName;

  public DropPipePluginTask(DropPipePluginStatement dropPipePluginStatement) {
    this.pluginName = dropPipePluginStatement.getPluginName();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.dropPipePlugin(pluginName);
  }
}
