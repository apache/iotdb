package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.CreatePipePluginStatement;

import com.google.common.util.concurrent.ListenableFuture;

public class CreatePipePluginTask implements IConfigTask {

  private final CreatePipePluginStatement createPipePluginStatement;

  public CreatePipePluginTask(CreatePipePluginStatement createPipePluginStatement) {
    this.createPipePluginStatement = createPipePluginStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createPipePlugin(createPipePluginStatement);
  }
}
