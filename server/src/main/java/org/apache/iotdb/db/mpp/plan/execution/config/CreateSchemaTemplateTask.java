package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.CreateSchemaTemplateStatement;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 */
public class CreateSchemaTemplateTask implements IConfigTask {

  private final CreateSchemaTemplateStatement createSchemaTemplateStatement;

  public CreateSchemaTemplateTask(CreateSchemaTemplateStatement createSchemaTemplateStatement) {
    this.createSchemaTemplateStatement = createSchemaTemplateStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.createSchemaTemplate(this.createSchemaTemplateStatement);
  }
}
