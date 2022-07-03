package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.confignode.rpc.thrift.TCreateSchemaTemplateReq;
import org.apache.iotdb.db.metadata.template.Template;
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

  public static TCreateSchemaTemplateReq constructTCreateSchemaTemplateReq(
      CreateSchemaTemplateStatement createSchemaTemplateStatement) {
    TCreateSchemaTemplateReq req = new TCreateSchemaTemplateReq();
    try {
      Template template = new Template(createSchemaTemplateStatement);
      req.setName(template.getName());
      req.setSerializedTemplate(Template.template2ByteBuffer(template));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return req;
  }
}
