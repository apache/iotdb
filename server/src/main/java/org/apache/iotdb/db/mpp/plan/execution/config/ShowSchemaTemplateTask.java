package org.apache.iotdb.db.mpp.plan.execution.config;

import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 */
public class ShowSchemaTemplateTask implements IConfigTask {

  private final ShowSchemaTemplateStatement showSchemaTemplateStatement;

  public ShowSchemaTemplateTask(ShowSchemaTemplateStatement showSchemaTemplateStatement) {
    this.showSchemaTemplateStatement = showSchemaTemplateStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showSchemaTemplate(this.showSchemaTemplateStatement);
  }

  public static void buildTSBlock(
      List<Template> templateList, SettableFuture<ConfigTaskResult> future) {
    TsBlockBuilder builder =
        new TsBlockBuilder(HeaderConstant.showSchemaTemplate.getRespDataTypes());
    Optional<List<Template>> optional = Optional.ofNullable(templateList);
    optional.orElse(new ArrayList<>()).stream()
        .forEach(
            template -> {
              builder.getTimeColumnBuilder().writeLong(0L);
              builder.getColumnBuilder(0).writeBinary(new Binary(template.getName()));
              builder.declarePosition();
            });
    DatasetHeader datasetHeader = HeaderConstant.showSchemaTemplate;
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
