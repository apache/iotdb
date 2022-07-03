package org.apache.iotdb.db.mpp.plan.execution.config;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTemplatesResp;
import org.apache.iotdb.db.metadata.template.Template;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.HeaderConstant;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.mpp.plan.statement.metadata.template.ShowSchemaTemplateStatement;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

/**
 * @author chenhuangyun
 * @date 2022/6/30
 **/
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

    public static void buildTSBlock(TGetAllTemplatesResp resp,
        SettableFuture<ConfigTaskResult> future) {
        TsBlockBuilder builder =
            new TsBlockBuilder(HeaderConstant.showSchemaTemplate.getRespDataTypes());
        List<ByteBuffer> list = resp.getTemplateList();
        Optional<List<ByteBuffer>> optional = Optional.ofNullable(list);
        optional.orElse(new ArrayList<>()).stream().forEach(
            item->{
                try {
                    Template template = Template.byteBuffer2Template(item);
                    builder.getTimeColumnBuilder().writeLong(0L);
                    builder.getColumnBuilder(0).writeBinary(new Binary(template.getName()));
                    builder.declarePosition();
                }catch (Exception e) {

                }
            }
        );
        DatasetHeader datasetHeader = HeaderConstant.showNodesInSchemaTemplate;
        future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
    }
}
