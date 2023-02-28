package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.commons.pipe.PipePluginInformation;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
import org.apache.iotdb.db.mpp.common.header.DatasetHeader;
import org.apache.iotdb.db.mpp.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.mpp.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.mpp.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.mpp.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowPipePluginsTask implements IConfigTask {

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showPipePlugins();
  }

  public static void buildTsBlock(
      List<ByteBuffer> allPipePluginsInformation, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPipePluginsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    List<PipePluginInformation> pipePluginInformationList = new ArrayList<>();
    if (allPipePluginsInformation != null && !allPipePluginsInformation.isEmpty()) {
      for (ByteBuffer pipePluginInformationByteBuffer : allPipePluginsInformation) {
        PipePluginInformation pipePluginInformation =
            PipePluginInformation.deserialize(pipePluginInformationByteBuffer);
        pipePluginInformationList.add(pipePluginInformation);
      }
    }

    pipePluginInformationList.sort(Comparator.comparing(PipePluginInformation::getPluginName));

    for (PipePluginInformation pipePluginInformation : pipePluginInformationList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(Binary.valueOf(pipePluginInformation.getPluginName()));
      builder.getColumnBuilder(1).writeBinary(Binary.valueOf(pipePluginInformation.getClassName()));
      builder
          .getColumnBuilder(2)
          .writeBinary(Binary.valueOf(pipePluginInformation.getPluginType()));
      builder.declarePosition();
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowPipePluginsHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }
}
