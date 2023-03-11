/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.mpp.plan.execution.config.metadata;

import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.db.mpp.common.header.ColumnHeader;
import org.apache.iotdb.db.mpp.common.header.ColumnHeaderConstant;
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
    final List<PipePluginMeta> pipePluginMetaList = new ArrayList<>();
    if (allPipePluginsInformation != null) {
      for (final ByteBuffer pipePluginInformationByteBuffer : allPipePluginsInformation) {
        pipePluginMetaList.add(PipePluginMeta.deserialize(pipePluginInformationByteBuffer));
      }
    }
    pipePluginMetaList.sort(Comparator.comparing(PipePluginMeta::getPluginName));

    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showPipePluginsColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final PipePluginMeta pipePluginMeta : pipePluginMetaList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(Binary.valueOf(pipePluginMeta.getPluginName()));
      builder.getColumnBuilder(1).writeBinary(Binary.valueOf(pipePluginMeta.getClassName()));
      builder.getColumnBuilder(2).writeBinary(Binary.valueOf(pipePluginMeta.getJarName()));
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getShowPipePluginsHeader()));
  }
}
