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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata;

import org.apache.iotdb.commons.pipe.agent.plugin.builtin.BuiltinPipePlugin;
import org.apache.iotdb.commons.pipe.agent.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BytesUtils;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ShowPipePluginsTask implements IConfigTask {

  private static final Binary PIPE_PLUGIN_TYPE_BUILTIN = BytesUtils.valueOf("Builtin");
  private static final Binary PIPE_PLUGIN_TYPE_EXTERNAL = BytesUtils.valueOf("External");

  private static final Binary PIPE_JAR_NAME_EMPTY_FIELD = BytesUtils.valueOf("");

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
      // some pipe plugins are not for user's direct use
      if (BuiltinPipePlugin.SHOW_PIPE_PLUGINS_BLACKLIST.contains(pipePluginMeta.getPluginName())) {
        continue;
      }

      builder.getTimeColumnBuilder().writeLong(0L);
      builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(pipePluginMeta.getPluginName()));
      builder
          .getColumnBuilder(1)
          .writeBinary(
              pipePluginMeta.isBuiltin() ? PIPE_PLUGIN_TYPE_BUILTIN : PIPE_PLUGIN_TYPE_EXTERNAL);
      builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(pipePluginMeta.getClassName()));
      builder
          .getColumnBuilder(3)
          .writeBinary(
              pipePluginMeta.getJarName() == null
                  ? PIPE_JAR_NAME_EMPTY_FIELD
                  : BytesUtils.valueOf(pipePluginMeta.getJarName()));
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getShowPipePluginsHeader()));
  }
}
