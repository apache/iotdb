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

package org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ShowCreatePipeTask implements IConfigTask {

  private final String pipeName;
  private final String userName;

  public ShowCreatePipeTask(final String pipeName, final String userName) {
    this.pipeName = pipeName;
    this.userName = userName;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showCreatePipe(pipeName, userName);
  }

  public static void buildTsBlock(
      final PipeMeta pipeMeta, final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showCreatePipeColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    builder.getTimeColumnBuilder().writeLong(0L);
    builder
        .getColumnBuilder(0)
        .writeBinary(
            new Binary(pipeMeta.getStaticMeta().getPipeName(), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(getShowCreatePipeSQL(pipeMeta), TSFileConfig.STRING_CHARSET));
    builder.declarePosition();

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowCreatePipeColumnHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  public static String getShowCreatePipeSQL(final PipeMeta pipeMeta) {
    final StringBuilder builder =
        new StringBuilder("CREATE PIPE ")
            .append(ShowCreateTableTask.getIdentifier(pipeMeta.getStaticMeta().getPipeName()));

    appendAttributesClause(
        builder,
        "WITH SOURCE",
        sanitizeSourceAttributes(pipeMeta.getStaticMeta().getSourceParameters().getAttribute()));
    appendAttributesClause(
        builder,
        "WITH PROCESSOR",
        sanitizeCommonAttributes(pipeMeta.getStaticMeta().getProcessorParameters().getAttribute()));
    appendAttributesClause(
        builder,
        "WITH SINK",
        sanitizeSinkAttributes(pipeMeta.getStaticMeta().getSinkParameters().getAttribute()));

    return builder.toString();
  }

  private static Map<String, String> sanitizeCommonAttributes(
      final Map<String, String> attributes) {
    final Map<String, String> result = new TreeMap<>(attributes);
    result
        .entrySet()
        .removeIf(entry -> entry.getKey().startsWith(SystemConstant.SYSTEM_PREFIX_KEY));
    result.entrySet().removeIf(entry -> entry.getKey().startsWith(SystemConstant.AUDIT_PREFIX_KEY));
    return result;
  }

  private static Map<String, String> sanitizeSourceAttributes(final Map<String, String> source) {
    final Map<String, String> result = sanitizeCommonAttributes(source);
    result.remove(PipeSourceConstant.EXTRACTOR_IOTDB_USER_ID);
    result.remove(PipeSourceConstant.SOURCE_IOTDB_USER_ID);
    result.remove(PipeSourceConstant.EXTRACTOR_IOTDB_CLI_HOSTNAME);
    result.remove(PipeSourceConstant.SOURCE_IOTDB_CLI_HOSTNAME);
    if (!hasAnyKey(
        result,
        PipeSourceConstant.EXTRACTOR_IOTDB_PASSWORD_KEY,
        PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY)) {
      result.remove(PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY);
      result.remove(PipeSourceConstant.SOURCE_IOTDB_USER_KEY);
      result.remove(PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY);
      result.remove(PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY);
    }
    return result;
  }

  private static Map<String, String> sanitizeSinkAttributes(final Map<String, String> sink) {
    final Map<String, String> result = sanitizeCommonAttributes(sink);
    result.remove(PipeSinkConstant.CONNECTOR_IOTDB_USER_ID);
    result.remove(PipeSinkConstant.SINK_IOTDB_USER_ID);
    result.remove(PipeSinkConstant.CONNECTOR_IOTDB_CLI_HOSTNAME);
    result.remove(PipeSinkConstant.SINK_IOTDB_CLI_HOSTNAME);
    if (!hasAnyKey(
        result,
        PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY,
        PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY)) {
      result.remove(PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY);
      result.remove(PipeSinkConstant.SINK_IOTDB_USER_KEY);
      result.remove(PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY);
      result.remove(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY);
    }
    return result;
  }

  private static void appendAttributesClause(
      final StringBuilder builder, final String clause, final Map<String, String> attributes) {
    if (attributes.isEmpty()) {
      return;
    }
    final List<String> pairs = new ArrayList<>(attributes.size());
    for (final Map.Entry<String, String> entry : attributes.entrySet()) {
      pairs.add(
          ShowCreateTableTask.getString(entry.getKey())
              + "="
              + ShowCreateTableTask.getString(entry.getValue()));
    }
    builder.append(" ").append(clause).append(" (").append(String.join(",", pairs)).append(")");
  }

  private static boolean hasAnyKey(final Map<String, String> attributes, final String... keys) {
    for (final String key : keys) {
      if (attributes.containsKey(key)) {
        return true;
      }
    }
    return false;
  }
}
