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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.subscription;

import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowTopics;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowTopicsStatement;
import org.apache.iotdb.db.subscription.agent.SubscriptionAgent;
import org.apache.iotdb.db.subscription.tagfilter.TagFilterMatcher;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ShowTopicsTask implements IConfigTask {

  private final ShowTopicsStatement showTopicsStatement;

  public ShowTopicsTask(final ShowTopicsStatement showTopicsStatement) {
    this.showTopicsStatement = showTopicsStatement;
  }

  public ShowTopicsTask(final ShowTopics showTopics) {
    this.showTopicsStatement = new ShowTopicsStatement();
    this.showTopicsStatement.setTopicName(showTopics.getTopicName());
    this.showTopicsStatement.setTableModel(true);
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showTopics(showTopicsStatement);
  }

  public static void buildTSBlock(
      final List<TShowTopicInfo> topicInfoList,
      final boolean isTableModel,
      final SettableFuture<ConfigTaskResult> future) {
    buildTSBlock(
        topicInfoList,
        isTableModel,
        topicName -> SubscriptionAgent.broker().getTagFilterMatcher(topicName, true),
        future);
  }

  static void buildTSBlock(
      final List<TShowTopicInfo> topicInfoList,
      final boolean isTableModel,
      final Function<String, TagFilterMatcher> matcherProvider,
      final SettableFuture<ConfigTaskResult> future) {
    final List<ColumnHeader> columnHeaders =
        isTableModel
            ? ColumnHeaderConstant.showTableTopicColumnHeaders
            : ColumnHeaderConstant.showTopicColumnHeaders;
    final TsBlockBuilder builder =
        new TsBlockBuilder(
            columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList()));

    for (final TShowTopicInfo topicInfo : topicInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(topicInfo.getTopicName(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(1)
          .writeBinary(new Binary(topicInfo.getTopicAttributes(), TSFileConfig.STRING_CHARSET));
      if (isTableModel) {
        final TagFilterMatcher matcher;
        try {
          matcher = matcherProvider.apply(topicInfo.getTopicName());
        } catch (final RuntimeException e) {
          writeTagFilterDiagnostic(builder, TagFilterMatcher.failure(e));
          builder.declarePosition();
          continue;
        }
        writeTagFilterDiagnostic(builder, matcher);
      }
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            isTableModel
                ? DatasetHeaderFactory.getShowTableTopicHeader()
                : DatasetHeaderFactory.getShowTopicHeader()));
  }

  private static void writeTagFilterDiagnostic(
      final TsBlockBuilder builder, final TagFilterMatcher matcher) {
    builder
        .getColumnBuilder(2)
        .writeBinary(new Binary(matcher.getRuntimeStatus().name(), TSFileConfig.STRING_CHARSET));
    if (matcher.isFailure()) {
      builder
          .getColumnBuilder(3)
          .writeBinary(new Binary(matcher.getFailureMessage(), TSFileConfig.STRING_CHARSET));
    } else {
      builder.getColumnBuilder(3).appendNull();
    }
  }
}
