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
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TSubscriptionProgressInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowSubscriptions;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.subscription.ShowSubscriptionsStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.List;
import java.util.stream.Collectors;

public class ShowSubscriptionsTask implements IConfigTask {

  private final ShowSubscriptionsStatement showSubscriptionsStatement;

  public ShowSubscriptionsTask(final ShowSubscriptionsStatement showSubscriptionsStatement) {
    this.showSubscriptionsStatement = showSubscriptionsStatement;
  }

  public ShowSubscriptionsTask(final ShowSubscriptions showSubscriptions) {
    this.showSubscriptionsStatement = new ShowSubscriptionsStatement();
    this.showSubscriptionsStatement.setTopicName(showSubscriptions.getTopicName());
    this.showSubscriptionsStatement.setDetails(showSubscriptions.isDetails());
    this.showSubscriptionsStatement.setTableModel(true);
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showSubscriptions(showSubscriptionsStatement);
  }

  public static void buildTSBlock(
      final List<TShowSubscriptionInfo> subscriptionInfoList,
      final SettableFuture<ConfigTaskResult> future) {
    final TsBlockBuilder builder =
        new TsBlockBuilder(
            ColumnHeaderConstant.showSubscriptionColumnHeaders.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));

    for (final TShowSubscriptionInfo tSubscriptionInfo : subscriptionInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      final StringBuilder subscriptionId =
          new StringBuilder(
              tSubscriptionInfo.getTopicName() + "_" + tSubscriptionInfo.getConsumerGroupId());
      if (tSubscriptionInfo.getCreationTime() != 0) {
        subscriptionId.append("_").append(tSubscriptionInfo.getCreationTime());
      }
      builder
          .getColumnBuilder(0)
          .writeBinary(new Binary(subscriptionId.toString(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(1)
          .writeBinary(new Binary(tSubscriptionInfo.getTopicName(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(2)
          .writeBinary(
              new Binary(tSubscriptionInfo.getConsumerGroupId(), TSFileConfig.STRING_CHARSET));
      builder
          .getColumnBuilder(3)
          .writeBinary(
              new Binary(
                  tSubscriptionInfo.getConsumerIds().toString(), TSFileConfig.STRING_CHARSET));
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getShowSubscriptionHeader()));
  }

  public static void buildDetailsTSBlock(
      final List<TSubscriptionProgressInfo> progressInfoList,
      final SettableFuture<ConfigTaskResult> future) {
    final TsBlockBuilder builder =
        new TsBlockBuilder(
            ColumnHeaderConstant.showSubscriptionDetailsColumnHeaders.stream()
                .map(ColumnHeader::getColumnType)
                .collect(Collectors.toList()));

    for (final TSubscriptionProgressInfo progressInfo : progressInfoList) {
      builder.getTimeColumnBuilder().writeLong(0L);
      writeText(builder, 0, progressInfo.getTopicName() + "_" + progressInfo.getConsumerGroupId());
      writeText(builder, 1, progressInfo.getTopicName());
      writeText(builder, 2, progressInfo.getConsumerGroupId());
      builder.getColumnBuilder(3).writeInt(progressInfo.getDataNodeId());
      writeText(builder, 4, progressInfo.getRegionId());
      writeText(builder, 5, progressInfo.getStatus());
      builder.getColumnBuilder(6).writeBoolean(progressInfo.isActive());
      builder.getColumnBuilder(7).writeBoolean(progressInfo.isInitialized());
      builder.getColumnBuilder(8).writeLong(progressInfo.getRemainingEventCount());
      builder.getColumnBuilder(9).writeLong(progressInfo.getRawWalGap());
      builder.getColumnBuilder(10).writeLong(progressInfo.getApproximateLag());
      builder.getColumnBuilder(11).writeLong(progressInfo.getInFlightEventCount());
      builder.getColumnBuilder(12).writeLong(progressInfo.getPrefetchedEventCount());
      builder.getColumnBuilder(13).writeLong(progressInfo.getPendingEventCount());
      builder.getColumnBuilder(14).writeLong(progressInfo.getCurrentWalSearchIndex());
      builder.getColumnBuilder(15).writeLong(progressInfo.getNextReadSearchIndex());
      builder.getColumnBuilder(16).writeLong(progressInfo.getLastProgressTimeMs());
      builder.getColumnBuilder(17).writeLong(progressInfo.getLastPollTimeMs());
      writeText(builder, 18, progressInfo.getLastConsumerId());
      builder.getColumnBuilder(19).writeLong(progressInfo.getSeekGeneration());
      builder.declarePosition();
    }

    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS,
            builder.build(),
            DatasetHeaderFactory.getShowSubscriptionDetailsHeader()));
  }

  private static void writeText(
      final TsBlockBuilder builder, final int columnIndex, final String value) {
    builder
        .getColumnBuilder(columnIndex)
        .writeBinary(new Binary(value == null ? "" : value, TSFileConfig.STRING_CHARSET));
  }
}
