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

import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.execution.config.metadata.relational.ShowCreateTableTask;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowCreateTopic;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConstant;

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

public class ShowCreateTopicTask implements IConfigTask {

  private final String topicName;

  public ShowCreateTopicTask(final ShowCreateTopic showCreateTopic) {
    this.topicName = showCreateTopic.getTopicName();
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(final IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showCreateTopic(topicName);
  }

  public static void buildTsBlock(
      final TopicMeta topicMeta, final SettableFuture<ConfigTaskResult> future) {
    final List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showCreateTopicColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    builder.getTimeColumnBuilder().writeLong(0L);
    builder
        .getColumnBuilder(0)
        .writeBinary(new Binary(topicMeta.getTopicName(), TSFileConfig.STRING_CHARSET));
    builder
        .getColumnBuilder(1)
        .writeBinary(new Binary(getShowCreateTopicSQL(topicMeta), TSFileConfig.STRING_CHARSET));
    builder.declarePosition();

    final DatasetHeader datasetHeader = DatasetHeaderFactory.getShowCreateTopicColumnHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  public static String getShowCreateTopicSQL(final TopicMeta topicMeta) {
    final StringBuilder builder =
        new StringBuilder("CREATE TOPIC ")
            .append(ShowCreateTableTask.getIdentifier(topicMeta.getTopicName()));

    final Map<String, String> sanitizedAttributes =
        sanitizeTopicAttributes(topicMeta.getConfig().getAttribute());
    if (!sanitizedAttributes.isEmpty()) {
      final List<String> pairs = new ArrayList<>(sanitizedAttributes.size());
      for (final Map.Entry<String, String> entry : sanitizedAttributes.entrySet()) {
        pairs.add(
            ShowCreateTableTask.getString(entry.getKey())
                + "="
                + ShowCreateTableTask.getString(entry.getValue()));
      }
      builder.append(" WITH (").append(String.join(",", pairs)).append(")");
    }

    return builder.toString();
  }

  private static Map<String, String> sanitizeTopicAttributes(final Map<String, String> attributes) {
    final Map<String, String> result = new TreeMap<>(attributes);
    result
        .entrySet()
        .removeIf(entry -> entry.getKey().startsWith(SystemConstant.SYSTEM_PREFIX_KEY));
    result.entrySet().removeIf(entry -> entry.getKey().startsWith(SystemConstant.AUDIT_PREFIX_KEY));
    result.remove(ConsumerConstant.USERNAME_KEY);
    result.remove(ConsumerConstant.PASSWORD_KEY);
    result.remove(ConsumerConstant.ENCRYPTED_PASSWORD_KEY);
    return result;
  }
}
