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

package org.apache.iotdb.tool.tsfile.subscription;

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.consumer.ISubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.payload.SubscriptionMessage;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;
import org.apache.iotdb.tool.common.Constants;

import org.apache.tsfile.external.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;

import static java.lang.System.out;

public class SubscriptionTableTsFile extends AbstractSubscriptionTsFile {

  @Override
  public void createTopics(String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    Properties properties = new Properties();
    properties.put(TopicConstant.MODE_KEY, Constants.MODE);
    properties.put(TopicConstant.FORMAT_KEY, Constants.HANDLER);
    properties.put(TopicConstant.STRICT_KEY, Constants.STRICT);
    properties.put(TopicConstant.LOOSE_RANGE_KEY, Constants.LOOSE_RANGE);
    if (StringUtils.isNotBlank(commonParam.getStartTime()))
      properties.put(TopicConstant.START_TIME_KEY, commonParam.getStartTime());
    if (StringUtils.isNotBlank(commonParam.getEndTime()))
      properties.put(TopicConstant.END_TIME_KEY, commonParam.getEndTime());
    if (StringUtils.isNotBlank(commonParam.getDatabase())
        || StringUtils.isNotBlank(commonParam.getTable())) {
      properties.put(TopicConstant.DATABASE_KEY, commonParam.getDatabase());
      properties.put(TopicConstant.TABLE_KEY, commonParam.getTable());
    }
    commonParam.getTableSubs().createTopic(topicName, properties);
  }

  @Override
  public void doClean() throws Exception {
    List<ISubscriptionTablePullConsumer> pullTableConsumers = commonParam.getPullTableConsumers();
    for (int i = commonParam.getStartIndex(); i < pullTableConsumers.size(); i++) {
      SubscriptionTablePullConsumer consumer =
          (SubscriptionTablePullConsumer) pullTableConsumers.get(i);
      String path =
          commonParam.getTargetDir()
              + File.separator
              + consumer.getConsumerGroupId()
              + File.separator
              + consumer.getConsumerId();
      File file = new File(path);
      if (file.exists()) {
        FileUtils.deleteFileOrDirectory(file);
      }
    }
    for (Topic topic : CommonParam.getTableSubs().getTopics()) {
      try {
        commonParam.getTableSubs().dropTopicIfExists(topic.getTopicName());
      } catch (Exception e) {

      }
    }
    commonParam.getTableSubs().close();
  }

  @Override
  public void createConsumers(String groupId) {
    commonParam.setPullTableConsumers(new ArrayList<>(CommonParam.getConsumerCount()));
    for (int i = commonParam.getStartIndex(); i < commonParam.getConsumerCount(); i++) {
      commonParam
          .getPullTableConsumers()
          .add(
              new SubscriptionTablePullConsumerBuilder()
                  .host(commonParam.getSrcHost())
                  .port(commonParam.getSrcPort())
                  .consumerId(Constants.CONSUMER_NAME_PREFIX + i)
                  .consumerGroupId(groupId)
                  .autoCommit(Constants.AUTO_COMMIT)
                  .fileSaveDir(commonParam.getTargetDir())
                  .build());
    }
    commonParam
        .getPullTableConsumers()
        .removeIf(
            consumer -> {
              try {
                consumer.open();
                return false;
              } catch (SubscriptionException e) {
                return true;
              }
            });
    commonParam.setConsumerCount(commonParam.getPullTableConsumers().size());
  }

  @Override
  public void subscribe(String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    List<ISubscriptionTablePullConsumer> pullTableConsumers = commonParam.getPullTableConsumers();
    for (int i = 0; i < pullTableConsumers.size(); i++) {
      try {
        pullTableConsumers.get(i).subscribe(topicName);
      } catch (Exception e) {
        e.printStackTrace(out);
      }
    }
  }

  @Override
  public void consumerPoll(ExecutorService executor, String topicName) {
    List<ISubscriptionTablePullConsumer> pullTableConsumers = commonParam.getPullTableConsumers();
    for (int i = commonParam.getStartIndex(); i < pullTableConsumers.size(); i++) {
      SubscriptionTablePullConsumer consumer =
          (SubscriptionTablePullConsumer) pullTableConsumers.get(i);
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
              final String consumerGroupId = consumer.getConsumerGroupId();
              while (!consumer.allTopicMessagesHaveBeenConsumed()) {
                try {
                  for (final SubscriptionMessage message :
                      consumer.poll(Constants.POLL_MESSAGE_TIMEOUT)) {
                    final SubscriptionTsFileHandler handler = message.getTsFileHandler();
                    ioTPrinter.println(handler.getFile().getName());
                    try {
                      handler.moveFile(
                          Paths.get(
                              commonParam.getTargetDir() + File.separator + consumerGroupId,
                              handler.getPath().getFileName().toString()));
                    } catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                    commonParam.getCountFile().incrementAndGet();
                    consumer.commitSync(message);
                  }
                } catch (Exception e) {
                  e.printStackTrace(System.out);
                }
              }
              consumer.unsubscribe(topicName);
            }
          });
    }
  }
}
