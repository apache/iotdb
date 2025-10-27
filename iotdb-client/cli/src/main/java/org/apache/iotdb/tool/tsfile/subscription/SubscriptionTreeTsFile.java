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
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;
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

public class SubscriptionTreeTsFile extends AbstractSubscriptionTsFile {

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
    properties.put(
        TopicConstant.PATH_KEY,
        StringUtils.isNotBlank(commonParam.getPath())
            ? commonParam.getPath()
            : commonParam.getPathFull());
    commonParam.getTreeSubs().createTopic(topicName, properties);
  }

  @Override
  public void doClean() throws Exception {
    List<SubscriptionTreePullConsumer> pullTreeConsumers = commonParam.getPullTreeConsumers();
    for (int i = commonParam.getStartIndex(); i < pullTreeConsumers.size(); i++) {
      SubscriptionTreePullConsumer consumer = pullTreeConsumers.get(i);
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
    for (Topic topic : commonParam.getTreeSubs().getTopics()) {
      try {
        commonParam.getTreeSubs().dropTopicIfExists(topic.getTopicName());
      } catch (Exception e) {

      }
    }
    commonParam.getTreeSubs().close();
  }

  @Override
  public void createConsumers(String groupId) {
    commonParam.setPullTreeConsumers(new ArrayList<>(commonParam.getConsumerCount()));
    for (int i = commonParam.getStartIndex(); i < commonParam.getConsumerCount(); i++) {
      commonParam
          .getPullTreeConsumers()
          .add(
              new SubscriptionTreePullConsumer.Builder()
                  .host(commonParam.getSrcHost())
                  .port(commonParam.getSrcPort())
                  .consumerId(Constants.CONSUMER_NAME_PREFIX + i)
                  .consumerGroupId(groupId)
                  .autoCommit(Constants.AUTO_COMMIT)
                  .fileSaveDir(commonParam.getTargetDir())
                  .buildPullConsumer());
    }
    commonParam
        .getPullTreeConsumers()
        .removeIf(
            consumer -> {
              try {
                consumer.open();
                return false;
              } catch (SubscriptionException e) {
                return true;
              }
            });
    commonParam.setConsumerCount(commonParam.getPullTreeConsumers().size());
  }

  @Override
  public void subscribe(String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    List<SubscriptionTreePullConsumer> pullTreeConsumers = commonParam.getPullTreeConsumers();
    for (int i = 0; i < pullTreeConsumers.size(); i++) {
      try {
        pullTreeConsumers.get(i).subscribe(topicName);
      } catch (Exception e) {
        e.printStackTrace(out);
      }
    }
  }

  @Override
  public void consumerPoll(ExecutorService executor, String topicName) {
    List<SubscriptionTreePullConsumer> pullTreeConsumers = commonParam.getPullTreeConsumers();
    for (int i = commonParam.getStartIndex(); i < pullTreeConsumers.size(); i++) {
      SubscriptionTreePullConsumer consumer = commonParam.getPullTreeConsumers().get(i);
      final String consumerGroupId = consumer.getConsumerGroupId();
      executor.submit(
          new Runnable() {
            @Override
            public void run() {
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
