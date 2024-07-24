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

package org.apache.iotdb;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.SubscriptionSession;
import org.apache.iotdb.session.subscription.consumer.ConsumeResult;
import org.apache.iotdb.session.subscription.consumer.SubscriptionPushConsumer;
import org.apache.iotdb.session.subscription.payload.SubscriptionTsFileHandler;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.locks.LockSupport;

public class PushConsumeLatency {
  private static final String HOST = "127.0.0.1";
  private static final int SRC_PORT = 6667;
  private static final int DIST_PORT = 6668;

  public static void main(String[] args) throws Exception {
    final String topicName = "topic1";
    try (final SubscriptionSession session = new SubscriptionSession(HOST, SRC_PORT)) {
      session.open();
      final Properties properties = new Properties();
      // properties.setProperty(TopicConstant.PATH_KEY, "root.**.d_0.*");
      properties.setProperty(TopicConstant.FORMAT_KEY, TopicConstant.FORMAT_TS_FILE_HANDLER_VALUE);
      properties.setProperty(TopicConstant.MODE_KEY, TopicConstant.MODE_SNAPSHOT_VALUE);
      session.createTopic(topicName, properties);
    }
    final Session session = new Session(HOST, DIST_PORT);
    session.open();
    final long[] currentTime = {System.currentTimeMillis()};
    final SubscriptionPushConsumer consumer =
        new SubscriptionPushConsumer.Builder()
            .host(HOST)
            .port(SRC_PORT)
            .consumerId("push_consumer")
            .consumeListener(
                message -> {
                  final SubscriptionTsFileHandler tsFileHandler = message.getTsFileHandler();
                  try {
                    session.executeNonQueryStatement(
                        String.format("load '%s'", tsFileHandler.getFile().getAbsolutePath()));
                  } catch (IoTDBConnectionException e) {
                    throw new RuntimeException(e);
                  } catch (StatementExecutionException e) {
                    throw new RuntimeException(e);
                  }
                  //                  for (final Iterator<Tablet> it =
                  //                          message.getSessionDataSetsHandler().tabletIterator();
                  //                      it.hasNext(); ) {
                  //                    it.next();
                  //                  }
                  System.out.println(
                      String.format(String.valueOf(new Date()))
                          + " | "
                          + (System.currentTimeMillis() - currentTime[0]));
                  currentTime[0] = System.currentTimeMillis();
                  return ConsumeResult.SUCCESS;
                })
            .consumerGroupId("dataset")
            .buildPushConsumer();
    consumer.open();
    consumer.subscribe(topicName);
    while (!consumer.allSnapshotTopicMessagesHaveBeenConsumed()) {
      LockSupport.parkNanos(1_000_000_000L);
    }
    //    final long endTime = currentTime[0] + Duration.ofSeconds(1200).toMillis();
    //    while (System.currentTimeMillis() < endTime) {
    //      LockSupport.parkNanos(1_000_000_000L);
    //    }
    consumer.close();
    session.close();
  }
}
