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

package org.apache.iotdb.subscription.consumer;

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.subscription.api.SubscriptionConfiguration;
import org.apache.iotdb.subscription.api.consumer.IConsumer;
import org.apache.iotdb.subscription.api.dataset.ISubscriptionDataSet;
import org.apache.iotdb.subscription.api.exception.SubscriptionException;
import org.apache.iotdb.subscription.dataset.SubscriptionDataSet;
import org.apache.iotdb.subscription.rpc.thrift.TSubscriptionDataSet;
import org.apache.iotdb.subscription.service.SubscriptionService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Consumer implements IConsumer {
  private SubscriptionConfiguration subscriptionConfiguration;
  private boolean isClosed = true;
  public SubscriptionService subscriptionService;
  /**
   * lastValueTimeTags for watermark, eg. [{ key: "root.db1.d1.s1", value: 1679645059 }] key:
   * timeseries path, value: timestamp
   */
  private ConcurrentHashMap<String, Long> lastValueTimeTags = new ConcurrentHashMap<>();

  public Consumer(SubscriptionConfiguration subscriptionConfiguration) {
    this.subscriptionConfiguration = subscriptionConfiguration;
  }

  public void createPipe() throws SubscriptionException {
    try (ISession session =
        new Session.Builder()
            .host(subscriptionConfiguration.getHost())
            .port(subscriptionConfiguration.getPort())
            .username(subscriptionConfiguration.getUsername())
            .password(subscriptionConfiguration.getPassword())
            .build()) {
      session.open();
      session.executeNonQueryStatement("CREATE PIPE");
    } catch (IoTDBConnectionException e) {
      throw new RuntimeException(e);
    } catch (StatementExecutionException e) {
      throw new SubscriptionException("create Subscription Pipe failed", e);
    }
  }

  /** Open the subscription. */
  public void openSubscription() throws SubscriptionException {
    if (!isClosed) {
      return;
    }
    // TODO Open
    // createPipe();
    subscriptionService.start();
    isClosed = false;
  }

  /** Close the subscription. */
  public void closeSubscription() throws SubscriptionException {
    if (isClosed) {
      return;
    }
    subscriptionService.stop();
    // TODO Close
    isClosed = true;
  }

  /**
   * Check if the subscription is closed.
   *
   * @return true if the subscription is closed, false otherwise
   */
  public boolean isClosed() {
    return this.isClosed;
  }

  /**
   * Get the consumer group of the subscription.
   *
   * @return the consumer group
   * @throws SubscriptionException if the consumer group cannot be retrieved
   */
  public String consumerGroup() throws SubscriptionException {
    return this.subscriptionConfiguration.getGroup();
  }

  /**
   * Get the topics of the subscription.
   *
   * @return the topics
   * @throws SubscriptionException if the topics cannot be retrieved
   */
  public List<String> subscription() throws SubscriptionException {
    return this.subscriptionConfiguration.getTopicStrategy().getTopics();
  }

  @Override
  public void close() throws Exception {
    this.closeSubscription();
  }

  /**
   * filter data on Watermark 1. sort DataSet by time 2. filter data by lastValueTimeTags
   *
   * @param subscriptionDataSets
   */
  public List<ISubscriptionDataSet> handleDataOnWatermark(
      List<TSubscriptionDataSet> subscriptionDataSets) {
    if (lastValueTimeTags == null) {
      lastValueTimeTags = new ConcurrentHashMap<>();
    }
    subscriptionDataSets.sort(Comparator.comparingLong(dataSet -> dataSet.getTime()));
    List<ISubscriptionDataSet> result = new ArrayList<>();
    subscriptionDataSets.forEach(
        dataSet -> {
          SubscriptionDataSet newDataSet = new SubscriptionDataSet();
          newDataSet.setTime(dataSet.getTime());
          newDataSet.setColumnNames(new ArrayList<>());
          newDataSet.setColumnTypes(new ArrayList<>());
          newDataSet.setDataResult(new ArrayList<>());
          if (dataSet.getColumnNames() != null && dataSet.getColumnNames().size() > 0) {
            int columnSize = dataSet.getColumnNames().size();
            for (int index = 0; index < columnSize; index++) {
              if (checkTolerant(dataSet.getColumnNames().get(index), dataSet.getTime())) {
                newDataSet.getColumnNames().add(dataSet.getColumnNames().get(index));
                newDataSet.getColumnTypes().add(dataSet.getColumnTypes().get(index));
                newDataSet.getDataResult().add(dataSet.getDataResult().get(index));
              }
            }
          }
          result.add(newDataSet);
        });
    return result;
  }

  /**
   * Check current point is tolerant，tolerant: currentPointTime > lastValueTimeTag - Watermark
   *
   * @param path timeseries path
   * @param currentPointTime current point data time
   * @return true: current point is tolerant Data will be filtered。false: current point isn't
   *     tolerant, Data will be retained
   */
  private boolean checkTolerant(String path, Long currentPointTime) {
    if (lastValueTimeTags.containsKey(path)) {
      Long lastValueTime = lastValueTimeTags.get(path);
      return currentPointTime
          >= lastValueTime
              - this.subscriptionConfiguration.getDisorderHandlingStrategy().getWatermark();
    } else {
      lastValueTimeTags.put(path, currentPointTime);
      return true;
    }
  }
}
