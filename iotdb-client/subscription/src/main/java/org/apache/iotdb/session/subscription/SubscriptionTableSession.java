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

package org.apache.iotdb.session.subscription;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.AbstractSessionBuilder;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.session.subscription.model.Topic;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public class SubscriptionTableSession extends AbstractSubscriptionSession
    implements ISubscriptionTableSession {

  public SubscriptionTableSession(final AbstractSessionBuilder builder) {
    super(new SubscriptionSessionWrapper(builder));
  }

  /////////////////////////////// open & close ///////////////////////////////

  @Override
  public void open() throws IoTDBConnectionException {
    super.open();
  }

  @Override
  public void close() throws IoTDBConnectionException {
    super.close();
  }

  /////////////////////////////// topic ///////////////////////////////

  @Override
  public void createTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    super.createTopic(topicName);
  }

  @Override
  public void createTopicIfNotExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    super.createTopicIfNotExists(topicName);
  }

  @Override
  public void createTopic(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    super.createTopic(topicName, properties);
  }

  @Override
  public void createTopicIfNotExists(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    super.createTopicIfNotExists(topicName, properties);
  }

  @Override
  public void dropTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    super.dropTopic(topicName);
  }

  @Override
  public void dropTopicIfExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    super.dropTopicIfExists(topicName);
  }

  @Override
  public Set<Topic> getTopics() throws IoTDBConnectionException, StatementExecutionException {
    return super.getTopics();
  }

  @Override
  public Optional<Topic> getTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    return super.getTopic(topicName);
  }

  /////////////////////////////// subscription ///////////////////////////////

  @Override
  public Set<Subscription> getSubscriptions()
      throws IoTDBConnectionException, StatementExecutionException {
    return super.getSubscriptions();
  }

  @Override
  public Set<Subscription> getSubscriptions(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    return super.getSubscriptions(topicName);
  }

  @Override
  public void dropSubscription(final String subscriptionId)
      throws IoTDBConnectionException, StatementExecutionException {
    super.dropSubscription(subscriptionId);
  }

  @Override
  public void dropSubscriptionIfExists(final String subscriptionId)
      throws IoTDBConnectionException, StatementExecutionException {
    super.dropSubscriptionIfExists(subscriptionId);
  }
}
