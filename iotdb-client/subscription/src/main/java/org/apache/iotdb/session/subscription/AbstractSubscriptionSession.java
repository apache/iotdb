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

import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.session.subscription.util.IdentifierUtils;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

abstract class AbstractSubscriptionSession {

  private final SubscriptionSessionWrapper session;

  protected AbstractSubscriptionSession(final SubscriptionSessionWrapper session) {
    this.session = session;
  }

  public SubscriptionSessionConnection getSessionConnection() throws IoTDBConnectionException {
    return session.getSessionConnection();
  }

  public int getThriftMaxFrameSize() {
    return session.getThriftMaxFrameSize();
  }

  /////////////////////////////// open & close ///////////////////////////////

  protected void open() throws IoTDBConnectionException {
    session.open();
  }

  protected void close() throws IoTDBConnectionException {
    session.close();
  }

  /////////////////////////////// topic ///////////////////////////////

  protected void createTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("CREATE TOPIC %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  protected void createTopicIfNotExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("CREATE TOPIC IF NOT EXISTS %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  protected void createTopic(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    createTopic(topicName, properties, false);
  }

  protected void createTopicIfNotExists(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    createTopic(topicName, properties, true);
  }

  private void createTopic(
      final String topicName, final Properties properties, final boolean isSetIfNotExistsCondition)
      throws IoTDBConnectionException, StatementExecutionException {
    if (Objects.isNull(properties) || properties.isEmpty()) {
      if (isSetIfNotExistsCondition) {
        createTopicIfNotExists(topicName);
      } else {
        createTopic(topicName);
      }
      return;
    }
    final StringBuilder sb = new StringBuilder();
    sb.append('(');
    properties.forEach(
        (k, v) ->
            sb.append('\'')
                .append(k)
                .append('\'')
                .append('=')
                .append('\'')
                .append(v)
                .append('\'')
                .append(','));
    sb.deleteCharAt(sb.length() - 1);
    sb.append(')');
    final String sql =
        isSetIfNotExistsCondition
            ? String.format("CREATE TOPIC IF NOT EXISTS %s WITH %s", topicName, sb)
            : String.format("CREATE TOPIC %s WITH %s", topicName, sb);
    session.executeNonQueryStatement(sql);
  }

  protected void dropTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("DROP TOPIC %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  protected void dropTopicIfExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("DROP TOPIC IF EXISTS %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  protected Set<Topic> getTopics() throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW TOPICS";
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToTopics(dataSet);
    }
  }

  protected Optional<Topic> getTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("SHOW TOPIC %s", topicName);
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      final Set<Topic> topics = convertDataSetToTopics(dataSet);
      if (topics.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(topics.iterator().next());
    }
  }

  /////////////////////////////// subscription ///////////////////////////////

  protected Set<Subscription> getSubscriptions()
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW SUBSCRIPTIONS";
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
  }

  protected Set<Subscription> getSubscriptions(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(topicName); // ignore the parse result
    final String sql = String.format("SHOW SUBSCRIPTIONS ON %s", topicName);
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
  }

  protected void dropSubscription(final String subscriptionId)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(subscriptionId); // ignore the parse result
    final String sql = String.format("DROP SUBSCRIPTION %s", subscriptionId);
    session.executeNonQueryStatement(sql);
  }

  protected void dropSubscriptionIfExists(final String subscriptionId)
      throws IoTDBConnectionException, StatementExecutionException {
    IdentifierUtils.checkAndParseIdentifier(subscriptionId); // ignore the parse result
    final String sql = String.format("DROP SUBSCRIPTION IF EXISTS %s", subscriptionId);
    session.executeNonQueryStatement(sql);
  }

  /////////////////////////////// utility ///////////////////////////////

  private Set<Topic> convertDataSetToTopics(final SessionDataSet dataSet)
      throws IoTDBConnectionException, StatementExecutionException {
    final Set<Topic> topics = new HashSet<>();
    while (dataSet.hasNext()) {
      final RowRecord record = dataSet.next();
      final List<Field> fields = record.getFields();
      if (fields.size() != 2) {
        throw new SubscriptionException(
            String.format(
                "Unexpected fields %s was obtained during SHOW TOPIC...",
                fields.stream().map(Object::toString).collect(Collectors.joining(", "))));
      }
      topics.add(new Topic(fields.get(0).getStringValue(), fields.get(1).getStringValue()));
    }
    return topics;
  }

  private Set<Subscription> convertDataSetToSubscriptions(final SessionDataSet dataSet)
      throws IoTDBConnectionException, StatementExecutionException {
    final Set<Subscription> subscriptions = new HashSet<>();
    while (dataSet.hasNext()) {
      final RowRecord record = dataSet.next();
      final List<Field> fields = record.getFields();
      if (fields.size() != 4) {
        throw new SubscriptionException(
            String.format(
                "Unexpected fields %s was obtained during SHOW SUBSCRIPTION...",
                fields.stream().map(Object::toString).collect(Collectors.joining(", "))));
      }
      subscriptions.add(
          new Subscription(
              fields.get(0).getStringValue(),
              fields.get(1).getStringValue(),
              fields.get(2).getStringValue(),
              fields.get(3).getStringValue()));
    }
    return subscriptions;
  }
}
