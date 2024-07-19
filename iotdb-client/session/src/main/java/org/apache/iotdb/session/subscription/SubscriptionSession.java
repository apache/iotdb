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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionParameterNotValidException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionConnection;
import org.apache.iotdb.session.subscription.model.Subscription;
import org.apache.iotdb.session.subscription.model.Topic;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionSession extends Session {

  public SubscriptionSession(final String host, final int port) {
    this(host, port, SessionConfig.DEFAULT_USER, SessionConfig.DEFAULT_PASSWORD);
  }

  public SubscriptionSession(
      final String host, final int port, final String username, final String password) {
    // TODO: more configs control
    super(
        new Session.Builder()
            .host(host)
            .port(port)
            .username(username)
            .password(password)
            // disable auto fetch
            .enableAutoFetch(false)
            // disable redirection
            .enableRedirection(false)
            // TODO: config
            .thriftMaxFrameSize(Integer.MAX_VALUE));
  }

  @Override
  public SessionConnection constructSessionConnection(
      final Session session, final TEndPoint endpoint, final ZoneId zoneId)
      throws IoTDBConnectionException {
    if (Objects.isNull(endpoint)) {
      throw new SubscriptionParameterNotValidException(
          "Subscription session must be configured with an endpoint.");
    }
    return new SubscriptionSessionConnection(
        session, endpoint, zoneId, availableNodes, maxRetryCount, retryIntervalInMs);
  }

  /////////////////////////////// topic ///////////////////////////////

  public void createTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("CREATE TOPIC %s", topicName);
    executeNonQueryStatement(sql);
  }

  public void createTopic(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    if (properties.isEmpty()) {
      createTopic(topicName);
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
    final String sql = String.format("CREATE TOPIC %s WITH %s", topicName, sb);
    executeNonQueryStatement(sql);
  }

  public void dropTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("DROP TOPIC %s", topicName);
    executeNonQueryStatement(sql);
  }

  public Set<Topic> getTopics() throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW TOPICS";
    try (final SessionDataSet dataSet = executeQueryStatement(sql)) {
      return convertDataSetToTopics(dataSet);
    }
  }

  public Optional<Topic> getTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("SHOW TOPIC %s", topicName);
    try (final SessionDataSet dataSet = executeQueryStatement(sql)) {
      final Set<Topic> topics = convertDataSetToTopics(dataSet);
      if (topics.isEmpty()) {
        return Optional.empty();
      }
      return Optional.of(topics.iterator().next());
    }
  }

  /////////////////////////////// subscription ///////////////////////////////

  public Set<Subscription> getSubscriptions()
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW SUBSCRIPTIONS";
    try (final SessionDataSet dataSet = executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
  }

  public Set<Subscription> getSubscriptions(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("SHOW SUBSCRIPTIONS ON %s", topicName);
    try (final SessionDataSet dataSet = executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
  }

  /////////////////////////////// utility ///////////////////////////////

  public Set<Topic> convertDataSetToTopics(final SessionDataSet dataSet)
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

  public Set<Subscription> convertDataSetToSubscriptions(final SessionDataSet dataSet)
      throws IoTDBConnectionException, StatementExecutionException {
    final Set<Subscription> subscriptions = new HashSet<>();
    while (dataSet.hasNext()) {
      final RowRecord record = dataSet.next();
      final List<Field> fields = record.getFields();
      if (fields.size() != 3) {
        throw new SubscriptionException(
            String.format(
                "Unexpected fields %s was obtained during SHOW SUBSCRIPTION...",
                fields.stream().map(Object::toString).collect(Collectors.joining(", "))));
      }
      subscriptions.add(
          new Subscription(
              fields.get(0).getStringValue(),
              fields.get(1).getStringValue(),
              fields.get(2).getStringValue()));
    }
    return subscriptions;
  }
}
