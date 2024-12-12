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
import org.apache.iotdb.isession.subscription.ISubscriptionSession;
import org.apache.iotdb.isession.subscription.model.Subscription;
import org.apache.iotdb.isession.subscription.model.Topic;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.rpc.subscription.exception.SubscriptionException;
import org.apache.iotdb.session.AbstractSessionBuilder;

import org.apache.tsfile.read.common.Field;
import org.apache.tsfile.read.common.RowRecord;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

public class SubscriptionSession implements ISubscriptionSession {

  private final SubscriptionSessionWrapper session;

  public SubscriptionSession(final AbstractSessionBuilder builder) {
    this.session = new SubscriptionSessionWrapper(builder);
  }

  public SubscriptionSession(final String host, final int port) {
    this.session =
        new SubscriptionSessionWrapper(new SubscriptionSessionBuilder().host(host).port(port));
  }

  public SubscriptionSession(
      final String host,
      final int port,
      final String username,
      final String password,
      final int thriftMaxFrameSize) {
    this.session =
        new SubscriptionSessionWrapper(
            new SubscriptionSessionBuilder()
                .host(host)
                .port(port)
                .username(username)
                .password(password)
                .thriftMaxFrameSize(thriftMaxFrameSize));
  }

  public SubscriptionSessionConnection getSessionConnection() {
    return session.getSessionConnection();
  }

  protected int getThriftMaxFrameSize() {
    return session.getThriftMaxFrameSize();
  }

  /////////////////////////////// open & close ///////////////////////////////

  @Override
  public void open() throws IoTDBConnectionException {
    session.open();
  }

  @Override
  public void close() throws IoTDBConnectionException {
    session.close();
  }

  /////////////////////////////// topic ///////////////////////////////

  /**
   * Creates a topic with the specified name.
   *
   * <p>If the topic name contains single quotes, it must be enclosed in backticks (`). For example,
   * to create a topic named 'topic', the value passed in as topicName should be `'topic'`
   *
   * @param topicName If the created topic name contains single quotes, the passed parameter needs
   *     to be enclosed in backticks.
   * @throws IoTDBConnectionException If there is an issue with the connection to IoTDB.
   * @throws StatementExecutionException If there is an issue executing the SQL statement.
   */
  @Override
  public void createTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("CREATE TOPIC %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  /**
   * Creates a topic with the specified name only if it does not already exist.
   *
   * <p>This method is similar to {@link #createTopic(String)}, but includes the 'IF NOT EXISTS'
   * condition. If the topic name contains single quotes, it must be enclosed in backticks (`).
   *
   * @param topicName If the created topic name contains single quotes, the passed parameter needs
   *     to be enclosed in backticks.
   * @throws IoTDBConnectionException If there is an issue with the connection to IoTDB.
   * @throws StatementExecutionException If there is an issue executing the SQL statement.
   */
  @Override
  public void createTopicIfNotExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("CREATE TOPIC IF NOT EXISTS %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  /**
   * Creates a topic with the specified name and properties.
   *
   * <p>Topic names with single quotes must be enclosed in backticks (`). Property keys and values
   * are included in the SQL statement automatically.
   *
   * @param topicName If the created topic name contains single quotes, the passed parameter needs
   *     to be enclosed in backticks.
   * @param properties A {@link Properties} object containing the topic's properties.
   * @throws IoTDBConnectionException If a connection issue occurs with IoTDB.
   * @throws StatementExecutionException If a statement execution issue occurs.
   */
  @Override
  public void createTopic(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    createTopic(topicName, properties, false);
  }

  /**
   * Creates a topic with the specified properties if it does not already exist. Topic names with
   * single quotes must be enclosed in backticks (`).
   *
   * @param topicName If the created topic name contains single quotes, the passed parameter needs
   *     to be enclosed in backticks.
   * @param properties A {@link Properties} object containing the topic's properties.
   * @throws IoTDBConnectionException If a connection issue occurs.
   * @throws StatementExecutionException If the SQL statement execution fails.
   */
  @Override
  public void createTopicIfNotExists(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException {
    createTopic(topicName, properties, true);
  }

  private void createTopic(
      final String topicName, final Properties properties, final boolean isSetIfNotExistsCondition)
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
    final String sql =
        isSetIfNotExistsCondition
            ? String.format("CREATE TOPIC IF NOT EXISTS %s WITH %s", topicName, sb)
            : String.format("CREATE TOPIC %s WITH %s", topicName, sb);
    session.executeNonQueryStatement(sql);
  }

  /**
   * Drops the specified topic.
   *
   * <p>This method removes the specified topic from the database. If the topic name contains single
   * quotes, it must be enclosed in backticks (`).
   *
   * @param topicName The name of the topic to be deleted, if it contains single quotes, needs to be
   *     enclosed in backticks.
   * @throws IoTDBConnectionException If there is an issue with the connection to IoTDB.
   * @throws StatementExecutionException If there is an issue executing the SQL statement.
   */
  @Override
  public void dropTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("DROP TOPIC %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  /**
   * Drops the specified topic if it exists.
   *
   * <p>This method is similar to {@link #dropTopic(String)}, but includes the 'IF EXISTS'
   * condition. If the topic name contains single quotes, it must be enclosed in backticks (`).
   *
   * @param topicName The name of the topic to be deleted, if it contains single quotes, needs to be
   *     enclosed in backticks.
   * @throws IoTDBConnectionException If there is an issue with the connection to IoTDB.
   * @throws StatementExecutionException If there is an issue executing the SQL statement.
   */
  @Override
  public void dropTopicIfExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("DROP TOPIC IF EXISTS %s", topicName);
    session.executeNonQueryStatement(sql);
  }

  @Override
  public Set<Topic> getTopics() throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW TOPICS";
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToTopics(dataSet);
    }
  }

  @Override
  public Optional<Topic> getTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
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

  @Override
  public Set<Subscription> getSubscriptions()
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = "SHOW SUBSCRIPTIONS";
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
  }

  @Override
  public Set<Subscription> getSubscriptions(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException {
    final String sql = String.format("SHOW SUBSCRIPTIONS ON %s", topicName);
    try (final SessionDataSet dataSet = session.executeQueryStatement(sql)) {
      return convertDataSetToSubscriptions(dataSet);
    }
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

  /////////////////////////////// builder ///////////////////////////////

  // for forward compatibility
  public static class Builder extends AbstractSessionBuilder {

    public Builder() {
      // use tree model
      super.sqlDialect = "tree";
      // disable auto fetch
      super.enableAutoFetch = false;
      // disable redirection
      super.enableRedirection = false;
    }

    public Builder host(final String host) {
      super.host = host;
      return this;
    }

    public Builder port(final int port) {
      super.rpcPort = port;
      return this;
    }

    public Builder username(final String username) {
      super.username = username;
      return this;
    }

    public Builder password(final String password) {
      super.pw = password;
      return this;
    }

    public Builder thriftMaxFrameSize(final int thriftMaxFrameSize) {
      super.thriftMaxFrameSize = thriftMaxFrameSize;
      return this;
    }

    public ISubscriptionSession build() {
      return new SubscriptionSession(this);
    }
  }
}
