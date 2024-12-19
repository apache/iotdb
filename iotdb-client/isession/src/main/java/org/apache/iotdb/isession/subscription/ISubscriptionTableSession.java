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

package org.apache.iotdb.isession.subscription;

import org.apache.iotdb.isession.subscription.model.Subscription;
import org.apache.iotdb.isession.subscription.model.Topic;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;

import java.util.Optional;
import java.util.Properties;
import java.util.Set;

public interface ISubscriptionTableSession extends AutoCloseable {

  void open() throws IoTDBConnectionException;

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
  void createTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

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
  void createTopicIfNotExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

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
  void createTopic(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException;

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
  void createTopicIfNotExists(final String topicName, final Properties properties)
      throws IoTDBConnectionException, StatementExecutionException;

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
  void dropTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

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
  void dropTopicIfExists(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

  Set<Topic> getTopics() throws IoTDBConnectionException, StatementExecutionException;

  Optional<Topic> getTopic(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;

  /////////////////////////////// subscription ///////////////////////////////

  Set<Subscription> getSubscriptions() throws IoTDBConnectionException, StatementExecutionException;

  Set<Subscription> getSubscriptions(final String topicName)
      throws IoTDBConnectionException, StatementExecutionException;
}
