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

package org.apache.iotdb.subscription.it.consensus.local;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.itbase.env.BaseNodeWrapper;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.SubscriptionTableSessionBuilder;
import org.apache.iotdb.session.subscription.SubscriptionTreeSession;
import org.apache.iotdb.session.subscription.model.Topic;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.iotdb.subscription.it.IoTDBSubscriptionITConstant.FORCE_SCALABLE_SINGLE_NODE_MODE;

public abstract class AbstractSubscriptionConsensusLocalIT extends AbstractSubscriptionIT {

  @BeforeClass
  public static void setUpCluster() throws Exception {
    FORCE_SCALABLE_SINGLE_NODE_MODE.accept(EnvFactory.getEnv());
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);

    EnvFactory.getEnv().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDownCluster() {
    EnvFactory.getEnv().cleanClusterEnvironment();
  }

  @Override
  @After
  public void tearDown() throws Exception {
    final BaseEnv env = EnvFactory.getEnv();
    final List<BaseNodeWrapper> configNodes = new ArrayList<>(env.getConfigNodeWrapperList());
    for (int i = 0; i < configNodes.size(); i++) {
      if (!configNodes.get(i).isAlive()) {
        env.startConfigNode(i);
      }
    }
    env.ensureNodeStatus(configNodes, Collections.nCopies(configNodes.size(), NodeStatus.Running));

    final List<BaseNodeWrapper> dataNodes = new ArrayList<>(env.getDataNodeWrapperList());
    for (int i = 0; i < dataNodes.size(); i++) {
      if (!dataNodes.get(i).isAlive()) {
        env.startDataNode(i);
      }
    }
    env.ensureNodeStatus(dataNodes, Collections.nCopies(dataNodes.size(), NodeStatus.Running));

    // The test bodies close their consumers in finally blocks. Do not let a best-effort cleanup
    // leave topics or data for a later method, especially one subscribing to all databases.
    try (final SubscriptionTreeSession session =
        new SubscriptionTreeSession(
            EnvFactory.getEnv().getIP(), Integer.parseInt(EnvFactory.getEnv().getPort()))) {
      session.open();
      for (final Topic topic : session.getTopics()) {
        session.dropTopic(topic.getTopicName());
      }
    }
    try (final ISubscriptionTableSession session =
        new SubscriptionTableSessionBuilder()
            .host(EnvFactory.getEnv().getIP())
            .port(Integer.parseInt(EnvFactory.getEnv().getPort()))
            .build()) {
      session.open();
      for (final Topic topic : session.getTopics()) {
        session.dropTopic(topic.getTopicName());
      }
    }
    try (final Connection connection = EnvFactory.getEnv().getConnection();
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          databases.add(resultSet.getString(1));
        }
      }
      for (final String database : databases) {
        statement.execute("DELETE DATABASE " + database);
      }
    }
    try (final Connection connection =
            EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        final Statement statement = connection.createStatement()) {
      final List<String> databases = new ArrayList<>();
      try (final ResultSet resultSet = statement.executeQuery("SHOW DATABASES")) {
        while (resultSet.next()) {
          final String database = resultSet.getString(1);
          if (!"information_schema".equals(database)) {
            databases.add(database);
          }
        }
      }
      for (final String database : databases) {
        statement.execute("DROP DATABASE " + database);
      }
    }
  }
}
