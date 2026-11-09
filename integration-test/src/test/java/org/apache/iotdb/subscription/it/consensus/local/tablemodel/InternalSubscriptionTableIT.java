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

package org.apache.iotdb.subscription.it.consensus.local.tablemodel;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.internal.subscription.InternalSubscriptionTablePullConsumerBuilder;
import org.apache.iotdb.commons.client.internal.subscription.InternalSubscriptionTableSessionBuilder;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.isession.INodeSupplier;
import org.apache.iotdb.isession.ITableSession;
import org.apache.iotdb.isession.SessionConfig;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.it.utils.InternalSessionITUtils;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.rpc.subscription.config.TopicConstant;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.subscription.ISubscriptionTableSession;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.subscription.it.AbstractSubscriptionIT;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category(TableClusterIT.class)
public class InternalSubscriptionTableIT extends AbstractSubscriptionIT {

  private static final int DEVICE_COUNT = 48;
  private static final String TABLE = "t";

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setConfigNodeConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setSchemaRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS)
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS)
        .setSchemaReplicationFactor(1)
        .setDataReplicationFactor(1)
        .setDataRegionGroupExtensionPolicy("CUSTOM")
        .setDefaultDataRegionGroupNumPerDatabase(3)
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false);
    EnvFactory.getEnv().initClusterEnvironment(1, 3);
  }

  @Override
  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanClusterEnvironment();
    super.tearDown();
  }

  @Test
  public void testInternalPullConsumerPollsAcrossDataNodes() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers identifiers =
        ConsensusSubscriptionTableITSupport.newIdentifiers("internal_pull_consumer");
    final String database = identifiers.getDatabase();
    final INodeSupplier nodeSupplier = InternalSessionITUtils.createInternalNodeSupplier();
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database, TABLE, "tag1 STRING TAG, value INT64 FIELD");
      createInitialTopic(nodeSupplier, identifiers.getTopic(), database, TABLE);

      consumer =
          (SubscriptionTablePullConsumer)
              new InternalSubscriptionTablePullConsumerBuilder(nodeSupplier)
                  .consumerId(identifiers.getConsumerId())
                  .consumerGroupId(identifiers.getConsumerGroupId())
                  .autoCommit(false)
                  .build();
      consumer.open();
      consumer.subscribe(identifiers.getTopic());

      final Set<String> expectedRows = new LinkedHashSet<>();
      final Set<TEndPoint> leaders = new HashSet<>();
      try (ITableSession session = EnvFactory.getEnv().getTableSessionConnection()) {
        session.executeNonQueryStatement("use " + database);
        for (int deviceIndex = 0; deviceIndex < DEVICE_COUNT; deviceIndex++) {
          final long timestamp = deviceIndex + 1L;
          session.insert(createTablet(deviceIndex, timestamp));
          expectedRows.add(ConsensusSubscriptionTableITSupport.rowKey(database, TABLE, timestamp));
        }
        session.executeNonQueryStatement("flush");

        final Session underlyingSession = InternalSessionITUtils.getUnderlyingSession(session);
        final Map<?, TEndPoint> deviceToEndpoint =
            InternalSessionITUtils.getSessionField(
                underlyingSession, "tableModelDeviceIdToEndpoint");
        leaders.addAll(deviceToEndpoint.values());
      }

      Assert.assertTrue(
          "Expected the inserted devices to span multiple DataNodes, actual leaders: " + leaders,
          leaders.size() > 1);

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRows.size(), 120);
      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRows, consumed);
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, identifiers.getTopic(), database);
    }
  }

  private static void createInitialTopic(
      final INodeSupplier nodeSupplier,
      final String topic,
      final String database,
      final String table)
      throws Exception {
    try (ISubscriptionTableSession session =
        new InternalSubscriptionTableSessionBuilder(nodeSupplier)
            .username(SessionConfig.DEFAULT_USER)
            .password(SessionConfig.DEFAULT_PASSWORD)
            .build()) {
      final Properties properties = new Properties();
      properties.put(TopicConstant.MODE_KEY, TopicConstant.MODE_INITIAL_VALUE);
      properties.put(TopicConstant.DATABASE_KEY, database);
      properties.put(TopicConstant.TABLE_KEY, table);
      session.createTopic(topic, properties);
    }
  }

  private static Tablet createTablet(final int deviceIndex, final long timestamp) {
    final List<IMeasurementSchema> schemas =
        Arrays.asList(
            new MeasurementSchema("tag1", TSDataType.STRING),
            new MeasurementSchema("value", TSDataType.INT64));
    final Tablet tablet =
        new Tablet(
            TABLE,
            IMeasurementSchema.getMeasurementNameList(schemas),
            IMeasurementSchema.getDataTypeList(schemas),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, timestamp);
    tablet.addValue("tag1", 0, "device-" + deviceIndex);
    tablet.addValue("value", 0, (long) deviceIndex);
    return tablet;
  }
}
