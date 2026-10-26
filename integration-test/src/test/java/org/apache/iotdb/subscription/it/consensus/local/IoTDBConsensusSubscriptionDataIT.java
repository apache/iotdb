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

import org.apache.iotdb.isession.ISession;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.consumer.tree.SubscriptionTreePullConsumer;

import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedHashSet;
import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionDataIT extends AbstractSubscriptionConsensusLocalIT {

  private static final long ONE_WEEK_PLUS_ONE_MS = 604_800_001L;

  @Test
  public void testNonAlignedPrimitiveTypes() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("data_non_aligned_primitive_types");
    SubscriptionTreePullConsumer consumer = null;

    try {
      ConsensusSubscriptionITSupport.bootstrapDatabase(ids.getDatabase());
      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final String device = ids.getDatabase() + ".d_types";
      final Set<String> expectedRowKeys = new LinkedHashSet<>();
      try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
        for (int i = 0; i < 12; i++) {
          final long timestamp = 100L + i;
          session.executeNonQueryStatement(
              String.format(
                  "insert into %s(time, s_int32, s_int64, s_float, s_double, s_bool, s_text) "
                      + "values (%d, %d, %d, %.1f, %.2f, %s, 'text_%d')",
                  device,
                  timestamp,
                  i,
                  i * 100L,
                  i + 0.5f,
                  i + 0.25d,
                  i % 2 == 0 ? "true" : "false",
                  i));
          expectedRowKeys.add(ConsensusSubscriptionITSupport.rowKey(device, timestamp));
        }
        session.executeNonQueryStatement("flush");
      }

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertTrue(
          "Expected all primitive type measurements in the consumed columns, actual="
              + consumed.getSeenColumns(),
          consumed
              .getSeenColumns()
              .containsAll(
                  ConsensusSubscriptionITSupport.measurementPaths(
                      device, "s_int32", "s_int64", "s_float", "s_double", "s_bool", "s_text")));
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }

  @Test
  public void testAlignedCrossPartitionRows() throws Exception {
    final ConsensusSubscriptionITSupport.TestIdentifiers ids =
        ConsensusSubscriptionITSupport.newIdentifiers("data_aligned_cross_partition");
    SubscriptionTreePullConsumer consumer = null;

    try {
      final String alignedDevice = ids.getDatabase() + ".d_aligned";
      try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
        session.executeNonQueryStatement("create database " + ids.getDatabase());
        session.executeNonQueryStatement(
            String.format(
                "create aligned timeseries %s"
                    + "(s_int32 INT32, s_int64 INT64, s_float FLOAT, "
                    + "s_double DOUBLE, s_bool BOOLEAN, s_text TEXT)",
                alignedDevice));
        session.executeNonQueryStatement(
            String.format(
                "insert into %s(time, s_int32, s_int64, s_float, s_double, s_bool, s_text) "
                    + "values (0, 0, 0, 0.0, 0.0, false, 'bootstrap')",
                alignedDevice));
        session.executeNonQueryStatement("flush");
      }

      ConsensusSubscriptionITSupport.createConsensusTopic(
          ids.getTopic(), ids.getDatabase() + ".**");

      consumer =
          ConsensusSubscriptionITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      final Set<String> expectedRowKeys = new LinkedHashSet<>();
      try (final ISession session = EnvFactory.getEnv().getSessionConnection()) {
        for (int i = 0; i < 6; i++) {
          final long timestamp = 100L + ONE_WEEK_PLUS_ONE_MS * i;
          session.executeNonQueryStatement(
              String.format(
                  "insert into %s(time, s_int32, s_int64, s_float, s_double, s_bool, s_text) "
                      + "values (%d, %d, %d, %.1f, %.2f, %s, 'aligned_%d')",
                  alignedDevice,
                  timestamp,
                  i + 1,
                  (i + 1) * 100L,
                  i + 1.5f,
                  i + 1.25d,
                  i % 2 == 0 ? "true" : "false",
                  i));
          expectedRowKeys.add(ConsensusSubscriptionITSupport.rowKey(alignedDevice, timestamp));
        }
        session.executeNonQueryStatement("flush");
      }

      final ConsensusSubscriptionITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 60);

      ConsensusSubscriptionITSupport.assertExactRowKeys(expectedRowKeys, consumed);
      Assert.assertTrue(
          "Expected aligned measurements in consumed columns, actual=" + consumed.getSeenColumns(),
          consumed
              .getSeenColumns()
              .containsAll(
                  ConsensusSubscriptionITSupport.measurementPaths(
                      alignedDevice,
                      "s_int32",
                      "s_int64",
                      "s_float",
                      "s_double",
                      "s_bool",
                      "s_text")));
    } finally {
      ConsensusSubscriptionITSupport.cleanup(consumer, ids.getTopic(), ids.getDatabase());
    }
  }
}
