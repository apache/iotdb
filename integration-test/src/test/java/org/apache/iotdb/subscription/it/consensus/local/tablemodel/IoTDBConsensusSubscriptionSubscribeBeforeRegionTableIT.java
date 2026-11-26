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

import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.LocalStandaloneIT;
import org.apache.iotdb.session.subscription.consumer.table.SubscriptionTablePullConsumer;
import org.apache.iotdb.subscription.it.consensus.local.AbstractSubscriptionConsensusLocalIT;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

@RunWith(IoTDBTestRunner.class)
@Category({LocalStandaloneIT.class})
public class IoTDBConsensusSubscriptionSubscribeBeforeRegionTableIT
    extends AbstractSubscriptionConsensusLocalIT {

  @Test
  public void testSubscribeBeforeRegionCreation() throws Exception {
    final ConsensusSubscriptionTableITSupport.TestIdentifiers ids =
        ConsensusSubscriptionTableITSupport.newIdentifiers(
            "table_subscribe_before_region_creation");
    final String database = ids.getDatabase();
    final String tableName = "t1";
    SubscriptionTablePullConsumer consumer = null;

    try {
      ConsensusSubscriptionTableITSupport.createConsensusTopic(ids.getTopic(), database, ".*");

      consumer =
          ConsensusSubscriptionTableITSupport.createConsumer(
              ids.getConsumerId(), ids.getConsumerGroupId());
      consumer.subscribe(ids.getTopic());

      ConsensusSubscriptionTableITSupport.createDatabaseAndTable(
          database, tableName, ConsensusSubscriptionTableITSupport.DEFAULT_TABLE_SCHEMA);
      ConsensusSubscriptionTableITSupport.pause(1000);

      final Set<String> expectedRowKeys =
          ConsensusSubscriptionTableITSupport.insertRows(database, tableName, 1L, 12, true);

      final ConsensusSubscriptionTableITSupport.ConsumedRecords consumed =
          ConsensusSubscriptionTableITSupport.pollAndCommitUntilAtLeast(
              consumer, expectedRowKeys.size(), 50);

      ConsensusSubscriptionTableITSupport.assertExactRowKeys(expectedRowKeys, consumed);
    } finally {
      ConsensusSubscriptionTableITSupport.cleanup(consumer, ids.getTopic(), database);
    }
  }
}
