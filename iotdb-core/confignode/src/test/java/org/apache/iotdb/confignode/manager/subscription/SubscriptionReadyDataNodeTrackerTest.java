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

package org.apache.iotdb.confignode.manager.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerMeta;
import org.apache.iotdb.mpp.rpc.thrift.TPushConsumerGroupMetaResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class SubscriptionReadyDataNodeTrackerTest {

  @Test
  public void testTracksSuccessfulFullAndSinglePushes() {
    final SubscriptionReadyDataNodeTracker tracker = new SubscriptionReadyDataNodeTracker();
    final ConsumerGroupMeta group = createGroup("group");
    group.addSubscription("consumer", Set.of("topic"));
    final Map<Integer, TPushConsumerGroupMetaResp> fullPushResponses =
        new HashMap<>(
            Map.of(3, successfulResponse(), 1, successfulResponse(), 2, failedResponse()));

    tracker.replaceFromFullPush(Collections.singleton(group), fullPushResponses);

    assertEquals(
        Arrays.asList(1, 3),
        tracker.getReadyDataNodeIds("topic", "group", false, Set.of(1, 2, 3, 4)));

    tracker.recordSinglePush(group, group, Map.of(1, failedResponse(), 2, successfulResponse()));
    assertEquals(
        Arrays.asList(2, 3),
        tracker.getReadyDataNodeIds("topic", "group", false, Set.of(1, 2, 3, 4)));
  }

  private static ConsumerGroupMeta createGroup(final String consumerGroupId) {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("consumer_group_id", consumerGroupId);
    attributes.put("sql_dialect", "tree");
    return new ConsumerGroupMeta(consumerGroupId, 1L, new ConsumerMeta("consumer", 1L, attributes));
  }

  private static TPushConsumerGroupMetaResp successfulResponse() {
    return new TPushConsumerGroupMetaResp()
        .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
  }

  private static TPushConsumerGroupMetaResp failedResponse() {
    return new TPushConsumerGroupMetaResp()
        .setStatus(new TSStatus(TSStatusCode.CONSUMER_PUSH_META_ERROR.getStatusCode()));
  }
}
