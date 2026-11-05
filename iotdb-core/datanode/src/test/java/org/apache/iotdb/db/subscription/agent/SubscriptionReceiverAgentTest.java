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

package org.apache.iotdb.db.subscription.agent;

import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.rpc.subscription.config.ConsumerConfig;
import org.apache.iotdb.rpc.subscription.payload.request.PipeSubscribeHandshakeReq;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

public class SubscriptionReceiverAgentTest {
  @Test
  public void testRatisRejectsSubscriptionBeforeCreatingReceiver() throws Exception {
    final String originalConsensus =
        IoTDBDescriptor.getInstance().getConfig().getDataRegionConsensusProtocolClass();
    SubscriptionReceiverAgent agent = null;
    try {
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
      agent = new SubscriptionReceiverAgent();
      Assert.assertEquals(
          TSStatusCode.UNSUPPORTED_OPERATION.getStatusCode(),
          agent
              .handle(
                  PipeSubscribeHandshakeReq.toTPipeSubscribeReq(
                      new ConsumerConfig(
                          Map.of("consumer-group-id", "group", "consumer-id", "consumer"))),
                  "root")
              .getStatus()
              .getCode());
      final Field receiver =
          SubscriptionReceiverAgent.class.getDeclaredField("receiverThreadLocal");
      receiver.setAccessible(true);
      Assert.assertNull(((ThreadLocal<?>) receiver.get(agent)).get());
    } finally {
      if (agent != null) {
        final Field checker =
            SubscriptionReceiverAgent.class.getDeclaredField("receiverTimeoutChecker");
        checker.setAccessible(true);
        ((ScheduledExecutorService) checker.get(agent)).shutdownNow();
      }
      IoTDBDescriptor.getInstance()
          .getConfig()
          .setDataRegionConsensusProtocolClass(originalConsensus);
    }
  }
}
