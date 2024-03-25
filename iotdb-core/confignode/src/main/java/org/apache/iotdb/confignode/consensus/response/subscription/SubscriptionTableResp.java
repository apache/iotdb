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

package org.apache.iotdb.confignode.consensus.response.subscription;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.commons.subscription.meta.subscription.SubscriptionMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SubscriptionTableResp implements DataSet {
  private final TSStatus status;
  private final List<SubscriptionMeta> allSubscriptionMeta; // use for show subscription
  private final List<ConsumerGroupMeta> allConsumerGroupMeta; // use for meta sync

  public SubscriptionTableResp(
      TSStatus status,
      List<SubscriptionMeta> allSubscriptionMeta,
      List<ConsumerGroupMeta> allConsumerGroupMeta) {
    this.status = status;
    this.allSubscriptionMeta = allSubscriptionMeta;
    this.allConsumerGroupMeta = allConsumerGroupMeta;
  }

  public SubscriptionTableResp filter(String topicName) {
    if (topicName == null) {
      return this;
    } else {
      final List<SubscriptionMeta> filteredSubscriptionMeta = new ArrayList<>();
      for (SubscriptionMeta subscriptionMeta : allSubscriptionMeta) {
        if (subscriptionMeta.getTopicName().equals(topicName)) {
          filteredSubscriptionMeta.add(subscriptionMeta);
          break;
        }
      }
      return new SubscriptionTableResp(status, filteredSubscriptionMeta, allConsumerGroupMeta);
    }
  }

  public TShowSubscriptionResp convertToTShowSubscriptionResp() {
    final List<TShowSubscriptionInfo> showSubscriptionInfoList = new ArrayList<>();

    for (SubscriptionMeta subscriptionMeta : allSubscriptionMeta) {
      showSubscriptionInfoList.add(
          new TShowSubscriptionInfo(
              subscriptionMeta.getTopicName(),
              subscriptionMeta.getConsumerGroupID(),
              subscriptionMeta.getConsumerIDs()));
    }
    return new TShowSubscriptionResp(status).setSubscriptionInfoList(showSubscriptionInfoList);
  }

  public TGetAllSubscriptionInfoResp convertToTGetAllSubscriptionInfoResp() throws IOException {
    final List<ByteBuffer> subscriptionInfoByteBuffers = new ArrayList<>();
    for (ConsumerGroupMeta consumerGroupMeta : allConsumerGroupMeta) {
      subscriptionInfoByteBuffers.add(consumerGroupMeta.serialize());
    }
    return new TGetAllSubscriptionInfoResp(status, subscriptionInfoByteBuffers);
  }
}
