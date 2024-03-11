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

package org.apache.iotdb.confignode.consensus.response.pipe.mq;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.subscription.meta.PipeMQSubscriptionMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllSubscriptionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowSubscriptionResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeMQSubscriptionTableResp implements DataSet {
  private final TSStatus status;
  private final List<PipeMQSubscriptionMeta> allPipeMQSubscriptionMeta;

  public PipeMQSubscriptionTableResp(
      TSStatus status, List<PipeMQSubscriptionMeta> allPipeMQSubscriptionMeta) {
    this.status = status;
    this.allPipeMQSubscriptionMeta = allPipeMQSubscriptionMeta;
  }

  public PipeMQSubscriptionTableResp filter(String topicName) {
    if (topicName == null) {
      return this;
    } else {
      final List<PipeMQSubscriptionMeta> filteredPipeMQSubscriptionMeta = new ArrayList<>();
      for (PipeMQSubscriptionMeta pipeMQSubscriptionMeta : allPipeMQSubscriptionMeta) {
        if (pipeMQSubscriptionMeta.getTopicName().equals(topicName)) {
          filteredPipeMQSubscriptionMeta.add(pipeMQSubscriptionMeta);
          break;
        }
      }
      return new PipeMQSubscriptionTableResp(status, filteredPipeMQSubscriptionMeta);
    }
  }

  public TShowSubscriptionResp convertToTShowSubscriptionResp() {
    final List<TShowSubscriptionInfo> showSubscriptionInfoList = new ArrayList<>();

    for (PipeMQSubscriptionMeta pipeMQSubscriptionMeta : allPipeMQSubscriptionMeta) {
      showSubscriptionInfoList.add(
          new TShowSubscriptionInfo(
              pipeMQSubscriptionMeta.getTopicName(),
              pipeMQSubscriptionMeta.getConsumerGroupID(),
              pipeMQSubscriptionMeta.getConsumerIDs()));
    }
    return new TShowSubscriptionResp(status);
  }

  public TGetAllSubscriptionInfoResp convertToTGetAllSubscriptionInfoResp() throws IOException {
    final List<ByteBuffer> pipeMQSubscriptionInfoByteBuffers = new ArrayList<>();
    for (PipeMQSubscriptionMeta pipeMQSubscriptionMeta : allPipeMQSubscriptionMeta) {
      pipeMQSubscriptionInfoByteBuffers.add(pipeMQSubscriptionMeta.serialize());
    }
    return new TGetAllSubscriptionInfoResp(status, pipeMQSubscriptionInfoByteBuffers);
  }
}
