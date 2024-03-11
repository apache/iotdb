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
import org.apache.iotdb.commons.subscription.meta.PipeMQTopicMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PipeMQTopicTableResp implements DataSet {
  private final TSStatus status;
  private final List<PipeMQTopicMeta> allPipeMQTopicMeta;

  public PipeMQTopicTableResp(TSStatus status, List<PipeMQTopicMeta> allPipeMQTopicMeta) {
    this.status = status;
    this.allPipeMQTopicMeta = allPipeMQTopicMeta;
  }

  public PipeMQTopicTableResp filter(String topicName) {
    if (topicName == null) {
      return this;
    } else {
      final List<PipeMQTopicMeta> filteredPipeMQTopicMeta = new ArrayList<>();
      for (PipeMQTopicMeta pipeMQTopicMeta : allPipeMQTopicMeta) {
        if (pipeMQTopicMeta.getTopicName().equals(topicName)) {
          filteredPipeMQTopicMeta.add(pipeMQTopicMeta);
          break;
        }
      }
      return new PipeMQTopicTableResp(status, filteredPipeMQTopicMeta);
    }
  }

  public TShowTopicResp convertToTShowTopicResp() {
    final List<TShowTopicInfo> showTopicInfoList = new ArrayList<>();

    for (PipeMQTopicMeta pipeMQTopicMeta : allPipeMQTopicMeta) {
      showTopicInfoList.add(
          new TShowTopicInfo(pipeMQTopicMeta.getTopicName(), pipeMQTopicMeta.getCreationTime())
              .setTopicAttributes(pipeMQTopicMeta.getConfig().toString()));
    }
    return new TShowTopicResp(status).setTopicInfoList(showTopicInfoList);
  }

  public TGetAllTopicInfoResp convertToTGetAllTopicInfoResp() throws IOException {
    final List<ByteBuffer> pipeMQTopicInfoByteBuffers = new ArrayList<>();
    for (PipeMQTopicMeta pipeMQTopicMeta : allPipeMQTopicMeta) {
      pipeMQTopicInfoByteBuffers.add(pipeMQTopicMeta.serialize());
    }
    return new TGetAllTopicInfoResp(status, pipeMQTopicInfoByteBuffers);
  }
}
