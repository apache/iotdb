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
import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.rpc.thrift.TGetAllTopicInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicInfo;
import org.apache.iotdb.confignode.rpc.thrift.TShowTopicResp;
import org.apache.iotdb.consensus.common.DataSet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class TopicTableResp implements DataSet {
  private final TSStatus status;
  private final List<TopicMeta> allTopicMeta;

  public TopicTableResp(TSStatus status, List<TopicMeta> allTopicMeta) {
    this.status = status;
    this.allTopicMeta = allTopicMeta;
  }

  public TopicTableResp filter(String topicName, boolean isTableModel) {
    return new TopicTableResp(
        status,
        allTopicMeta.stream()
            .filter(
                topicMeta ->
                    (Objects.isNull(topicName)
                            || Objects.equals(topicMeta.getTopicName(), topicName))
                        && topicMeta.visibleUnder(isTableModel))
            .collect(Collectors.toList()));
  }

  public TShowTopicResp convertToTShowTopicResp() {
    final List<TShowTopicInfo> showTopicInfoList = new ArrayList<>();

    for (TopicMeta topicMeta : allTopicMeta) {
      showTopicInfoList.add(
          new TShowTopicInfo(topicMeta.getTopicName(), topicMeta.getCreationTime())
              .setTopicAttributes(topicMeta.getConfig().toString()));
    }
    return new TShowTopicResp(status).setTopicInfoList(showTopicInfoList);
  }

  public TGetAllTopicInfoResp convertToTGetAllTopicInfoResp() throws IOException {
    final List<ByteBuffer> topicInfoByteBuffers = new ArrayList<>();
    for (TopicMeta topicMeta : allTopicMeta) {
      topicInfoByteBuffers.add(topicMeta.serialize());
    }
    return new TGetAllTopicInfoResp(status, topicInfoByteBuffers);
  }
}
