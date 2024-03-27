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

package org.apache.iotdb.confignode.consensus.request.write.subscription.topic.runtime;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TopicHandleMetaChangePlan extends ConfigPhysicalPlan {

  private List<TopicMeta> topicMetaList = new ArrayList<>();

  public TopicHandleMetaChangePlan() {
    super(ConfigPhysicalPlanType.TopicHandleMetaChange);
  }

  public TopicHandleMetaChangePlan(List<TopicMeta> topicMetaList) {
    super(ConfigPhysicalPlanType.TopicHandleMetaChange);
    this.topicMetaList = topicMetaList;
  }

  public List<TopicMeta> getTopicMetaList() {
    return topicMetaList;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(topicMetaList.size());
    for (TopicMeta topicMeta : topicMetaList) {
      topicMeta.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      TopicMeta topicMeta = TopicMeta.deserialize(buffer);
      topicMetaList.add(topicMeta);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    TopicHandleMetaChangePlan that = (TopicHandleMetaChangePlan) obj;
    return Objects.equals(this.topicMetaList, that.topicMetaList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicMetaList);
  }
}
