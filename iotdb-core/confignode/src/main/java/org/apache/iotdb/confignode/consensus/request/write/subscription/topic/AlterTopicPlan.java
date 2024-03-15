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

package org.apache.iotdb.confignode.consensus.request.write.subscription.topic;

import org.apache.iotdb.commons.subscription.meta.topic.TopicMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class AlterTopicPlan extends ConfigPhysicalPlan {

  private TopicMeta topicMeta;

  public AlterTopicPlan() {
    super(ConfigPhysicalPlanType.AlterTopic);
  }

  public AlterTopicPlan(TopicMeta topicMeta) {
    super(ConfigPhysicalPlanType.AlterTopic);
    this.topicMeta = topicMeta;
  }

  public TopicMeta getTopicMeta() {
    return topicMeta;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    topicMeta.serialize(stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    topicMeta = TopicMeta.deserialize(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AlterTopicPlan that = (AlterTopicPlan) obj;
    return Objects.equals(topicMeta, that.topicMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicMeta);
  }

  @Override
  public String toString() {
    return "AlterTopicPlan{" + "topicMeta='" + topicMeta + "'}";
  }
}
