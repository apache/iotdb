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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DropTopicPlan extends ConfigPhysicalPlan {

  private String topicName;
  private boolean isTableModel;
  private boolean isTableModelSet;

  public DropTopicPlan() {
    super(ConfigPhysicalPlanType.DropTopic);
  }

  public DropTopicPlan(String topicName) {
    super(ConfigPhysicalPlanType.DropTopic);
    this.topicName = topicName;
  }

  public DropTopicPlan(String topicName, boolean isTableModel) {
    this(topicName);
    this.isTableModel = isTableModel;
    this.isTableModelSet = true;
  }

  public String getTopicName() {
    return topicName;
  }

  public boolean isTableModel() {
    return isTableModel;
  }

  public boolean isTableModelSet() {
    return isTableModelSet;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ReadWriteIOUtils.write(topicName, stream);
    if (isTableModelSet) {
      ReadWriteIOUtils.write(isTableModel, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    topicName = ReadWriteIOUtils.readString(buffer);
    isTableModelSet = buffer.hasRemaining();
    if (isTableModelSet) {
      isTableModel = ReadWriteIOUtils.readBool(buffer);
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
    DropTopicPlan that = (DropTopicPlan) obj;
    return isTableModel == that.isTableModel
        && isTableModelSet == that.isTableModelSet
        && Objects.equals(topicName, that.topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(topicName, isTableModel, isTableModelSet);
  }

  @Override
  public String toString() {
    return "DropTopicPlan{" + "topicName='" + topicName + "', isTableModel=" + isTableModel + '}';
  }
}
