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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AlterMultipleTopicsPlan extends ConfigPhysicalPlan {

  private List<AlterTopicPlan> subPlans;

  public AlterMultipleTopicsPlan() {
    super(ConfigPhysicalPlanType.AlterMultipleTopics);
  }

  public AlterMultipleTopicsPlan(List<AlterTopicPlan> subPlans) {
    super(ConfigPhysicalPlanType.AlterMultipleTopics);
    this.subPlans = subPlans;
  }

  public List<AlterTopicPlan> getSubPlans() {
    return subPlans;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    if (subPlans != null) {
      ReadWriteIOUtils.write(true, stream);
      ReadWriteIOUtils.write(subPlans.size(), stream);
      for (AlterTopicPlan subPlan : subPlans) {
        subPlan.serializeImpl(stream);
      }
    } else {
      ReadWriteIOUtils.write(false, stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    if (ReadWriteIOUtils.readBool(buffer)) {
      subPlans = new ArrayList<>();
      int size = ReadWriteIOUtils.readInt(buffer);
      for (int i = 0; i < size; i++) {
        // This will read the type of the plan (AlterTopicPlan), which will be ignored
        ReadWriteIOUtils.readShort(buffer);

        AlterTopicPlan subPlan = new AlterTopicPlan();
        subPlan.deserializeImpl(buffer);
        subPlans.add(subPlan);
      }
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
    AlterMultipleTopicsPlan that = (AlterMultipleTopicsPlan) obj;
    return Objects.equals(this.subPlans, that.subPlans);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subPlans);
  }

  @Override
  public String toString() {
    return "AlterMultipleTopicPlan{" + "subPlans='" + subPlans + "'}";
  }
}
