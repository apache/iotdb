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

package org.apache.iotdb.confignode.consensus.request.write.subscription.consumer.runtime;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ConsumerGroupHandleMetaChangePlan extends ConfigPhysicalPlan {

  private List<ConsumerGroupMeta> consumerGroupMetaList = new ArrayList<>();

  public ConsumerGroupHandleMetaChangePlan() {
    super(ConfigPhysicalPlanType.ConsumerGroupHandleMetaChange);
  }

  public ConsumerGroupHandleMetaChangePlan(List<ConsumerGroupMeta> consumerGroupMetaList) {
    super(ConfigPhysicalPlanType.ConsumerGroupHandleMetaChange);
    this.consumerGroupMetaList = consumerGroupMetaList;
  }

  public List<ConsumerGroupMeta> getConsumerGroupMetaList() {
    return consumerGroupMetaList;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());

    stream.writeInt(consumerGroupMetaList.size());
    for (ConsumerGroupMeta consumerGroupMeta : consumerGroupMetaList) {
      consumerGroupMeta.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    int size = buffer.getInt();
    for (int i = 0; i < size; i++) {
      ConsumerGroupMeta consumerGroupMeta = ConsumerGroupMeta.deserialize(buffer);
      consumerGroupMetaList.add(consumerGroupMeta);
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
    ConsumerGroupHandleMetaChangePlan that = (ConsumerGroupHandleMetaChangePlan) obj;
    return Objects.equals(this.consumerGroupMetaList, that.consumerGroupMetaList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupMetaList);
  }
}
