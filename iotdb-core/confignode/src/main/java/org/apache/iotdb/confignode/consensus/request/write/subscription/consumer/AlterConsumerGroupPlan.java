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

package org.apache.iotdb.confignode.consensus.request.write.subscription.consumer;

import org.apache.iotdb.commons.subscription.meta.consumer.ConsumerGroupMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class AlterConsumerGroupPlan extends ConfigPhysicalPlan {

  private ConsumerGroupMeta consumerGroupMeta;

  public AlterConsumerGroupPlan() {
    super(ConfigPhysicalPlanType.AlterConsumerGroup);
  }

  public AlterConsumerGroupPlan(ConsumerGroupMeta consumerGroupMeta) {
    super(ConfigPhysicalPlanType.AlterConsumerGroup);
    this.consumerGroupMeta = consumerGroupMeta;
  }

  public ConsumerGroupMeta getConsumerGroupMeta() {
    return consumerGroupMeta;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    consumerGroupMeta.serialize(stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    consumerGroupMeta = ConsumerGroupMeta.deserialize(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AlterConsumerGroupPlan that = (AlterConsumerGroupPlan) obj;
    return Objects.equals(consumerGroupMeta, that.consumerGroupMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(consumerGroupMeta);
  }

  @Override
  public String toString() {
    return "AlterConsumerGroupPlan{" + "consumerGroupMeta='" + consumerGroupMeta + "'}";
  }
}
