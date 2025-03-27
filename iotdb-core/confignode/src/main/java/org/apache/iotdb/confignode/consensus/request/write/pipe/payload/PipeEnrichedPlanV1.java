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

package org.apache.iotdb.confignode.consensus.request.write.pipe.payload;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeEnrichedPlanV1 extends ConfigPhysicalPlan {

  protected ConfigPhysicalPlan innerPlan;

  public PipeEnrichedPlanV1() {
    super(ConfigPhysicalPlanType.PipeEnrichedV1);
  }

  public PipeEnrichedPlanV1(ConfigPhysicalPlan innerPlan) {
    super(ConfigPhysicalPlanType.PipeEnrichedV1);
    this.innerPlan = innerPlan;
  }

  protected PipeEnrichedPlanV1(ConfigPhysicalPlanType type) {
    super(type);
  }

  public ConfigPhysicalPlan getInnerPlan() {
    return innerPlan;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    ByteBuffer buffer = innerPlan.serializeToByteBuffer();
    stream.write(buffer.array(), 0, buffer.limit());
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    innerPlan = ConfigPhysicalPlan.Factory.create(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeEnrichedPlanV1 that = (PipeEnrichedPlanV1) obj;
    return innerPlan.equals(that.innerPlan);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerPlan);
  }

  @Override
  public String toString() {
    return "PipeEnrichedPlanV1{" + "innerPlan='" + innerPlan + "'}";
  }
}
