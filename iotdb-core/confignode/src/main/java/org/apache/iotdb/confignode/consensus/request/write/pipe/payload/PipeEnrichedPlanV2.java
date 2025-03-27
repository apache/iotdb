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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class PipeEnrichedPlanV2 extends PipeEnrichedPlanV1 {

  private String originClusterId;

  public PipeEnrichedPlanV2() {
    super(ConfigPhysicalPlanType.PipeEnrichedV2);
  }

  public PipeEnrichedPlanV2(ConfigPhysicalPlan innerPlan, String originClusterId) {
    super(ConfigPhysicalPlanType.PipeEnrichedV2);
    this.innerPlan = innerPlan;
    this.originClusterId = originClusterId;
  }

  public String getOriginClusterId() {
    return originClusterId;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    super.serializeImpl(stream);
    ReadWriteIOUtils.write(originClusterId, stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    super.deserializeImpl(buffer);
    originClusterId = ReadWriteIOUtils.readString(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    PipeEnrichedPlanV2 that = (PipeEnrichedPlanV2) obj;
    return innerPlan.equals(that.innerPlan)
        && Objects.equals(originClusterId, that.originClusterId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(innerPlan, originClusterId);
  }

  @Override
  public String toString() {
    return "PipeEnrichedPlanV2{" + "innerPlan='" + innerPlan + "'}";
  }
}
