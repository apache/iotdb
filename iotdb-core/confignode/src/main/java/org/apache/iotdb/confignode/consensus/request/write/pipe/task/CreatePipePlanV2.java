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

package org.apache.iotdb.confignode.consensus.request.write.pipe.task;

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeRuntimeMeta;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeStaticMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public class CreatePipePlanV2 extends ConfigPhysicalPlan {

  private PipeStaticMeta pipeStaticMeta;
  private PipeRuntimeMeta pipeRuntimeMeta;

  public CreatePipePlanV2() {
    super(ConfigPhysicalPlanType.CreatePipeV2);
  }

  public CreatePipePlanV2(PipeStaticMeta pipeStaticMeta, PipeRuntimeMeta pipeRuntimeMeta) {
    super(ConfigPhysicalPlanType.CreatePipeV2);
    this.pipeStaticMeta = pipeStaticMeta;
    this.pipeRuntimeMeta = pipeRuntimeMeta;
  }

  public PipeStaticMeta getPipeStaticMeta() {
    return pipeStaticMeta;
  }

  public PipeRuntimeMeta getPipeRuntimeMeta() {
    return pipeRuntimeMeta;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    pipeStaticMeta.serialize(stream);
    pipeRuntimeMeta.serialize(stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeStaticMeta = PipeStaticMeta.deserialize(buffer);
    pipeRuntimeMeta = PipeRuntimeMeta.deserialize(buffer);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreatePipePlanV2 that = (CreatePipePlanV2) obj;
    return pipeStaticMeta.equals(that.pipeStaticMeta)
        && pipeRuntimeMeta.equals(that.pipeRuntimeMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeStaticMeta, pipeRuntimeMeta);
  }

  @Override
  public String toString() {
    return "CreatePipePlanV2{"
        + "pipeStaticMeta='"
        + pipeStaticMeta
        + "', pipeRuntimeMeta='"
        + pipeRuntimeMeta
        + "'}";
  }
}
