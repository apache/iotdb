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

public class AlterPipePlanV2 extends ConfigPhysicalPlan {

  private PipeStaticMeta currentPipeStaticMeta;
  private PipeStaticMeta pipeStaticMeta;
  private PipeRuntimeMeta pipeRuntimeMeta;
  private boolean currentPipeStaticMetaSet;

  public AlterPipePlanV2() {
    super(ConfigPhysicalPlanType.AlterPipeV2);
  }

  public AlterPipePlanV2(PipeStaticMeta pipeStaticMeta, PipeRuntimeMeta pipeRuntimeMeta) {
    super(ConfigPhysicalPlanType.AlterPipeV2);
    this.currentPipeStaticMeta = pipeStaticMeta;
    this.pipeStaticMeta = pipeStaticMeta;
    this.pipeRuntimeMeta = pipeRuntimeMeta;
  }

  public AlterPipePlanV2(
      PipeStaticMeta currentPipeStaticMeta,
      PipeStaticMeta pipeStaticMeta,
      PipeRuntimeMeta pipeRuntimeMeta) {
    super(ConfigPhysicalPlanType.AlterPipeV2);
    this.currentPipeStaticMeta = currentPipeStaticMeta;
    this.pipeStaticMeta = pipeStaticMeta;
    this.pipeRuntimeMeta = pipeRuntimeMeta;
    this.currentPipeStaticMetaSet = true;
  }

  public PipeStaticMeta getCurrentPipeStaticMeta() {
    return currentPipeStaticMeta;
  }

  public PipeStaticMeta getPipeStaticMeta() {
    return pipeStaticMeta;
  }

  public PipeRuntimeMeta getPipeRuntimeMeta() {
    return pipeRuntimeMeta;
  }

  public boolean isCurrentPipeStaticMetaSet() {
    return currentPipeStaticMetaSet;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    stream.writeShort(getType().getPlanType());
    pipeStaticMeta.serialize(stream);
    pipeRuntimeMeta.serialize(stream);
    if (currentPipeStaticMetaSet) {
      currentPipeStaticMeta.serialize(stream);
    }
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) throws IOException {
    pipeStaticMeta = PipeStaticMeta.deserialize(buffer);
    pipeRuntimeMeta = PipeRuntimeMeta.deserialize(buffer);
    deserializeCurrentPipeStaticMeta(buffer.hasRemaining(), buffer);
  }

  void deserializeImpl(final ByteBuffer buffer, final boolean isLastSubPlan) {
    pipeStaticMeta = PipeStaticMeta.deserialize(buffer);
    pipeRuntimeMeta = PipeRuntimeMeta.deserialize(buffer);
    deserializeCurrentPipeStaticMeta(
        OperateMultiplePipesPlanV2.hasTrailingFieldInSubPlan(buffer, isLastSubPlan), buffer);
  }

  private void deserializeCurrentPipeStaticMeta(
      final boolean hasCurrentPipeStaticMeta, final ByteBuffer buffer) {
    currentPipeStaticMetaSet = hasCurrentPipeStaticMeta;
    currentPipeStaticMeta =
        hasCurrentPipeStaticMeta ? PipeStaticMeta.deserialize(buffer) : pipeStaticMeta;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    AlterPipePlanV2 that = (AlterPipePlanV2) obj;
    return currentPipeStaticMeta.equals(that.currentPipeStaticMeta)
        && currentPipeStaticMetaSet == that.currentPipeStaticMetaSet
        && pipeStaticMeta.equals(that.pipeStaticMeta)
        && pipeRuntimeMeta.equals(that.pipeRuntimeMeta);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        currentPipeStaticMeta, pipeStaticMeta, pipeRuntimeMeta, currentPipeStaticMetaSet);
  }

  @Override
  public String toString() {
    return "AlterPipePlanV2{"
        + "currentPipeStaticMeta='"
        + currentPipeStaticMeta
        + "', pipeStaticMeta='"
        + pipeStaticMeta
        + "', pipeRuntimeMeta='"
        + pipeRuntimeMeta
        + "'}";
  }
}
