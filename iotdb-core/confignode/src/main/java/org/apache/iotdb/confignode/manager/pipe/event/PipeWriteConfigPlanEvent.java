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

package org.apache.iotdb.confignode.manager.pipe.event;

import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeWriteConfigPlanEvent extends PipeWritePlanEvent {
  private ConfigPhysicalPlan physicalPlan;

  public PipeWriteConfigPlanEvent() {
    // Used for deserialization
    this(null, false);
  }

  public PipeWriteConfigPlanEvent(ConfigPhysicalPlan physicalPlan, boolean isGeneratedByPipe) {
    this(physicalPlan, isGeneratedByPipe, null, null, null);
  }

  public PipeWriteConfigPlanEvent(
      ConfigPhysicalPlan physicalPlan,
      boolean isGeneratedByPipe,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      String pattern) {
    super(isGeneratedByPipe, pipeName, pipeTaskMeta, pattern);
    this.physicalPlan = physicalPlan;
  }

  public ConfigPhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName, PipeTaskMeta pipeTaskMeta, String pattern, long startTime, long endTime) {
    return new PipeWriteConfigPlanEvent(
        physicalPlan, isGeneratedByPipe, pipeName, pipeTaskMeta, pattern);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer planBuffer = physicalPlan.serializeToByteBuffer();
    ByteBuffer result = ByteBuffer.allocate(Byte.BYTES * 2 + planBuffer.capacity());
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_PLAN.getType(), result);
    result.put(planBuffer);
    ReadWriteIOUtils.write(isGeneratedByPipe, result);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(ByteBuffer buffer) throws IOException {
    physicalPlan = ConfigPhysicalPlan.Factory.create(buffer);
    isGeneratedByPipe = ReadWriteIOUtils.readBool(buffer);
  }
}
