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
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PipeConfigRegionWritePlanEvent extends PipeWritePlanEvent {

  private ConfigPhysicalPlan configPhysicalPlan;

  public PipeConfigRegionWritePlanEvent() {
    // Used for deserialization
    this(null, false);
  }

  public PipeConfigRegionWritePlanEvent(
      ConfigPhysicalPlan configPhysicalPlan, boolean isGeneratedByPipe) {
    this(configPhysicalPlan, null, null, null, isGeneratedByPipe);
  }

  public PipeConfigRegionWritePlanEvent(
      ConfigPhysicalPlan configPhysicalPlan,
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      boolean isGeneratedByPipe) {
    super(pipeName, pipeTaskMeta, pattern, isGeneratedByPipe);
    this.configPhysicalPlan = configPhysicalPlan;
  }

  public ConfigPhysicalPlan getConfigPhysicalPlan() {
    return configPhysicalPlan;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      long startTime,
      long endTime) {
    return new PipeConfigRegionWritePlanEvent(
        configPhysicalPlan, pipeName, pipeTaskMeta, pattern, false);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    ByteBuffer planBuffer = configPhysicalPlan.serializeToByteBuffer();
    ByteBuffer result = ByteBuffer.allocate(Byte.BYTES * 2 + planBuffer.limit());
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_WRITE_PLAN.getType(), result);
    ReadWriteIOUtils.write(isGeneratedByPipe, result);
    result.put(planBuffer);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(ByteBuffer buffer) throws IOException {
    isGeneratedByPipe = ReadWriteIOUtils.readBool(buffer);
    configPhysicalPlan = ConfigPhysicalPlan.Factory.create(buffer);
  }
}
