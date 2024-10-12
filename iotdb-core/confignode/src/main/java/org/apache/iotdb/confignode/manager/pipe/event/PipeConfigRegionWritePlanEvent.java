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

import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeWritePlanEvent;
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
      final ConfigPhysicalPlan configPhysicalPlan, final boolean isGeneratedByPipe) {
    this(configPhysicalPlan, null, 0, null, null, null, isGeneratedByPipe);
  }

  public PipeConfigRegionWritePlanEvent(
      final ConfigPhysicalPlan configPhysicalPlan,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final boolean isGeneratedByPipe) {
    super(pipeName, creationTime, pipeTaskMeta, treePattern, tablePattern, isGeneratedByPipe);
    this.configPhysicalPlan = configPhysicalPlan;
  }

  public ConfigPhysicalPlan getConfigPhysicalPlan() {
    return configPhysicalPlan;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final long startTime,
      final long endTime) {
    return new PipeConfigRegionWritePlanEvent(
        configPhysicalPlan, pipeName, creationTime, pipeTaskMeta, treePattern, tablePattern, false);
  }

  @Override
  public ByteBuffer serializeToByteBuffer() {
    final ByteBuffer planBuffer = configPhysicalPlan.serializeToByteBuffer();
    final ByteBuffer result = ByteBuffer.allocate(Byte.BYTES * 2 + planBuffer.limit());
    ReadWriteIOUtils.write(PipeConfigSerializableEventType.CONFIG_WRITE_PLAN.getType(), result);
    ReadWriteIOUtils.write(isGeneratedByPipe, result);
    result.put(planBuffer);
    return result;
  }

  @Override
  public void deserializeFromByteBuffer(final ByteBuffer buffer) throws IOException {
    isGeneratedByPipe = ReadWriteIOUtils.readBool(buffer);
    configPhysicalPlan = ConfigPhysicalPlan.Factory.create(buffer);
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeConfigRegionWritePlanEvent{configPhysicalPlan=%s}", configPhysicalPlan)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeConfigRegionWritePlanEvent{configPhysicalPlan=%s}", configPhysicalPlan)
        + " - "
        + super.coreReportMessage();
  }
}
