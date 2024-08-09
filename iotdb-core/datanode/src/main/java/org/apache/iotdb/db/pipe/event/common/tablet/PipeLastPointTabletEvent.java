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

package org.apache.iotdb.db.pipe.event.common.tablet;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.pattern.PipePattern;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.processor.downsampling.PartialPathLastObjectCache;
import org.apache.iotdb.db.pipe.processor.downsampling.lastpoint.LastPointFilter;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.pipe.resource.memory.PipeTabletMemoryBlock;

import org.apache.tsfile.write.record.Tablet;

public class PipeLastPointTabletEvent extends EnrichedEvent {

  private Tablet tablet;

  private PipeTabletMemoryBlock allocatedMemoryBlock;

  private PartialPathLastObjectCache<LastPointFilter<?>> partialPathToLatestTimeCache;

  public PipeLastPointTabletEvent(
      final Tablet tablet,
      final PartialPathLastObjectCache<LastPointFilter<?>> partialPathToLatestTimeCache,
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final PipePattern pipePattern,
      final long startTime,
      final long endTime) {
    super(pipeName, creationTime, pipeTaskMeta, pipePattern, startTime, endTime);
    this.tablet = tablet;
    this.partialPathToLatestTimeCache = partialPathToLatestTimeCache;
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    allocatedMemoryBlock = PipeDataNodeResourceManager.memory().forceAllocateWithRetry(tablet);
    return true;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    allocatedMemoryBlock.close();
    tablet = null;
    return true;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return MinimumProgressIndex.INSTANCE;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      String pipeName,
      long creationTime,
      PipeTaskMeta pipeTaskMeta,
      PipePattern pattern,
      long startTime,
      long endTime) {
    return new PipeLastPointTabletEvent(
        tablet,
        partialPathToLatestTimeCache,
        pipeName,
        creationTime,
        pipeTaskMeta,
        pipePattern,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return true;
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return true;
  }

  /////////////////////////// Object ///////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeLastPointTabletEvent{tablet=%s, allocatedMemoryBlock=%s}",
            tablet, allocatedMemoryBlock)
        + " - "
        + super.toString();
  }

  /////////////////////////// Getter ///////////////////////////

  public Tablet getTablet() {
    return tablet;
  }

  public PartialPathLastObjectCache<LastPointFilter<?>> getPartialPathToLatestTimeCache() {
    return partialPathToLatestTimeCache;
  }
}
