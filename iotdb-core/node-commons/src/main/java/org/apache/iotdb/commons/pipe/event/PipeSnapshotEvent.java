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

package org.apache.iotdb.commons.pipe.event;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.resource.snapshot.PipeSnapshotResourceManager;

import java.util.Set;
import java.util.stream.Collectors;

public abstract class PipeSnapshotEvent extends EnrichedEvent implements SerializableEvent {
  protected final PipeSnapshotResourceManager resourceManager;

  protected ProgressIndex progressIndex;
  protected Set<Short> transferredTypes;

  protected PipeSnapshotEvent(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final PipeSnapshotResourceManager resourceManager) {
    super(
        pipeName,
        creationTime,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
    this.resourceManager = resourceManager;
  }

  @Override
  public void bindProgressIndex(final ProgressIndex progressIndex) {
    this.progressIndex = progressIndex;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return progressIndex;
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

  /////////////////////////////// Type parsing ///////////////////////////////

  public String toSealTypeString() {
    return String.join(
        ",",
        transferredTypes.stream().map(type -> Short.toString(type)).collect(Collectors.toSet()));
  }

  /////////////////////////////// Object ///////////////////////////////

  @Override
  public String toString() {
    return String.format(
            "PipeSnapshotEvent{progressIndex=%s, transferredTypes=%s}",
            progressIndex, transferredTypes)
        + " - "
        + super.toString();
  }

  @Override
  public String coreReportMessage() {
    return String.format(
            "PipeSnapshotEvent{progressIndex=%s, transferredTypes=%s}",
            progressIndex, transferredTypes)
        + " - "
        + super.coreReportMessage();
  }
}
