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

package org.apache.iotdb.db.pipe.event.common.heartbeat;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.task.meta.PipeTaskMeta;
import org.apache.iotdb.db.pipe.event.EnrichedEvent;

/**
 * {@link PipeHeartbeatEvent} is used to sample the latency of the pipeline and trigger connector
 * flushing and retrying.
 */
public class PipeHeartbeatEvent extends EnrichedEvent {

  public PipeHeartbeatEvent() {
    super(null, null);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(String holderMessage) {
    return false;
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(String holderMessage) {
    return false;
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return null;
  }

  @Override
  public EnrichedEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      PipeTaskMeta pipeTaskMeta, String pattern) {
    return null;
  }

  @Override
  public boolean isGeneratedByPipe() {
    return false;
  }
}
