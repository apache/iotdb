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

package org.apache.iotdb.commons.pipe.agent.task.progress.interval;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.interval.Interval;

import java.util.List;
import java.util.Objects;

public class PipeCommitInterval extends Interval<PipeCommitInterval> {

  private ProgressIndex currentIndex;
  private List<Runnable> onCommittedHooks;
  private final PipeTaskMeta pipeTaskMeta;

  public PipeCommitInterval(
      final long start,
      final long end,
      final ProgressIndex currentIndex,
      final List<Runnable> onCommittedHooks,
      final PipeTaskMeta pipeTaskMeta) {
    super(start, end);
    this.pipeTaskMeta = pipeTaskMeta;
    this.currentIndex =
        Objects.nonNull(currentIndex) ? currentIndex : MinimumProgressIndex.INSTANCE;
    this.onCommittedHooks = onCommittedHooks;
  }

  @Override
  public void onMerged(final PipeCommitInterval another) {
    currentIndex = currentIndex.updateToMinimumEqualOrIsAfterProgressIndex(another.currentIndex);

    // Keep in order
    if (this.start <= another.start) {
      onCommittedHooks.addAll(another.onCommittedHooks);
    } else {
      // Note that if merged, another interval is not supposed to be reused
      // thus we can arbitrarily alter its hooks
      another.onCommittedHooks.addAll(this.onCommittedHooks);
      this.onCommittedHooks = another.onCommittedHooks;
    }
  }

  @Override
  public void onRemoved() {
    if (Objects.nonNull(pipeTaskMeta)) {
      pipeTaskMeta.updateProgressIndex(currentIndex);
    }
    onCommittedHooks.forEach(Runnable::run);
  }

  @Override
  public String toString() {
    return "PipeCommitInterval{"
        + "progressIndex='"
        + currentIndex
        + "', range="
        + super.toString()
        + "}";
  }
}
