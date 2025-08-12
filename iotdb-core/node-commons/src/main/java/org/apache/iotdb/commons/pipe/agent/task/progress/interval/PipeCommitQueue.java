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

import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.interval.IntervalManager;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;

public class PipeCommitQueue {
  private final IntervalManager<PipeCommitInterval> intervalManager = new IntervalManager<>();
  private long lastCommitted = 0;

  public void offer(final EnrichedEvent event) {
    final PipeCommitInterval interval =
        new PipeCommitInterval(
            event.getCommitId(),
            event.getCommitId(),
            event.isShouldReportOnCommit()
                ? event.getProgressIndex()
                : MinimumProgressIndex.INSTANCE,
            event.getOnCommittedHooks(),
            event.getPipeTaskMeta());
    intervalManager.addInterval(interval);
    if (interval.start == lastCommitted + 1) {
      intervalManager.remove(interval);
      lastCommitted = interval.end;
    }
  }

  public int size() {
    return intervalManager.size();
  }

  @Override
  public String toString() {
    return "PipeCommitQueue{"
        + "lastCommitted='"
        + lastCommitted
        + "', IntervalManager="
        + intervalManager
        + "}";
  }
}
