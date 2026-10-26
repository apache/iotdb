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

package org.apache.iotdb.confignode.manager.lease;

import java.util.Collection;

/**
 * Pure decision logic for metadata-broadcast verdicts.
 *
 * <p>A DataNode that failed to ack is assumed to support self-fencing (enforced by the caller). The
 * overall verdict is {@code PROCEED} when every unacked DataNode has been silent for at least
 * {@code T_proceed}; otherwise {@code WAIT} until the retry budget is exhausted, then {@code FAIL}.
 */
public final class MetadataBroadcastVerdict {

  public enum Verdict {
    PROCEED,
    WAIT,
    FAIL
  }

  private MetadataBroadcastVerdict() {}

  /** Per-DataNode inputs for one broadcast round. */
  public static final class DataNodeState {
    private final boolean executeSuccess;
    private final long elapsedMsSinceLastSuccessfulHeartbeatResponse;

    public DataNodeState(
        final boolean executeSuccess, final long elapsedMsSinceLastSuccessfulHeartbeatResponse) {
      this.executeSuccess = executeSuccess;
      this.elapsedMsSinceLastSuccessfulHeartbeatResponse =
          elapsedMsSinceLastSuccessfulHeartbeatResponse;
    }
  }

  public static Verdict decide(
      final Collection<DataNodeState> states,
      final long fenceTimeOutsMs,
      final boolean waitBudgetExhausted) {
    for (final DataNodeState state : states) {
      if (!state.executeSuccess
          && state.elapsedMsSinceLastSuccessfulHeartbeatResponse < fenceTimeOutsMs) {
        return waitBudgetExhausted ? Verdict.FAIL : Verdict.WAIT;
      }
    }
    return Verdict.PROCEED;
  }
}
