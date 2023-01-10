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

package org.apache.iotdb.db.mpp.execution.schedule.task;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.schedule.queue.ID;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

/** the class of id of the pipeline driver task */
public class DriverTaskId implements ID, Comparable<DriverTaskId> {

  private final FragmentInstanceId fragmentInstanceId;
  // TODO Create another field to store id of driver level
  // Currently, we just save pipelineId in driverTask since it's one-to-one relation.
  private final int pipelineId;
  private final String fullId;

  public DriverTaskId(FragmentInstanceId id, int pipelineId) {
    this.fragmentInstanceId = id;
    this.pipelineId = pipelineId;
    this.fullId = String.format("%s.%d", id.getFullId(), pipelineId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fragmentInstanceId, pipelineId);
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DriverTaskId
        && ((DriverTaskId) o).fragmentInstanceId.equals(fragmentInstanceId)
        && ((DriverTaskId) o).pipelineId == pipelineId;
  }

  public String toString() {
    return fullId;
  }

  public FragmentInstanceId getFragmentInstanceId() {
    return fragmentInstanceId;
  }

  public PlanFragmentId getFragmentId() {
    return fragmentInstanceId.getFragmentId();
  }

  public QueryId getQueryId() {
    return fragmentInstanceId.getQueryId();
  }

  public int getPipelineId() {
    return pipelineId;
  }

  public String getFullId() {
    return fullId;
  }

  // This is the default comparator of FragmentInstanceID
  @Override
  public int compareTo(@NotNull DriverTaskId o) {
    return String.CASE_INSENSITIVE_ORDER.compare(this.toString(), o.toString());
  }
}
