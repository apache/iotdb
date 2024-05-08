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

/** the class of id of the fragment instance task */
public class DriverTaskID implements ID, Comparable<DriverTaskID> {

  private final FragmentInstanceId id;

  public DriverTaskID(FragmentInstanceId id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof DriverTaskID && ((DriverTaskID) o).id.equals(id);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  public String toString() {
    return id.getFullId();
  }

  public String getInstanceId() {
    return id.getInstanceId();
  }

  public PlanFragmentId getFragmentId() {
    return id.getFragmentId();
  }

  public QueryId getQueryId() {
    return id.getQueryId();
  }

  // This is the default comparator of FragmentInstanceID
  @Override
  public int compareTo(@NotNull DriverTaskID o) {
    return String.CASE_INSENSITIVE_ORDER.compare(this.toString(), o.toString());
  }
}
