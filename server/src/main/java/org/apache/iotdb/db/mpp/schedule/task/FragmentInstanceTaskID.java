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
package org.apache.iotdb.db.mpp.schedule.task;

import org.apache.iotdb.db.mpp.common.FragmentId;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.schedule.queue.ID;

import org.jetbrains.annotations.NotNull;

/** the class of id of the fragment instance task */
public class FragmentInstanceTaskID implements ID, Comparable<FragmentInstanceTaskID> {

  private final FragmentInstanceId id;

  public FragmentInstanceTaskID(FragmentInstanceId id) {
    this.id = id;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FragmentInstanceTaskID
        && id.getQueryId().equals(((FragmentInstanceTaskID) o).getQueryId())
        && id.getFragmentId().getId() == ((FragmentInstanceTaskID) o).getFragmentId().getId()
        && id.getInstanceId().equals(((FragmentInstanceTaskID) o).getInstanceId());
  }

  public String toString() {
    return id.getFullId();
  }

  public String getInstanceId() {
    return id.getInstanceId();
  }

  public FragmentId getFragmentId() {
    return id.getFragmentId();
  }

  public QueryId getQueryId() {
    return id.getQueryId();
  }

  // This is the default comparator of FragmentInstanceID
  @Override
  public int compareTo(@NotNull FragmentInstanceTaskID o) {
    return String.CASE_INSENSITIVE_ORDER.compare(this.toString(), o.toString());
  }
}
