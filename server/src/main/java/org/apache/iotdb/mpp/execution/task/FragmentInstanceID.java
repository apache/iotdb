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
package org.apache.iotdb.mpp.execution.task;

import org.apache.iotdb.mpp.execution.queue.ID;

import org.jetbrains.annotations.NotNull;

/** the class of id of the fragment instance */
public class FragmentInstanceID implements ID, Comparable<FragmentInstanceTask> {

  private final String instanceId;
  private final String fragmentId;
  private final String queryId;

  public FragmentInstanceID(String queryId, String fragmentId, String instanceId) {
    this.queryId = queryId;
    this.fragmentId = fragmentId;
    this.instanceId = instanceId;
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof FragmentInstanceID
        && queryId.equals(((FragmentInstanceID) o).getQueryId())
        && fragmentId.equals(((FragmentInstanceID) o).getFragmentId())
        && instanceId.equals(((FragmentInstanceID) o).getInstanceId());
  }

  public String toString() {
    return String.format("%s.%s.%s", getInstanceId(), getFragmentId(), getQueryId());
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getFragmentId() {
    return fragmentId;
  }

  public String getQueryId() {
    return queryId;
  }

  // This is the default comparator of FragmentInstanceID
  @Override
  public int compareTo(@NotNull FragmentInstanceTask o) {
    return String.CASE_INSENSITIVE_ORDER.compare(this.toString(), o.toString());
  }
}
