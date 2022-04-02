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
package org.apache.iotdb.db.mpp.schedule;

import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.QueryId;
import org.apache.iotdb.db.mpp.execution.ExecFragmentInstance;

import java.util.List;

/** the interface of fragment instance scheduling */
public interface IFragmentInstanceScheduler {

  /**
   * Submit one or more {@link ExecFragmentInstance} in one query for later scheduling.
   *
   * @param queryId the queryId these instances belong to.
   * @param instances the submitted instances.
   */
  void submitFragmentInstances(QueryId queryId, List<ExecFragmentInstance> instances);

  /**
   * Abort all the instances in this query.
   *
   * @param queryId the id of the query to be aborted.
   */
  void abortQuery(QueryId queryId);

  /**
   * Abort the fragment instance.
   *
   * @param instanceId the id of the fragment instance to be aborted.
   */
  void abortFragmentInstance(FragmentInstanceId instanceId);

  /** Fetch an {@link ExecFragmentInstance}. */
  void fetchFragmentInstance(ExecFragmentInstance instance);

  /**
   * Return the schedule priority of a fragment.
   *
   * @param instanceId the fragment instance id.
   * @return the schedule priority.
   * @throws IllegalStateException if the instance has already been cleared.
   */
  double getSchedulePriority(FragmentInstanceId instanceId);
}
