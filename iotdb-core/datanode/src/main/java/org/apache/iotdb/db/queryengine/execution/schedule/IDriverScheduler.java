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

package org.apache.iotdb.db.queryengine.execution.schedule;

import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.exception.CpuNotEnoughException;
import org.apache.iotdb.db.queryengine.exception.MemoryNotEnoughException;
import org.apache.iotdb.db.queryengine.execution.driver.IDriver;

import java.util.List;

/** The interface of fragment instance scheduling. */
public interface IDriverScheduler {

  /**
   * Submit one or more {@link IDriver} in one query for later scheduling.
   *
   * @param queryId the queryId these instances belong to.
   * @param instances the submitted instances.
   * @param timeOut the query timeout
   * @throws CpuNotEnoughException throw exception if CPU resources are not enough.
   * @throws MemoryNotEnoughException throw exception if Memory resources are not enough.
   */
  void submitDrivers(
      QueryId queryId, List<IDriver> instances, long timeOut, SessionInfo sessionInfo)
      throws CpuNotEnoughException, MemoryNotEnoughException;

  /**
   * Abort all the instances in this query.
   *
   * @param queryId the id of the query to be aborted.
   */
  void abortQuery(QueryId queryId);

  /**
   * Abort all Drivers of the fragment instance. If the instance is not existed, nothing will
   * happen.
   *
   * @param instanceId the id of the fragment instance to be aborted.
   */
  void abortFragmentInstance(FragmentInstanceId instanceId, Throwable t);
}
