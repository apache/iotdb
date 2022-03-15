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
package org.apache.iotdb.mpp.execution;

import org.apache.iotdb.mpp.execution.task.FragmentInstanceID;

/** the interface of fragment instance scheduling */
public interface IFragmentInstanceManager {

  void submitFragmentInstance();

  /**
   * the notifying interface for {@link org.apache.iotdb.mpp.shuffle.IDataBlockManager} when
   * upstream data comes.
   *
   * @param instanceID the fragment instance to be notified.
   * @param upstreamInstanceId the upstream instance id.
   */
  void inputBlockAvailable(FragmentInstanceID instanceID, FragmentInstanceID upstreamInstanceId);

  /**
   * the notifying interface for {@link org.apache.iotdb.mpp.shuffle.IDataBlockManager} when
   * downstream data has been consumed.
   *
   * @param instanceID the fragment instance to be notified.
   */
  void outputBlockAvailable(FragmentInstanceID instanceID);

  /**
   * abort all the instances in this query
   *
   * @param queryId the id of the query to be aborted.
   */
  void abortQuery(String queryId);
}
