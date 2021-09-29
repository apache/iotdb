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

package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import java.util.List;
import java.util.Map;

/** BatchPlan contains multiple sub-plans. */
public interface BatchPlan {

  /**
   * Mark the sub-plan at position i as executed.
   *
   * @param i the position of the sub-plan
   */
  void setIsExecuted(int i);

  /**
   * Mark the sub-plan at position i as not executed.
   *
   * @param i the position of the sub-plan
   */
  void unsetIsExecuted(int i);

  /**
   * Whether the sub-plan at position i has been executed.
   *
   * @param i the position of the sub-plan
   * @return whether the sub-plan at position i has been executed.
   */
  boolean isExecuted(int i);

  /**
   * Return how many sub-plans are in the plan
   *
   * @return how many sub-plans are in the plan.
   */
  int getBatchSize();

  /**
   * Return execution status for each path
   *
   * @return execution status for each path
   */
  Map<Integer, TSStatus> getResults();

  /**
   * Return prefix paths of all sub-plans
   *
   * @return prefix paths of all sub-plans
   */
  List<PartialPath> getPrefixPaths();
}
