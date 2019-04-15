/**
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
package org.apache.iotdb.cluster.query.coordinatornode.manager;

import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public interface IClusterQueryManager {

  /**
   * Register a query, divide physical plan into several sub physical plans according to timeseries
   * full path.
   *
   * @param jobId Query job id assigned by QueryResourceManager.
   * @param plan Physical plan parsed by QueryProcessor
   */
  void registerQuery(Long jobId, PhysicalPlan plan);

  /**
   * Get physical plan of select path
   *
   * @param jobId Query job id assigned by QueryResourceManager.
   * @param path Timeseries full path in select paths
   */
  PhysicalPlan getSelectPathPhysicalPlan(Long jobId, String path);

  /**
   * Get physical plan of filter path
   *
   * @param jobId Query job id assigned by QueryResourceManager.
   * @param path Timeseries full path in filter
   */
  PhysicalPlan getFilterPathPhysicalPlan(Long jobId, String path);


  /**
   * Remove resource of a job id
   */
  void remove(Long jobId);
}
