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
package org.apache.iotdb.cluster.query.manager.coordinatornode;

import java.util.Map;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

/**
 * Manage all query series reader resources which fetch data from remote query nodes in coordinator
 * node
 */
public interface IClusterRpcQueryManager {

  /**
   * Add a query
   *
   * @param jobId job id assigned by QueryResourceManager
   * @param physicalPlan physical plan
   */
  void addSingleQuery(long jobId, QueryPlan physicalPlan);

  /**
   * Get full task id (local address + job id)
   */
  String createTaskId(long jobId);

  /**
   * Get query manager by jobId
   *
   * @param jobId job id assigned by QueryResourceManager
   */
  ClusterRpcSingleQueryManager getSingleQuery(long jobId);

  /**
   * Get query manager by taskId
   *
   * @param taskId task id assigned by getAndIncreaTaskId() method
   */
  ClusterRpcSingleQueryManager getSingleQuery(String taskId);

  /**
   * Release query resource
   *
   * @param jobId job id
   */
  void releaseQueryResource(long jobId) throws RaftConnectionException;

  /**
   * Get all read usage count group by data group id, key is group id, value is usage count
   */
  Map<String, Integer> getAllReadUsage();

  /**
   * Close manager
   */
  void close() throws RaftConnectionException;
}
