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

import com.alipay.sofa.jraft.entity.PeerId;
import java.io.IOException;
import org.apache.iotdb.cluster.exception.RaftConnectionException;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.query.QueryType;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

/**
 * Manage a single query.
 */
public interface IClusterRpcSingleQueryManager {

  /**
   * Divide physical plan into several sub physical plans according to timeseries full path.
   * @param queryType
   */
  void init(QueryType queryType, int readDataConsistencyLevel)
      throws PathErrorException, IOException, RaftConnectionException;

  /**
   * Fetch batch data from remote query node
   */
  void fetchData(String groupId, PathType pathType) throws RaftConnectionException;

  /**
   * Get query plan of select path
   *
   * @param fullPath Timeseries full path in select paths
   */
  QueryPlan getSelectPathQueryPlan(String fullPath);

  /**
   * Get query plan of filter path
   *
   * @param fullPath Timeseries full path in filter
   */
  QueryPlan getFilterPathQueryPlan(String fullPath);

  /**
   * Set reader node of a data group
   *
   * @param groupId data group id
   * @param readerNode peer id
   */
  void setDataGroupReaderNode(String groupId, PeerId readerNode);

  /**
   * Get reader node of a data group by group id
   * @param groupId data group id
   * @return peer id of reader node
   */
  PeerId getDataGroupReaderNode(String groupId);

  /**
   * Release query resource in remote query node
   */
  void releaseQueryResource() throws RaftConnectionException;
}
