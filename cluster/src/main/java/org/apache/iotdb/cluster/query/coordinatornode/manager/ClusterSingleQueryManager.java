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

import com.alipay.sofa.jraft.entity.PeerId;
import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;

public class ClusterSingleQueryManager implements IClusterSingleQueryManager {

  /**
   * Query job id assigned by QueryResourceManager.
   */
  private long jobId;

  /**
   * Origin query plan parsed by QueryProcessor
   */
  private QueryPlan originPhysicalPlan;

  /**
   * Represent selected reader nodes, key is group id and value is selected peer id
   */
  private Map<String, PeerId> readerNodes = new HashMap<>();

  /**
   * Physical plans of select paths which are divided from originPhysicalPlan
   */
  private Map<String, QueryPlan> selectPathPlans = new HashMap<>();

  /**
   * Physical plans of filter paths which are divided from originPhysicalPlan
   */
  private Map<String, QueryPlan> filterPathPlans = new HashMap<>();

  public ClusterSingleQueryManager(long jobId,
      QueryPlan originPhysicalPlan) {
    this.jobId = jobId;
    this.originPhysicalPlan = originPhysicalPlan;
  }

  @Override
  public void dividePhysicalPlan() {
//    List<Path>
//    MManager.getInstance().getFileNameByPath()
  }

  @Override
  public PhysicalPlan getSelectPathPhysicalPlan(String fullPath) {
    return selectPathPlans.get(fullPath);
  }

  @Override
  public PhysicalPlan getFilterPathPhysicalPlan(String fullPath) {
    return filterPathPlans.get(fullPath);
  }

  @Override
  public void setDataGroupReaderNode(String groupId, PeerId readerNode) {
    readerNodes.put(groupId, readerNode);
  }

  @Override
  public PeerId getDataGroupReaderNode(String groupId) {
    return readerNodes.get(groupId);
  }

  @Override
  public void releaseQueryResource() {

  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public PhysicalPlan getOriginPhysicalPlan() {
    return originPhysicalPlan;
  }

  public void setOriginPhysicalPlan(QueryPlan originPhysicalPlan) {
    this.originPhysicalPlan = originPhysicalPlan;
  }
}
