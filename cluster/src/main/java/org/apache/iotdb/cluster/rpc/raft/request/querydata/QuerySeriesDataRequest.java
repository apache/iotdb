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
package org.apache.iotdb.cluster.rpc.raft.request.querydata;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.request.BasicQueryRequest;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;

public class QuerySeriesDataRequest extends BasicQueryRequest {

  private static final long serialVersionUID = 7132891920951977625L;

  /**
   * Request stage
   */
  private Stage stage;

  /**
   * Corresponding jobid in remote query node
   */
  private long jobId;

  /**
   * Type of series
   */
  private PathType pathType;

  /**
   * Series list
   */
  private List<String> paths;

  /**
   * Physical plan list
   */
  private List<PhysicalPlan> physicalPlans;

  public QuerySeriesDataRequest(String groupID, int readConsistencyLevel,
      List<PhysicalPlan> physicalPlans, PathType pathType)
      throws IOException {
    super(groupID, readConsistencyLevel);
    this.physicalPlans = physicalPlans;
    this.stage = Stage.INITIAL;
    this.pathType = pathType;
  }

  public QuerySeriesDataRequest(String groupID, long jobId, List<String> paths, PathType pathType)
      throws IOException {
    super(groupID);
    this.paths = paths;
    stage = Stage.READ_DATA;
    this.jobId = jobId;
    this.pathType = pathType;
  }

  public Stage getStage() {
    return stage;
  }

  public void setStage(Stage stage) {
    this.stage = stage;
  }

  public PathType getPathType() {
    return pathType;
  }

  public void setPathType(PathType pathType) {
    this.pathType = pathType;
  }

  public List<String> getPaths() {
    return paths;
  }

  public void setPaths(List<String> paths) {
    this.paths = paths;
  }

  public long getJobId() {
    return jobId;
  }

  public void setJobId(long jobId) {
    this.jobId = jobId;
  }

  public List<PhysicalPlan> getPhysicalPlans() {
    return physicalPlans;
  }

  public void setPhysicalPlans(List<PhysicalPlan> physicalPlans) {
    this.physicalPlans = physicalPlans;
  }
}
