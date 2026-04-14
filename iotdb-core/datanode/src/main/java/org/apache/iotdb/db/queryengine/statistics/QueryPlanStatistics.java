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

package org.apache.iotdb.db.queryengine.statistics;

public class QueryPlanStatistics {
  private static final String DEFAULT_PLAN_CACHE_STATUS = "DISABLED";
  private static final String DEFAULT_PLAN_CACHE_STATE = "N/A";

  private long analyzeCost;
  private long fetchPartitionCost;
  private long fetchSchemaCost;
  private long logicalPlanCost;
  private long logicalOptimizationCost;
  private long distributionPlanCost;
  private long dispatchCost = 0;
  private String planCacheStatus = DEFAULT_PLAN_CACHE_STATUS;
  private String planCacheState = DEFAULT_PLAN_CACHE_STATE;
  private String planCacheReason = "";
  private long planCacheLookupCost;
  private long savedLogicalPlanningCost;
  private long reusablePlanningCost;
  private long firstResponseLatency;

  public void setAnalyzeCost(long analyzeCost) {
    this.analyzeCost = analyzeCost;
  }

  public void setFetchPartitionCost(long fetchPartitionCost) {
    this.fetchPartitionCost = fetchPartitionCost;
  }

  public void setFetchSchemaCost(long fetchSchemaCost) {
    this.fetchSchemaCost = fetchSchemaCost;
  }

  public void setLogicalPlanCost(long logicalPlanCost) {
    this.logicalPlanCost = logicalPlanCost;
  }

  public void setDistributionPlanCost(long distributionPlanCost) {
    this.distributionPlanCost = distributionPlanCost;
  }

  public void setLogicalOptimizationCost(long logicalOptimizationCost) {
    this.logicalOptimizationCost = logicalOptimizationCost;
  }

  public long getAnalyzeCost() {
    return analyzeCost;
  }

  public long getFetchPartitionCost() {
    return fetchPartitionCost;
  }

  public long getFetchSchemaCost() {
    return fetchSchemaCost;
  }

  public long getLogicalPlanCost() {
    return logicalPlanCost;
  }

  public long getDistributionPlanCost() {
    return distributionPlanCost;
  }

  public long getLogicalOptimizationCost() {
    return logicalOptimizationCost;
  }

  public void recordDispatchCost(long dispatchCost) {
    this.dispatchCost += dispatchCost;
  }

  public long getDispatchCost() {
    return dispatchCost;
  }

  public void setPlanCacheStatus(String planCacheStatus) {
    this.planCacheStatus = planCacheStatus;
    this.planCacheReason = "";
  }

  public void setPlanCacheStatus(String planCacheStatus, String planCacheReason) {
    this.planCacheStatus = planCacheStatus;
    this.planCacheReason = planCacheReason;
  }

  public String getPlanCacheStatus() {
    return planCacheStatus;
  }

  public String getPlanCacheState() {
    return planCacheState;
  }

  public void setPlanCacheState(String planCacheState) {
    this.planCacheState = planCacheState;
  }

  public String getPlanCacheReason() {
    return planCacheReason;
  }

  public long getPlanCacheLookupCost() {
    return planCacheLookupCost;
  }

  public void setPlanCacheLookupCost(long planCacheLookupCost) {
    this.planCacheLookupCost = planCacheLookupCost;
  }

  public long getSavedLogicalPlanningCost() {
    return savedLogicalPlanningCost;
  }

  public void setSavedLogicalPlanningCost(long savedLogicalPlanningCost) {
    this.savedLogicalPlanningCost = savedLogicalPlanningCost;
  }

  public long getReusablePlanningCost() {
    return reusablePlanningCost;
  }

  public void setReusablePlanningCost(long reusablePlanningCost) {
    this.reusablePlanningCost = reusablePlanningCost;
  }

  public long getFirstResponseLatency() {
    return firstResponseLatency;
  }

  public void setFirstResponseLatency(long firstResponseLatency) {
    this.firstResponseLatency = firstResponseLatency;
  }
}
