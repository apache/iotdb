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

  // Plan cache profile diagnostics (populated when verbose)
  private double ewmaReusablePlanningCost;
  private double ewmaFirstResponseLatency;
  private double ewmaBenefitRatio;
  private long profileSampleCount;
  private long profileHitCount;
  private long profileMissCount;
  private long profileBypassCount;
  private long minReusablePlanningCostThreshold;
  private double admitRatioThreshold;
  private double bypassRatioThreshold;

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

  public double getEwmaReusablePlanningCost() {
    return ewmaReusablePlanningCost;
  }

  public void setEwmaReusablePlanningCost(double ewmaReusablePlanningCost) {
    this.ewmaReusablePlanningCost = ewmaReusablePlanningCost;
  }

  public double getEwmaFirstResponseLatency() {
    return ewmaFirstResponseLatency;
  }

  public void setEwmaFirstResponseLatency(double ewmaFirstResponseLatency) {
    this.ewmaFirstResponseLatency = ewmaFirstResponseLatency;
  }

  public double getEwmaBenefitRatio() {
    return ewmaBenefitRatio;
  }

  public void setEwmaBenefitRatio(double ewmaBenefitRatio) {
    this.ewmaBenefitRatio = ewmaBenefitRatio;
  }

  public long getProfileSampleCount() {
    return profileSampleCount;
  }

  public void setProfileSampleCount(long profileSampleCount) {
    this.profileSampleCount = profileSampleCount;
  }

  public long getProfileHitCount() {
    return profileHitCount;
  }

  public void setProfileHitCount(long profileHitCount) {
    this.profileHitCount = profileHitCount;
  }

  public long getProfileMissCount() {
    return profileMissCount;
  }

  public void setProfileMissCount(long profileMissCount) {
    this.profileMissCount = profileMissCount;
  }

  public long getProfileBypassCount() {
    return profileBypassCount;
  }

  public void setProfileBypassCount(long profileBypassCount) {
    this.profileBypassCount = profileBypassCount;
  }

  public long getMinReusablePlanningCostThreshold() {
    return minReusablePlanningCostThreshold;
  }

  public void setMinReusablePlanningCostThreshold(long minReusablePlanningCostThreshold) {
    this.minReusablePlanningCostThreshold = minReusablePlanningCostThreshold;
  }

  public double getAdmitRatioThreshold() {
    return admitRatioThreshold;
  }

  public void setAdmitRatioThreshold(double admitRatioThreshold) {
    this.admitRatioThreshold = admitRatioThreshold;
  }

  public double getBypassRatioThreshold() {
    return bypassRatioThreshold;
  }

  public void setBypassRatioThreshold(double bypassRatioThreshold) {
    this.bypassRatioThreshold = bypassRatioThreshold;
  }
}
