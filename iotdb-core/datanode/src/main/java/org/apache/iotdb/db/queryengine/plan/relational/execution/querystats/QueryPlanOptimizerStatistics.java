/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.execution.querystats;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed
 * across versions.
 */
public class QueryPlanOptimizerStatistics {
  private final String rule;
  private final long invocations;
  private final long applied;
  private final long totalTime;
  private final long failures;

  public QueryPlanOptimizerStatistics(
      String rule, long invocations, long applied, long totalTime, long failures) {
    this.rule = requireNonNull(rule, "rule is null");
    this.invocations = invocations;
    this.applied = applied;
    this.totalTime = totalTime;
    this.failures = failures;
  }

  public String rule() {
    return rule;
  }

  public long invocations() {
    return invocations;
  }

  public long applied() {
    return applied;
  }

  public long totalTime() {
    return totalTime;
  }

  public long failures() {
    return failures;
  }
}
