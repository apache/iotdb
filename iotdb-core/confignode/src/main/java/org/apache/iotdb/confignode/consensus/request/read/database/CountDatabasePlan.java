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

package org.apache.iotdb.confignode.consensus.request.read.database;

import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.read.ConfigPhysicalReadPlan;

import java.util.Arrays;
import java.util.List;

public class CountDatabasePlan extends ConfigPhysicalReadPlan {

  private final String[] storageGroupPattern;
  private final PathPatternTree scope;

  public CountDatabasePlan(final List<String> storageGroupPattern, final PathPatternTree scope) {
    super(ConfigPhysicalPlanType.CountDatabase);
    this.storageGroupPattern = storageGroupPattern.toArray(new String[0]);
    this.scope = scope;
  }

  public CountDatabasePlan(
      final ConfigPhysicalPlanType type,
      final List<String> storageGroupPattern,
      final PathPatternTree scope) {
    super(type);
    this.storageGroupPattern = storageGroupPattern.toArray(new String[0]);
    this.scope = scope;
  }

  public String[] getDatabasePattern() {
    return storageGroupPattern;
  }

  public PathPatternTree getScope() {
    return scope;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CountDatabasePlan that = (CountDatabasePlan) o;
    return Arrays.equals(storageGroupPattern, that.storageGroupPattern);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(storageGroupPattern);
  }
}
