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

package org.apache.iotdb.confignode.manager.pipe.transfer.extractor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.datastructure.PipeInclusionSubstituter.getPartialPaths;

/**
 * {@link PipeConfigPlanFilter} is to classify the {@link ConfigPhysicalPlan}s to help linkedList
 * and pipe to collect, and to help receiver execute.
 *
 * <p>Note that we do not transfer the rollback version of {@link CommitSetSchemaTemplatePlan}
 * because the rollback is usually useless. Consensus layer ensures that a failed plan won't be
 * written to peer, consequently won't be extracted by linkedListQueue. Under that circumstance, a
 * rollback operation, if extracted, will cause an empty inverse operation on the receiver cluster,
 * and result in failure eventually.
 */
class PipeConfigPlanFilter {

  private static final Map<PartialPath, ConfigPhysicalPlanType> PLAN_MAP = new HashMap<>();

  static {
    try {
      PLAN_MAP.put(
          new PartialPath("schema.database.create"), ConfigPhysicalPlanType.CreateDatabase);
      PLAN_MAP.put(new PartialPath("schema.database.alter"), ConfigPhysicalPlanType.AlterDatabase);
      PLAN_MAP.put(new PartialPath("schema.database.drop"), ConfigPhysicalPlanType.DeleteDatabase);

      PLAN_MAP.put(
          new PartialPath("schema.template.create"), ConfigPhysicalPlanType.CreateSchemaTemplate);
      PLAN_MAP.put(
          new PartialPath("schema.template.set"), ConfigPhysicalPlanType.CommitSetSchemaTemplate);
      PLAN_MAP.put(
          new PartialPath("schema.template.alter"), ConfigPhysicalPlanType.ExtendSchemaTemplate);
      PLAN_MAP.put(
          new PartialPath("schema.template.drop"), ConfigPhysicalPlanType.DropSchemaTemplate);
      PLAN_MAP.put(new PartialPath("schema.template.unset"), ConfigPhysicalPlanType.UnsetTemplate);

      PLAN_MAP.put(new PartialPath("auth.role.create"), ConfigPhysicalPlanType.CreateRole);
      PLAN_MAP.put(new PartialPath("auth.role.drop"), ConfigPhysicalPlanType.DropRole);
      PLAN_MAP.put(new PartialPath("auth.role.grant"), ConfigPhysicalPlanType.GrantRole);
      PLAN_MAP.put(new PartialPath("auth.role.revoke"), ConfigPhysicalPlanType.RevokeRole);

      PLAN_MAP.put(new PartialPath("auth.user.create"), ConfigPhysicalPlanType.CreateUser);
      PLAN_MAP.put(new PartialPath("auth.user.alter"), ConfigPhysicalPlanType.UpdateUser);
      PLAN_MAP.put(new PartialPath("auth.user.drop"), ConfigPhysicalPlanType.DropUser);
      PLAN_MAP.put(new PartialPath("auth.user.grant"), ConfigPhysicalPlanType.GrantUser);
      PLAN_MAP.put(new PartialPath("auth.user.revoke"), ConfigPhysicalPlanType.RevokeUser);

      PLAN_MAP.put(new PartialPath("auth.grantRoleToUser"), ConfigPhysicalPlanType.GrantRoleToUser);
      PLAN_MAP.put(
          new PartialPath("auth.revokeRoleFromUser"), ConfigPhysicalPlanType.RevokeRoleFromUser);

      PLAN_MAP.put(new PartialPath("ttl"), ConfigPhysicalPlanType.SetTTL);
    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static boolean shouldBeListenedByQueue(ConfigPhysicalPlan plan) {
    ConfigPhysicalPlanType type = plan.getType();
    if (type.equals(ConfigPhysicalPlanType.CommitSetSchemaTemplate)
        && ((CommitSetSchemaTemplatePlan) plan).isRollback()) {
      return false;
    }
    return PLAN_MAP.containsValue(type);
  }

  static Set<ConfigPhysicalPlanType> getPipeListenSet(
      List<String> inclusions, List<String> exclusions, boolean forwardPipeRequests)
      throws IllegalPathException, IllegalArgumentException {
    Set<ConfigPhysicalPlanType> planTypes = new HashSet<>();
    List<PartialPath> inclusionPath = getPartialPaths(inclusions);
    List<PartialPath> exclusionPath = getPartialPaths(exclusions);
    List<String> exceptionMessages = new ArrayList<>();
    inclusionPath.forEach(
        inclusion -> {
          Set<ConfigPhysicalPlanType> types =
              PLAN_MAP.keySet().stream()
                  .filter(path -> path.overlapWithFullPathPrefix(inclusion))
                  .map(PLAN_MAP::get)
                  .collect(Collectors.toSet());
          if (types.isEmpty()) {
            exceptionMessages.add(
                String.format("The inclusion argument %s is not legal", inclusion.getFullPath()));
          }
          planTypes.addAll(types);
        });
    exclusionPath.forEach(
        exclusion -> {
          Set<ConfigPhysicalPlanType> types =
              PLAN_MAP.keySet().stream()
                  .filter(path -> path.overlapWithFullPathPrefix(exclusion))
                  .map(PLAN_MAP::get)
                  .collect(Collectors.toSet());
          if (types.isEmpty()) {
            exceptionMessages.add(
                String.format("The exclusion argument %s is not legal", exclusion.getFullPath()));
          }
          planTypes.removeAll(types);
        });

    if (forwardPipeRequests) {
      planTypes.add(ConfigPhysicalPlanType.PipeEnriched);
    }
    if (!exceptionMessages.isEmpty()) {
      throw new IllegalArgumentException(String.join("; ", exceptionMessages));
    }
    return planTypes;
  }

  private PipeConfigPlanFilter() {
    // Utility class
  }
}
