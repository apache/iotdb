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
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.PipeInclusionNormalizer.getPartialPaths;

/**
 * {@link PipeConfigPlanFilter} is to classify the {@link ConfigPhysicalPlan}s to help {@link
 * ConfigPlanListeningQueue} and pipe to collect, and to help receiver execute.
 *
 * <p>Note that we do not transfer the rollback version of {@link CommitSetSchemaTemplatePlan}
 * because the rollback is usually useless. Consensus layer ensures that a failed plan won't be
 * written to peer, consequently won't be extracted by {@link ConfigPlanListeningQueue}.
 */
public class PipeConfigPlanFilter {

  private static final Map<PartialPath, List<ConfigPhysicalPlanType>> PLAN_MAP = new HashMap<>();

  static {
    try {
      PLAN_MAP.put(
          new PartialPath("schema.database.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateDatabase));
      PLAN_MAP.put(
          new PartialPath("schema.database.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.AlterDatabase));
      PLAN_MAP.put(
          new PartialPath("schema.database.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DeleteDatabase));

      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateSchemaTemplate));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.set"),
          Collections.singletonList(ConfigPhysicalPlanType.CommitSetSchemaTemplate));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.ExtendSchemaTemplate));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropSchemaTemplate));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.unset"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeUnsetTemplate));

      PLAN_MAP.put(
          new PartialPath("schema.timeseries.ordinary.delete"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeleteTimeSeries));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.view.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeleteLogicalView));
      PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.deactivate"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeactivateTemplate));

      PLAN_MAP.put(
          new PartialPath("schema.ttl"), Collections.singletonList(ConfigPhysicalPlanType.SetTTL));

      PLAN_MAP.put(
          new PartialPath("auth.role.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateRole));
      PLAN_MAP.put(
          new PartialPath("auth.role.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropRole));
      PLAN_MAP.put(
          new PartialPath("auth.role.grant"),
          Collections.singletonList(ConfigPhysicalPlanType.GrantRole));
      PLAN_MAP.put(
          new PartialPath("auth.role.revoke"),
          Collections.singletonList(ConfigPhysicalPlanType.RevokeRole));

      PLAN_MAP.put(
          new PartialPath("auth.user.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateUser));
      PLAN_MAP.put(
          new PartialPath("auth.user.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.UpdateUser));
      PLAN_MAP.put(
          new PartialPath("auth.user.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropUser));
      PLAN_MAP.put(
          new PartialPath("auth.user.grant"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.GrantUser, ConfigPhysicalPlanType.GrantRoleToUser)));
      PLAN_MAP.put(
          new PartialPath("auth.user.revoke"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.RevokeUser, ConfigPhysicalPlanType.RevokeRoleFromUser)));
    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static boolean shouldBeListenedByQueue(ConfigPhysicalPlan plan) {
    ConfigPhysicalPlanType type = plan.getType();
    // Do not transfer roll back set template plan
    if (type.equals(ConfigPhysicalPlanType.CommitSetSchemaTemplate)
        && ((CommitSetSchemaTemplatePlan) plan).isRollback()) {
      return false;
    }
    // Do not transfer system database plan
    if (type.equals(ConfigPhysicalPlanType.CreateDatabase)
        && ((DatabaseSchemaPlan) plan)
            .getSchema()
            .getName()
            .equals(SchemaConstant.SYSTEM_DATABASE)) {
      return false;
    }
    return type.equals(ConfigPhysicalPlanType.PipeEnriched)
        || type.equals(ConfigPhysicalPlanType.UnsetTemplate)
        || PLAN_MAP.values().stream().anyMatch(types -> types.contains(type));
  }

  public static Set<ConfigPhysicalPlanType> getPipeListenSet(PipeParameters parameters)
      throws IllegalPathException, IllegalArgumentException {
    String inclusionStr =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
            EXTRACTOR_INCLUSION_DEFAULT_VALUE);
    String exclusionStr =
        parameters.getStringOrDefault(
            Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
            EXTRACTOR_EXCLUSION_DEFAULT_VALUE);

    Set<ConfigPhysicalPlanType> planTypes = new HashSet<>();
    List<PartialPath> inclusionPath = getPartialPaths(inclusionStr);
    List<PartialPath> exclusionPath = getPartialPaths(exclusionStr);
    inclusionPath.forEach(
        inclusion ->
            planTypes.addAll(
                PLAN_MAP.keySet().stream()
                    .filter(path -> path.overlapWithFullPathPrefix(inclusion))
                    .map(PLAN_MAP::get)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet())));
    exclusionPath.forEach(
        exclusion ->
            planTypes.removeAll(
                PLAN_MAP.keySet().stream()
                    .filter(path -> path.overlapWithFullPathPrefix(exclusion))
                    .map(PLAN_MAP::get)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet())));

    return planTypes;
  }

  private PipeConfigPlanFilter() {
    // Utility class
  }
}
