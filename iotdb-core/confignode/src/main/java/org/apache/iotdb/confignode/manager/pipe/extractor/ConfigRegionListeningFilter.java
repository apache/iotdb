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

package org.apache.iotdb.confignode.manager.pipe.extractor;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
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

import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.getExclusionString;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.getInclusionString;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.parseOptions;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.tableOnlySyncPrefix;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.treeOnlySyncPrefix;

/**
 * {@link ConfigRegionListeningFilter} is to classify the {@link ConfigPhysicalPlan}s to help {@link
 * ConfigRegionListeningQueue} and pipe to collect, and to help receiver execute.
 *
 * <p>Note that we do not transfer the rollback version of {@link CommitSetSchemaTemplatePlan}
 * because the rollback is usually useless. Consensus layer ensures that a failed plan won't be
 * written to peer, consequently won't be extracted by {@link ConfigRegionListeningQueue}.
 */
public class ConfigRegionListeningFilter {

  private static final Map<PartialPath, List<ConfigPhysicalPlanType>> OPTION_PLAN_MAP =
      new HashMap<>();

  static {
    try {
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.database.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateDatabase));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.database.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.AlterDatabase));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.database.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DeleteDatabase));

      // Tree model
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateSchemaTemplate));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.set"),
          Collections.singletonList(ConfigPhysicalPlanType.CommitSetSchemaTemplate));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.ExtendSchemaTemplate));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropSchemaTemplate));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.unset"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.UnsetTemplate, ConfigPhysicalPlanType.PipeUnsetTemplate)));

      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.ordinary.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeleteTimeSeries));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.view.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeleteLogicalView));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.deactivate"),
          Collections.singletonList(ConfigPhysicalPlanType.PipeDeactivateTemplate));

      // Table Model
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.table.create"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.CommitCreateTable,
                  ConfigPhysicalPlanType.PipeCreateTable,
                  ConfigPhysicalPlanType.AddTableColumn)));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.table.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.SetTableProperties));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.table.drop"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.CommitDeleteTable,
                  ConfigPhysicalPlanType.CommitDeleteColumn,
                  ConfigPhysicalPlanType.PipeDeleteDevices)));

      OPTION_PLAN_MAP.put(
          new PartialPath("schema.ttl"), Collections.singletonList(ConfigPhysicalPlanType.SetTTL));

      OPTION_PLAN_MAP.put(
          new PartialPath("auth.role.create"),
          Collections.singletonList(ConfigPhysicalPlanType.CreateRole));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.role.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropRole));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.role.grant"),
          Collections.singletonList(ConfigPhysicalPlanType.GrantRole));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.role.revoke"),
          Collections.singletonList(ConfigPhysicalPlanType.RevokeRole));

      OPTION_PLAN_MAP.put(
          new PartialPath("auth.user.create"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.CreateUser,
                  ConfigPhysicalPlanType.CreateUserWithRawPassword)));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.user.alter"),
          Collections.singletonList(ConfigPhysicalPlanType.UpdateUser));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.user.drop"),
          Collections.singletonList(ConfigPhysicalPlanType.DropUser));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.user.grant"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.GrantUser, ConfigPhysicalPlanType.GrantRoleToUser)));
      OPTION_PLAN_MAP.put(
          new PartialPath("auth.user.revoke"),
          Collections.unmodifiableList(
              Arrays.asList(
                  ConfigPhysicalPlanType.RevokeUser, ConfigPhysicalPlanType.RevokeRoleFromUser)));
    } catch (final IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static boolean shouldPlanBeListened(final ConfigPhysicalPlan plan) {
    final ConfigPhysicalPlanType type = plan.getType();

    // Do not transfer roll back set template plan
    if (type.equals(ConfigPhysicalPlanType.CommitSetSchemaTemplate)
        && ((CommitSetSchemaTemplatePlan) plan).isRollback()) {
      return false;
    }

    // PipeEnriched & UnsetTemplate are not listened directly,
    // but their inner plan or converted plan are listened.
    return type.equals(ConfigPhysicalPlanType.PipeEnriched)
        || type.equals(ConfigPhysicalPlanType.UnsetTemplate)
        || OPTION_PLAN_MAP.values().stream().anyMatch(types -> types.contains(type));
  }

  public static Set<ConfigPhysicalPlanType> parseListeningPlanTypeSet(
      final PipeParameters parameters) throws IllegalPathException {
    final Set<ConfigPhysicalPlanType> planTypes = new HashSet<>();
    final Set<PartialPath> inclusionOptions = parseOptions(getInclusionString(parameters));
    final Set<PartialPath> exclusionOptions = parseOptions(getExclusionString(parameters));
    inclusionOptions.forEach(inclusion -> planTypes.addAll(getOptionsByPrefix(inclusion)));
    exclusionOptions.forEach(exclusion -> planTypes.removeAll(getOptionsByPrefix(exclusion)));

    if (!TreePattern.isTreeModelDataAllowToBeCaptured(parameters)) {
      planTypes.removeAll(getOptionsByPrefix(treeOnlySyncPrefix));
    }

    if (!TablePattern.isTableModelDataAllowToBeCaptured(parameters)) {
      planTypes.removeAll(getOptionsByPrefix(tableOnlySyncPrefix));
    }

    return planTypes;
  }

  private static Set<ConfigPhysicalPlanType> getOptionsByPrefix(final PartialPath prefix) {
    return OPTION_PLAN_MAP.keySet().stream()
        .filter(path -> path.overlapWithFullPathPrefix(prefix))
        .map(OPTION_PLAN_MAP::get)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private ConfigRegionListeningFilter() {
    // Utility class
  }
}
