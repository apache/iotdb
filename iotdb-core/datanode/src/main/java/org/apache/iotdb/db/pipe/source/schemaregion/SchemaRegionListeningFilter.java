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

package org.apache.iotdb.db.pipe.source.schemaregion;

import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
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
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.tableOnlySyncPrefixes;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.treeOnlySyncPrefixes;

/**
 * {@link SchemaRegionListeningFilter} is to classify the {@link PlanNode}s to help {@link
 * SchemaRegionListeningQueue} and pipe to collect.
 */
public class SchemaRegionListeningFilter {

  private static final Map<PartialPath, List<PlanNodeType>> OPTION_PLAN_MAP = new HashMap<>();

  static {
    try {
      // Tree model
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.view.create"),
          Collections.singletonList(PlanNodeType.CREATE_LOGICAL_VIEW));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.view.alter"),
          Collections.singletonList(PlanNodeType.ALTER_LOGICAL_VIEW));

      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.ordinary.create"),
          Collections.unmodifiableList(
              Arrays.asList(
                  PlanNodeType.CREATE_TIME_SERIES,
                  PlanNodeType.CREATE_ALIGNED_TIME_SERIES,
                  PlanNodeType.CREATE_MULTI_TIME_SERIES,
                  PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES,
                  PlanNodeType.INTERNAL_CREATE_TIME_SERIES)));
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.ordinary.alter"),
          Collections.singletonList(PlanNodeType.ALTER_TIME_SERIES));

      OPTION_PLAN_MAP.put(
          new PartialPath("schema.timeseries.template.activate"),
          Collections.unmodifiableList(
              Arrays.asList(
                  PlanNodeType.ACTIVATE_TEMPLATE,
                  PlanNodeType.BATCH_ACTIVATE_TEMPLATE,
                  PlanNodeType.INTERNAL_BATCH_ACTIVATE_TEMPLATE)));

      // Table model
      OPTION_PLAN_MAP.put(
          new PartialPath("schema.table.alter"),
          Collections.unmodifiableList(
              Arrays.asList(
                  PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE,
                  PlanNodeType.TABLE_DEVICE_ATTRIBUTE_UPDATE)));

    } catch (final IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static boolean shouldPlanBeListened(final PlanNode node) {
    try {
      return node.getType() == PlanNodeType.PIPE_ENRICHED_WRITE
          || node.getType() == PlanNodeType.PIPE_ENRICHED_NON_WRITE
          || OPTION_PLAN_MAP.values().stream().anyMatch(types -> types.contains(node.getType()))
              && (node.getType() != PlanNodeType.CREATE_OR_UPDATE_TABLE_DEVICE
                  || !((CreateOrUpdateTableDeviceNode) node).getAttributeNameList().isEmpty());
    } catch (final Exception e) {
      // Some plan nodes may not contain "getType()" implementation
      return false;
    }
  }

  public static boolean shouldSchemaRegionBeListened(
      final int consensusGroupId, final PipeParameters parameters) throws IllegalPathException {
    final boolean isTableModel =
        PathUtils.isTableModelDatabase(
            SchemaEngine.getInstance()
                .getSchemaRegion(new SchemaRegionId(consensusGroupId))
                .getDatabaseFullPath());
    return (TreePattern.isTreeModelDataAllowToBeCaptured(parameters) && !isTableModel
            || TablePattern.isTableModelDataAllowToBeCaptured(parameters) && isTableModel)
        && !parseListeningPlanTypeSet(parameters).isEmpty();
  }

  public static Set<PlanNodeType> parseListeningPlanTypeSet(final PipeParameters parameters)
      throws IllegalPathException {
    final Set<PlanNodeType> planTypes = new HashSet<>();
    final Set<PartialPath> inclusionOptions = parseOptions(getInclusionString(parameters));
    final Set<PartialPath> exclusionOptions = parseOptions(getExclusionString(parameters));
    inclusionOptions.forEach(inclusion -> planTypes.addAll(getOptionsByPrefix(inclusion)));
    exclusionOptions.forEach(exclusion -> planTypes.removeAll(getOptionsByPrefix(exclusion)));

    if (!TreePattern.isTreeModelDataAllowToBeCaptured(parameters)) {
      treeOnlySyncPrefixes.forEach(prefix -> planTypes.removeAll(getOptionsByPrefix(prefix)));
    }

    if (!TablePattern.isTableModelDataAllowToBeCaptured(parameters)) {
      tableOnlySyncPrefixes.forEach(prefix -> planTypes.removeAll(getOptionsByPrefix(prefix)));
    }
    return planTypes;
  }

  private static Set<PlanNodeType> getOptionsByPrefix(final PartialPath prefix) {
    return OPTION_PLAN_MAP.keySet().stream()
        .filter(path -> path.overlapWithFullPathPrefix(prefix))
        .map(OPTION_PLAN_MAP::get)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet());
  }

  private SchemaRegionListeningFilter() {
    // Utility class
  }
}
