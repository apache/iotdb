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

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;
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

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.parseOptions;

/**
 * {@link SchemaRegionListeningFilter} is to classify the {@link PlanNode}s to help {@link
 * SchemaRegionListeningQueue} and pipe to collect.
 */
public class SchemaRegionListeningFilter {

  private static final Map<PartialPath, List<PlanNodeType>> OPTION_PLAN_MAP = new HashMap<>();

  static {
    try {
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
    } catch (IllegalPathException ignore) {
      // There won't be any exceptions here
    }
  }

  static boolean shouldPlanBeListened(PlanNode node) {
    try {
      return node.getType().getNodeType() == PlanNodeType.PIPE_ENRICHED_WRITE.getNodeType()
          || node.getType().getNodeType() == PlanNodeType.PIPE_ENRICHED_NON_WRITE.getNodeType()
          || OPTION_PLAN_MAP.values().stream().anyMatch(types -> types.contains(node.getType()));
    } catch (Exception e) {
      // Some plan nodes may not contain "getType()" implementation
      return false;
    }
  }

  public static Set<PlanNodeType> parseListeningPlanTypeSet(PipeParameters parameters)
      throws IllegalPathException {
    Set<PlanNodeType> planTypes = new HashSet<>();
    Set<PartialPath> inclusionOptions =
        parseOptions(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE));
    Set<PartialPath> exclusionOptions =
        parseOptions(
            parameters.getStringOrDefault(
                Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                EXTRACTOR_EXCLUSION_DEFAULT_VALUE));
    inclusionOptions.forEach(
        inclusion ->
            planTypes.addAll(
                OPTION_PLAN_MAP.keySet().stream()
                    .filter(path -> path.overlapWithFullPathPrefix(inclusion))
                    .map(OPTION_PLAN_MAP::get)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet())));
    exclusionOptions.forEach(
        exclusion ->
            planTypes.removeAll(
                OPTION_PLAN_MAP.keySet().stream()
                    .filter(path -> path.overlapWithFullPathPrefix(exclusion))
                    .map(OPTION_PLAN_MAP::get)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet())));
    return planTypes;
  }

  private SchemaRegionListeningFilter() {
    // Utility class
  }
}
