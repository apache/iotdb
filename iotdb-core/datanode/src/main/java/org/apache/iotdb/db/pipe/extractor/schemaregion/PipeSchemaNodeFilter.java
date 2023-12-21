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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeType;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link PipeSchemaNodeFilter} is to classify the {@link PlanNode}s to help linkedList and pipe to
 * collect.
 */
public class PipeSchemaNodeFilter {

  // The "default" schema synchronization set
  private static final Set<PlanNodeType> defaultNodeSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  PlanNodeType.CREATE_TIME_SERIES,
                  PlanNodeType.CREATE_ALIGNED_TIME_SERIES,
                  PlanNodeType.CREATE_MULTI_TIME_SERIES,
                  PlanNodeType.ALTER_TIME_SERIES,
                  PlanNodeType.INTERNAL_CREATE_TIMESERIES,
                  PlanNodeType.INTERNAL_CREATE_MULTI_TIMESERIES,
                  PlanNodeType.ACTIVATE_TEMPLATE,
                  PlanNodeType.BATCH_ACTIVATE_TEMPLATE,
                  PlanNodeType.INTERNAL_BATCH_ACTIVATE_TEMPLATE,
                  PlanNodeType.CREATE_LOGICAL_VIEW,
                  PlanNodeType.ALTER_LOGICAL_VIEW,
                  PlanNodeType.PIPE_ENRICHED_WRITE_SCHEMA)));

  // Deletion schema synchronization set
  private static final Set<PlanNodeType> deletionNodeSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  PlanNodeType.DEACTIVATE_TEMPLATE_NODE,
                  PlanNodeType.DELETE_TIMESERIES,
                  PlanNodeType.DELETE_LOGICAL_VIEW,
                  PlanNodeType.PIPE_ENRICHED_DELETE_SCHEMA)));

  private PipeSchemaNodeFilter() {
    // Utility class
  }
}
