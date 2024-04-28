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

package org.apache.iotdb.db.pipe.extractor.schemaregion;

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;

import java.util.Optional;

/**
 * The {@link PipePlanPatternParseVisitor} will transform the schema {@link PlanNode}s using {@link
 * IoTDBPipePattern}. Rule:
 *
 * <p>1. All patterns in the output {@link PlanNode} will be the intersection of the original {@link
 * PlanNode}'s patterns and the given {@link IoTDBPipePattern}.
 *
 * <p>2. If a pattern does not intersect with the {@link IoTDBPipePattern}, it's dropped.
 *
 * <p>3. If all the patterns in the {@link PlanNode} is dropped, the {@link PlanNode} is dropped.
 *
 * <p>4. The output {@link PlanNode} shall be a copied form of the original one because the original
 * one is used in the {@link PipeSchemaRegionWritePlanEvent} in {@link SchemaRegionListeningQueue}.
 */
public class PipePlanPatternParseVisitor extends PlanVisitor<Optional<PlanNode>, IoTDBPipePattern> {
  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final IoTDBPipePattern pattern) {
    return Optional.of(node);
  }
}
