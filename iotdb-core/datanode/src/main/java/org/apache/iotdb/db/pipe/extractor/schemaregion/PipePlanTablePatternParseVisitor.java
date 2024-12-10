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

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class PipePlanTablePatternParseVisitor
    extends PlanVisitor<Optional<PlanNode>, TablePattern> {
  @Override
  public Optional<PlanNode> visitPlan(final PlanNode node, final TablePattern pattern) {
    return Optional.of(node);
  }

  @Override
  public Optional<PlanNode> visitCreateOrUpdateTableDevice(
      final CreateOrUpdateTableDeviceNode node, final TablePattern pattern) {
    return pattern.matchesDatabase(node.getDatabase()) && pattern.matchesTable(node.getTableName())
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitTableDeviceAttributeUpdate(
      final TableDeviceAttributeUpdateNode node, final TablePattern pattern) {
    return pattern.matchesDatabase(node.getDatabase()) && pattern.matchesTable(node.getTableName())
        ? Optional.of(node)
        : Optional.empty();
  }

  @Override
  public Optional<PlanNode> visitDeleteData(
      final RelationalDeleteDataNode node, final TablePattern pattern) {
    final List<TableDeletionEntry> deletionEntries =
        node.getModEntries().stream()
            .filter(entry -> pattern.matchesTable(entry.getTableName()))
            .collect(Collectors.toList());
    return !deletionEntries.isEmpty()
        ? Optional.of(new RelationalDeleteDataNode(node.getPlanNodeId(), deletionEntries))
        : Optional.empty();
  }
}
