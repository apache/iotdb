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

package org.apache.iotdb.db.pipe.source;

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.pipe.source.schemaregion.IoTDBSchemaRegionSource;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.CreateOrUpdateTableDeviceNode;
import org.apache.iotdb.db.queryengine.plan.relational.planner.node.schema.TableDeviceAttributeUpdateNode;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;

import org.apache.tsfile.read.common.TimeRange;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;

public class PipePlanTablePatternParseVisitorTest {

  private static final String MATCH_DATABASE = "db1";
  private static final String MISMATCH_DATABASE = "da";
  private static final String MATCH_TABLE = "ab";
  private static final String MISMATCH_TABLE = "ac";

  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");

  @Test
  public void testTableDevicePlans() {
    final List<BiFunction<String, String, PlanNode>> planFactories =
        Arrays.asList(this::createOrUpdateTableDeviceNode, this::tableDeviceAttributeUpdateNode);

    planFactories.forEach(this::assertTableDevicePlanMatches);
  }

  @Test
  public void testDeleteData() {
    final TableDeletionEntry matchedEntry = deletionEntry(MATCH_TABLE);
    final TableDeletionEntry filteredEntry = deletionEntry(MISMATCH_TABLE);

    Assert.assertEquals(
        deleteDataNode(MATCH_DATABASE, matchedEntry),
        IoTDBSchemaRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(deleteDataNode(MATCH_DATABASE, matchedEntry, filteredEntry), tablePattern)
            .orElse(null));
  }

  @Test
  public void testDeleteDataIsFilteredWhenDatabaseOrTableDoesNotMatch() {
    assertPatternFilters(deleteDataNode(null, deletionEntry(MATCH_TABLE)));
    assertPatternFilters(deleteDataNode(MISMATCH_DATABASE, deletionEntry(MATCH_TABLE)));
    assertPatternFilters(deleteDataNode(MATCH_DATABASE, deletionEntry(MISMATCH_TABLE)));
  }

  private void assertTableDevicePlanMatches(
      final BiFunction<String, String, PlanNode> planFactory) {
    assertPatternMatches(
        planFactory.apply(MATCH_DATABASE, MATCH_TABLE),
        planFactory.apply(MATCH_DATABASE, MISMATCH_TABLE),
        planFactory.apply(MISMATCH_DATABASE, MATCH_TABLE));
  }

  private void assertPatternMatches(final PlanNode matchedInput, final PlanNode... filteredInputs) {
    Assert.assertEquals(
        matchedInput,
        IoTDBSchemaRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(matchedInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Arrays.stream(filteredInputs).forEach(this::assertPatternFilters);
  }

  private void assertPatternFilters(final PlanNode filteredInput) {
    Assert.assertFalse(
        IoTDBSchemaRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(filteredInput, tablePattern)
            .isPresent());
  }

  private CreateOrUpdateTableDeviceNode createOrUpdateTableDeviceNode(
      final String database, final String table) {
    return new CreateOrUpdateTableDeviceNode(
        new PlanNodeId(""),
        database,
        table,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList());
  }

  private TableDeviceAttributeUpdateNode tableDeviceAttributeUpdateNode(
      final String database, final String table) {
    return new TableDeviceAttributeUpdateNode(
        new PlanNodeId(""), database, table, null, null, null, null, null, null);
  }

  private RelationalDeleteDataNode deleteDataNode(
      final String database, final TableDeletionEntry... entries) {
    return new RelationalDeleteDataNode(new PlanNodeId(""), Arrays.asList(entries), database);
  }

  private TableDeletionEntry deletionEntry(final String table) {
    return new TableDeletionEntry(
        new DeletionPredicate(table), new TimeRange(Long.MIN_VALUE, Long.MAX_VALUE));
  }
}
