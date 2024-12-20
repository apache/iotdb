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

import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

public class PipeConfigPhysicalPlanTablePatternParseVisitorTest {
  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");

  @Test
  public void testCreateDatabase() {
    testInput(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.db1")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.da")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("root.dc")));
  }

  @Test
  public void testAlterDatabase() {
    testInput(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.db1")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.da")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("root.dc")));
  }

  @Test
  public void testDeleteDatabaseV2() {
    testInput(
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.DeleteDatabaseV2, new TDatabaseSchema("root.db1")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.DeleteDatabaseV2, new TDatabaseSchema("root.da")),
        new DatabaseSchemaPlan(
            ConfigPhysicalPlanType.DeleteDatabaseV2, new TDatabaseSchema("root.dc")));
  }

  @Test
  public void testCreateTable() {
    testInput(
        new PipeCreateTablePlan("root.db1", new TsTable("ab")),
        new PipeCreateTablePlan("root.db1", new TsTable("ac")),
        new PipeCreateTablePlan("root.da", new TsTable("a2b")));
  }

  @Test
  public void testAddTableColumn() {
    testInput(
        new AddTableColumnPlan("root.db1", "ab", new ArrayList<>(), false),
        new AddTableColumnPlan("root.db1", "ac", new ArrayList<>(), false),
        new AddTableColumnPlan("root.da", "ac", new ArrayList<>(), false));
  }

  @Test
  public void testSetTableProperties() {
    testInput(
        new SetTablePropertiesPlan("root.db1", "ab", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("root.db1", "ac", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("root.da", "ac", Collections.singletonMap("ttl", "2")));
  }

  @Test
  public void testCommitDeleteColumn() {
    testInput(
        new CommitDeleteColumnPlan("root.db1", "ab", "a"),
        new CommitDeleteColumnPlan("root.db1", "ac", "a"),
        new CommitDeleteColumnPlan("root.da", "ac", "a"));
  }

  @Test
  public void testRenameTableColumn() {
    testInput(
        new RenameTableColumnPlan("root.db1", "ab", "old", "new"),
        new RenameTableColumnPlan("root.db1", "ac", "old", "new"),
        new RenameTableColumnPlan("root.da", "ac", "old", "new"));
  }

  @Test
  public void testCommitDeleteTable() {
    testInput(
        new CommitDeleteTablePlan("root.db1", "ab"),
        new CommitDeleteTablePlan("root.db1", "ac"),
        new CommitDeleteTablePlan("root.da", "ac"));
  }

  private void testInput(
      final ConfigPhysicalPlan trueInput,
      final ConfigPhysicalPlan falseInput1,
      final ConfigPhysicalPlan falseInput2) {
    Assert.assertEquals(
        trueInput,
        IoTDBConfigRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(trueInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput1, tablePattern)
            .isPresent());
    Assert.assertFalse(
        IoTDBConfigRegionExtractor.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput2, tablePattern)
            .isPresent());
  }
}
