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

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
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
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("db1")),
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("da")),
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema("dc")));
  }

  @Test
  public void testAlterDatabase() {
    testInput(
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("db1")),
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("da")),
        new DatabaseSchemaPlan(ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema("dc")));
  }

  @Test
  public void testDeleteDatabase() {
    testInput(
        new DeleteDatabasePlan("db1"), new DeleteDatabasePlan("da"), new DeleteDatabasePlan("dc"));
  }

  @Test
  public void testCreateTable() {
    testInput(
        new PipeCreateTablePlan("db1", new TsTable("ab")),
        new PipeCreateTablePlan("db1", new TsTable("ac")),
        new PipeCreateTablePlan("da", new TsTable("a2b")));
  }

  @Test
  public void testAddTableColumn() {
    testInput(
        new AddTableColumnPlan("db1", "ab", new ArrayList<>(), false),
        new AddTableColumnPlan("db1", "ac", new ArrayList<>(), false),
        new AddTableColumnPlan("da", "ac", new ArrayList<>(), false));
  }

  @Test
  public void testSetTableProperties() {
    testInput(
        new SetTablePropertiesPlan("db1", "ab", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("db1", "ac", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("da", "ac", Collections.singletonMap("ttl", "2")));
  }

  @Test
  public void testCommitDeleteColumn() {
    testInput(
        new CommitDeleteColumnPlan("db1", "ab", "a"),
        new CommitDeleteColumnPlan("db1", "ac", "a"),
        new CommitDeleteColumnPlan("da", "ac", "a"));
  }

  @Test
  public void testRenameTableColumn() {
    testInput(
        new RenameTableColumnPlan("db1", "ab", "old", "new"),
        new RenameTableColumnPlan("db1", "ac", "old", "new"),
        new RenameTableColumnPlan("da", "ac", "old", "new"));
  }

  @Test
  public void testCommitDeleteTable() {
    testInput(
        new CommitDeleteTablePlan("db1", "ab"),
        new CommitDeleteTablePlan("db1", "ac"),
        new CommitDeleteTablePlan("da", "ac"));
  }

  @Test
  public void testAuth() {
    testInput(
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantRoleAll, "", "role", "", "", -1, false),
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserDBPriv,
            "user",
            "",
            "da",
            "",
            PrivilegeType.SELECT.ordinal(),
            false),
        new AuthorRelationalPlan(
            ConfigPhysicalPlanType.RGrantUserTBPriv,
            "user",
            "",
            "db1",
            "ac",
            PrivilegeType.DROP.ordinal(),
            false));
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
