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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeCreateTableOrViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.payload.PipeDeleteDevicesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AddTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.AlterColumnDataTypePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.CommitDeleteTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTableColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.RenameTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableColumnCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTableCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.SetTablePropertiesPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.AddTableViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.CommitDeleteViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewColumnPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.RenameViewPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewCommentPlan;
import org.apache.iotdb.confignode.consensus.request.write.table.view.SetViewPropertiesPlan;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;

import org.apache.tsfile.enums.TSDataType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

public class PipeConfigTablePatternParseVisitorTest {
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
  public void testCreateTableOrView() {
    testInput(
        new PipeCreateTableOrViewPlan("db1", new TsTable("ab")),
        new PipeCreateTableOrViewPlan("db1", new TsTable("ac")),
        new PipeCreateTableOrViewPlan("da", new TsTable("a2b")));
  }

  @Test
  public void testAddTableColumn() {
    testInput(
        new AddTableColumnPlan("db1", "ab", new ArrayList<>(), false),
        new AddTableColumnPlan("db1", "ac", new ArrayList<>(), false),
        new AddTableColumnPlan("da", "ac", new ArrayList<>(), false));
  }

  @Test
  public void testAddViewColumn() {
    testInput(
        new AddTableViewColumnPlan("db1", "ab", new ArrayList<>(), false),
        new AddTableViewColumnPlan("db1", "ac", new ArrayList<>(), false),
        new AddTableViewColumnPlan("da", "ac", new ArrayList<>(), false));
  }

  @Test
  public void testSetTableProperties() {
    testInput(
        new SetTablePropertiesPlan("db1", "ab", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("db1", "ac", Collections.singletonMap("ttl", "2")),
        new SetTablePropertiesPlan("da", "ac", Collections.singletonMap("ttl", "2")));
  }

  @Test
  public void testSetViewProperties() {
    testInput(
        new SetViewPropertiesPlan("db1", "ab", Collections.singletonMap("ttl", "2")),
        new SetViewPropertiesPlan("db1", "ac", Collections.singletonMap("ttl", "2")),
        new SetViewPropertiesPlan("da", "ac", Collections.singletonMap("ttl", "2")));
  }

  @Test
  public void testCommitDeleteColumn() {
    testInput(
        new CommitDeleteColumnPlan("db1", "ab", "a"),
        new CommitDeleteColumnPlan("db1", "ac", "a"),
        new CommitDeleteColumnPlan("da", "ac", "a"));
  }

  @Test
  public void testCommitDeleteViewColumn() {
    testInput(
        new CommitDeleteViewColumnPlan("db1", "ab", "a"),
        new CommitDeleteViewColumnPlan("db1", "ac", "a"),
        new CommitDeleteViewColumnPlan("da", "ac", "a"));
  }

  @Test
  public void testRenameTableColumn() {
    testInput(
        new RenameTableColumnPlan("db1", "ab", "old", "new"),
        new RenameTableColumnPlan("db1", "ac", "old", "new"),
        new RenameTableColumnPlan("da", "ac", "old", "new"));
  }

  @Test
  public void testRenameViewColumn() {
    testInput(
        new RenameViewColumnPlan("db1", "ab", "old", "new"),
        new RenameViewColumnPlan("db1", "ac", "old", "new"),
        new RenameViewColumnPlan("da", "ac", "old", "new"));
  }

  @Test
  public void testCommitDeleteTable() {
    testInput(
        new CommitDeleteTablePlan("db1", "ab"),
        new CommitDeleteTablePlan("db1", "ac"),
        new CommitDeleteTablePlan("da", "ac"));
  }

  @Test
  public void testCommitDeleteView() {
    testInput(
        new CommitDeleteViewPlan("db1", "ab"),
        new CommitDeleteViewPlan("db1", "ac"),
        new CommitDeleteViewPlan("da", "ac"));
  }

  // Match the oldName instead of the new one
  @Test
  public void testRenameTable() {
    testInput(
        new RenameTablePlan("db1", "ab", "ac"),
        new RenameTablePlan("db1", "ac", "ab"),
        new RenameTablePlan("da", "ac", "ab"));
  }

  @Test
  public void testRenameView() {
    testInput(
        new RenameViewPlan("db1", "ab", "ac"),
        new RenameViewPlan("db1", "ac", "ab"),
        new RenameViewPlan("da", "ac", "ab"));
  }

  @Test
  public void testPipeDeleteDevices() {
    testInput(
        new PipeDeleteDevicesPlan("db1", "ab", new byte[0], new byte[0], new byte[0]),
        new PipeDeleteDevicesPlan("db1", "ac", new byte[0], new byte[0], new byte[0]),
        new PipeDeleteDevicesPlan("da", "ac", new byte[0], new byte[0], new byte[0]));
  }

  @Test
  public void testSetTableComment() {
    testInput(
        new SetTableCommentPlan("db1", "ab", "a"),
        new SetTableCommentPlan("db1", "ac", "a"),
        new SetTableCommentPlan("da", "ac", "a"));
  }

  @Test
  public void testSetViewComment() {
    testInput(
        new SetViewCommentPlan("db1", "ab", "a"),
        new SetViewCommentPlan("db1", "ac", "a"),
        new SetViewCommentPlan("da", "ac", "a"));
  }

  @Test
  public void testSetTableColumnComment() {
    testInput(
        new SetTableColumnCommentPlan("db1", "ab", "a", "a"),
        new SetTableColumnCommentPlan("db1", "ac", "a", "a"),
        new SetTableColumnCommentPlan("da", "ac", "a", "a"));
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
        IoTDBConfigRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(trueInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Assert.assertFalse(
        IoTDBConfigRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput1, tablePattern)
            .isPresent());
    Assert.assertFalse(
        IoTDBConfigRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(falseInput2, tablePattern)
            .isPresent());
  }

  @Test
  public void testAlterTableColumnDataType() {
    testInput(
        new AlterColumnDataTypePlan("db1", "ab", "a", TSDataType.INT64),
        new AlterColumnDataTypePlan("db1", "ac", "a", TSDataType.BLOB),
        new AlterColumnDataTypePlan("da", "ac", "a", TSDataType.DATE));
  }
}
