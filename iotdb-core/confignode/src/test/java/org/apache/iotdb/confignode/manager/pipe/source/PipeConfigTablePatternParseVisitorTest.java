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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class PipeConfigTablePatternParseVisitorTest {

  private static final String MATCH_DATABASE = "db1";
  private static final String QUALIFIED_MATCH_DATABASE = "root.db1";
  private static final String MISMATCH_DATABASE = "da";
  private static final String MATCH_TABLE = "ab";
  private static final String MISMATCH_TABLE = "ac";

  private final TablePattern tablePattern = new TablePattern(true, "^db[0-9]", "a.*b");

  @Test
  public void testDatabasePlans() {
    final List<Function<String, ConfigPhysicalPlan>> planFactories =
        Arrays.asList(
            database ->
                new DatabaseSchemaPlan(
                    ConfigPhysicalPlanType.CreateDatabase, new TDatabaseSchema(database)),
            database ->
                new DatabaseSchemaPlan(
                    ConfigPhysicalPlanType.AlterDatabase, new TDatabaseSchema(database)),
            DeleteDatabasePlan::new);

    planFactories.forEach(this::assertDatabasePlanMatches);
  }

  @Test
  public void testTablePlans() {
    final List<BiFunction<String, String, ConfigPhysicalPlan>> planFactories =
        Arrays.asList(
            (database, table) -> new PipeCreateTableOrViewPlan(database, new TsTable(table)),
            (database, table) ->
                new AddTableColumnPlan(database, table, Collections.emptyList(), false),
            (database, table) ->
                new AddTableViewColumnPlan(database, table, Collections.emptyList(), false),
            (database, table) ->
                new SetTablePropertiesPlan(database, table, Collections.singletonMap("ttl", "2")),
            (database, table) ->
                new SetViewPropertiesPlan(database, table, Collections.singletonMap("ttl", "2")),
            (database, table) -> new CommitDeleteColumnPlan(database, table, "a"),
            (database, table) -> new CommitDeleteViewColumnPlan(database, table, "a"),
            (database, table) -> new RenameTableColumnPlan(database, table, "old", "new"),
            (database, table) -> new RenameViewColumnPlan(database, table, "old", "new"),
            CommitDeleteTablePlan::new,
            CommitDeleteViewPlan::new,
            (database, table) ->
                new PipeDeleteDevicesPlan(database, table, new byte[0], new byte[0], new byte[0]),
            (database, table) -> new SetTableCommentPlan(database, table, "a"),
            (database, table) -> new SetViewCommentPlan(database, table, "a"),
            (database, table) -> new SetTableColumnCommentPlan(database, table, "a", "a"),
            (database, table) -> new RenameTablePlan(database, table, MISMATCH_TABLE),
            (database, table) -> new RenameViewPlan(database, table, MISMATCH_TABLE),
            (database, table) ->
                new AlterColumnDataTypePlan(database, table, "a", TSDataType.INT64));

    planFactories.forEach(this::assertTablePlanMatches);
  }

  @Test
  public void testAuthorDatabasePlans() {
    final List<ConfigPhysicalPlanType> planTypes =
        Arrays.asList(
            ConfigPhysicalPlanType.RGrantUserDBPriv,
            ConfigPhysicalPlanType.RGrantRoleDBPriv,
            ConfigPhysicalPlanType.RRevokeUserDBPriv,
            ConfigPhysicalPlanType.RRevokeRoleDBPriv);

    planTypes.forEach(this::assertAuthorDatabasePlanMatches);
  }

  @Test
  public void testAuthorTablePlans() {
    final List<ConfigPhysicalPlanType> planTypes =
        Arrays.asList(
            ConfigPhysicalPlanType.RGrantUserTBPriv,
            ConfigPhysicalPlanType.RGrantRoleTBPriv,
            ConfigPhysicalPlanType.RRevokeUserTBPriv,
            ConfigPhysicalPlanType.RRevokeRoleTBPriv);

    planTypes.forEach(this::assertAuthorTablePlanMatches);
  }

  @Test
  public void testUnscopedAuthorPlanPassesThrough() {
    assertPatternMatches(
        authorPlan(ConfigPhysicalPlanType.RGrantRoleAll, "", "", PrivilegeType.SELECT));
  }

  private void assertDatabasePlanMatches(final Function<String, ConfigPhysicalPlan> planFactory) {
    assertPatternMatches(
        planFactory.apply(QUALIFIED_MATCH_DATABASE), planFactory.apply(MISMATCH_DATABASE));
  }

  private void assertTablePlanMatches(
      final BiFunction<String, String, ConfigPhysicalPlan> planFactory) {
    assertPatternMatches(
        planFactory.apply(QUALIFIED_MATCH_DATABASE, MATCH_TABLE),
        planFactory.apply(QUALIFIED_MATCH_DATABASE, MISMATCH_TABLE),
        planFactory.apply(MISMATCH_DATABASE, MATCH_TABLE));
  }

  private void assertAuthorDatabasePlanMatches(final ConfigPhysicalPlanType type) {
    assertPatternMatches(
        authorPlan(type, MATCH_DATABASE, "", PrivilegeType.SELECT),
        authorPlan(type, MISMATCH_DATABASE, "", PrivilegeType.SELECT));
  }

  private void assertAuthorTablePlanMatches(final ConfigPhysicalPlanType type) {
    assertPatternMatches(
        authorPlan(type, MATCH_DATABASE, MATCH_TABLE, PrivilegeType.SELECT),
        authorPlan(type, MATCH_DATABASE, MISMATCH_TABLE, PrivilegeType.SELECT),
        authorPlan(type, MISMATCH_DATABASE, MATCH_TABLE, PrivilegeType.SELECT));
  }

  private void assertPatternMatches(
      final ConfigPhysicalPlan matchedInput, final ConfigPhysicalPlan... filteredInputs) {
    Assert.assertEquals(
        matchedInput,
        IoTDBConfigRegionSource.TABLE_PATTERN_PARSE_VISITOR
            .process(matchedInput, tablePattern)
            .orElseThrow(AssertionError::new));
    Arrays.stream(filteredInputs)
        .forEach(
            filteredInput ->
                Assert.assertFalse(
                    IoTDBConfigRegionSource.TABLE_PATTERN_PARSE_VISITOR
                        .process(filteredInput, tablePattern)
                        .isPresent()));
  }

  private AuthorRelationalPlan authorPlan(
      final ConfigPhysicalPlanType type,
      final String database,
      final String table,
      final PrivilegeType privilegeType) {
    return new AuthorRelationalPlan(
        type, "user", "role", database, table, privilegeType.ordinal(), false);
  }
}
