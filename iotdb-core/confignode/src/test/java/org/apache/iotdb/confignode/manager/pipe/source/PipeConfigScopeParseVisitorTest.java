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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.stream.Collectors;

public class PipeConfigScopeParseVisitorTest {
  @Test
  public void testTreeScopeParsing() {
    testTreeScopeParsing(ConfigPhysicalPlanType.GrantRole, false);
    testTreeScopeParsing(ConfigPhysicalPlanType.RevokeRole, false);
    testTreeScopeParsing(ConfigPhysicalPlanType.GrantUser, true);
    testTreeScopeParsing(ConfigPhysicalPlanType.RevokeUser, true);
  }

  private void testTreeScopeParsing(final ConfigPhysicalPlanType type, final boolean isUser) {
    Assert.assertEquals(
        new AuthorTreePlan(
            type,
            isUser ? "user" : "",
            isUser ? "" : "role",
            "",
            "",
            new HashSet<>(
                Arrays.stream(PrivilegeType.values())
                    .filter(PrivilegeType::forRelationalSys)
                    .map(Enum::ordinal)
                    .collect(Collectors.toList())),
            false,
            Collections.singletonList(new PartialPath(new String[] {"root", "**"}))),
        IoTDBConfigRegionSource.TREE_SCOPE_PARSE_VISITOR
            .process(
                new AuthorTreePlan(
                    type,
                    isUser ? "user" : "",
                    isUser ? "" : "role",
                    "",
                    "",
                    new HashSet<>(
                        Arrays.stream(PrivilegeType.values())
                            .filter(privilegeType -> !privilegeType.isRelationalPrivilege())
                            .map(Enum::ordinal)
                            .collect(Collectors.toList())),
                    false,
                    Collections.singletonList(new PartialPath(new String[] {"root", "**"}))),
                null)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testTableScopeParsing() {
    testTableScopeParsing(
        ConfigPhysicalPlanType.GrantRole, ConfigPhysicalPlanType.RGrantRoleAll, false);
    testTableScopeParsing(
        ConfigPhysicalPlanType.RevokeRole, ConfigPhysicalPlanType.RRevokeRoleAll, false);
    testTableScopeParsing(
        ConfigPhysicalPlanType.GrantUser, ConfigPhysicalPlanType.RGrantUserAll, true);
    testTableScopeParsing(
        ConfigPhysicalPlanType.RevokeUser, ConfigPhysicalPlanType.RRevokeUserAll, true);
  }

  private void testTableScopeParsing(
      final ConfigPhysicalPlanType outputType,
      final ConfigPhysicalPlanType inputType,
      final boolean isUser) {
    Assert.assertEquals(
        new AuthorTreePlan(
            outputType,
            isUser ? "user" : "",
            isUser ? "" : "role",
            "",
            "",
            new HashSet<>(
                Arrays.stream(PrivilegeType.values())
                    .filter(PrivilegeType::forRelationalSys)
                    .map(Enum::ordinal)
                    .collect(Collectors.toList())),
            true,
            Collections.emptyList()),
        IoTDBConfigRegionSource.TABLE_SCOPE_PARSE_VISITOR
            .process(
                new AuthorRelationalPlan(
                    inputType, isUser ? "user" : "", isUser ? "" : "role", "", "", -1, true),
                null)
            .orElseThrow(AssertionError::new));
  }
}
