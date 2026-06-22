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
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
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
        treeAuthorPlan(
            type, isUser, privileges(PrivilegeType::forRelationalSys), false, rootPattern()),
        IoTDBConfigRegionSource.TREE_SCOPE_PARSE_VISITOR
            .process(
                treeAuthorPlan(
                    type,
                    isUser,
                    privileges(privilegeType -> !privilegeType.isRelationalPrivilege()),
                    false,
                    rootPattern()),
                null)
            .orElseThrow(AssertionError::new));
  }

  @Test
  public void testTreeScopeParsingSkipsWithoutRelationalSystemPrivilege() {
    Assert.assertFalse(
        IoTDBConfigRegionSource.TREE_SCOPE_PARSE_VISITOR
            .process(
                treeAuthorPlan(
                    ConfigPhysicalPlanType.GrantUser,
                    true,
                    Collections.singleton(PrivilegeType.READ_DATA.ordinal()),
                    false,
                    rootPattern()),
                null)
            .isPresent());
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
        treeAuthorPlan(
            outputType,
            isUser,
            privileges(PrivilegeType::forRelationalSys),
            true,
            Collections.emptyList()),
        IoTDBConfigRegionSource.TABLE_SCOPE_PARSE_VISITOR
            .process(relationalAuthorPlan(inputType, isUser, true), null)
            .orElseThrow(AssertionError::new));
  }

  private AuthorTreePlan treeAuthorPlan(
      final ConfigPhysicalPlanType type,
      final boolean isUser,
      final Set<Integer> permissions,
      final boolean grantOption,
      final List<PartialPath> paths) {
    return new AuthorTreePlan(
        type, isUser ? "user" : "", isUser ? "" : "role", "", "", permissions, grantOption, paths);
  }

  private AuthorRelationalPlan relationalAuthorPlan(
      final ConfigPhysicalPlanType type, final boolean isUser, final boolean grantOption) {
    return new AuthorRelationalPlan(
        type, isUser ? "user" : "", isUser ? "" : "role", "", "", -1, grantOption);
  }

  private Set<Integer> privileges(final Predicate<PrivilegeType> predicate) {
    return Arrays.stream(PrivilegeType.values())
        .filter(predicate)
        .map(Enum::ordinal)
        .collect(Collectors.toSet());
  }

  private List<PartialPath> rootPattern() {
    return Collections.singletonList(new PartialPath(new String[] {"root", "**"}));
  }
}
