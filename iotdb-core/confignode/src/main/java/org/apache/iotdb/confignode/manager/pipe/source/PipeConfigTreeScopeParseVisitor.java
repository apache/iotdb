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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeConfigTreeScopeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, Void> {
  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(final ConfigPhysicalPlan plan, final Void context) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantRole(
      final AuthorTreePlan grantRolePlan, final Void context) {
    return visitTreeAuthorPlan(grantRolePlan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitGrantUser(
      final AuthorTreePlan grantUserPlan, final Void context) {
    return visitTreeAuthorPlan(grantUserPlan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeUser(
      final AuthorTreePlan revokeUserPlan, final Void context) {
    return visitTreeAuthorPlan(revokeUserPlan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRevokeRole(
      final AuthorTreePlan revokeRolePlan, final Void context) {
    return visitTreeAuthorPlan(revokeRolePlan);
  }

  private Optional<ConfigPhysicalPlan> visitTreeAuthorPlan(final AuthorTreePlan authorTreePlan) {
    final Set<Integer> permissions =
        authorTreePlan.getPermissions().stream()
            .filter(permission -> PrivilegeType.values()[permission].forRelationalSys())
            .collect(Collectors.toSet());
    return !permissions.isEmpty()
        ? Optional.of(
            new AuthorTreePlan(
                authorTreePlan.getAuthorType(),
                authorTreePlan.getUserName(),
                authorTreePlan.getRoleName(),
                authorTreePlan.getPassword(),
                authorTreePlan.getNewPassword(),
                permissions,
                authorTreePlan.getGrantOpt(),
                authorTreePlan.getNodeNameList()))
        : Optional.empty();
  }
}
