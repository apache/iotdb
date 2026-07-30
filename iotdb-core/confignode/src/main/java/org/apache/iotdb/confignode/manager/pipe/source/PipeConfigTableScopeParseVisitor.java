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
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanVisitor;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorRelationalPlan;
import org.apache.iotdb.confignode.consensus.request.write.auth.AuthorTreePlan;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PipeConfigTableScopeParseVisitor
    extends ConfigPhysicalPlanVisitor<Optional<ConfigPhysicalPlan>, Void> {
  @Override
  public Optional<ConfigPhysicalPlan> visitPlan(final ConfigPhysicalPlan plan, final Void context) {
    return Optional.of(plan);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantUserAll(
      final AuthorRelationalPlan plan, final Void context) {
    return visitTableAuthorPlan(plan, ConfigPhysicalPlanType.GrantUser);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRGrantRoleAll(
      final AuthorRelationalPlan plan, final Void context) {
    return visitTableAuthorPlan(plan, ConfigPhysicalPlanType.GrantRole);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeUserAll(
      final AuthorRelationalPlan plan, final Void context) {
    return visitTableAuthorPlan(plan, ConfigPhysicalPlanType.RevokeUser);
  }

  @Override
  public Optional<ConfigPhysicalPlan> visitRRevokeRoleAll(
      final AuthorRelationalPlan plan, final Void context) {
    return visitTableAuthorPlan(plan, ConfigPhysicalPlanType.RevokeRole);
  }

  private Optional<ConfigPhysicalPlan> visitTableAuthorPlan(
      final AuthorRelationalPlan authorRelationalPlan, final ConfigPhysicalPlanType type) {
    final Set<Integer> permissions =
        Arrays.stream(PrivilegeType.values())
            .filter(PrivilegeType::forRelationalSys)
            .map(Enum::ordinal)
            .collect(Collectors.toSet());
    return !permissions.isEmpty()
        ? Optional.of(
            new AuthorTreePlan(
                type,
                authorRelationalPlan.getUserName(),
                authorRelationalPlan.getRoleName(),
                authorRelationalPlan.getPassword(),
                authorRelationalPlan.getNewPassword(),
                permissions,
                authorRelationalPlan.getGrantOpt(),
                Collections.emptyList()))
        : Optional.empty();
  }
}
