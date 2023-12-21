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

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * {@link PipeConfigPlanFilter} is to classify the {@link ConfigPhysicalPlan}s to help linkedList
 * and pipe to collect, and to help receiver execute.
 *
 * <p>Note that we do not transfer the rollback version of {@link CommitSetSchemaTemplatePlan}
 * because the rollback is usually useless. Consensus layer ensures that a failed plan won't be
 * written to peer, consequently won't be extracted by linkedListQueue. Under that circumstance, a
 * rollback operation, if extracted, will cause an empty inverse operation on the receiver cluster,
 * and result in failure eventually.
 */
class PipeConfigPlanFilter {

  // The "default" schema synchronization set
  private static final Set<ConfigPhysicalPlanType> schemaDefaultPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConfigPhysicalPlanType.CreateDatabase,
                  ConfigPhysicalPlanType.AlterDatabase,
                  ConfigPhysicalPlanType.CreateSchemaTemplate,
                  ConfigPhysicalPlanType.ExtendSchemaTemplate,
                  ConfigPhysicalPlanType.CommitSetSchemaTemplate)));

  // Deletion schema synchronization set
  private static final Set<ConfigPhysicalPlanType> schemaDeletionPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConfigPhysicalPlanType.DeleteDatabase,
                  ConfigPhysicalPlanType.DropSchemaTemplate,
                  ConfigPhysicalPlanType.UnsetTemplate)));

  // The "default" authority synchronization set
  private static final Set<ConfigPhysicalPlanType> authorityDefaultPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConfigPhysicalPlanType.CreateUser,
                  ConfigPhysicalPlanType.CreateRole,
                  ConfigPhysicalPlanType.GrantRole,
                  ConfigPhysicalPlanType.GrantUser,
                  ConfigPhysicalPlanType.GrantRoleToUser,
                  ConfigPhysicalPlanType.UpdateUser)));

  // Deletion authority synchronization set
  private static final Set<ConfigPhysicalPlanType> authorityDeletionPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  ConfigPhysicalPlanType.DropUser,
                  ConfigPhysicalPlanType.DropRole,
                  ConfigPhysicalPlanType.RevokeUser,
                  ConfigPhysicalPlanType.RevokeRole,
                  ConfigPhysicalPlanType.RevokeRoleFromUser)));

  // The "default" TTL synchronization set
  private static final Set<ConfigPhysicalPlanType> TTLDefaultPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(Collections.singletonList(ConfigPhysicalPlanType.SetTTL)));

  // The "default" function synchronization set
  private static final Set<ConfigPhysicalPlanType> functionDefaultPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(Collections.singletonList(ConfigPhysicalPlanType.CreateFunction)));

  // Deletion function synchronization set
  private static final Set<ConfigPhysicalPlanType> functionDeletionPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(Collections.singletonList(ConfigPhysicalPlanType.DropFunction)));

  // The "default" trigger synchronization set
  private static final Set<ConfigPhysicalPlanType> triggerDefaultPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(
              Collections.singletonList(ConfigPhysicalPlanType.UpdateTriggerStateInTable)));

  // Deletion trigger synchronization set
  private static final Set<ConfigPhysicalPlanType> triggerDeletionPlanSet =
      Collections.unmodifiableSet(
          new HashSet<>(Collections.singletonList(ConfigPhysicalPlanType.DeleteTriggerInTable)));

  static boolean shouldBeListenedByQueue(ConfigPhysicalPlan plan) {
    ConfigPhysicalPlanType type = plan.getType();
    if (type.equals(ConfigPhysicalPlanType.CommitSetSchemaTemplate)
        && ((CommitSetSchemaTemplatePlan) plan).isRollback()) {
      return false;
    }
    return schemaDefaultPlanSet.contains(type)
        || schemaDeletionPlanSet.contains(type)
        || authorityDefaultPlanSet.contains(type)
        || authorityDeletionPlanSet.contains(type)
        || TTLDefaultPlanSet.contains(type)
        || functionDefaultPlanSet.contains(type)
        || functionDeletionPlanSet.contains(type)
        || triggerDefaultPlanSet.contains(type)
        || triggerDeletionPlanSet.contains(type);
  }

  private PipeConfigPlanFilter() {
    // Utility class
  }
}
