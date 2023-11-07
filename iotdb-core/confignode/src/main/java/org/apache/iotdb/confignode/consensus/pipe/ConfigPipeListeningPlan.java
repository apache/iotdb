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

package org.apache.iotdb.confignode.consensus.pipe;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import com.google.common.collect.ImmutableSet;

public class ConfigPipeListeningPlan {

  public static ImmutableSet<ConfigPhysicalPlanType> SchemaPlan =
      ImmutableSet.of(
          // DataBase
          ConfigPhysicalPlanType.CreateDatabase,
          ConfigPhysicalPlanType.AlterDatabase,

          // Schema Template
          ConfigPhysicalPlanType.CreateSchemaTemplate,
          ConfigPhysicalPlanType.SetSchemaTemplate,
          ConfigPhysicalPlanType.ExtendSchemaTemplate);

  public static ImmutableSet<ConfigPhysicalPlanType> SchemaDeletionPlan =
      ImmutableSet.of(
          // DataBase
          ConfigPhysicalPlanType.DeleteDatabase,

          // Schema Template
          ConfigPhysicalPlanType.UnsetTemplate,
          ConfigPhysicalPlanType.DropSchemaTemplate);

  public static ImmutableSet<ConfigPhysicalPlanType> TTLPlan =
      ImmutableSet.of(ConfigPhysicalPlanType.SetTTL);

  public static ImmutableSet<ConfigPhysicalPlanType> authorityPlan =
      ImmutableSet.of(
          ConfigPhysicalPlanType.CreateUser,
          ConfigPhysicalPlanType.CreateRole,
          ConfigPhysicalPlanType.GrantRole,
          ConfigPhysicalPlanType.GrantRoleToUser,
          ConfigPhysicalPlanType.GrantUser,
          ConfigPhysicalPlanType.UpdateUser);

  public static ImmutableSet<ConfigPhysicalPlanType> authorityDeletionPlan =
      ImmutableSet.of(ConfigPhysicalPlanType.DropRole, ConfigPhysicalPlanType.DropUser);

  private ConfigPipeListeningPlan() {
    // Util class
  }
}
