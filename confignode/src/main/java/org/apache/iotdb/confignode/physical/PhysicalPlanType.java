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
package org.apache.iotdb.confignode.physical;

public enum PhysicalPlanType {
  RegisterDataNode,
  QueryDataNodeInfo,
  SetStorageGroup,
  DeleteStorageGroup,
  QueryStorageGroupSchema,
  CreateRegion,
  QueryDataPartition,
  ApplyDataPartition,
  QuerySchemaPartition,
  ApplySchemaPartition,
  AUTHOR,
  CREATE_USER,
  CREATE_ROLE,
  DROP_USER,
  DROP_ROLE,
  GRANT_ROLE,
  GRANT_USER,
  GRANT_ROLE_TO_USER,
  REVOKE_USER,
  REVOKE_ROLE,
  REVOKE_ROLE_FROM_USER,
  UPDATE_USER,
  LIST_USER,
  LIST_ROLE,
  LIST_USER_PRIVILEGE,
  LIST_ROLE_PRIVILEGE,
  LIST_USER_ROLES,
  LIST_ROLE_USERS
}
