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
package org.apache.iotdb.db.queryengine.plan.relational.type;

// When adding new types that need to be converted to ConfigPhysicalPlanType, you can refer to this
// document:
// https://docs.google.com/document/d/1WvAyuLn1y988svLl8rGUEcesMkAyH6UTT597KQAVm1A/edit?usp=sharing
public enum AuthorRType {
  CREATE_USER,
  CREATE_ROLE,
  UPDATE_USER,
  DROP_USER,
  DROP_ROLE,
  GRANT_USER_ROLE,
  REVOKE_USER_ROLE,
  GRANT_USER_ANY,
  GRANT_ROLE_ANY,
  GRANT_USER_ALL,
  GRANT_ROLE_ALL,
  GRANT_USER_DB,
  GRANT_USER_TB,
  GRANT_ROLE_DB,
  GRANT_ROLE_TB,
  REVOKE_USER_ANY,
  REVOKE_ROLE_ANY,
  REVOKE_USER_ALL,
  REVOKE_ROLE_ALL,
  REVOKE_USER_DB,
  REVOKE_USER_TB,
  REVOKE_ROLE_DB,
  REVOKE_ROLE_TB,
  GRANT_USER_SYS,
  GRANT_ROLE_SYS,
  REVOKE_USER_SYS,
  REVOKE_ROLE_SYS,
  LIST_USER,
  LIST_ROLE,
  LIST_USER_PRIV,
  LIST_ROLE_PRIV,
  UPDATE_MAX_USER_SESSION,
  UPDATE_MIN_USER_SESSION,
  // Remind to renew the convert codes in ConfigNodeRPCServiceProcessor
  RENAME_USER,
  ACCOUNT_UNLOCK
}
