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

package org.apache.iotdb.db.queryengine.plan.statement;

// If you need to add a new type, you need to add it to the end because the ordinal will be used as
// offset to calculate ConfigPhysicalPlanType
// When adding new types that need to be converted to ConfigPhysicalPlanType, you can refer to this
// document:
// https://docs.google.com/document/d/1WvAyuLn1y988svLl8rGUEcesMkAyH6UTT597KQAVm1A/edit?usp=sharing
public enum AuthorType {
  // The ConfigPhysicalPlanType in this part can be automatically converted according to the offset
  // in this class
  CREATE_USER,
  CREATE_ROLE,
  DROP_USER,
  DROP_ROLE,
  GRANT_ROLE,
  GRANT_USER,
  GRANT_USER_ROLE,
  REVOKE_USER,
  REVOKE_ROLE,
  REVOKE_USER_ROLE,
  UPDATE_USER,
  LIST_USER,
  LIST_ROLE,
  LIST_USER_PRIVILEGE,
  LIST_ROLE_PRIVILEGE,

  ACCOUNT_UNLOCK,

  // Remind to renew the convert codes in ConfigNodeRPCServiceProcessor
  // If the config node plan is involved, the type defined in following lines needs to manually add
  // conversion logic
  RENAME_USER,
  UPDATE_USER_MAX_SESSION,
  UPDATE_USER_MIN_SESSION,
  ;
}
