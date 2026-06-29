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

package org.apache.iotdb.commons.auth.utils;

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.path.PathPatternTree;

import static org.apache.iotdb.commons.schema.table.Audit.TREE_MODEL_AUDIT_DATABASE_PATH_PATTERN;

public class AuthUtils {

  private AuthUtils() {}

  public static void constructAuthorityScope(
      PathPatternTree patternTree, Role role, PrivilegeType permission) {
    for (PathPrivilege path : role.getPathPrivilegeList()) {
      if (path.checkPrivilege(permission)) {
        patternTree.appendPathPattern(path.getPath());
      }
    }
    // for audit admin, we need to add authority scope root.__audit.**
    if (permission == PrivilegeType.READ_DATA || permission == PrivilegeType.READ_SCHEMA) {
      for (PrivilegeType globalPrivilege : role.getSysPrivilege()) {
        if (globalPrivilege == PrivilegeType.AUDIT) {
          patternTree.appendPathPattern(TREE_MODEL_AUDIT_DATABASE_PATH_PATTERN);
        }
      }
    }
  }
}
