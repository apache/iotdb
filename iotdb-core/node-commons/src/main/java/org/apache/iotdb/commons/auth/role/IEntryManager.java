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
package org.apache.iotdb.commons.auth.role;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.util.List;

/** We can call user or role as entry of access control, they all can obtain privileges */
public interface IEntryManager extends SnapshotProcessor {

  /**
   * Get an entry object.
   *
   * @param entryName The name of the role.
   * @return A role object whose name is entryName or null if such role does not exist.
   * @throws AuthException if exception is raised while getting the role.
   */
  Role getEntry(String entryName) throws AuthException;

  /**
   * Create a role/user with given entryName. New roles/users will only be granted no privileges.
   *
   * @param entryName is not null or empty
   * @return True if the role is successfully created, false when the role already exists.
   */
  boolean createRole(String entryName);

  /**
   * Delete an entry.
   *
   * @param entryName the name of the user/role.
   * @return boolean, true means we have the role in entryManager.
   */
  boolean deleteEntry(String entryName);

  /**
   * Grant a privilege to an entry.
   *
   * @param entryName The name of the entry to which the privilege should be added.
   * @param privilegeUnion The privilege will be granted to entry.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
   */
  void grantPrivilegeToEntry(String entryName, PrivilegeUnion privilegeUnion) throws AuthException;

  /**
   * Revoke a privilege on seriesPath from an entry.
   *
   * @param entryName The name of the entry from which the privilege should be removed.
   * @param privilegeUnion The privilege will be granted to entry.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
   */
  void revokePrivilegeFromEntry(String entryName, PrivilegeUnion privilegeUnion)
      throws AuthException;

  /** Re-initialize this object. */
  void reset() throws AuthException;

  /**
   * List all users/roles in the database.
   *
   * @return A list that contains names of all roles.
   */
  List<String> listAllEntries();
}
