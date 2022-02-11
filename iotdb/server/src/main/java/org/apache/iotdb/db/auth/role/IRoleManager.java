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
package org.apache.iotdb.db.auth.role;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.Role;

import java.util.List;
import java.util.Map;

/** This interface maintains roles in memory and is responsible for their modifications. */
public interface IRoleManager {

  /**
   * Get a role object.
   *
   * @param rolename The name of the role.
   * @return A role object whose name is rolename or null if such role does not exist.
   * @throws AuthException if exception is raised while getting the role.
   */
  Role getRole(String rolename) throws AuthException;

  /**
   * Create a role with given rolename. New roles will only be granted no privileges.
   *
   * @param rolename is not null or empty
   * @return True if the role is successfully created, false when the role already exists.
   * @throws AuthException f the given rolename is iIllegal.
   */
  boolean createRole(String rolename) throws AuthException;

  /**
   * Delete a role.
   *
   * @param rolename the rolename of the role.
   * @return True if the role is successfully deleted, false if the role does not exists.
   * @throws AuthException if exception is raised while finding the role.
   */
  boolean deleteRole(String rolename) throws AuthException;

  /**
   * Grant a privilege on a seriesPath to a role.
   *
   * @param rolename The rolename of the role to which the privilege should be added.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @return True if the permission is successfully added, false if the permission already exists.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
   */
  boolean grantPrivilegeToRole(String rolename, String path, int privilegeId) throws AuthException;

  /**
   * Revoke a privilege on seriesPath from a role.
   *
   * @param rolename The rolename of the role from which the privilege should be removed.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege like 'CREATE_USER', this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @return True if the permission is successfully revoked, false if the permission does not
   *     exists.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
   */
  boolean revokePrivilegeFromRole(String rolename, String path, int privilegeId)
      throws AuthException;

  /** Re-initialize this object. */
  void reset();

  /**
   * List all roles in the database.
   *
   * @return A list that contains names of all roles.
   */
  List<String> listAllRoles();

  /**
   * clear all old roles info, replace the old roles with the new one. The caller should guarantee
   * that no other methods of this interface are invoked concurrently when this method is called.
   *
   * @param roles new roles info
   * @throws AuthException
   */
  void replaceAllRoles(Map<String, Role> roles) throws AuthException;
}
