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
package org.apache.iotdb.commons.auth.user;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.util.List;
import java.util.Map;

/** This interface provides accesses to users. */
public interface IUserManager extends SnapshotProcessor {

  /**
   * Get a user object.
   *
   * @param username The name of the user.
   * @return A user object whose name is username or null if such user does not exist.
   * @throws AuthException if an exception is raised when interacting with the lower storage.
   */
  User getUser(String username) throws AuthException;

  /**
   * Create a user with given username and password. New users will only be granted no privileges.
   *
   * @param username is not null or empty
   * @param password is not null or empty
   * @return True if the user is successfully created, false when the user already exists.
   * @throws AuthException if the given username or password is illegal.
   */
  boolean createUser(String username, String password) throws AuthException;

  /**
   * Delete a user.
   *
   * @param username the username of the user.
   * @return True if the user is successfully deleted, false if the user does not exists.
   * @throws AuthException .
   */
  boolean deleteUser(String username) throws AuthException;

  /**
   * Grant a privilege on a seriesPath to a user.
   *
   * @param username The username of the user to which the privilege should be added.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @return True if the permission is successfully added, false if the permission already exists.
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal.
   */
  boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException;

  /**
   * Revoke a privilege on seriesPath from a user.
   *
   * @param username The username of the user from which the privilege should be removed.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @return True if the permission is successfully revoked, false if the permission does not
   *     exists.
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal.
   */
  boolean revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException;

  /**
   * Modify the password of a user.
   *
   * @param username The user whose password is to be modified.
   * @param newPassword The new password.
   * @return True if the password is successfully modified, false if the new password is illegal.
   * @throws AuthException If the user does not exists.
   */
  boolean updateUserPassword(String username, String newPassword) throws AuthException;

  /**
   * Add a role to a user.
   *
   * @param roleName The name of the role to be added.
   * @param username The name of the user to which the role is added.
   * @return True if the role is successfully added, false if the role already exists.
   * @throws AuthException If the user does not exist.
   */
  boolean grantRoleToUser(String roleName, String username) throws AuthException;

  /**
   * Revoke a role from a user.
   *
   * @param roleName The name of the role to be removed.
   * @param username The name of the user from which the role is removed.
   * @return True if the role is successfully removed, false if the role does not exist.
   * @throws AuthException If the user does not exist.
   */
  boolean revokeRoleFromUser(String roleName, String username) throws AuthException;

  /** Re-initialize this object. */
  void reset() throws AuthException;

  /**
   * List all users in the database.
   *
   * @return A list that contains all users'name.
   */
  List<String> listAllUsers();

  /**
   * Whether data water-mark is enabled for user 'userName'.
   *
   * @param userName
   * @return
   * @throws AuthException if the user does not exist
   */
  boolean isUserUseWaterMark(String userName) throws AuthException;

  /**
   * Enable or disable data water-mark for user 'userName'.
   *
   * @param userName
   * @param useWaterMark
   * @throws AuthException if the user does not exist.
   */
  void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException;

  /**
   * clear all old users info, replace the old users with the new one. The caller should guarantee
   * that no other methods of this interface are invoked concurrently when this method is called.
   *
   * @param users new users info
   * @throws AuthException
   */
  void replaceAllUsers(Map<String, User> users) throws AuthException;
}
