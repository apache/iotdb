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

package org.apache.iotdb.commons.auth.authorizer;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** This interface provides all authorization-relative operations. */
public interface IAuthorizer extends SnapshotProcessor {

  /**
   * Login for a user.
   *
   * @param username The username of the user.
   * @param password The password of the user.
   * @return True if such user exists and the given password is correct, else return false.
   * @throws AuthException if exception raised when searching for the user.
   */
  boolean login(String username, String password) throws AuthException;

  /**
   * Create a user with given username and password. New users will only be granted no privileges.
   *
   * @param username is not null or empty
   * @param password is not null or empty
   * @throws AuthException if the given username or password is illegal or the user already exists.
   */
  void createUser(String username, String password) throws AuthException;

  /**
   * Delete a user.
   *
   * @param username the username of the user.
   * @throws AuthException When attempting to delete the default administrator or the user does not
   *     exists.
   */
  void deleteUser(String username) throws AuthException;

  /**
   * Grant a privilege on a seriesPath to a user.
   *
   * @param username The username of the user to which the privilege should be added.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal
   *     or the permission already exists.
   */
  void grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException;

  /**
   * Revoke a privilege on seriesPath from a user.
   *
   * @param username The username of the user from which the privilege should be removed.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal
   *     or if the permission does not exist.
   */
  void revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException;

  /**
   * Add a role.
   *
   * @param roleName the name of the role to be added.
   * @throws AuthException if exception raised when adding the role or the role already exists.
   */
  void createRole(String roleName) throws AuthException;

  /**
   * Delete a role.
   *
   * @param roleName the name of the role tobe deleted.
   * @throws AuthException if exception raised when deleting the role or the role does not exists.
   */
  void deleteRole(String roleName) throws AuthException;

  /**
   * Add a privilege on a seriesPath to a role.
   *
   * @param roleName The name of the role to which the privilege is added.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal
   *     or the privilege already exists.
   */
  void grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException;

  /**
   * Remove a privilege on a seriesPath from a role.
   *
   * @param roleName The name of the role from which the privilege is removed.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal
   *     or the privilege does not exists.
   */
  void revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException;

  /**
   * Add a role to a user.
   *
   * @param roleName The name of the role to be added.
   * @param username The name of the user to which the role is added.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  void grantRoleToUser(String roleName, String username) throws AuthException;

  /**
   * Revoke a role from a user.
   *
   * @param roleName The name of the role to be removed.
   * @param username The name of the user from which the role is removed.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  void revokeRoleFromUser(String roleName, String username) throws AuthException;

  /**
   * Get the all the privileges of a user on a seriesPath.
   *
   * @param username The user whose privileges are to be queried.
   * @param path The seriesPath on which the privileges take effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @return A set of integers each present a privilege.
   * @throws AuthException if exception raised when finding the privileges.
   */
  Set<Integer> getPrivileges(String username, String path) throws AuthException;

  /**
   * Modify the password of a user.
   *
   * @param username The user whose password is to be modified.
   * @param newPassword The new password.
   * @throws AuthException If the user does not exists or the new password is illegal.
   */
  void updateUserPassword(String username, String newPassword) throws AuthException;

  /**
   * Check if the user have the privilege on the seriesPath.
   *
   * @param username The name of the user whose privileges are checked.
   * @param path The seriesPath on which the privilege takes effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @param privilegeId An integer that represents a privilege.
   * @return True if the user has such privilege, false if the user does not have such privilege.
   * @throws AuthException If the seriesPath or the privilege is illegal.
   */
  boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException;

  /** Reset the Authorizer to initiative status. */
  void reset() throws AuthException;

  /**
   * List existing users in the database.
   *
   * @return A list contains all usernames.
   */
  List<String> listAllUsers();

  /**
   * List existing roles in the database.
   *
   * @return A list contains all roleNames.
   */
  List<String> listAllRoles();

  /**
   * Find a role by its name.
   *
   * @param roleName the name of the role.
   * @return A role whose name is roleName or null if such role does not exist.
   */
  Role getRole(String roleName) throws AuthException;

  /**
   * Find a user by its name.
   *
   * @param username the name of the user.
   * @return A user whose name is username or null if such user does not exist.
   */
  User getUser(String username) throws AuthException;

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
   * get all user water mark status
   *
   * @return key->userName, value->useWaterMark or not
   */
  Map<String, Boolean> getAllUserWaterMarkStatus();

  /**
   * get all user
   *
   * @return key-> userName, value->user
   */
  Map<String, User> getAllUsers();

  /**
   * get all role
   *
   * @return key->userName, value->role
   */
  Map<String, Role> getAllRoles();

  /**
   * clear all old users info, replace the old users with the new one
   *
   * @param users new users info
   * @throws AuthException
   */
  void replaceAllUsers(Map<String, User> users) throws AuthException;

  /**
   * clear all old roles info, replace the old roles with the new one
   *
   * @param roles new roles info
   * @throws AuthException
   */
  void replaceAllRoles(Map<String, Role> roles) throws AuthException;
}
