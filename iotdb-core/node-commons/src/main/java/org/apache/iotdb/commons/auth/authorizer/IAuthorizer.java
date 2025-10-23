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
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.auth.entity.Role;
import org.apache.iotdb.commons.auth.entity.User;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.rpc.thrift.TListUserInfo;

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
   */
  boolean login(String username, String password) throws AuthException;

  /**
   * Login for a user in pipe.
   *
   * @param username The username of the user.
   * @param password The password of the user.
   * @return The hashed password
   */
  String login4Pipe(String username, String password);

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
   *     exist.
   */
  void deleteUser(String username) throws AuthException;

  /**
   * Grant a privilege to a user.
   *
   * @param userName The username of the user to which the privilege should be added.
   * @param union A combination of user permissions, scope, and tags
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal.
   */
  void grantPrivilegeToUser(String userName, PrivilegeUnion union) throws AuthException;

  /**
   * Revoke a privilege from a user.
   *
   * @param userName The name of the user from which the privilege should be removed.
   * @param union A combination of user permissions, scope, and tags
   * @throws AuthException If the user does not exist or the privilege or the seriesPath is illegal
   *     or if the permission does not exist.
   */
  void revokePrivilegeFromUser(String userName, PrivilegeUnion union) throws AuthException;

  void revokeAllPrivilegeFromUser(String userName) throws AuthException;

  /**
   * Create a role.
   *
   * @param roleName the name of the role to be added.
   * @throws AuthException if exception raised when adding the role or the role already exists.
   */
  void createRole(String roleName) throws AuthException;

  /**
   * Delete a role.
   *
   * @param roleName the name of the role tobe deleted.
   * @throws AuthException if exception raised when deleting the role or the role does not exist.
   */
  void deleteRole(String roleName) throws AuthException;

  /**
   * Add a privilege to a role.
   *
   * @param roleName The name of the role to which the privilege is added.
   * @param union A combination of user permissions, scope, and tags.
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
   */
  void grantPrivilegeToRole(String roleName, PrivilegeUnion union) throws AuthException;

  /**
   * Remove a privilege from a role.
   *
   * @param roleName The name of the role from which the privilege is removed.
   * @param union A combination of user permissions, scope, and tags
   * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal
   */
  void revokePrivilegeFromRole(String roleName, PrivilegeUnion union) throws AuthException;

  void revokeAllPrivilegeFromRole(String roleName) throws AuthException;

  /**
   * Grant a role to a user.
   *
   * @param roleName The name of the role to be added.
   * @param userName The name of the user to which the role is added.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  void grantRoleToUser(String roleName, String userName) throws AuthException;

  /**
   * Revoke a role from a user.
   *
   * @param roleName The name of the role to be removed.
   * @param userName The name of the user from which the role is removed.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  void revokeRoleFromUser(String roleName, String userName) throws AuthException;

  /**
   * Get the all the privileges of a user on a seriesPath.
   *
   * @param userName The user whose privileges are to be queried.
   * @param path The path where the privileges take effect.
   * @return A set of privilege each present a privilege.
   * @throws AuthException if exception raised when finding the privileges.
   */
  Set<PrivilegeType> getPrivileges(String userName, PartialPath path) throws AuthException;

  /**
   * Modify the password of a user.
   *
   * @param userName The user whose password is to be modified.
   * @param newPassword The new password.
   * @throws AuthException If the user does not exist or the new password is illegal.
   */
  void updateUserPassword(String userName, String newPassword) throws AuthException;

  /**
   * Rename the specified user.
   *
   * @param username The original name of the specified user.
   * @param newUsername The new name to be specified.
   * @throws AuthException If the original name does not exist or the new name is already existed.
   */
  void renameUser(String username, String newUsername) throws AuthException;

  /**
   * Check if the user have the privilege or grant option on the target.
   *
   * @param userName The name of the user whose privileges are checked.
   * @param union A combination of user permissions, scope, and tags
   * @return True if the user has such privilege, false if the user does not have such privilege.
   * @throws AuthException If the seriesPath or the privilege is illegal.
   */
  boolean checkUserPrivileges(String userName, PrivilegeUnion union) throws AuthException;

  /** Reset the Authorizer to initiative status. */
  void reset() throws AuthException;

  /**
   * List existing users in the database.
   *
   * @return A list contains all usernames.
   */
  List<String> listAllUsers();

  /**
   * List existing users info in the database.
   *
   * @return A list contains all users' baisc info including userid, username,maxSessionPerUser and
   *     minSessionPerUser.
   */
  List<TListUserInfo> listAllUsersInfo();

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
   * Find a user by its userId.
   *
   * @param userId the index of the user.
   * @return A user whose id is userId or null if such user does not exist.
   */
  User getUser(long userId) throws AuthException;

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
   * Create a user with given username and password. New users will only be granted no privileges.
   *
   * @param username is not null or empty
   * @param password is not null or empty
   * @throws AuthException if the given username or password is illegal or the user already exists.
   */
  void createUserWithoutCheck(String username, String password) throws AuthException;

  void createUserWithRawPassword(String username, String password) throws AuthException;
}
