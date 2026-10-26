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

package org.apache.iotdb.commons.i18n;

public final class AuthMessages {

  // ======================== BasicAuthorizer ========================

  public static final String AUTHORIZER_INIT_COMPLETE = "Initialization of Authorizer completes";
  public static final String AUTHORIZER_UNINITIALIZED = "Authorizer uninitialized";
  public static final String AUTHORIZER_PROVIDER_CLASS = "Authorizer provider class: {}";
  public static final String AUTHORIZER_INIT_FAILED = "Authorizer could not be initialized!";

  public static final String NO_SUCH_ROLE = "No such role : %s";
  public static final String NO_SUCH_USER = "No such user : %s";

  public static final String USER_NOT_EXIST = "The user %s does not exist.";
  public static final String INCORRECT_PASSWORD = "Incorrect password.";
  public static final String USER_ALREADY_EXISTS = "User %s already exists";
  public static final String USER_DOES_NOT_EXIST = "User %s does not exist";
  public static final String ROLE_ALREADY_EXISTS = "Role %s already exists";
  public static final String ROLE_DOES_NOT_EXIST = "Role %s does not exist";

  public static final String ADMIN_CANNOT_BE_DELETED = "Default administrator cannot be deleted";
  public static final String ADMIN_ALREADY_HAS_ALL_PRIVILEGES =
      "Invalid operation, administrator already has all privileges";
  public static final String ADMIN_MUST_HAVE_ALL_PRIVILEGES =
      "Invalid operation, administrator must have all privileges";
  public static final String ADMIN_CANNOT_REVOKE_PRIVILEGES =
      "Invalid operation, administrator cannot revoke privileges";
  public static final String CANNOT_GRANT_ROLE_TO_ADMIN =
      "Invalid operation, cannot grant role to administrator";
  public static final String CANNOT_REVOKE_ROLE_FROM_ADMIN =
      "Invalid operation, cannot revoke role from administrator ";

  public static final String REVOKE_ROLE_FROM_USER_ERROR =
      "Error encountered when revoking a role {} from user {} after deletion";

  public static final String PASSWORD_IS_ILLEGAL = "password %s is illegal";

  public static final String GET_ALL_USERS_FAILED = "get all users failed, No such user: {}";
  public static final String GET_ALL_ROLES_FAILED = "get all roles failed, No such role: {}";

  public static final String ROLE_ALREADY_EXISTS_LOG = "Role {} already exists";

  // ======================== BasicRoleManager ========================

  public static final String NO_SUCH_ROLE_FMT = "No such role %s";
  public static final String NO_SUCH_USER_FMT = "No such user %s";

  public static final String NOT_SUPPORT_MODEL_TYPE = "Not support model type {}";

  public static final String LOAD_ROLE_EXCEPTION = "Get exception when load role {}";

  // ======================== BasicUserManager ========================

  public static final String WRONG_PATH_INIT = "Got a wrong path for {} to init";
  public static final String INTERNAL_USER_INITIALIZED = "Internal user {} initialized";
  public static final String LOAD_MAX_USER_ID_ERROR = "meet error in load max userId.";
  public static final String PASSWORD_SAME_AS_USERNAME =
      "Password cannot be the same as user name";
  public static final String BUILTIN_USERNAME_IN_USE =
      "Builtin username of admin is already in use";
  public static final String LOAD_USER_EXCEPTION = "Get exception when load user {}";

  public static final String RENAME_USER_TARGET_EXISTS =
      "Cannot rename user %s to %s, because the target username is already existed.";

  // ======================== LocalFileRoleAccessor ========================

  public static final String NEW_PROFILE_RENAME_FAILED = "New profile renaming not succeed.";
  public static final String FAILED_TO_CREATE_ROLE_DIR = "Failed to create role dir {}";
  public static final String SAVE_ROLE_ERROR = "meet error when save role: {}";
  public static final String CANNOT_DELETE_ROLE_FILE = "Cannot delete role file of %s";
  public static final String FAILED_TO_LOAD_ROLE_SNAPSHOT =
      "Failed to load role folder snapshot and rollback.";
  public static final String NO_ROLES_TO_LOAD = "There are no roles to load.";
  public static final String ROLE_INFO_DIR_CREATED = "role info dir {} is created";
  public static final String ROLE_INFO_DIR_CREATE_FAILED = "role info dir {} can not be created";
  public static final String MOVE_OLD_ROLE_DIR_FAILED = "move old role dir fail: {}";
  public static final String ROLE_FOLDER_NOT_EXISTS = "Role folder not exists";
  public static final String FAILED_TO_CREATE_USER_DIR = "Failed to create user dir {}";
  public static final String SAVE_USER_ID_ERROR = "meet error when save userId: {}";

  // ======================== LocalFileUserAccessor ========================

  public static final String SAVE_USER_ROLES_ERROR =
      "Get Exception when save user {}'s roles";
  public static final String FAILED_TO_LOAD_USER_SNAPSHOT =
      "Failed to load user folder snapshot and rollback.";
  public static final String CATCH_ERROR_DELETE_USER_ROLE =
      "Catch error when delete %s 's role";

  // ======================== PrivilegeType ========================

  public static final String UNEXPECTED_PRIVILEGE_VALUE = "Unexpected value:";

  private AuthMessages() {}

  public static final String NAME_CANNOT_CONTAIN_SPACES = "The name cannot contain spaces";
  public static final String PASSWORD_CANNOT_CONTAIN_SPACES = "The password cannot contain spaces";
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_ACCESS_DENIED_E128C1A1 = "Access Denied: ";
  public static final String EXCEPTION_ENCRYPT_DECRYPT_FAILED_163764B5 = "Encrypt or decrypt failed.";
  public static final String EXCEPTION_BACKSLASH_QUOTE_D4C5D637 = "\"";

}
