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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncryptFactory;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUtils {
  private static final String ROOT_PREFIX = IoTDBConstant.PATH_ROOT;
  public static final String ROOT_PATH_PRIVILEGE =
      IoTDBConstant.PATH_ROOT
          + IoTDBConstant.PATH_SEPARATOR
          + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
  private static final int MIN_PASSWORD_LENGTH = 4;
  private static final int MIN_USERNAME_LENGTH = 4;
  private static final int MIN_ROLENAME_LENGTH = 4;

  private AuthUtils() {
    // Empty constructor
  }

  /**
   * Validate password
   *
   * @param password user password
   * @throws AuthException contains message why password is invalid
   */
  public static void validatePassword(String password) throws AuthException {
    if (password.length() < MIN_PASSWORD_LENGTH) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "Password's size must be greater than or equal to " + MIN_PASSWORD_LENGTH);
    }
    if (password.contains(" ")) {
      throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, "Password cannot contain spaces");
    }
  }

  /**
   * Checking whether origin password is mapping to encrypt password by encryption
   *
   * @param originPassword the password before encryption
   * @param encryptPassword the password after encryption
   */
  public static boolean validatePassword(String originPassword, String encryptPassword) {
    return AsymmetricEncryptFactory.getEncryptProvider(
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProvider(),
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProviderParameter())
        .validate(originPassword, encryptPassword);
  }

  /**
   * Validate username
   *
   * @param username username
   * @throws AuthException contains message why username is invalid
   */
  public static void validateUsername(String username) throws AuthException {
    if (username.length() < MIN_USERNAME_LENGTH) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "Username's size must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }
    if (username.contains(" ")) {
      throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, "Username cannot contain spaces");
    }
  }

  /**
   * Validate role name
   *
   * @param rolename role name
   * @throws AuthException contains message why rolename is invalid
   */
  public static void validateRolename(String rolename) throws AuthException {
    if (rolename.length() < MIN_ROLENAME_LENGTH) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "Role name's size must be greater than or equal to " + MIN_ROLENAME_LENGTH);
    }
    if (rolename.contains(" ")) {
      throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, "Role name cannot contain spaces");
    }
  }

  /**
   * Validate privilege
   *
   * @param privilegeId privilege ID
   * @throws AuthException contains message why privilege is invalid
   */
  public static void validatePrivilege(int privilegeId) throws AuthException {
    if (privilegeId < 0 || privilegeId >= PrivilegeType.values().length) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER, String.format("Invalid privilegeId %d", privilegeId));
    }
  }

  /**
   * Validate path
   *
   * @param path series path
   * @throws AuthException contains message why path is invalid
   */
  public static void validatePath(String path) throws AuthException {
    if (!path.startsWith(ROOT_PREFIX)) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          String.format(
              "Illegal seriesPath %s, seriesPath should start with \"%s\"", path, ROOT_PREFIX));
    }
  }

  /**
   * Validate privilege on path
   *
   * @param path the path of privilege
   * @param privilegeId privilege Id
   * @throws AuthException contains message why path is invalid
   */
  public static void validatePrivilegeOnPath(String path, int privilegeId) throws AuthException {
    validatePrivilege(privilegeId);
    PrivilegeType type = PrivilegeType.values()[privilegeId];
    if (!path.equals(ROOT_PATH_PRIVILEGE)) {
      validatePath(path);
      switch (type) {
        case READ_TIMESERIES:
        case CREATE_DATABASE:
        case DELETE_DATABASE:
        case CREATE_TIMESERIES:
        case DELETE_TIMESERIES:
        case INSERT_TIMESERIES:
        case ALTER_TIMESERIES:
        case CREATE_TRIGGER:
        case DROP_TRIGGER:
        case START_TRIGGER:
        case STOP_TRIGGER:
        case APPLY_TEMPLATE:
          return;
        default:
          throw new AuthException(
              TSStatusCode.UNKNOWN_AUTH_PRIVILEGE,
              String.format("Illegal privilege %s on seriesPath %s", type, path));
      }
    } else {
      switch (type) {
        case READ_TIMESERIES:
        case CREATE_DATABASE:
        case DELETE_DATABASE:
        case CREATE_TIMESERIES:
        case DELETE_TIMESERIES:
        case INSERT_TIMESERIES:
        case ALTER_TIMESERIES:
          validatePath(path);
          return;
        default:
          return;
      }
    }
  }

  /**
   * Encrypt password
   *
   * @param password password
   * @return encrypted password if success
   */
  public static String encryptPassword(String password) {
    return AsymmetricEncryptFactory.getEncryptProvider(
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProvider(),
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProviderParameter())
        .encrypt(password);
  }

  /**
   * Check if pathA belongs to pathB according to path pattern.
   *
   * @param pathA sub-path
   * @param pathB path
   * @exception AuthException throw if pathA or pathB is invalid
   * @return True if pathA is a sub pattern of pathB, e.g. pathA = "root.a.b.c" and pathB =
   *     "root.a.b.*", "root.a.**", "root.a.*.c", "root.**.c" or "root.*.b.**"
   */
  public static boolean pathBelongsTo(String pathA, String pathB) throws AuthException {
    try {
      PartialPath partialPathA = new PartialPath(pathA);
      PartialPath partialPathB = new PartialPath(pathB);
      return partialPathB.matchFullPath(partialPathA);
    } catch (IllegalPathException e) {
      throw new AuthException(TSStatusCode.ILLEGAL_PARAMETER, e);
    }
  }

  /**
   * Check privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure
   * @exception AuthException throw if path is invalid or path in privilege is invalid
   * @return True if privilege-check passed
   */
  public static boolean checkPrivilege(
      String path, int privilegeId, List<PathPrivilege> privilegeList) throws AuthException {
    if (privilegeList == null) {
      return false;
    }
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (path != null) {
        if (pathPrivilege.getPath() != null
            && AuthUtils.pathBelongsTo(path, pathPrivilege.getPath())
            && pathPrivilege.getPrivileges().contains(privilegeId)) {
          return true;
        }
      } else {
        if (pathPrivilege.getPath() == null
            && pathPrivilege.getPrivileges().contains(privilegeId)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get privileges
   *
   * @param path The seriesPath on which the privileges take effect. If seriesPath-free privileges
   *     are desired, this should be null
   * @exception AuthException throw if path is invalid or path in privilege is invalid
   * @return The privileges granted to the role
   */
  public static Set<Integer> getPrivileges(String path, List<PathPrivilege> privilegeList)
      throws AuthException {
    if (privilegeList == null) {
      return new HashSet<>();
    }
    Set<Integer> privileges = new HashSet<>();
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (path != null) {
        if (pathPrivilege.getPath() != null
            && AuthUtils.pathBelongsTo(path, pathPrivilege.getPath())) {
          privileges.addAll(pathPrivilege.getPrivileges());
        }
      } else {
        if (pathPrivilege.getPath() == null) {
          privileges.addAll(pathPrivilege.getPrivileges());
        }
      }
    }
    return privileges;
  }

  /**
   * Check if series path has this privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure
   * @return True if series path has this privilege
   */
  public static boolean hasPrivilege(
      String path, int privilegeId, List<PathPrivilege> privilegeList) {
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)
          && pathPrivilege.getPrivileges().contains(privilegeId)) {
        pathPrivilege.getReferenceCnt().incrementAndGet();
        return true;
      }
    }
    return false;
  }

  /**
   * Add privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure of user or role
   */
  public static void addPrivilege(String path, int privilegeId, List<PathPrivilege> privilegeList) {
    PathPrivilege targetPathPrivilege = null;
    // check PathPrivilege of target path is already existed
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        targetPathPrivilege = pathPrivilege;
        break;
      }
    }
    // if not, then create new PathPrivilege
    if (targetPathPrivilege == null) {
      targetPathPrivilege = new PathPrivilege(path);
      privilegeList.add(targetPathPrivilege);
    }
    // add privilegeId into targetPathPrivilege
    if (privilegeId != PrivilegeType.ALL.ordinal()) {
      targetPathPrivilege.getPrivileges().add(privilegeId);
    } else {
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        targetPathPrivilege.getPrivileges().add(privilegeType.ordinal());
      }
    }
  }

  /**
   * Remove privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure of user or role
   */
  public static void removePrivilege(
      String path, int privilegeId, List<PathPrivilege> privilegeList) {
    PathPrivilege targetPathPrivilege = null;
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        targetPathPrivilege = pathPrivilege;
        break;
      }
    }
    if (targetPathPrivilege != null) {
      if (privilegeId == PrivilegeType.ALL.ordinal()) {
        // remove all privileges on target path
        privilegeList.remove(targetPathPrivilege);
      } else {
        // remove privilege on target path
        targetPathPrivilege.getPrivileges().remove(privilegeId);
        if (targetPathPrivilege.getPrivileges().isEmpty()) {
          privilegeList.remove(targetPathPrivilege);
        }
      }
    }
  }

  /** Generate empty permission response when failed */
  public static TPermissionInfoResp generateEmptyPermissionInfoResp() {
    TPermissionInfoResp permissionInfoResp = new TPermissionInfoResp();
    permissionInfoResp.setUserInfo(
        new TUserResp("", "", new ArrayList<>(), new ArrayList<>(), false));
    Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put("", new TRoleResp("", new ArrayList<>()));
    permissionInfoResp.setRoleInfo(roleInfo);
    return permissionInfoResp;
  }

  /**
   * Transform permission from name to privilegeId
   *
   * @param authorizationList the list of privilege name
   * @return the list of privilege Ids
   * @throws AuthException throws if there are no privilege matched
   */
  public static Set<Integer> strToPermissions(String[] authorizationList) throws AuthException {
    Set<Integer> result = new HashSet<>();
    if (authorizationList == null) {
      return result;
    }
    PrivilegeType[] types = PrivilegeType.values();
    for (String authorization : authorizationList) {
      boolean legal = false;
      if ("SET_STORAGE_GROUP".equalsIgnoreCase(authorization)) {
        authorization = PrivilegeType.CREATE_DATABASE.name();
      }
      if ("DELETE_STORAGE_GROUP".equalsIgnoreCase(authorization)) {
        authorization = PrivilegeType.DELETE_DATABASE.name();
      }
      for (PrivilegeType privilegeType : types) {
        if (authorization.equalsIgnoreCase(privilegeType.name())) {
          result.add(privilegeType.ordinal());
          legal = true;
          break;
        }
      }
      if (!legal) {
        throw new AuthException(
            TSStatusCode.UNKNOWN_AUTH_PRIVILEGE, "No such privilege " + authorization);
      }
    }
    return result;
  }
}
