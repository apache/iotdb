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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUtils {

  private static final Logger logger = LoggerFactory.getLogger(AuthUtils.class);

  private static final int MIN_PASSWORD_LENGTH = 4;
  private static final int MIN_USERNAME_LENGTH = 4;
  private static final int MIN_ROLENAME_LENGTH = 4;
  private static final String ROOT_PREFIX = IoTDBConstant.PATH_ROOT;
  private static final String ENCRYPT_ALGORITHM = "MD5";
  private static final String STRING_ENCODING = "utf-8";

  public static final String ROOT_PATH_PRIVILEGE =
      IoTDBConstant.PATH_ROOT
          + IoTDBConstant.PATH_SEPARATOR
          + IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;

  private AuthUtils() {}

  /**
   * validate password size.
   *
   * @param password user password
   * @throws AuthException Authenticate Exception
   */
  public static void validatePassword(String password) throws AuthException {
    if (password.length() < MIN_PASSWORD_LENGTH) {
      throw new AuthException(
          "Password's size must be greater than or equal to " + MIN_PASSWORD_LENGTH);
    }
    if (password.contains(" ")) {
      throw new AuthException("Password cannot contain spaces");
    }
  }

  /**
   * validate username.
   *
   * @param username username
   * @throws AuthException Authenticate Exception
   */
  public static void validateUsername(String username) throws AuthException {
    if (username.length() < MIN_USERNAME_LENGTH) {
      throw new AuthException(
          "Username's size must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }
    if (username.contains(" ")) {
      throw new AuthException("Username cannot contain spaces");
    }
  }

  /**
   * validate role name.
   *
   * @param rolename role name
   * @throws AuthException Authenticate Exception
   */
  public static void validateRolename(String rolename) throws AuthException {
    if (rolename.length() < MIN_ROLENAME_LENGTH) {
      throw new AuthException(
          "Role name's size must be greater than or equal to " + MIN_ROLENAME_LENGTH);
    }
    if (rolename.contains(" ")) {
      throw new AuthException("Rolename cannot contain spaces");
    }
  }

  /**
   * validate privilege.
   *
   * @param privilegeId privilege ID
   * @throws AuthException Authenticate Exception
   */
  public static void validatePrivilege(int privilegeId) throws AuthException {
    if (privilegeId < 0 || privilegeId >= PrivilegeType.values().length) {
      throw new AuthException(String.format("Invalid privilegeId %d", privilegeId));
    }
  }

  /**
   * validate series path.
   *
   * @param path series path
   * @throws AuthException Authenticate Exception
   */
  public static void validatePath(String path) throws AuthException {
    if (!path.startsWith(ROOT_PREFIX)) {
      throw new AuthException(
          String.format(
              "Illegal seriesPath %s, seriesPath should start with \"%s\"", path, ROOT_PREFIX));
    }
  }

  /**
   * validate privilege on path.
   *
   * @param path series path
   * @param privilegeId privilege ID
   * @throws AuthException Authenticate Exception
   */
  public static void validatePrivilegeOnPath(String path, int privilegeId) throws AuthException {
    validatePrivilege(privilegeId);
    PrivilegeType type = PrivilegeType.values()[privilegeId];
    if (!path.equals(ROOT_PATH_PRIVILEGE)) {
      validatePath(path);
      switch (type) {
        case READ_TIMESERIES:
        case SET_STORAGE_GROUP:
        case DELETE_STORAGE_GROUP:
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
              String.format("Illegal privilege %s on seriesPath %s", type, path));
      }
    } else {
      switch (type) {
        case READ_TIMESERIES:
        case SET_STORAGE_GROUP:
        case DELETE_STORAGE_GROUP:
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
   * encrypt password.
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

  public static boolean validatePassword(String originPassword, String encryptPassword) {
    return AsymmetricEncryptFactory.getEncryptProvider(
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProvider(),
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProviderParameter())
        .validate(originPassword, encryptPassword);
  }

  /**
   * check if pathA belongs to pathB according to path pattern.
   *
   * @param pathA sub-path
   * @param pathB path
   * @return True if pathA is a sub pattern of pathB, e.g. pathA = "root.a.b.c" and pathB =
   *     "root.a.b.*", "root.a.**", "root.a.*.c", "root.**.c" or "root.*.b.**"
   */
  public static boolean pathBelongsTo(String pathA, String pathB) throws AuthException {
    try {
      PartialPath partialPathA = new PartialPath(pathA);
      PartialPath partialPathB = new PartialPath(pathB);
      return partialPathB.matchFullPath(partialPathA);
    } catch (IllegalPathException e) {
      throw new AuthException(e);
    }
  }

  /**
   * check privilege.
   *
   * @param path series path
   * @param privilegeId privilege ID
   * @param privilegeList privileges in List structure
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
   * get privileges.
   *
   * @param path The seriesPath on which the privileges take effect. If seriesPath-free privileges
   *     are desired, this should be null.
   * @return The privileges granted to the role.
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
   * check if series path has this privilege.
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
   * add privilege.
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure
   */
  public static void addPrivilege(String path, int privilegeId, List<PathPrivilege> privilegeList) {
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        if (privilegeId != PrivilegeType.ALL.ordinal()) {
          pathPrivilege.getPrivileges().add(privilegeId);
        } else {
          for (PrivilegeType privilegeType : PrivilegeType.values()) {
            pathPrivilege.getPrivileges().add(privilegeType.ordinal());
          }
        }
        return;
      }
    }
    PathPrivilege pathPrivilege = new PathPrivilege(path);
    if (privilegeId != PrivilegeType.ALL.ordinal()) {
      pathPrivilege.getPrivileges().add(privilegeId);
    } else {
      for (PrivilegeType privilegeType : PrivilegeType.values()) {
        pathPrivilege.getPrivileges().add(privilegeType.ordinal());
      }
    }
    privilegeList.add(pathPrivilege);
  }

  /**
   * remove privilege.
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure
   */
  public static void removePrivilege(
      String path, int privilegeId, List<PathPrivilege> privilegeList) {
    PathPrivilege emptyPrivilege = null;
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        if (privilegeId != PrivilegeType.ALL.ordinal()) {
          pathPrivilege.getPrivileges().remove(privilegeId);
        } else {
          privilegeList.remove(pathPrivilege);
          return;
        }
        if (pathPrivilege.getPrivileges().isEmpty()) {
          emptyPrivilege = pathPrivilege;
        }
        break;
      }
    }
    if (emptyPrivilege != null) {
      privilegeList.remove(emptyPrivilege);
    }
  }

  public static TPermissionInfoResp generateEmptyPermissionInfoResp() {
    TPermissionInfoResp permissionInfoResp = new TPermissionInfoResp();
    permissionInfoResp.setUserInfo(
        new TUserResp("", "", new ArrayList<>(), new ArrayList<>(), false));
    Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put("", new TRoleResp("", new ArrayList<>()));
    permissionInfoResp.setRoleInfo(roleInfo);
    return permissionInfoResp;
  }

  public static Set<Integer> strToPermissions(String[] authorizationList) throws AuthException {
    Set<Integer> result = new HashSet<>();
    if (authorizationList == null) {
      return result;
    }
    for (String s : authorizationList) {
      PrivilegeType[] types = PrivilegeType.values();
      boolean legal = false;
      for (PrivilegeType privilegeType : types) {
        if (s.equalsIgnoreCase(privilegeType.name())) {
          result.add(privilegeType.ordinal());
          legal = true;
          break;
        }
      }
      if (!legal) {
        throw new AuthException("No such privilege " + s);
      }
    }
    return result;
  }
}
