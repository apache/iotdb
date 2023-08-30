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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathDeserializeUtil;
import org.apache.iotdb.commons.path.PathPatternUtil;
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncryptFactory;
import org.apache.iotdb.confignode.rpc.thrift.TAuthizedPatternTreeResp;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.rpc.thrift.TRoleResp;
import org.apache.iotdb.confignode.rpc.thrift.TUserResp;
import org.apache.iotdb.rpc.TSStatusCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUtils {
  private static final Logger logger = LoggerFactory.getLogger(AuthUtils.class);
  private static final String ROOT_PREFIX = IoTDBConstant.PATH_ROOT;
  public static PartialPath ROOT_PATH_PRIVILEGE_PATH;
  private static final int MIN_LENGTH = 4;
  private static final int MAX_LENGTH = 64;
  private static final String REX_PATTERN = "^[-\\w]*$";

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
    validateNameOrPassword(password);
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
    validateNameOrPassword(username);
  }

  /**
   * Validate role name
   *
   * @param rolename role name
   * @throws AuthException contains message why rolename is invalid
   */
  public static void validateRolename(String rolename) throws AuthException {
    validateNameOrPassword(rolename);
  }

  public static void validateNameOrPassword(String str) throws AuthException {
    int length = str.length();
    if (length < MIN_LENGTH) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "The length of name or password must be greater than or equal to " + MIN_LENGTH);
    } else if (length > MAX_LENGTH) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "The length of name or password must be less than or equal to " + MAX_LENGTH);
    } else if (str.contains(" ")) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER, "The name or password cannot contain spaces");
    } else if (!str.matches(REX_PATTERN)) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          "The name or password can only contain letters, numbers, and underscores");
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
  public static void validatePath(PartialPath path) throws AuthException {
    if (!path.getFirstNode().equals(ROOT_PREFIX)) {
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          String.format(
              "Illegal seriesPath %s, seriesPath should start with \"%s\"", path, ROOT_PREFIX));
    }
  }

  public static void validatePatternPath(PartialPath path) throws AuthException {
    if (!path.hasWildcard()) {
      return;
    } else if (!PathPatternUtil.hasWildcard(path.getTailNode())) {
      // check a.b.*.c/a.b.**.c/a.b*.c
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          String.format(
              "Illegal pattern path: %s, only pattern path that end with wildcards are supported.",
              path));
    }
    for (int i = 0; i < path.getNodeLength() - 1; i++) {
      if (PathPatternUtil.hasWildcard(path.getNodes()[i])) {
        throw new AuthException(
            TSStatusCode.ILLEGAL_PARAMETER,
            String.format(
                "Illegal pattern path: %s, only pattern path that end with wildcards are supported.",
                path));
      }
    }
  }

  /**
   * Validate privilege on path
   *
   * @param path the path of privilege
   * @param privilegeId privilege Id
   * @throws AuthException contains message why path is invalid
   */
  public static void validatePrivilege(PartialPath path, int privilegeId) throws AuthException {
    validatePrivilege(privilegeId);
    PrivilegeType type = PrivilegeType.values()[privilegeId];
    if (path != null) {
      validatePath(path);
      switch (type) {
        case READ_SCHEMA:
        case WRITE_SCHEMA:
        case READ_DATA:
        case WRITE_DATA:
          return;
        default:
          throw new AuthException(
              TSStatusCode.UNKNOWN_AUTH_PRIVILEGE,
              String.format("Illegal privilege %s on seriesPath", type));
      }
    } else {
      switch (type) {
        case MANAGE_DATABASE:
        case MANAGE_USER:
        case MANAGE_ROLE:
        case USE_TRIGGER:
        case USE_CQ:
        case USE_PIPE:
        case USE_UDF:
        case EXTEND_TEMPLATE:
        case MAINTAIN:
        case AUDIT:
          return;
        default:
          throw new AuthException(
              TSStatusCode.UNKNOWN_AUTH_PRIVILEGE, String.format("Illegal privilege %s", type));
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
   * Check path privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure
   * @exception AuthException throw if path is invalid or path in privilege is invalid
   * @return True if privilege-check passed
   */
  public static boolean checkPathPrivilege(
      PartialPath path, int privilegeId, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return false;
    }
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path)
          && pathPrivilege.getPrivileges().contains(privilegeId)) {
        return true;
      }
    }
    return false;
  }

  public static boolean checkPathPrivilegeGrantOpt(
      PartialPath path, int privilegeId, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return false;
    }
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path)
          && pathPrivilege.getPrivileges().contains(privilegeId)
          && pathPrivilege.getGrantOpt().contains(privilegeId)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get privileges
   *
   * @param path The seriesPath on which the privileges take effect.
   * @exception AuthException throw if path is invalid or path in privilege is invalid
   * @return The privileges granted to the role
   */
  public static Set<Integer> getPrivileges(PartialPath path, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return new HashSet<>();
    }
    Set<Integer> privileges = new HashSet<>();
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path)) {
        privileges.addAll(pathPrivilege.getPrivileges());
        // shall we return immediately?
        return privileges;
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
      PartialPath path, int privilegeId, List<PathPrivilege> privilegeList) {
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)
          && pathPrivilege.getPrivileges().contains(privilegeId)) {
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
  public static void addPrivilege(
      PartialPath path, int privilegeId, List<PathPrivilege> privilegeList, boolean grantOption) {
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
    targetPathPrivilege.grantPrivilege(privilegeId, grantOption);
  }

  /**
   * Remove privilege
   *
   * @param path series path
   * @param privilegeId privilege Id
   * @param privilegeList privileges in List structure of user or role
   */
  public static void removePrivilege(
      PartialPath path, int privilegeId, List<PathPrivilege> privilegeList) {
    PathPrivilege targetPathPrivilege = null;
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().equals(path)) {
        targetPathPrivilege = pathPrivilege;
        break;
      }
    }
    if (targetPathPrivilege != null) {
      targetPathPrivilege.revokePrivilege(privilegeId);
      if (targetPathPrivilege.getPrivileges().isEmpty()) {
        privilegeList.remove(targetPathPrivilege);
      }
    }
  }

  /** Generate empty permission response when failed */
  public static TPermissionInfoResp generateEmptyPermissionInfoResp() {
    TPermissionInfoResp permissionInfoResp = new TPermissionInfoResp();
    permissionInfoResp.setUserInfo(
        new TUserResp(
            "", "", new ArrayList<>(), new HashSet<>(), new HashSet<>(), new ArrayList<>(), false));
    Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put("", new TRoleResp("", new ArrayList<>(), new HashSet<>(), new HashSet<>()));
    permissionInfoResp.setRoleInfo(roleInfo);
    return permissionInfoResp;
  }

  public static TAuthizedPatternTreeResp generateEmptyAuthizedPTree(
      String username, int privilegeId) {
    TAuthizedPatternTreeResp resp = new TAuthizedPatternTreeResp();
    resp.setUsername(username);
    resp.setPrivilegeId(privilegeId);
    resp.setPermissionInfo(generateEmptyPermissionInfoResp());
    return resp;
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

  public static ByteBuffer serializePartialPathList(List<PartialPath> paths) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      dataOutputStream.writeInt(paths.size());
      for (PartialPath path : paths) {
        path.serialize(dataOutputStream);
      }
    } catch (IOException e) {
      logger.error("Failed to serialize PartialPath list", e);
    }
    return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
  }

  public static List<PartialPath> deserializePartialPathList(ByteBuffer buffer) {
    int size = buffer.getInt();
    List<PartialPath> paths = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      paths.add((PartialPath) PathDeserializeUtil.deserialize(buffer));
    }
    return paths;
  }
}
