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
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncrypt;
import org.apache.iotdb.commons.security.encrypt.AsymmetricEncryptFactory;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AuthUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(AuthUtils.class);
  private static final String ROOT_PREFIX = IoTDBConstant.PATH_ROOT;
  private static final int MIN_LENGTH = 4;
  private static final int MAX_LENGTH = 32;

  // match number, character, and !@#$%^*()_+-=
  // pattern: ^[-\w!@#\$%\^\(\)\+=]*$
  private static final String REX_PATTERN = "^[-\\w!@#$%^&*()+=]*$";

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
        .validate(originPassword, encryptPassword, AsymmetricEncrypt.DigestAlgorithm.SHA_256);
  }

  /**
   * Checking whether origin password is mapping to encrypt password by encryption
   *
   * @param originPassword the password before encryption
   * @param encryptPassword the password after encryption
   * @param digestAlgorithm the algorithm for encryption
   */
  public static boolean validatePassword(
      String originPassword,
      String encryptPassword,
      AsymmetricEncrypt.DigestAlgorithm digestAlgorithm) {
    return AsymmetricEncryptFactory.getEncryptProvider(
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProvider(),
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProviderParameter())
        .validate(originPassword, encryptPassword, digestAlgorithm);
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
   * @param roleName role name
   * @throws AuthException contains message why roleName is invalid
   */
  public static void validateRolename(String roleName) throws AuthException {
    validateNameOrPassword(roleName);
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
          "The name or password can only contain letters, numbers or !@#$%^*()_+-=");
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
    validatePath(path);
    if (!path.hasWildcard()) {
      return;
    } else if (!PathPatternUtil.isMultiLevelMatchWildcard(path.getTailNode())) {
      // check a.b.*.c/ a.b.**.c/ a.b*.c/ a.b.c.*
      throw new AuthException(
          TSStatusCode.ILLEGAL_PARAMETER,
          String.format(
              "Illegal pattern path: %s, only pattern path that end with ** are supported.", path));
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
   * Encrypt password
   *
   * @param password password
   * @return encrypted password if success
   */
  public static String encryptPassword(String password) {
    return AsymmetricEncryptFactory.getEncryptProvider(
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProvider(),
            CommonDescriptor.getInstance().getConfig().getEncryptDecryptProviderParameter())
        .encrypt(password, AsymmetricEncrypt.DigestAlgorithm.SHA_256);
  }

  /**
   * Check path privilege
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure
   * @return True if privilege-check passed
   */
  public static boolean checkPathPrivilege(
      PartialPath path, PrivilegeType priv, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return false;
    }
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path) && pathPrivilege.checkPrivilege(priv)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Check path privilege grant option
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure
   * @return True if privilege-check passed
   */
  public static boolean checkPathPrivilegeGrantOpt(
      PartialPath path, PrivilegeType priv, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return false;
    }
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path)
          && pathPrivilege.getPrivileges().contains(priv)
          && pathPrivilege.getGrantOpt().contains(priv)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get privileges
   *
   * @param path The seriesPath on which the privileges take effect.
   * @return The privileges granted to the role
   */
  public static Set<PrivilegeType> getPrivileges(
      PartialPath path, List<PathPrivilege> privilegeList) {
    if (privilegeList == null) {
      return new HashSet<>();
    }
    Set<PrivilegeType> privileges = new HashSet<>();
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (pathPrivilege.getPath().matchFullPath(path)) {
        for (Integer item : pathPrivilege.getPrivilegeIntSet()) {
          privileges.add(PrivilegeType.values()[item]);
        }
      }
    }
    return privileges;
  }

  /**
   * Check if series path has this privilege to revoke
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure
   * @return True if series path has this privilege
   */
  public static boolean hasPrivilegeToRevoke(
      PartialPath path, PrivilegeType priv, List<PathPrivilege> privilegeList) {
    for (PathPrivilege pathPrivilege : privilegeList) {
      if (path.matchFullPath(pathPrivilege.getPath())
          && pathPrivilege.getPrivileges().contains(priv)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Add privilege
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure of user or role
   */
  public static void addPrivilege(
      PartialPath path,
      PrivilegeType priv,
      List<PathPrivilege> privilegeList,
      boolean grantOption) {
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
    targetPathPrivilege.grantPrivilege(priv, grantOption);
  }

  /**
   * Remove privilege
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure of user or role
   */
  public static void removePrivilege(
      PartialPath path, PrivilegeType priv, List<PathPrivilege> privilegeList) {
    Iterator<PathPrivilege> it = privilegeList.iterator();
    while (it.hasNext()) {
      PathPrivilege pathPri = it.next();
      if (path.matchFullPath(pathPri.getPath())) {
        pathPri.revokePrivilege(priv);
        if (pathPri.getPrivilegeIntSet().isEmpty()) {
          it.remove();
        }
      }
    }
  }

  /**
   * Remove privilege grant option
   *
   * @param path series path
   * @param priv privilege type
   * @param privilegeList privileges in List structure of user or role
   */
  public static void removePrivilegeGrantOption(
      PartialPath path, PrivilegeType priv, List<PathPrivilege> privilegeList) {
    for (PathPrivilege pathPri : privilegeList) {
      if (path.matchFullPath(pathPri.getPath())) {
        pathPri.revokeGrantOpt(priv);
      }
    }
  }

  /** Generate empty permission response when failed */
  public static TPermissionInfoResp generateEmptyPermissionInfoResp() {
    TPermissionInfoResp permissionInfoResp = new TPermissionInfoResp();
    permissionInfoResp.setUserInfo(
        new TUserResp(
            new TRoleResp(
                "",
                new ArrayList<>(),
                new HashSet<>(),
                new HashSet<>(),
                new HashMap<>(),
                new HashSet<>(),
                new HashSet<>()),
            "",
            new HashSet<>(),
            false));
    Map<String, TRoleResp> roleInfo = new HashMap<>();
    roleInfo.put(
        "",
        new TRoleResp(
            "",
            new ArrayList<>(),
            new HashSet<>(),
            new HashSet<>(),
            new HashMap<>(),
            new HashSet<>(),
            new HashSet<>()));
    permissionInfoResp.setRoleInfo(roleInfo);
    return permissionInfoResp;
  }

  /**
   * Transform permission from name to privilegeId
   *
   * @param authorizationList the list of privilege name
   * @return the set of privilege type
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

  public static ByteBuffer serializePartialPathList(List<? extends PartialPath> paths) {
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);

    try {
      dataOutputStream.writeInt(paths.size());
      for (PartialPath path : paths) {
        path.serialize(dataOutputStream);
      }
    } catch (IOException e) {
      LOGGER.error("Failed to serialize PartialPath list", e);
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

  // deserialize privilege type from an int mask.
  public static PrivilegeType posToSysPri(int pos) {
    switch (pos) {
      case 0:
        return PrivilegeType.MANAGE_DATABASE;
      case 1:
        return PrivilegeType.MANAGE_USER;
      case 2:
        return PrivilegeType.MANAGE_ROLE;
      case 3:
        return PrivilegeType.USE_TRIGGER;
      case 4:
        return PrivilegeType.USE_UDF;
      case 5:
        return PrivilegeType.USE_CQ;
      case 6:
        return PrivilegeType.USE_PIPE;
      case 7:
        return PrivilegeType.EXTEND_TEMPLATE;
      case 8:
        return PrivilegeType.MAINTAIN;
      case 9:
        return PrivilegeType.USE_MODEL;
      default:
        // Not reach here.
        LOGGER.warn("Not support position");
        throw new RuntimeException("Not support position");
    }
  }

  public static int sysPriToPos(PrivilegeType priv) {
    switch (priv) {
      case MANAGE_DATABASE:
        return 0;
      case MANAGE_USER:
        return 1;
      case MANAGE_ROLE:
        return 2;
      case USE_TRIGGER:
        return 3;
      case USE_UDF:
        return 4;
      case USE_CQ:
        return 5;
      case USE_PIPE:
        return 6;
      case EXTEND_TEMPLATE:
        return 7;
      case MAINTAIN:
        return 8;
      case USE_MODEL:
        return 9;
      default:
        return -1;
    }
  }

  public static int pathPosToPri(int pos) {
    switch (pos) {
      case 0:
        return PrivilegeType.READ_DATA.ordinal();
      case 1:
        return PrivilegeType.WRITE_DATA.ordinal();
      case 2:
        return PrivilegeType.READ_SCHEMA.ordinal();
      case 3:
        return PrivilegeType.WRITE_SCHEMA.ordinal();
      default:
        return -1;
    }
  }

  public static int pathPriToPos(PrivilegeType pri) {
    switch (pri) {
      case READ_DATA:
        return 0;
      case WRITE_DATA:
        return 1;
      case READ_SCHEMA:
        return 2;
      case WRITE_SCHEMA:
        return 3;
      default:
        throw new RuntimeException("Not support PrivilegeType " + pri);
    }
  }

  public static PrivilegeType posToObjPri(int pos) {
    switch (pos) {
      case 0:
        return PrivilegeType.CREATE;
      case 1:
        return PrivilegeType.DROP;
      case 2:
        return PrivilegeType.ALTER;
      case 3:
        return PrivilegeType.SELECT;
      case 4:
        return PrivilegeType.INSERT;
      case 5:
        return PrivilegeType.DELETE;
      default:
        throw new RuntimeException("Not support position");
    }
  }

  public static int objPriToPos(PrivilegeType pri) {
    switch (pri) {
      case CREATE:
        return 0;
      case DROP:
        return 1;
      case ALTER:
        return 2;
      case SELECT:
        return 3;
      case INSERT:
        return 4;
      case DELETE:
        return 5;
      default:
        throw new RuntimeException("Not support position");
    }
  }
}
