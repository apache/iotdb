/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.utils;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PathPrivilege;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.apache.iotdb.db.conf.IoTDBConstant;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AuthUtils {
    private static final int MIN_PASSWORD_LENGTH = 4;
    private static final int MIN_USERNAME_LENGTH = 4;
    private static final int MIN_ROLENAME_LENGTH = 4;
    private static final String ROOT_PREFIX = IoTDBConstant.PATH_ROOT;
    private static final String ENCRYPT_ALGORITHM = "MD5";
    private static final String STRING_ENCODING = "utf-8";

    public static void validatePassword(String password) throws AuthException {
        if (password.length() < MIN_PASSWORD_LENGTH)
            throw new AuthException("Password's length must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }

    public static void validateUsername(String username) throws AuthException {
        if (username.length() < MIN_USERNAME_LENGTH)
            throw new AuthException("Username's length must be greater than or equal to " + MIN_USERNAME_LENGTH);
    }

    public static void validateRolename(String rolename) throws AuthException {
        if (rolename.length() < MIN_ROLENAME_LENGTH)
            throw new AuthException("Role name's length must be greater than or equal to " + MIN_ROLENAME_LENGTH);
    }

    public static void validatePrivilege(int privilegeId) throws AuthException {
        if (privilegeId < 0 || privilegeId >= PrivilegeType.values().length) {
            throw new AuthException(String.format("Invalid privilegeId %d", privilegeId));
        }
    }

    public static void validatePath(String path) throws AuthException {
        if (!path.startsWith(ROOT_PREFIX))
            throw new AuthException(
                    String.format("Illegal seriesPath %s, seriesPath should start with \"%s\"", path, ROOT_PREFIX));
    }

    public static void validatePrivilegeOnPath(String path, int privilegeId) throws AuthException {
        validatePrivilege(privilegeId);
        PrivilegeType type = PrivilegeType.values()[privilegeId];
        if (!path.equals(IoTDBConstant.PATH_ROOT)) {
            validatePath(path);
            switch (type) {
            case READ_TIMESERIES:
            case SET_STORAGE_GROUP:
            case DELETE_TIMESERIES:
            case INSERT_TIMESERIES:
            case UPDATE_TIMESERIES:
                return;
            default:
                throw new AuthException(String.format("Illegal privilege %s on seriesPath %s", type.toString(), path));
            }
        } else {
            switch (type) {
            case READ_TIMESERIES:
            case SET_STORAGE_GROUP:
            case DELETE_TIMESERIES:
            case INSERT_TIMESERIES:
            case UPDATE_TIMESERIES:
                validatePath(path);
            default:
                return;
            }
        }
    }

    public static String encryptPassword(String password) {
        try {
            MessageDigest messageDigest = MessageDigest.getInstance(ENCRYPT_ALGORITHM);
            messageDigest.update(password.getBytes(STRING_ENCODING));
            return new String(messageDigest.digest(), STRING_ENCODING);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            return password;
        }
    }

    /**
     *
     * @param pathA
     * @param pathB
     * @return True if pathA == pathB, or pathA is an extension of pathB, e.g. pathA = "root.a.b.c" and pathB = "root.a"
     */
    public static boolean pathBelongsTo(String pathA, String pathB) {
        return pathA.equals(pathB)
                || (pathA.startsWith(pathB) && pathA.charAt(pathB.length()) == IoTDBConstant.PATH_SEPARATER);
    }

    public static boolean checkPrivilege(String path, int privilegeId, List<PathPrivilege> privilegeList) {
        if (privilegeList == null)
            return false;
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (path != null) {
                if (pathPrivilege.path != null && AuthUtils.pathBelongsTo(path, pathPrivilege.path)) {
                    if (pathPrivilege.privileges.contains(privilegeId))
                        return true;
                }
            } else {
                if (pathPrivilege.path == null) {
                    if (pathPrivilege.privileges.contains(privilegeId))
                        return true;
                }
            }
        }
        return false;
    }

    /**
     *
     * @param path
     *            The seriesPath on which the privileges take effect. If seriesPath-free privileges are desired, this
     *            should be null.
     * @return The privileges granted to the role.
     */
    public static Set<Integer> getPrivileges(String path, List<PathPrivilege> privilegeList) {
        if (privilegeList == null)
            return null;
        Set<Integer> privileges = new HashSet<>();
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (path != null) {
                if (pathPrivilege.path != null && AuthUtils.pathBelongsTo(path, pathPrivilege.path)) {
                    privileges.addAll(pathPrivilege.privileges);
                }
            } else {
                if (pathPrivilege.path == null) {
                    privileges.addAll(pathPrivilege.privileges);
                }
            }
        }
        return privileges;
    }

    public static boolean hasPrivilege(String path, int privilegeId, List<PathPrivilege> privilegeList) {
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (pathPrivilege.path.equals(path) && pathPrivilege.privileges.contains(privilegeId)) {
                pathPrivilege.referenceCnt.incrementAndGet();
                return true;
            }
        }
        return false;
    }

    public static void addPrivilege(String path, int privilgeId, List<PathPrivilege> privilegeList) {
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (pathPrivilege.path.equals(path)) {
                if (privilgeId != PrivilegeType.ALL.ordinal())
                    pathPrivilege.privileges.add(privilgeId);
                else
                    for (PrivilegeType privilegeType : PrivilegeType.values())
                        pathPrivilege.privileges.add(privilegeType.ordinal());
                return;
            }
        }
        PathPrivilege pathPrivilege = new PathPrivilege(path);
        if (privilgeId != PrivilegeType.ALL.ordinal())
            pathPrivilege.privileges.add(privilgeId);
        else
            for (PrivilegeType privilegeType : PrivilegeType.values())
                pathPrivilege.privileges.add(privilegeType.ordinal());
        privilegeList.add(pathPrivilege);
    }

    public static void removePrivilege(String path, int privilgeId, List<PathPrivilege> privilegeList) {
        PathPrivilege emptyPrivilege = null;
        for (PathPrivilege pathPrivilege : privilegeList) {
            if (pathPrivilege.path.equals(path)) {
                if (privilgeId != PrivilegeType.ALL.ordinal())
                    pathPrivilege.privileges.remove(privilgeId);
                else {
                    privilegeList.remove(pathPrivilege);
                    return;
                }
                if (pathPrivilege.privileges.size() == 0)
                    emptyPrivilege = pathPrivilege;
                break;
            }
        }
        if (emptyPrivilege != null)
            privilegeList.remove(emptyPrivilege);
    }

}
