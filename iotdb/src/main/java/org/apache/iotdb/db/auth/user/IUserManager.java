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
package org.apache.iotdb.db.auth.user;

import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.auth.entity.User;

import java.util.List;

/**
 * This interface provides accesses to users.
 */
public interface IUserManager {

    /**
     * Get a user object.
     * 
     * @param username
     *            The name of the user.
     * @return A user object whose name is username or null if such user does not exist.
     * @throws AuthException
     */
    User getUser(String username) throws AuthException;

    /**
     * Create a user with given username and password. New users will only be granted no privileges.
     *
     * @param username
     *            is not null or empty
     * @param password
     *            is not null or empty
     * @return True if the user is successfully created, false when the user already exists.
     * @throws AuthException
     *             if the given username or password is illegal.
     */
    boolean createUser(String username, String password) throws AuthException;

    /**
     * Delete a user.
     *
     * @param username
     *            the username of the user.
     * @return True if the user is successfully deleted, false if the user does not exists.
     * @throws AuthException
     *             .
     */
    boolean deleteUser(String username) throws AuthException;

    /**
     * Grant a privilege on a seriesPath to a user.
     *
     * @param username
     *            The username of the user to which the privilege should be added.
     * @param path
     *            The seriesPath on which the privilege takes effect. If the privilege is a seriesPath-free privilege,
     *            this should be "root".
     * @param privilegeId
     *            An integer that represents a privilege.
     * @return True if the permission is successfully added, false if the permission already exists.
     * @throws AuthException
     *             If the user does not exist or the privilege or the seriesPath is illegal.
     */
    boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException;

    /**
     * Revoke a privilege on seriesPath from a user.
     *
     * @param username
     *            The username of the user from which the privilege should be removed.
     * @param path
     *            The seriesPath on which the privilege takes effect. If the privilege is a seriesPath-free privilege,
     *            this should be "root".
     * @param privilegeId
     *            An integer that represents a privilege.
     * @return True if the permission is successfully revoked, false if the permission does not exists.
     * @throws AuthException
     *             If the user does not exist or the privilege or the seriesPath is illegal.
     */
    boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException;

    /**
     * Modify the password of a user.
     *
     * @param username
     *            The user whose password is to be modified.
     * @param newPassword
     *            The new password.
     * @return True if the password is successfully modified, false if the new password is illegal.
     * @throws AuthException
     *             If the user does not exists.
     */
    boolean updateUserPassword(String username, String newPassword) throws AuthException;

    /**
     * Add a role to a user.
     *
     * @param roleName
     *            The name of the role to be added.
     * @param username
     *            The name of the user to which the role is added.
     * @return True if the role is successfully added, false if the role already exists.
     * @throws AuthException
     *             If the user does not exist.
     */
    boolean grantRoleToUser(String roleName, String username) throws AuthException;

    /**
     * Revoke a role from a user.
     *
     * @param roleName
     *            The name of the role to be removed.
     * @param username
     *            The name of the user from which the role is removed.
     * @return True if the role is successfully removed, false if the role does not exist.
     * @throws AuthException
     *             If the user does not exist.
     */
    boolean revokeRoleFromUser(String roleName, String username) throws AuthException;

    /**
     * Re-initialize this object.
     */
    void reset() throws AuthException;

    /**
     *
     * @return A list that contains all users'name.
     */
    List<String> listAllUsers();
}
