package cn.edu.tsinghua.iotdb.auth.authorizer;
import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.entity.Role;
import cn.edu.tsinghua.iotdb.auth.entity.User;

import java.util.List;
import java.util.Set;

/**
 * This interface provides all authorization-relative operations.
 */
public interface IAuthorizer {
    /**
     * Login for a user.
     *
     * @param username The username of the user.
     * @param password The password of the user.
     * @return True if such user exists and the given password is correct, else return false.
     * @throws AuthException
     */
     boolean login(String username, String password) throws AuthException;

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
     * @throws AuthException When attempting to delete the default administrator
     */
    boolean deleteUser(String username) throws AuthException;

    /**
     * Grant a privilege on a path to a user.
     *
     * @param username The username of the user to which the privilege should be added.
     * @param path  The path on which the privilege takes effect. If the privilege is a path-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the permission is successfully added, false if the permission already exists.
     * @throws AuthException If the user does not exist or the privilege or the path is illegal.
     */
    boolean grantPrivilegeToUser(String username, String path, int privilegeId) throws AuthException;

    /**
     * Revoke a privilege on path from a user.
     *
     * @param username The username of the user from which the privilege should be removed.
     * @param path The path on which the privilege takes effect. If the privilege is a path-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the permission is successfully revoked, false if the permission does not exists.
     * @throws AuthException If the user does not exist or the privilege or the path is illegal.
     */
    boolean revokePrivilegeFromUser(String username, String path, int privilegeId) throws AuthException;

    /**
     * Add a role.
     *
     * @param roleName the name of the role to be added.
     * @return True if the role is successfully added, false if the role already exists
     * @throws AuthException
     */
     boolean createRole(String roleName) throws AuthException;

    /**
     * Delete a role.
     *
     * @param roleName the name of the role tobe deleted.
     * @return True if the role is successfully deleted, false if the role does not exists.
     * @throws AuthException
     */
     boolean deleteRole(String roleName) throws AuthException;

    /**
     * Add a privilege on a path to a role.
     *
     * @param roleName The name of the role to which the privilege is added.
     * @param path The path on which the privilege takes effect. If the privilege is a path-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the privilege is successfully granted, false if the privilege already exists.
     * @throws AuthException If the role does not exist or the privilege or the path is illegal.
     */
     boolean grantPrivilegeToRole(String roleName, String path, int privilegeId) throws AuthException;

    /**
     * Remove a privilege on a path from a role.
     *
     * @param roleName The name of the role from which the privilege is removed.
     * @param path The path on which the privilege takes effect. If the privilege is a path-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the privilege is successfully revoked, false if the privilege does not exists.
     * @throws AuthException If the role does not exist or the privilege or the path is illegal.
     */
    boolean revokePrivilegeFromRole(String roleName, String path, int privilegeId) throws AuthException;

    /**
     * Add a role to a user.
     *
     * @param roleName The name of the role to be added.
     * @param username The name of the user to which the role is added.
     * @return True if the role is successfully added, false if the role already exists.
     * @throws AuthException If either the role or the user does not exist.
     */
    boolean grantRoleToUser(String roleName, String username) throws AuthException;

    /**
     * Revoke a role from a user.
     *
     * @param roleName The name of the role to be removed.
     * @param username The name of the user from which the role is removed.
     * @return True if the role is successfully removed, false if the role already exists.
     * @throws AuthException If either the role or the user does not exist.
     */
    boolean revokeRoleFromUser(String roleName, String username) throws AuthException;

    /**
     * Get the all the privileges of a user on a path.
     *
     * @param username The user whose privileges are to be queried.
     * @param path The path on which the privileges take effect. If the privilege is a path-free privilege, this should be "root".
     * @return A set of integers each present a privilege.
     * @throws AuthException
     */
     Set<Integer> getPrivileges(String username, String path) throws AuthException;

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
     * Check if the user have the privilege on the path.
     *
     * @param username The name of the user whose privileges are checked.
     * @param path The path on which the privilege takes effect. If the privilege is a path-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return  True if the user has such privilege, false if the user does not have such privilege.
     * @throws AuthException If the path or the privilege is illegal.
     */
    boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException;

    /**
     * Reset the Authorizer to initiative status.
     */
    void reset() throws AuthException;

    /**
     *
     * @return A list contains all usernames.
     */
    List<String> listAllUsers();

    /**
     *
     * @return A list contains all roleNames.
     */
    List<String> listAllRoles();

    /**
     *
     * @param roleName
     * @return A role whose name is roleName or null if such role does not exist.
     */
    Role getRole(String roleName) throws AuthException;

    /**
     *
     * @param username
     * @return A user whose name is username or null if such user does not exist.
     */
    User getUser(String username) throws AuthException;
}
