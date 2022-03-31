package org.apache.iotdb.db.auth;

import org.apache.iotdb.db.auth.entity.Role;
import org.apache.iotdb.db.auth.entity.User;
import org.apache.iotdb.db.metadata.LocalConfigManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalAuthorizer {

  private static final Logger logger = LoggerFactory.getLogger(LocalAuthorizer.class);

  private LocalConfigManager configManager = LocalConfigManager.getInstance();

  // Singleton
  private static class LocalAuthorizerHolder {

    private LocalAuthorizerHolder() {
      // allowed to do nothing
    }

    private static final LocalAuthorizer INSTANCE = new LocalAuthorizer();
  }

  /** we should not use this function in other place, but only in IoTDB class */
  public static LocalAuthorizer getInstance() {
    return LocalAuthorizerHolder.INSTANCE;
  }

  protected LocalAuthorizer() {}

  /**
   * Login for a user.
   *
   * @param username The username of the user.
   * @param password The password of the user.
   * @return True if such user exists and the given password is correct, else return false.
   * @throws AuthException if exception raised when searching for the user.
   */
  public boolean login(String username, String password) throws AuthException {
    return configManager.login(username, password);
  }

  /**
   * Create a user with given username and password. New users will only be granted no privileges.
   *
   * @param username is not null or empty
   * @param password is not null or empty
   * @throws AuthException if the given username or password is illegal or the user already exists.
   */
  public void createUser(String username, String password) throws AuthException {
    configManager.createUser(username, password);
  }

  /**
   * Delete a user.
   *
   * @param username the username of the user.
   * @throws AuthException When attempting to delete the default administrator or the user does not
   *     exists.
   */
  public void deleteUser(String username) throws AuthException {
    configManager.deleteUser(username);
  }

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
  public void grantPrivilegeToUser(String username, String path, int privilegeId)
      throws AuthException {
    configManager.grantPrivilegeToUser(username, path, privilegeId);
  }

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
  public void revokePrivilegeFromUser(String username, String path, int privilegeId)
      throws AuthException {
    configManager.revokePrivilegeFromUser(username, path, privilegeId);
  }

  /**
   * Add a role.
   *
   * @param roleName the name of the role to be added.
   * @throws AuthException if exception raised when adding the role or the role already exists.
   */
  public void createRole(String roleName) throws AuthException {
    configManager.createRole(roleName);
  }

  /**
   * Delete a role.
   *
   * @param roleName the name of the role tobe deleted.
   * @throws AuthException if exception raised when deleting the role or the role does not exists.
   */
  public void deleteRole(String roleName) throws AuthException {
    configManager.deleteRole(roleName);
  }

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
  public void grantPrivilegeToRole(String roleName, String path, int privilegeId)
      throws AuthException {
    configManager.grantPrivilegeToRole(roleName, path, privilegeId);
  }

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
  public void revokePrivilegeFromRole(String roleName, String path, int privilegeId)
      throws AuthException {
    configManager.revokePrivilegeFromRole(roleName, path, privilegeId);
  }

  /**
   * Add a role to a user.
   *
   * @param roleName The name of the role to be added.
   * @param username The name of the user to which the role is added.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  public void grantRoleToUser(String roleName, String username) throws AuthException {
    configManager.grantRoleToUser(roleName, username);
  }

  /**
   * Revoke a role from a user.
   *
   * @param roleName The name of the role to be removed.
   * @param username The name of the user from which the role is removed.
   * @throws AuthException If either the role or the user does not exist or the role already exists.
   */
  public void revokeRoleFromUser(String roleName, String username) throws AuthException {
    configManager.revokeRoleFromUser(roleName, username);
  }

  /**
   * Get the all the privileges of a user on a seriesPath.
   *
   * @param username The user whose privileges are to be queried.
   * @param path The seriesPath on which the privileges take effect. If the privilege is a
   *     seriesPath-free privilege, this should be "root".
   * @return A set of integers each present a privilege.
   * @throws AuthException if exception raised when finding the privileges.
   */
  public Set<Integer> getPrivileges(String username, String path) throws AuthException {
    return configManager.getPrivileges(username, path);
  }

  /**
   * Modify the password of a user.
   *
   * @param username The user whose password is to be modified.
   * @param newPassword The new password.
   * @throws AuthException If the user does not exists or the new password is illegal.
   */
  public void updateUserPassword(String username, String newPassword) throws AuthException {
    configManager.updateUserPassword(username, newPassword);
  }

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
  public boolean checkUserPrivileges(String username, String path, int privilegeId)
      throws AuthException {
    return configManager.checkUserPrivileges(username, path, privilegeId);
  }

  /** Reset the Authorizer to initiative status. */
  public void reset() throws AuthException {
    configManager.reset();
  }

  /**
   * List existing users in the database.
   *
   * @return A list contains all usernames.
   */
  public List<String> listAllUsers() {
    return configManager.listAllUsers();
  }

  /**
   * List existing roles in the database.
   *
   * @return A list contains all roleNames.
   */
  public List<String> listAllRoles() {
    return configManager.listAllRoles();
  }

  /**
   * Find a role by its name.
   *
   * @param roleName the name of the role.
   * @return A role whose name is roleName or null if such role does not exist.
   */
  public Role getRole(String roleName) throws AuthException {
    return configManager.getRole(roleName);
  }

  /**
   * Find a user by its name.
   *
   * @param username the name of the user.
   * @return A user whose name is username or null if such user does not exist.
   */
  public User getUser(String username) throws AuthException {
    return configManager.getUser(username);
  }

  /**
   * Whether data water-mark is enabled for user 'userName'.
   *
   * @param userName
   * @return
   * @throws AuthException if the user does not exist
   */
  public boolean isUserUseWaterMark(String userName) throws AuthException {
    return configManager.isUserUseWaterMark(userName);
  }

  /**
   * Enable or disable data water-mark for user 'userName'.
   *
   * @param userName
   * @param useWaterMark
   * @throws AuthException if the user does not exist.
   */
  public void setUserUseWaterMark(String userName, boolean useWaterMark) throws AuthException {
    configManager.setUserUseWaterMark(userName, useWaterMark);
  }

  /**
   * get all user water mark status
   *
   * @return key->userName, value->useWaterMark or not
   */
  public Map<String, Boolean> getAllUserWaterMarkStatus() {
    return configManager.getAllUserWaterMarkStatus();
  }

  /**
   * get all user
   *
   * @return key-> userName, value->user
   */
  public Map<String, User> getAllUsers() {
    return configManager.getAllUsers();
  }

  /**
   * get all role
   *
   * @return key->userName, value->role
   */
  public Map<String, Role> getAllRoles() {
    return configManager.getAllRoles();
  }

  /**
   * clear all old users info, replace the old users with the new one
   *
   * @param users new users info
   * @throws AuthException
   */
  public void replaceAllUsers(Map<String, User> users) throws AuthException {
    configManager.replaceAllUsers(users);
  }

  /**
   * clear all old roles info, replace the old roles with the new one
   *
   * @param roles new roles info
   * @throws AuthException
   */
  public void replaceAllRoles(Map<String, Role> roles) throws AuthException {
    configManager.replaceAllRoles(roles);
  }
}
