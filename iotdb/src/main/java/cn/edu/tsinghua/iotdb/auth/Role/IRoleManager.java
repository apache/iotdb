package cn.edu.tsinghua.iotdb.auth.Role;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.entity.Role;

import java.util.List;

/**
 * This interface maintains roles in memory and is responsible for their modifications.
 */
public interface IRoleManager {
    /**
     * Get a role object.
     * @param rolename The name of the role.
     * @return A role object whose name is rolename or null if such role does not exist.
     * @throws AuthException
     */
    Role getRole(String rolename) throws AuthException;

    /**
     * Create a role with given rolename. New roles will only be granted no privileges.
     *
     * @param rolename is not null or empty
     * @return True if the role is successfully created, false when the role already exists.
     * @throws AuthException f the given rolename is iIllegal.
     */
    boolean createRole(String rolename) throws AuthException;

    /**
     * Delete a role.
     *
     * @param rolename the rolename of the role.
     * @return True if the role is successfully deleted, false if the role does not exists.
     * @throws AuthException
     */
    boolean deleteRole(String rolename) throws AuthException;

    /**
     * Grant a privilege on a seriesPath to a role.
     *
     * @param rolename The rolename of the role to which the privilege should be added.
     * @param path  The seriesPath on which the privilege takes effect. If the privilege is a seriesPath-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the permission is successfully added, false if the permission already exists.
     * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
     */
    boolean grantPrivilegeToRole(String rolename, String path, int privilegeId) throws AuthException;

    /**
     * Revoke a privilege on seriesPath from a role.
     *
     * @param rolename The rolename of the role from which the privilege should be removed.
     * @param path The seriesPath on which the privilege takes effect. If the privilege is a seriesPath-free privilege, this should be "root".
     * @param privilegeId An integer that represents a privilege.
     * @return True if the permission is successfully revoked, false if the permission does not exists.
     * @throws AuthException If the role does not exist or the privilege or the seriesPath is illegal.
     */
    boolean revokePrivilegeFromRole(String rolename, String path, int privilegeId) throws AuthException;

    /**
     * Re-initialize this object.
     */
    void reset();

    /**
     *
     * @return A list that contains names of all roles.
     */
    List<String> listAllRoles();
}
