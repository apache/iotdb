package cn.edu.tsinghua.iotdb.auth.dao;

import java.util.Set;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.model.Role;
import cn.edu.tsinghua.iotdb.auth.model.User;

/**
 * @author liukun
 */
public class Authorizer {

    private static AuthDaoWrap authDaoWrap = new AuthDaoWrap();

    /**
     * Check the information for login
     *
     * @param username
     * @param password
     * @return
     * @throws AuthException
     */
    public static synchronized boolean login(String username, String password) throws AuthException {

        boolean status = false;
        status = authDaoWrap.checkUser(username, password);
        if (status == false) {
            throw new AuthException("The username or the password is not correct");
        }
        return status;
    }

    /**
     * Add user
     *
     * @param username is not null or empty
     * @param password is not null or empty
     * @return true: add user successfully, false: add user unsuccessfully
     * @throws AuthException
     */
    public static boolean createUser(String username, String password) throws AuthException {
        boolean status = false;
        
        if (username == null || password == null || "".equals(username) || "".equals(password)) {
            throw new AuthException("Username or password can't be empty");
        }
        
        User user = new User(username, password);

        status = authDaoWrap.addUser(user);
        if (status == false) {
            throw new AuthException("The user is exist");
        }
        return status;
    }

    /**
     * Delete user
     *
     * @param username
     * @return true: delete user successfully, false: delete user unsuccessfully
     * @throws AuthException
     */
    public static boolean deleteUser(String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUser(username);
        if (status == false) {
            throw new AuthException("The user is not exist");
        }
        return status;
    }

    /**
     * Add permission to user
     *
     * @param username
     * @param nodeName
     * @param permissionId
     * @return true: add permission successfully, false: add permission unsuccessfully
     * @throws AuthException
     */
    public static boolean addPmsToUser(String username, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addUserPermission(username, nodeName, permissionId);
        return status;
    }

    /**
     * Delete permission from user
     *
     * @param userName
     * @param nodeName
     * @param permissionId
     * @return true: delete permission from user successfully, false: delete permission from user unsuccessfully
     * @throws AuthException
     */
    public static boolean removePmsFromUser(String userName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUserPermission(userName, nodeName, permissionId);
        return status;
    }

    /**
     * Add role
     *
     * @param roleName
     * @return true: add role successfully, false: add role unsuccessfully
     * @throws Exception
     */
    public static boolean createRole(String roleName) throws AuthException {
        boolean status = false;
        Role role = new Role(roleName);
        status = authDaoWrap.addRole(role);
        if (status == false) {
            throw new AuthException("The role is exist");
        }
        return status;
    }

    /**
     * Delete role
     *
     * @param roleName
     * @return true: delete role successfully, false: delete role unsuccessfully
     * @throws Exception
     */
    public static boolean deleteRole(String roleName) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteRole(roleName);
        if (status == false) {
            throw new AuthException("The role is not exist");
        }
        return status;
    }

    /**
     * Add permission to role
     *
     * @param roleName
     * @param nodeName
     * @param permissionId
     * @return true: add permission to role successfully, false: add permission to role unsuccessfully
     * @throws AuthException
     */
    public static boolean addPmsToRole(String roleName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addRolePermission(roleName, nodeName, permissionId);
        return status;
    }

    /**
     * Delete permission from role
     *
     * @param roleName
     * @param nodeName
     * @param permissionId
     * @return true: delete permission from role successfully, false: delete permission from role unsuccessfully
     * @throws AuthException
     */
    public static boolean removePmsFromRole(String roleName, String nodeName, int permissionId) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteRolePermission(roleName, nodeName, permissionId);
        return status;
    }

    /**
     * Add role to user
     *
     * @param roleName
     * @param username
     * @return true: add role to user successfully, false: add role to user unsuccessfully
     * @throws AuthException
     */
    public static boolean grantRoleToUser(String roleName, String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.addUserRoleRel(username, roleName);
        return status;
    }

    /**
     * Delete role from user
     *
     * @param roleName
     * @param username
     * @return true: delete role from user successfully, false: delete role from user unsuccessfully
     * @throws AuthException
     */
    public static boolean revokeRoleFromUser(String roleName, String username) throws AuthException {
        boolean status = false;
        status = authDaoWrap.deleteUserRoleRel(username, roleName);
        return status;
    }

    /**
     * Get the all permission of the user
     *
     * @param username
     * @param nodeName
     * @return
     * @throws AuthException
     */
    public static Set<Integer> getPermission(String username, String nodeName) throws AuthException {
        Set<Integer> permissionSets = null;
        permissionSets = authDaoWrap.getAllUserPermissions(username, nodeName);
        return permissionSets;
    }

    /**
     * Modify the password
     *
     * @param username
     * @param newPassword
     * @return true: update the password successfully, false: update the password unsuccessfully
     * @throws AuthException
     */
    public static boolean updateUserPassword(String username, String newPassword) throws AuthException {
        boolean status = false;
        status = authDaoWrap.updateUserPassword(username, newPassword);
        if (status == false) {
            throw new AuthException("The username or the password is not correct");
        }
        return status;
    }

    /**
     * Check the permission belong to the user
     *
     * @param username
     * @param nodeName
     * @param permissionId
     * @return true: the user has this permission, false: the user does not have the permission
     */
    public static boolean checkUserPermission(String username, String nodeName, int permissionId) {
        boolean status = false;
        status = authDaoWrap.checkUserPermission(username, nodeName, permissionId);
        return status;
    }
    /**
     * just for unit test
     */
    public static void reset(){
    	authDaoWrap.reset();
    }

}
