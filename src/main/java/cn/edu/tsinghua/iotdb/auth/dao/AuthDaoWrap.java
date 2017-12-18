package cn.edu.tsinghua.iotdb.auth.dao;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.tsinghua.iotdb.auth.AuthException;
import cn.edu.tsinghua.iotdb.auth.model.Role;
import cn.edu.tsinghua.iotdb.auth.model.RolePermission;
import cn.edu.tsinghua.iotdb.auth.model.User;
import cn.edu.tsinghua.iotdb.auth.model.UserPermission;
import cn.edu.tsinghua.iotdb.auth.model.UserRoleRel;

/**
 * @author liukun
 */
public class AuthDaoWrap {

    // Must init the DBdao before use this class
    private Statement statement = DBDao.getStatement();


    public boolean addUser(User user) {
        UserDao dao = new UserDao();
        boolean state = false;
        // Check the user exist or not
        if (dao.getUser(statement, user.getUserName()) == null) {
            dao.createUser(statement, user);
            state = true;
        }
        return state;
    }

    public boolean addRole(Role role) {
        RoleDao dao = new RoleDao();
        boolean state = false;
        // check the user exist or not
        if (dao.getRole(statement, role.getRoleName()) == null) {
            dao.createRole(statement, role);
            state = true;
        }
        return state;
    }

    public boolean addUserRoleRel(String userName, String roleName) throws AuthException {
        UserDao userDao = new UserDao();
        RoleDao roleDao = new RoleDao();
        UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
        int userId;
        int roleId;
        User user = null;
        Role role = null;
        boolean state = false;
        if ((user = userDao.getUser(statement, userName)) != null) {
            if ((role = roleDao.getRole(statement, roleName)) != null) {
                userId = user.getId();
                roleId = role.getId();
                UserRoleRel userRoleRel = new UserRoleRel(userId, roleId);

                if (userRoleRelDao.getUserRoleRel(statement, userRoleRel) == null) {
                    state = true;
                    userRoleRelDao.createUserRoleRel(statement, userRoleRel);
                } else {
                    throw new AuthException(String.format("The user of %s already has the role of %s", userName, role.getRoleName()));
                }
            } else {
                throw new AuthException("The role is not exist");
            }
        } else {
            throw new AuthException("The user is not exist");
        }
        return state;
    }

    public boolean addUserPermission(String userName, String nodeName, int permissionId) throws AuthException {
        UserDao userDao = new UserDao();
        UserPermissionDao userPermissionDao = new UserPermissionDao();
        boolean state = false;
        User user = null;
        if ((user = userDao.getUser(statement, userName)) != null) {
            int userId = user.getId();
            UserPermission userPermission = new UserPermission(userId, nodeName, permissionId);
            if (userPermissionDao.getUserPermission(statement, userPermission) == null) {
                state = true;
                userPermissionDao.createUserPermission(statement, userPermission);
            } else {
                throw new AuthException("The permission is exist");
            }
        } else {
            throw new AuthException("The user is not exist");
        }
        return state;
    }

    public boolean addRolePermission(String roleName, String nodeName, int permissionId) throws AuthException {
        RoleDao roleDao = new RoleDao();
        RolePermissionDao rolePermissionDao = new RolePermissionDao();
        boolean state = false;
        Role role = null;
        if ((role = roleDao.getRole(statement, roleName)) != null) {
            int roleId = role.getId();
            RolePermission rolePermission = new RolePermission(roleId, nodeName, permissionId);
            if (rolePermissionDao.getRolePermission(statement, rolePermission) == null) {
                state = true;
                rolePermissionDao.createRolePermission(statement, rolePermission);
            } else {
                throw new AuthException("The permission is exist");
            }
        } else {
            throw new AuthException("The role is not exist");
        }
        return state;
    }

    public boolean deleteUser(String userName) {
        UserDao userDao = new UserDao();
        boolean state = false;
        if (userDao.deleteUser(statement, userName) > 0) {
            state = true;
        }
        return state;
    }

    public boolean deleteRole(String roleName) {
        RoleDao roleDao = new RoleDao();
        boolean state = false;
        if (roleDao.deleteRole(statement, roleName) > 0) {
            state = true;
        }
        return state;
    }

    public boolean deleteUserRoleRel(String userName, String roleName) throws AuthException {
        UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
        UserDao userDao = new UserDao();
        RoleDao roleDao = new RoleDao();
        int userId;
        int roleId;
        User user = null;
        Role role = null;
        boolean state = false;

        if ((user = userDao.getUser(statement, userName)) != null) {
            if ((role = roleDao.getRole(statement, roleName)) != null) {
                userId = user.getId();
                roleId = role.getId();
                UserRoleRel userRoleRel = new UserRoleRel(userId, roleId);
                if (userRoleRelDao.deleteUserRoleRel(statement, userRoleRel) > 0) {
                    state = true;
                } else {
                    throw new AuthException(String.format("The user of %s does not have the role of %s"), userName, role.getRoleName());
                }
            } else {
                throw new AuthException("The role is not exist");
            }
        } else {
            throw new AuthException("The user is not exist");
        }
        return state;
    }

    public boolean deleteUserPermission(String userName, String nodeName, int permissionId) throws AuthException {
        UserDao userDao = new UserDao();
        UserPermissionDao userPermissionDao = new UserPermissionDao();
        int userId;
        User user = null;
        boolean state = false;
        if ((user = userDao.getUser(statement, userName)) != null) {
            userId = user.getId();
            UserPermission userPermission = new UserPermission(userId, nodeName, permissionId);
            if (userPermissionDao.deleteUserPermission(statement, userPermission) > 0) {
                state = true;
            } else {
                throw new AuthException("The permission is not exist");
            }
        } else {
            throw new AuthException("The user is not exist");
        }
        return state;
    }

    public boolean deleteRolePermission(String roleName, String nodeName, int permissionId) throws AuthException {
        RoleDao roleDao = new RoleDao();
        RolePermissionDao rolePermissionDao = new RolePermissionDao();
        Role role = null;
        int roleId;
        boolean state = false;
        if ((role = roleDao.getRole(statement, roleName)) != null) {
            roleId = role.getId();
            RolePermission rolePermission = new RolePermission(roleId, nodeName, permissionId);
            if (rolePermissionDao.deleteRolePermission(statement, rolePermission) > 0) {
                state = true;
            } else {
                throw new AuthException("The permission is not exist");
            }
        } else {
            throw new AuthException("The role is not exist");
        }
        return state;
    }

    public User getUser(String userName) {
        UserDao userDao = new UserDao();
        User user = null;
        user = userDao.getUser(statement, userName);
        return user;
    }

    public List<User> getUsers() {
        UserDao userDao = new UserDao();
        List<User> users = userDao.getUsers(statement);
        return users;
    }

    public List<Role> getRoles() {
        RoleDao roleDao = new RoleDao();
        List<Role> roles = roleDao.getRoles(statement);
        return roles;
    }

    public List<UserRoleRel> getAllUserRoleRel() {
        UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
        List<UserRoleRel> userRoleRels = userRoleRelDao.getUserRoleRels(statement);
        return userRoleRels;
    }

    private List<Role> getRolesByUser(String userName) {
        UserDao userDao = new UserDao();
        UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
        RoleDao roleDao = new RoleDao();
        ArrayList<Role> roles = new ArrayList<>();
        User user = userDao.getUser(statement, userName);
        if (user != null) {
            int userId = user.getId();
            List<UserRoleRel> userRoleRels = userRoleRelDao.getUserRoleRelByUser(statement, userId);
            for (UserRoleRel userRoleRel : userRoleRels) {
                int roleId = userRoleRel.getRoleId();
                Role role = roleDao.getRole(statement, roleId);
                roles.add(role);
            }
        }
        return roles;
    }

    private UserPermission getUserPermission(String userName, String nodeName, int permissionId) {
        UserPermission userPermission = null;
        UserDao userDao = new UserDao();
        User user = userDao.getUser(statement, userName);
        if (user != null) {
            int userId = user.getId();
            userPermission = new UserPermission(userId, nodeName, permissionId);
            UserPermissionDao userPermissionDao = new UserPermissionDao();
            userPermission = userPermissionDao.getUserPermission(statement, userPermission);
        }
        return userPermission;
    }

    private List<UserPermission> getUserPermissions(String userName, String nodeName) throws AuthException {
        UserDao userDao = new UserDao();
        UserPermissionDao userPermissionDao = new UserPermissionDao();
        List<UserPermission> userPermissions = new ArrayList<>();
        User user = userDao.getUser(statement, userName);
        if (user != null) {
            userPermissions = userPermissionDao.getUserPermissionByUserAndNodeName(statement, user.getId(), nodeName);
        } else {
            throw new AuthException("The user is not exist");
        }
        return userPermissions;
    }

    private List<RolePermission> getRolePermissions(String roleName, String nodeName) {
        RoleDao roleDao = new RoleDao();
        RolePermissionDao rolePermissionDao = new RolePermissionDao();
        List<RolePermission> rolePermissions = new ArrayList<>();
        Role role = roleDao.getRole(statement, roleName);
        if (role != null) {
            rolePermissions = rolePermissionDao.getRolePermissionByRoleAndNodeName(statement, role.getId(), nodeName);
        }
        return rolePermissions;
    }

    public Set<Integer> getAllUserPermissions(String userName, String nodeName) throws AuthException {
        // permission set
        Set<Integer> permissionSet = new HashSet<>();
        // userpermission
        List<UserPermission> userPermissions = getUserPermissions(userName, nodeName);
        for (UserPermission userPermission : userPermissions) {
            permissionSet.add(userPermission.getPermissionId());
        }
        // rolepermission
        List<Role> roles = getRolesByUser(userName);
        for (Role role : roles) {
            List<RolePermission> rolePermissions = getRolePermissions(role.getRoleName(), nodeName);
            // operation add the permission into the set
            for (RolePermission rolePermission : rolePermissions) {
                permissionSet.add(rolePermission.getPermissionId());
            }
        }
        return permissionSet;
    }

    public boolean updateUserPassword(String userName, String newPassword) {
        boolean state = false;
        UserDao userDao = new UserDao();
        int change = userDao.updateUserPassword(statement, userName, newPassword);
        if (change > 0) {
            state = true;
        }
        return state;
    }

    public boolean checkUserPermission(String userName, String nodeName, int permissionId) {
        boolean state = false;
        UserPermission userPermission = getUserPermission(userName, nodeName, permissionId);
        if (userPermission != null) {
            state = true;
        }
        return state;
    }

    public boolean checkUser(String userName, String password) {
        
    	boolean state = false;
        User user = getUser(userName, password);
        if (user != null) {
            state = true;
        }
        return state;
    }
    private User getUser(String userName, String password) {
        
    	UserDao userDao = new UserDao();
        
        return userDao.getUser(statement, userName, password);
    }
    /**
     * just for unit test
     */
    public void reset(){
    	statement = DBDao.getStatement();
    }
}
