package cn.edu.thu.tsfiledb.auth.dao;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfiledb.auth.model.Role;
import cn.edu.thu.tsfiledb.auth.model.RolePermission;
import cn.edu.thu.tsfiledb.auth.model.User;
import cn.edu.thu.tsfiledb.auth.model.UserPermission;
import cn.edu.thu.tsfiledb.auth.model.UserRoleRel;

/**
 * @author liukun
 *
 */
public class AuthDao {

	/**
	 * if the user don't exist in the db, return true else return false
	 * 
	 * @param statement
	 * @param user
	 * @return
	 */
	public boolean addUser(Statement statement, User user) {
		UserDao dao = new UserDao();
		boolean state = false;
		// Check the user exist or not
		if (dao.getUser(statement, user.getUserName()) == null) {
			dao.createUser(statement, user);
			state = true;
		}
		return state;
	}

	/**
	 * if the role isn't exist in the db, will return true else return false
	 * 
	 * @param statement
	 * @param role
	 * @return
	 */
	public boolean addRole(Statement statement, Role role) {
		RoleDao dao = new RoleDao();
		boolean state = false;
		// check the user exist or not
		if (dao.getRole(statement, role.getRoleName()) == null) {
			dao.createRole(statement, role);
			state = true;
		}
		return state;
	}

	/**
	 * the user and role is exist, the relation is not exist, will return true
	 * else return false
	 * 
	 * @param statement
	 * @param userName
	 * @param roleName
	 * @return
	 */
	public boolean addUserRoleRel(Statement statement, String userName, String roleName) {
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
				}
			}
		}
		return state;
	}

	public boolean addUserPermission(Statement statement, String userName, String nodeName, int permissionId) {
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
			}
		}
		return state;
	}

	public boolean addRolePermission(Statement statement, String roleName, String nodeName, int permissionId) {
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
			}
		}
		return state;
	}

	public boolean deleteUser(Statement statement, String userName) {
		UserDao userDao = new UserDao();
		boolean state = false;
		if (userDao.deleteUser(statement, userName) > 0) {
			state = true;
		}
		return state;
	}

	public boolean deleteRole(Statement statement, String roleName) {
		RoleDao roleDao = new RoleDao();
		boolean state = false;
		if (roleDao.deleteRole(statement, roleName) > 0) {
			state = true;
		}
		return state;
	}

	public boolean deleteUserRoleRel(Statement statement, String userName, String roleName) {
		UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
		UserDao userDao = new UserDao();
		RoleDao roleDao = new RoleDao();
		int userId;
		int roleId;
		User user = null;
		Role role = null;
		boolean state = false;

		if ((user = userDao.getUser(statement, userName)) != null
				&& (role = roleDao.getRole(statement, roleName)) != null) {
			userId = user.getId();
			roleId = role.getId();
			UserRoleRel userRoleRel = new UserRoleRel(userId, roleId);
			if (userRoleRelDao.deleteUserRoleRel(statement, userRoleRel) > 0) {
				state = true;
			}
		}
		return state;
	}

	public boolean deleteUserPermission(Statement statement, String userName, String nodeName, int permissionId) {
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
			}
		}
		return state;
	}

	public boolean deleteRolePermission(Statement statement, String roleName, String nodeName, int permissionId) {
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
			}
		}
		return state;
	}

	// 如果username或者nodename不存在怎么办？
	public List<User> getUsers(Statement statement) {
		UserDao userDao = new UserDao();
		List<User> users = userDao.getUsers(statement);
		return users;
	}

	public List<Role> getRoles(Statement statement) {
		RoleDao roleDao = new RoleDao();
		List<Role> roles = roleDao.getRoles(statement);
		return roles;
	}

	public List<UserRoleRel> getAllUserRoleRel(Statement statement) {
		UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
		List<UserRoleRel> userRoleRels = userRoleRelDao.getUserRoleRels(statement);
		return userRoleRels;
	}

	/*
	 * 返回值的问题
	 */
	public List<Role> getRolesByUser(Statement statement, String userName) {
		UserDao userDao = new UserDao();
		UserRoleRelDao userRoleRelDao = new UserRoleRelDao();
		RoleDao roleDao = new RoleDao();
		// 当 user不存在的情况下是返回 size = 0，还是 null
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

	/*
	 * 返回值的问题
	 */
	public List<UserPermission> getUserPermission(Statement statement, String userName, String nodeName) {
		UserDao userDao = new UserDao();
		UserPermissionDao userPermissionDao = new UserPermissionDao();
		List<UserPermission> userPermissions = new ArrayList<>();
		// 当user 不存在的时候 是返回size = 0，还是null
		User user = userDao.getUser(statement, userName);
		if (user != null) {
			userPermissions = userPermissionDao.getUserPermissionByUserAndNodeName(statement, user.getId(), nodeName);
		}
		// 返回值可能是null还是 没有结果 size = 0；
		return userPermissions;
	}

	public List<RolePermission> getRolePermission(Statement statement, String roleName, String nodeName) {
		RoleDao roleDao = new RoleDao();
		RolePermissionDao rolePermissionDao = new RolePermissionDao();
		List<RolePermission> rolePermissions = new ArrayList<>();
		Role role = roleDao.getRole(statement, roleName);
		if (role != null) {
			rolePermissions = rolePermissionDao.getRolePermissionByRoleAndNodeName(statement, role.getId(), nodeName);
		}
		return rolePermissions;
	}

	/*
	 * All user's permission: userPermission and rolePermission
	 */
	public Set<Integer> getAllUserPermission(Statement statement, String userName, String nodeName) {
		// permission set
		Set<Integer> permissionSet = new HashSet<>();
		// userpermission
		List<UserPermission> userPermissions = getUserPermission(statement, userName, nodeName);
		for (UserPermission userPermission : userPermissions) {
			permissionSet.add(userPermission.getPermissionId());
		}
		// rolepermission
		List<Role> roles = getRolesByUser(statement, userName);
		for (Role role : roles) {
			List<RolePermission> rolePermissions = getRolePermission(statement, role.getRoleName(), nodeName);
			// operation add the permission into the set
			for (RolePermission rolePermission : rolePermissions) {
				permissionSet.add(rolePermission.getPermissionId());
			}
		}
		return permissionSet;
	}
}
