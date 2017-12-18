package cn.edu.tsinghua.iotdb.auth;

import static org.junit.Assert.assertEquals;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.auth.dao.AuthDao;
import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.dao.RoleDao;
import cn.edu.tsinghua.iotdb.auth.dao.UserDao;
import cn.edu.tsinghua.iotdb.auth.model.Permission;
import cn.edu.tsinghua.iotdb.auth.model.Role;
import cn.edu.tsinghua.iotdb.auth.model.RolePermission;
import cn.edu.tsinghua.iotdb.auth.model.User;
import cn.edu.tsinghua.iotdb.auth.model.UserPermission;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class AuthTest {

	private DBDao dbDao = null;
	private Statement statement = null;
	private AuthDao authDao = null;
	// prepare the data
	private User user1;
	private User user2;
	private Role role1;
	private Role role2;

	@Before
	public void setUp() throws Exception {
		dbDao = new DBDao();
		authDao = new AuthDao();
		dbDao.open();
		EnvironmentUtils.envSetUp();
		statement = DBDao.getStatement();

		// init data
		user1 = new User("user1", "user1");
		user2 = new User("user2", "user2");

		role1 = new Role("role1");
		role2 = new Role("role2");
	}

	@After
	public void tearDown() throws Exception {
		dbDao.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testUser() {
		authDao.deleteUser(statement, user1.getUserName());
		// add user
		assertEquals(true, authDao.addUser(statement, user1));
		// check the user inserted
		UserDao userDao = new UserDao();
		User user = userDao.getUser(statement, user1.getUserName());
		assertEquals(user1.getUserName(), user.getUserName());
		assertEquals(user1.getPassWord(), user.getPassWord());
		// delete the user
		assertEquals(true, authDao.deleteUser(statement, user1.getUserName()));
		assertEquals(null, userDao.getUser(statement, user1.getUserName()));

		authDao.addUser(statement, user1);
		authDao.addUser(statement, user2);
		// add the root user
		assertEquals(3, authDao.getUsers(statement).size());
		authDao.deleteUser(statement, user1.getUserName());
		authDao.deleteUser(statement, user2.getUserName());
	}

	@Test
	public void testRole() {
		authDao.deleteRole(statement, role1.getRoleName());
		// add role
		assertEquals(true, authDao.addRole(statement, role1));
		// check the role inserted
		RoleDao roleDao = new RoleDao();
		Role role = roleDao.getRole(statement, role1.getRoleName());
		assertEquals(role1.getRoleName(), role.getRoleName());
		// delete the role
		assertEquals(true, authDao.deleteRole(statement, role1.getRoleName()));
		assertEquals(null, roleDao.getRole(statement, role1.getRoleName()));

		authDao.addRole(statement, role1);
		authDao.addRole(statement, role2);
		assertEquals(2, authDao.getRoles(statement).size());
		authDao.deleteRole(statement, role1.getRoleName());
		authDao.deleteRole(statement, role2.getRoleName());
	}

	@Test
	public void testUserRoleRel() {
		// add user and role
		authDao.addUser(statement, user1);
		authDao.addUser(statement, user2);
		authDao.addRole(statement, role1);
		authDao.addRole(statement, role2);

		// add relation
		assertEquals(true, authDao.addUserRoleRel(statement, user1.getUserName(), role1.getRoleName()));
		assertEquals(1, authDao.getAllUserRoleRel(statement).size());
		assertEquals(true, authDao.addUserRoleRel(statement, user1.getUserName(), role2.getRoleName()));
		List<Role> roles = authDao.getRolesByUser(statement, user1.getUserName());
		assertEquals(2, roles.size());
		for (Role role : roles) {
			assertEquals(true, (role.getRoleName().equals(role1.getRoleName()))
					|| (role.getRoleName().equals(role2.getRoleName())));
		}
		// delete the relation
		authDao.deleteUserRoleRel(statement, user1.getUserName(), role1.getRoleName());
		authDao.deleteUserRoleRel(statement, user1.getUserName(), role2.getRoleName());
		assertEquals(0, authDao.getAllUserRoleRel(statement).size());
		// delete user and role
		authDao.deleteUser(statement, user1.getUserName());
		authDao.deleteUser(statement, user2.getUserName());
		authDao.deleteRole(statement, role1.getRoleName());
		authDao.deleteRole(statement, role2.getRoleName());
	}

	@Test
	public void testUserPermission() {
		int permissionId1 = Permission.CREATE;
		int permissionId2 = Permission.DELETE;
		int permissionId3 = Permission.INSERT;
		String nodeName = "nodeName";
		authDao.addUser(statement, user1);

		// add permission
		authDao.addUserPermission(statement, user1.getUserName(), nodeName, permissionId1);
		authDao.addUserPermission(statement, user1.getUserName(), nodeName, permissionId2);
		authDao.addUserPermission(statement, user1.getUserName(), nodeName, permissionId3);
		assertEquals(false, authDao.addUserPermission(statement, user1.getUserName(), nodeName, permissionId1));
		List<UserPermission> userPermissions = authDao.getUserPermission(statement, user1.getUserName(), nodeName);
		assertEquals(3, userPermissions.size());
		List<Integer> list = new ArrayList<>();
		for (UserPermission userPermission : userPermissions) {
			list.add(userPermission.getPermissionId());
		}
		List<Integer> checkList = new ArrayList<>();
		checkList.add(permissionId1);
		checkList.add(permissionId2);
		checkList.add(permissionId3);
		checkList.removeAll(list);
		assertEquals(0, checkList.size());
		// delete permission
		authDao.deleteUserPermission(statement, user1.getUserName(), nodeName, permissionId1);
		assertEquals(2, authDao.getUserPermission(statement, user1.getUserName(), nodeName).size());
		authDao.deleteUserPermission(statement, user1.getUserName(), nodeName, permissionId2);
		assertEquals(1, authDao.getUserPermission(statement, user1.getUserName(), nodeName).size());
		authDao.deleteUserPermission(statement, user1.getUserName(), nodeName, permissionId3);
		assertEquals(0, authDao.getUserPermission(statement, user1.getUserName(), nodeName).size());
		// delete user
		authDao.deleteUser(statement, user1.getUserName());
	}

	@Test
	public void testRolePermission() {
		int permissionId1 = Permission.CREATE;
		int permissionId2 = Permission.DELETE;
		int permissionId3 = Permission.INSERT;
		String nodeName = "nodeName";
		authDao.addRole(statement, role1);
		// add permission
		authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId1);
		authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId2);
		authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId3);
		assertEquals(false, authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId1));

		List<RolePermission> rolePermissions = authDao.getRolePermission(statement, role1.getRoleName(), nodeName);
		assertEquals(3, rolePermissions.size());
		List<Integer> list = new ArrayList<>();
		for (RolePermission rolePermission : rolePermissions) {
			list.add(rolePermission.getPermissionId());
		}
		List<Integer> checkList = new ArrayList<>();
		checkList.add(permissionId1);
		checkList.add(permissionId2);
		checkList.add(permissionId3);
		checkList.removeAll(list);
		assertEquals(0, checkList.size());
		// delete permission
		authDao.deleteRolePermission(statement, role1.getRoleName(), nodeName, permissionId1);
		assertEquals(2, authDao.getRolePermission(statement, role1.getRoleName(), nodeName).size());
		authDao.deleteRolePermission(statement, role1.getRoleName(), nodeName, permissionId2);
		assertEquals(1, authDao.getRolePermission(statement, role1.getRoleName(), nodeName).size());
		authDao.deleteRolePermission(statement, role1.getRoleName(), nodeName, permissionId3);
		assertEquals(0, authDao.getRolePermission(statement, role1.getRoleName(), nodeName).size());
		// delete user
		authDao.deleteRole(statement, role1.getRoleName());
	}

	@Test
	public void testUserAllPermission() {
		String nodeName = "nodeName";
		String newNodeName = "newNodeName";
		int permissionId1 = Permission.CREATE;
		int permissionId2 = Permission.DELETE;
		int permissionId3 = Permission.INSERT;

		// add user
		authDao.addUser(statement, user1);
		// add role
		authDao.addRole(statement, role1);
		authDao.addRole(statement, role2);

		// only add permission to user
		authDao.addUserPermission(statement, user1.getUserName(), nodeName, permissionId1);
		assertEquals(1, authDao.getAllUserPermission(statement, user1.getUserName(), nodeName).size());
		// add role1 permission
		authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId1);
		authDao.addRolePermission(statement, role1.getRoleName(), nodeName, permissionId3);
		// add permission to different node
		authDao.addRolePermission(statement, role1.getRoleName(), newNodeName, permissionId2);
		// add role2 permission
		authDao.addRolePermission(statement, role2.getRoleName(), nodeName, permissionId1);
		// add roles(role1,role2) to user
		authDao.addUserRoleRel(statement, user1.getUserName(), role1.getRoleName());
		authDao.addUserRoleRel(statement, user1.getUserName(), role2.getRoleName());

		// check the all user's permission
		assertEquals(2, authDao.getAllUserPermission(statement, user1.getUserName(), nodeName).size());

		assertEquals(1, authDao.getAllUserPermission(statement, user1.getUserName(), newNodeName).size());
		
		// delete the user and role
		authDao.deleteUser(statement, user1.getUserName());
		authDao.deleteRole(statement, role1.getRoleName());
		authDao.deleteRole(statement, role2.getRoleName());
	}
}
