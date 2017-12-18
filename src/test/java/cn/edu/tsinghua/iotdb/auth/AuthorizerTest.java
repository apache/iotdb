package cn.edu.tsinghua.iotdb.auth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.auth.dao.Authorizer;
import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.model.User;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class AuthorizerTest {

	private DBDao dbdao = null;

	@Before
	public void setUp() throws Exception {
		dbdao = new DBDao();
		dbdao.open();
		EnvironmentUtils.envSetUp();
	}

	@After
	public void tearDown() throws Exception {
		dbdao.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void testAuthorizer() {

		/**
		 * login
		 */
		boolean status = false;
		try {
			status = Authorizer.login("root", "root");
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			status = Authorizer.login("root", "error");
		} catch (AuthException e) {
			assertEquals("The username or the password is not correct", e.getMessage());
		}
		/**
		 * create user,delete user
		 */
		User user = new User("user", "password");
		try {
			status = Authorizer.createUser(user.getUserName(), user.getPassWord());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.createUser(user.getUserName(), user.getPassWord());
		} catch (AuthException e) {
			assertEquals("The user is exist", e.getMessage());
		}
		try {
			status = Authorizer.login(user.getUserName(), user.getPassWord());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.deleteUser(user.getUserName());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.deleteUser(user.getUserName());
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}

		/**
		 * permission for user
		 */
		String nodeName = "root.laptop.d1";
		try {
			Authorizer.createUser(user.getUserName(), user.getPassWord());
			status = Authorizer.addPmsToUser(user.getUserName(), nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Authorizer.addPmsToUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is exist", e.getMessage());
		}
		try {
			Authorizer.addPmsToUser("error", nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}
		try {
			status = Authorizer.removePmsFromUser(user.getUserName(), nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.removePmsFromUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is not exist", e.getMessage());
		}
		try {
			Authorizer.deleteUser(user.getUserName());
			Authorizer.removePmsFromUser(user.getUserName(), nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The user is not exist", e.getMessage());
		}
		/**
		 * role
		 */
		String roleName = "role";
		try {
			status = Authorizer.createRole(roleName);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.createRole(roleName);
		} catch (AuthException e) {
			assertEquals("The role is exist", e.getMessage());
		}

		try {
			status = Authorizer.deleteRole(roleName);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}
		/**
		 * role permission
		 */
		try {
			status = Authorizer.createRole(roleName);
			status = Authorizer.addPmsToRole(roleName, nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			status = Authorizer.addPmsToRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is exist", e.getMessage());
		}

		try {
			status = Authorizer.removePmsFromRole(roleName, nodeName, 1);
			assertEquals(true, status);
		} catch (AuthException e1) {
			fail(e1.getMessage());
		}
		try {
			Authorizer.removePmsFromRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The permission is not exist", e.getMessage());
		}

		try {
			Authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}

		try {
			Authorizer.removePmsFromRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}
		try {
			Authorizer.addPmsToRole(roleName, nodeName, 1);
		} catch (AuthException e) {
			assertEquals("The role is not exist", e.getMessage());
		}

		/**
		 * user role
		 */
		try {
			Authorizer.createUser(user.getUserName(), user.getPassWord());
			Authorizer.createRole(roleName);
			status = Authorizer.grantRoleToUser(roleName, user.getUserName());
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Authorizer.addPmsToUser(user.getUserName(), nodeName, 1);
			Authorizer.addPmsToRole(roleName, nodeName, 2);
			Authorizer.addPmsToRole(roleName, nodeName, 3);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Set<Integer> permisssions = Authorizer.getPermission(user.getUserName(), nodeName);
			assertEquals(3, permisssions.size());
			assertEquals(true, permisssions.contains(1));
			assertEquals(true, permisssions.contains(2));
			assertEquals(true, permisssions.contains(3));
			assertEquals(false, permisssions.contains(4));
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			status = Authorizer.revokeRoleFromUser(roleName, user.getUserName());
			assertEquals(true, status);
			Set<Integer> permisssions = Authorizer.getPermission(user.getUserName(), nodeName);
			assertEquals(1, permisssions.size());
			assertEquals(true, permisssions.contains(1));
			assertEquals(false, permisssions.contains(2));
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		status = Authorizer.checkUserPermission(user.getUserName(), nodeName, 1);
		assertEquals(true, status);
		status = Authorizer.checkUserPermission(user.getUserName(), nodeName, 2);
		assertEquals(false, status);
		try {
			status = Authorizer.updateUserPassword(user.getUserName(), "new");
			status = Authorizer.login(user.getUserName(), "new");
			assertEquals(true, status);
		} catch (AuthException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		try {
			Authorizer.deleteUser(user.getUserName());
			Authorizer.deleteRole(roleName);
		} catch (AuthException e) {
			e.printStackTrace();
		}
	}

}
