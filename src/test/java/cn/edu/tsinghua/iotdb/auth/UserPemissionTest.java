package cn.edu.tsinghua.iotdb.auth;

import static org.junit.Assert.assertEquals;

import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.iotdb.auth.dao.DBDao;
import cn.edu.tsinghua.iotdb.auth.dao.UserDao;
import cn.edu.tsinghua.iotdb.auth.dao.UserPermissionDao;
import cn.edu.tsinghua.iotdb.auth.model.Permission;
import cn.edu.tsinghua.iotdb.auth.model.User;
import cn.edu.tsinghua.iotdb.auth.model.UserPermission;
import cn.edu.tsinghua.iotdb.utils.EnvironmentUtils;

public class UserPemissionTest {

	private DBDao DBdao = null;
	private UserDao userDao = null;
	private UserPermissionDao UserPermissionDao = null;
	private Statement statement = null;

	private String nodeName = "nodeName";
	private String newNodeName = "newNodeName";
	private int permission;
	private User user = new User("user1", "user1");

	
	@Before
	public void setUp() throws Exception {
		permission = Permission.CREATE;
		DBdao = new DBDao();
		DBdao.open();
		EnvironmentUtils.envSetUp();
		statement = DBDao.getStatement();
		userDao = new UserDao();
		UserPermissionDao = new UserPermissionDao();

		// if not exitst, create the user
		if (userDao.getUser(statement, user.getUserName()) == null) {
			userDao.createUser(statement, user);
		}
	}

	@After
	public void tearDown() throws Exception {
		userDao.deleteUser(statement, user.getUserName());
		DBdao.close();
		EnvironmentUtils.cleanEnv();
	}

	@Test
	public void test() {
		UserPermission userPermission = new UserPermission(userDao.getUser(statement, user.getUserName()).getId(),
				nodeName, permission);
		// if userpermission exist in the table ,and delete it
		UserPermissionDao.deleteUserPermission(statement, userPermission);
		// create permission
		UserPermissionDao.createUserPermission(statement, userPermission);
		UserPermission permission = UserPermissionDao.getUserPermission(statement, userPermission);
		assertEquals(userPermission.getUserId(), permission.getUserId());
		assertEquals(userPermission.getNodeName(), permission.getNodeName());
		assertEquals(userPermission.getPermissionId(), permission.getPermissionId());
		// delete permission
		int state = 0;
		state = UserPermissionDao.deleteUserPermission(statement, userPermission);
		assertEquals(1, state);
		permission = UserPermissionDao.getUserPermission(statement, userPermission);
		assertEquals(null, permission);
	}

	@Test
	public void testGetUserPermissions() {
		UserPermission userPermission1 = new UserPermission(userDao.getUser(statement, user.getUserName()).getId(),
				nodeName, Permission.CREATE);
		UserPermission userPermission2 = new UserPermission(userDao.getUser(statement, user.getUserName()).getId(),
				nodeName, Permission.DELETE);
		UserPermissionDao.createUserPermission(statement, userPermission1);
		UserPermissionDao.createUserPermission(statement, userPermission2);
		assertEquals(2, UserPermissionDao.getUserPermissions(statement).size());
		UserPermission userPermission3 = new UserPermission(userDao.getUser(statement, user.getUserName()).getId(),
				newNodeName, Permission.CREATE);
		UserPermissionDao.createUserPermission(statement, userPermission3);
		assertEquals(2, UserPermissionDao.getUserPermissionByUserAndNodeName(statement,
				userDao.getUser(statement, user.getUserName()).getId(), nodeName).size());
		assertEquals(1, UserPermissionDao.getUserPermissionByUserAndNodeName(statement,
				userDao.getUser(statement, user.getUserName()).getId(), newNodeName).size());
	}

}
