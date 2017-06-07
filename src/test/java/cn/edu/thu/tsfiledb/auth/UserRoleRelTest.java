package cn.edu.thu.tsfiledb.auth;

import static org.junit.Assert.assertEquals;

import java.sql.Statement;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.dao.RoleDao;
import cn.edu.thu.tsfiledb.auth.dao.UserDao;
import cn.edu.thu.tsfiledb.auth.dao.UserRoleRelDao;
import cn.edu.thu.tsfiledb.auth.model.Role;
import cn.edu.thu.tsfiledb.auth.model.User;
import cn.edu.thu.tsfiledb.auth.model.UserRoleRel;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;

public class UserRoleRelTest {

	private DBdao dbdao = null;
	private UserRoleRelDao userRoleRelDao = null;
	private UserDao userDao = null;
	private RoleDao roleDao = null;
	private Statement statement = null;
	User user1 = new User("user1", "user2");
	User user2 = new User("user2", "user2");
	Role role1 = new Role("role1");
	Role role2 = new Role("role2");

	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	
	@Before
	public void setUp() throws Exception {

		config.derbyHome = "";
		dbdao = new DBdao();
		dbdao.open();
		statement = DBdao.getStatement();
		userDao = new UserDao();
		roleDao = new RoleDao();
		userRoleRelDao = new UserRoleRelDao();

		// create user
		if (userDao.getUser(statement, user1.getUserName()) == null) {
			userDao.createUser(statement, user1);
		}
		if (userDao.getUser(statement, user2.getUserName()) == null) {
			userDao.createUser(statement, user2);
		}

		// create role
		if (roleDao.getRole(statement, role1.getRoleName()) == null) {
			roleDao.createRole(statement, role1);
		}
		if (roleDao.getRole(statement, role2.getRoleName()) == null) {
			roleDao.createRole(statement, role2);
		}
	}

	@After
	public void tearDown() throws Exception {

		userDao.deleteUser(statement, user1.getUserName());
		userDao.deleteUser(statement, user2.getUserName());
		roleDao.deleteRole(statement, role1.getRoleName());
		roleDao.deleteRole(statement, role2.getRoleName());
		dbdao.close();

	}

	@Test
	public void createUserRoleRelTest() {
		// create relation between user and role
		String userName = "user1";
		String roleName = "role1";
		int userId = userDao.getUser(statement, userName).getId();
		int roleId = roleDao.getRole(statement, roleName).getId();
		UserRoleRel userRoleRel = new UserRoleRel(userId, roleId);
		// if not exist, create the relation
		if (userRoleRelDao.getUserRoleRel(statement, userRoleRel) == null) {
			userRoleRelDao.createUserRoleRel(statement, userRoleRel);
		} else {
			userRoleRelDao.deleteUserRoleRel(statement, userRoleRel);
			userRoleRelDao.createUserRoleRel(statement, userRoleRel);
		}
		// get the relation
		userRoleRel = userRoleRelDao.getUserRoleRel(statement, userRoleRel);
		assertEquals(userId, userRoleRel.getUserId());
		assertEquals(roleId, userRoleRel.getRoleId());
		// delete the relation ana assert
		userRoleRelDao.deleteUserRoleRel(statement, userRoleRel);
		assertEquals(null, userRoleRelDao.getUserRoleRel(statement, userRoleRel));
	}

	@Test
	public void getRolesByUserTest() {
		String user1name = user1.getUserName();
		String role1name = role1.getRoleName();
		String role2name = role2.getRoleName();
		int user1Id = userDao.getUser(statement, user1name).getId();
		int role1Id = roleDao.getRole(statement, role1name).getId();
		int role2Id = roleDao.getRole(statement, role2name).getId();

		UserRoleRel userRoleRel1 = new UserRoleRel(user1Id, role1Id);
		UserRoleRel userRoleRel2 = new UserRoleRel(user1Id, role2Id);

		// if not exist, create the relations
		if (userRoleRelDao.getUserRoleRel(statement, userRoleRel1) == null) {
			userRoleRelDao.createUserRoleRel(statement, userRoleRel1);
		}
		if (userRoleRelDao.getUserRoleRel(statement, userRoleRel2) == null) {
			userRoleRelDao.createUserRoleRel(statement, userRoleRel2);
		}
		// get the relation and assert them
		ArrayList<UserRoleRel> arrayList = (ArrayList<UserRoleRel>) userRoleRelDao.getUserRoleRelByUser(statement,
				user1Id);
		ArrayList<Integer> roleIds = new ArrayList<>();
		for (UserRoleRel userRoleRel : arrayList) {
			roleIds.add(userRoleRel.getRoleId());
		}
		ArrayList<Integer> list = new ArrayList<>();
		list.add(role1Id);
		list.add(role2Id);

		roleIds.removeAll(list);
		assertEquals(0, roleIds.size());
		// delete the relations
		userRoleRelDao.deleteUserRoleRel(statement, userRoleRel1);
		userRoleRelDao.deleteUserRoleRel(statement, userRoleRel2);

	}

	@Test
	public void getUserByRoleTest() {
		String role1name = role1.getRoleName();
		String user1name = user1.getUserName();
		String user2name = user2.getUserName();
		int role1Id = roleDao.getRole(statement, role1name).getId();
		int user1Id = userDao.getUser(statement, user1name).getId();
		int user2Id = userDao.getUser(statement, user2name).getId();

		UserRoleRel userRoleRel1 = new UserRoleRel(user1Id, role1Id);
		UserRoleRel userRoleRel2 = new UserRoleRel(user2Id, role1Id);

		// if not exist, create the relations
		if (userRoleRelDao.getUserRoleRel(statement, userRoleRel1) == null) {
			userRoleRelDao.createUserRoleRel(statement, userRoleRel1);
		}
		if (userRoleRelDao.getUserRoleRel(statement, userRoleRel2) == null) {
			userRoleRelDao.createUserRoleRel(statement, userRoleRel2);
		}
		// get the relation and assert them
		ArrayList<UserRoleRel> arrayList = (ArrayList<UserRoleRel>) userRoleRelDao.getUserRoleRelByRole(statement,
				role1Id);
		ArrayList<Integer> userIds = new ArrayList<>();
		for (UserRoleRel userRoleRel : arrayList) {
			userIds.add(userRoleRel.getUserId());
		}
		ArrayList<Integer> list = new ArrayList<>();
		list.add(user1Id);
		list.add(user2Id);

		userIds.removeAll(list);
		assertEquals(0, userIds.size());
		// delete the relations
		userRoleRelDao.deleteUserRoleRel(statement, userRoleRel1);
		userRoleRelDao.deleteUserRoleRel(statement, userRoleRel2);
	}

}
