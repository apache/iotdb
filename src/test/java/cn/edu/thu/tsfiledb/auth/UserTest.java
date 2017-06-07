package cn.edu.thu.tsfiledb.auth;

import static org.junit.Assert.assertEquals;

import java.sql.Statement;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.dao.UserDao;
import cn.edu.thu.tsfiledb.auth.model.User;
import cn.edu.thu.tsfiledb.conf.TsfileDBConfig;
import cn.edu.thu.tsfiledb.conf.TsfileDBDescriptor;
import cn.edu.thu.tsfiledb.engine.overflow.io.EngineTestHelper;

public class UserTest {

	private Statement statement = null;
	private UserDao userDao = null;
	private DBdao dBdao = null;

	private String userName = "testuser";
	private String passWord = "password";
	private User user = new User(userName, passWord);
	private TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
	
	/**
	 * @throws Exception
	 *             prepare to connect the derby DB
	 */
	@Before
	public void setUp() throws Exception {
		config.derbyHome = "";
		EngineTestHelper.delete(config.derbyHome);
		dBdao = new DBdao();
		dBdao.open();
		statement = DBdao.getStatement();
		userDao = new UserDao();

	}

	@After
	public void tearDown() throws Exception {
		dBdao.close();
		EngineTestHelper.delete(config.derbyHome);
	}

	@Test
	public void createUserandDeleteUserTest() {
		User getUser = userDao.getUser(statement, userName);
		if (getUser != null) {
			int deleteCount = userDao.deleteUser(statement, userName);
			if (deleteCount > 0) {
				System.out.println("Delete the original record");
			}
		}
		// create user
		userDao.createUser(statement, user);
		getUser = userDao.getUser(statement, userName);
		assertEquals(userName, getUser.getUserName());
		assertEquals(passWord, getUser.getPassWord());
		// delete user
		userDao.deleteUser(statement, userName);
		getUser = userDao.getUser(statement, userName);
		assertEquals(null, getUser);
	}

	@Test
	public void getUsersTest() {
		User user1 = new User("user1", "user1");
		User user2 = new User("user2", "user2");
		ArrayList<User> arrayList = new ArrayList<>();
		arrayList.add(user1);
		arrayList.add(user2);
		if (userDao.getUser(statement, user1.getUserName()) == null) {
			userDao.createUser(statement, user1);
		}
		if (userDao.getUser(statement, user2.getUserName()) == null) {
			userDao.createUser(statement, user2);
		}
		ArrayList<User> list = (ArrayList<User>) userDao.getUsers(statement);
		// root user
		assertEquals(3, list.size());
		userDao.deleteUser(statement, user1.getUserName());
		userDao.deleteUser(statement, user2.getUserName());
		assertEquals(null, userDao.getUser(statement, user1.getUserName()));
		assertEquals(null, userDao.getUser(statement, user2.getUserName()));
	}

	@Test
	public void updateUserTest() {

		String username = "user";
		String oldPassword = username;
		User user = new User(username, oldPassword);

		if ((userDao.getUser(statement, user.getUserName())) == null) {
			userDao.createUser(statement, user);
		}
		user = userDao.getUser(statement, user.getUserName());
		assertEquals(username, user.getUserName());
		assertEquals(oldPassword, user.getPassWord());
		// update password

		String updatePassword = "password";
		userDao.updateUserPassword(statement, user.getUserName(), updatePassword);
		user = userDao.getUser(statement, user.getUserName());
		assertEquals(username, user.getUserName());
		assertEquals(updatePassword, user.getPassWord());
		userDao.deleteUser(statement, user.getUserName());
	}

}
