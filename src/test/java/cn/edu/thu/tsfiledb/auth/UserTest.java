package cn.edu.thu.tsfiledb.auth;

import static org.junit.Assert.*;

import java.sql.Statement;
import java.util.ArrayList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfiledb.auth.dao.DBdao;
import cn.edu.thu.tsfiledb.auth.dao.UserDao;
import cn.edu.thu.tsfiledb.auth.model.User;

public class UserTest {

	private Statement statement = null;
	private UserDao userDao = null;
	private DBdao dBdao = null;

	private String userName = "testuser";
	private String passWord = "password";
	private User user = new User(userName, passWord);

	/**
	 * @throws Exception
	 *             prepare to connect the derby DB
	 */
	@Before
	public void setUp() throws Exception {
		dBdao = new DBdao();
		dBdao.open();
		statement = dBdao.getStatement();
		userDao = new UserDao();

	}

	@After
	public void tearDown() throws Exception {
		dBdao.close();
	}

	/**
	 * Create user first and delete the user
	 */
	@Test
	public void test() {
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
		assertEquals(getUser, null);
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
		// add the root user
		assertEquals(3, list.size());
		// int count = 0;
		// // Some problems
		// for (User user : list) {
		// User testUser = arrayList.get(count);
		// count++;
		// if (user.getUserName().equals("root")) {
		// continue;
		// }
		// assertEquals(testUser.getUserName(), user.getUserName());
		// assertEquals(testUser.getPassWord(), user.getPassWord());
		// }
		userDao.deleteUser(statement, user1.getUserName());
		userDao.deleteUser(statement, user2.getUserName());
	}

	 @Test
	 public void updateUserTest(){
	 User user = new User("user", "user");
	 if ((userDao.getUser(statement, user.getUserName()))==null) {
	 userDao.createUser(statement, user);
	 }
	 user = userDao.getUser(statement, user.getUserName());
	 assertEquals("user", user.getUserName());
	 assertEquals("user", user.getPassWord());
	 // update password
	 String updatePassword = "password";
	 userDao.updateUserPassword(statement,
	 user.getUserName(), updatePassword);
	 user = userDao.getUser(statement, user.getUserName());
	 assertEquals("user", user.getUserName());
	 assertEquals(updatePassword, user.getPassWord());
	 userDao.deleteUser(statement, user.getUserName());
	 }

}
