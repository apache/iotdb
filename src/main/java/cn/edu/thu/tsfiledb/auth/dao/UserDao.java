package cn.edu.thu.tsfiledb.auth.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.auth.model.DBContext;
import cn.edu.thu.tsfiledb.auth.model.User;

/**
 * @author liukun
 *
 */
public class UserDao {

	public int createUser(Statement statement, User user) {

		String sql = "insert into " + DBContext.userTable + " (userName,passWord) " + " values ('" + user.getUserName()
				+ "','" + user.getPassWord() + "')";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public int deleteUser(Statement statement, String userName) {
		String sql = "delete from " + DBContext.userTable + " where userName=" + "'" + userName + "'";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public int updateUserPassword(Statement statement, String userName, String newPassword) {
		String sql = "update " + DBContext.userTable + " set password='" + newPassword + "'" + " where username='"
				+ userName + "'";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public List<User> getUsers(Statement statement) {
		String sql = "select * from " + DBContext.userTable;
		ArrayList<User> arrayList = new ArrayList<>();

		try {
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
				String userName = resultSet.getString(2);
				String passWord = resultSet.getString(3);
				boolean isLock = resultSet.getBoolean(4);
				String validTime = resultSet.getString(5);
				User user = new User(id, userName, passWord, isLock, validTime);
				arrayList.add(user);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return arrayList;
	}

	public User getUser(Statement statement, String userName) {
		String sql = "select * from " + DBContext.userTable + " where userName=" + "'" + userName + "'";
		User user = null;

		try {
			ResultSet resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				int id = resultSet.getInt(1);
				String name = userName;
				String passWord = resultSet.getString(3);
				boolean isLock = resultSet.getBoolean(4);
				String validTime = resultSet.getString(5);
				user = new User(id, name, passWord, isLock, validTime);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return user;

	}

	public User getUser(Statement statement, String userName, String password) {
		User user = null;
		String sql = "select * from " + DBContext.userTable + " where username='" + userName + "'" + " and password='"
				+ password + "'";
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				user = new User();
				user.setId(resultSet.getInt(1));
				user.setUserName(resultSet.getString(2));
				user.setPassWord(resultSet.getString(3));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return user;
	}

}
