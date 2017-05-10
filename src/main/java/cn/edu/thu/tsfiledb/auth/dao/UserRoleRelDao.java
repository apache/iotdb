package cn.edu.thu.tsfiledb.auth.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.auth.model.DBContext;
import cn.edu.thu.tsfiledb.auth.model.UserRoleRel;

/**
 * @author liukun
 *
 */
public class UserRoleRelDao {

	public List<UserRoleRel> getUserRoleRels(Statement statement) {
		String sql = "select * from " + DBContext.userRoleRel;
		ArrayList<UserRoleRel> arrayList = new ArrayList<>();

		try {
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {

				int id = resultSet.getInt(1);
				int userId = resultSet.getInt(2);
				int roleId = resultSet.getInt(3);
				UserRoleRel rel = new UserRoleRel(id, userId, roleId);
				arrayList.add(rel);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return arrayList;
	}

	public UserRoleRel getUserRoleRel(Statement statement, UserRoleRel rel) {
		String sql = "select * from " + DBContext.userRoleRel + " where userId=" + rel.getUserId() + " and roleId="
				+ rel.getRoleId();
		UserRoleRel userRoleRel = null;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				userRoleRel = new UserRoleRel(resultSet.getInt(1), resultSet.getInt(2), resultSet.getInt(3));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return userRoleRel;

	}

	public List<UserRoleRel> getUserRoleRelByUser(Statement statement, int userId) {
		String sql = "select * from " + DBContext.userRoleRel + " where userId = " + userId;
		ArrayList<UserRoleRel> arrayList = new ArrayList<>();
		try {
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
				int roleId = resultSet.getInt(3);
				UserRoleRel rel = new UserRoleRel(id, userId, roleId);
				arrayList.add(rel);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return arrayList;
	}

	public List<UserRoleRel> getUserRoleRelByRole(Statement statement, int roleId) {
		String sql = "select * from " + DBContext.userRoleRel + " where roleId=" + roleId;
		ArrayList<UserRoleRel> arrayList = new ArrayList<>();
		try {
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
				int userId = resultSet.getInt(2);
				UserRoleRel rel = new UserRoleRel(id, userId, roleId);
				arrayList.add(rel);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return arrayList;
	}

	public int createUserRoleRel(Statement statement, UserRoleRel rel) {
		String sql = "insert into " + DBContext.userRoleRel + " (userId,roleId) values" + "(" + rel.getUserId() + ","
				+ rel.getRoleId() + ")";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public int deleteUserRoleRel(Statement statement, UserRoleRel rel) {
		String sql = "delete from " + DBContext.userRoleRel + " where userId=" + rel.getUserId() + " and roleId="
				+ rel.getRoleId();
		int state = 0;

		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return state;
	}

}
