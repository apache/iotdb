package cn.edu.thu.tsfiledb.auth.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import cn.edu.thu.tsfiledb.auth.model.DBContext;
import cn.edu.thu.tsfiledb.auth.model.Role;

/**
 * @author liukun
 *
 */
public class RoleDao {

	public List<Role> getRoles(Statement statement) {
		ArrayList<Role> arrayList = new ArrayList<>();
		String sql = "select * from " + DBContext.roleTable;

		try {
			ResultSet resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				Role role = new Role();
				int id = resultSet.getInt(1);
				String roleName = resultSet.getString(2);
				role.setId(id);
				role.setRoleName(roleName);
				arrayList.add(role);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return arrayList;
	}

	public Role getRole(Statement statement, String roleName) {
		String sql = "select * from " + DBContext.roleTable + " where roleName=" + "'" + roleName + "'";
		Role role = null;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				int id = resultSet.getInt(1);
				String name = resultSet.getString(2);
				role = new Role(id, name);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return role;
	}

	public Role getRole(Statement statement, int roleId) {
		String sql = "select * from " + DBContext.roleTable + " where id=" + roleId;
		Role role = null;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				role = new Role(roleId, resultSet.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return role;
	}

	public int deleteRole(Statement statement, String roleName) {
		String sql = "delete from " + DBContext.roleTable + " where roleName=" + "'" + roleName + "'";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public int createRole(Statement statement, Role role) {
		String sql = "insert into " + DBContext.roleTable + " (" + "roleName" + ")" + " values('" + role.getRoleName()
				+ "')";

		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return state;
	}

	public int updateRole(Statement statement, String roleName, String newRoleName) {
		String sql = "update " + DBContext.roleTable + " set roleName='" + newRoleName + "'" + " where roleName='"
				+ roleName + "'";
		int state = 0;
		
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return state;
	}
}
