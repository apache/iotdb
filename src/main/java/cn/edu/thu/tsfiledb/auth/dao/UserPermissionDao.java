package cn.edu.thu.tsfiledb.auth.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfiledb.auth.AuthRuntimeException;
import cn.edu.thu.tsfiledb.auth.model.DBContext;
import cn.edu.thu.tsfiledb.auth.model.UserPermission;


/**
 * @author liukun
 *
 */
public class UserPermissionDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(UserPermissionDao.class);
	
	public int createUserPermission(Statement statement, UserPermission userPermission) {
		String sql = "insert into " + DBContext.userPermission + " (userId,nodeName,permissionId)" + " values("
				+ userPermission.getUserId() + ",'" + userPermission.getNodeName() + "',"
				+ userPermission.getPermissionId() + ")";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return state;
	}

	public int deleteUserPermission(Statement statement, UserPermission userPermission) {
		String sql = "delete from " + DBContext.userPermission + " where userId=" + userPermission.getUserId() + " and "
				+ "nodeName=" + "'" + userPermission.getNodeName() + "'" + " and " + "permissionId="
				+ userPermission.getPermissionId();
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return state;
	}

	public UserPermission getUserPermission(Statement statement, UserPermission userPermission) {
		String sql = "select * from " + DBContext.userPermission + " where userId=" + userPermission.getUserId()
				+ " and " + "nodeName=" + "'" + userPermission.getNodeName() + "'" + " and " + "permissionId="
				+ userPermission.getPermissionId();
		UserPermission permission = null;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				permission = new UserPermission(resultSet.getInt(1), resultSet.getInt(2), resultSet.getString(3),
						resultSet.getInt(4));
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return permission;
	}

	public ArrayList<UserPermission> getUserPermissionByUserAndNodeName(Statement statement, int userId,
			String nodeName) {
		ArrayList<UserPermission> userPermissions = new ArrayList<>();
		String sql = "select * from " + DBContext.userPermission + " where userId=" + userId + " and " + "nodeName="
				+ "'" + nodeName + "'";
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				UserPermission userPermission = new UserPermission(resultSet.getInt(1), resultSet.getInt(2),
						resultSet.getString(3), resultSet.getInt(4));
				userPermissions.add(userPermission);
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return userPermissions;
	}

	public List<UserPermission> getUserPermissions(Statement statement) {
		ArrayList<UserPermission> userPermissions = new ArrayList<>();
		String sql = "select * from " + DBContext.userPermission;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				UserPermission userPermission = new UserPermission(resultSet.getInt(1), resultSet.getInt(2),
						resultSet.getString(3), resultSet.getInt(4));
				userPermissions.add(userPermission);
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return userPermissions;
	}
}
