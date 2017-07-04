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
import cn.edu.thu.tsfiledb.auth.model.RolePermission;


/**
 * @author liukun
 *
 */
public class RolePermissionDao {

	private static final Logger LOGGER = LoggerFactory.getLogger(RolePermissionDao.class);
	
	public int createRolePermission(Statement statement, RolePermission rolePermission) {
		String sql = "insert into " + DBContext.rolePermission + " (roleId,nodeName,permissionId) values" + "("
				+ rolePermission.getRoleId() + ",'" + rolePermission.getNodeName() + "',"
				+ rolePermission.getPermissionId() + ")";
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return state;
	}

	public int deleteRolePermission(Statement statement, RolePermission rolePermission) {
		String sql = "delete from " + DBContext.rolePermission + " where roleId=" + rolePermission.getRoleId() + " and "
				+ "nodeName=" + "'" + rolePermission.getNodeName() + "'" + " and " + "permissionId="
				+ rolePermission.getPermissionId();
		int state = 0;
		try {
			state = statement.executeUpdate(sql);
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return state;
	}

	public RolePermission getRolePermission(Statement statement, RolePermission rolePermission) {
		String sql = "select * from " + DBContext.rolePermission + " where roleId=" + rolePermission.getRoleId()
				+ " and nodeName='" + rolePermission.getNodeName() + "' and permissionId="
				+ rolePermission.getPermissionId();
		RolePermission permission = null;
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			if (resultSet.next()) {
				permission = new RolePermission(resultSet.getInt(1), resultSet.getInt(2), resultSet.getString(3),
						resultSet.getInt(4));
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}

		return permission;
	}

	public List<RolePermission> getRolePermissions(Statement statement) {
		String sql = "select * from " + DBContext.rolePermission;
		List<RolePermission> rolePermissions = new ArrayList<>();
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				RolePermission rolePermission = new RolePermission(resultSet.getInt(1), resultSet.getInt(2),
						resultSet.getString(3), resultSet.getInt(4));
				rolePermissions.add(rolePermission);
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return rolePermissions;
	}

	public List<RolePermission> getRolePermissionByRoleAndNodeName(Statement statement, int roleId, String nodeName) {
		String sql = "select * from " + DBContext.rolePermission + " where roleId=" + roleId + " and nodeName='"
				+ nodeName + "'";
		List<RolePermission> rolePermissions = new ArrayList<>();
		ResultSet resultSet;
		try {
			resultSet = statement.executeQuery(sql);
			while (resultSet.next()) {
				RolePermission rolePermission = new RolePermission(resultSet.getInt(1), resultSet.getInt(2),
						resultSet.getString(3), resultSet.getInt(4));
				rolePermissions.add(rolePermission);
			}
		} catch (SQLException e) {
			LOGGER.error("Execute statement error, the statement is {}", sql);
			throw new AuthRuntimeException(e);
		}
		return rolePermissions;
	}

}
