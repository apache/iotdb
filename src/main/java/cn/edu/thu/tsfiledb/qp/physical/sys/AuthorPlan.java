package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.auth.model.AuthException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.executor.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;


public class AuthorPlan extends PhysicalPlan {
	private final AuthorType authorType;
	private String userName;
	private String roleName;
	private String password;
	private String newPassword;
	private String[] authorizationList;
	private Path nodeName;

	public AuthorPlan(AuthorType authorType, String userName, String roleName, String password, String newPassword,
			String[] authorizationList, Path nodeName) {
		super(false, OperatorType.AUTHOR);
		this.authorType = authorType;
		this.userName = userName;
		this.roleName = roleName;
		this.password = password;
		this.newPassword = newPassword;
		this.authorizationList = authorizationList;
		this.nodeName = nodeName;
	}

	public boolean processNonQuery(QueryProcessExecutor executor) throws ProcessorException{
		try {
			boolean flag = true;
			Set<Integer> permissions;
			switch (authorType) {
			case UPDATE_USER:
				return executor.updateUser(userName, newPassword);
			case CREATE_USER:
				return executor.createUser(userName, password);
			case CREATE_ROLE:
				return executor.createRole(roleName);
			case DROP_USER:
				return executor.deleteUser(userName);
			case DROP_ROLE:
				return executor.deleteRole(roleName);
			case GRANT_ROLE:
				permissions = strToInt(authorizationList);
				for (int i : permissions) {
					if (!executor.addPermissionToRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_USER:
				permissions = strToInt(authorizationList);
				for (int i : permissions) {
					if (!executor.addPermissionToUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_ROLE_TO_USER:
				return executor.grantRoleToUser(roleName, userName);
			case REVOKE_USER:
				permissions = strToInt(authorizationList);
				for (int i : permissions) {
					if (!executor.removePermissionFromUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE:
				permissions = strToInt(authorizationList);
				for (int i : permissions) {
					if (!executor.removePermissionFromRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE_FROM_USER:
				return executor.revokeRoleFromUser(roleName, userName);
			default:
				break;

			}
		} catch (AuthException e) {
			throw new ProcessorException(e.getMessage());
		}
		return false;
	}

	private Set<Integer> strToInt(String[] authorizationList) {
		Set<Integer> result = new HashSet<>();
		for (String s : authorizationList) {
			s = s.toUpperCase();
			switch (s) {
			case "CREATE":
				result.add(0);
				break;
			case "INSERT":
				result.add(1);
				break;
			case "MODIFY":
				result.add(2);
				break;
			case "READ":
				result.add(3);
				break;
			case "DELETE":
				result.add(4);
				break;
			default:
				break;
			}
		}
		return result;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> ret = new ArrayList<>();
		if (nodeName != null)
			ret.add(nodeName);
		return ret;
	}
}
