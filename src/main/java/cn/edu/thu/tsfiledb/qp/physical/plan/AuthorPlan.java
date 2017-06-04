package cn.edu.thu.tsfiledb.qp.physical.plan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfile.common.exception.ProcessorException;
import cn.edu.thu.tsfiledb.auth.model.AuthException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfiledb.qp.exec.QueryProcessExecutor;
import cn.edu.thu.tsfiledb.qp.logical.operator.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.operator.author.AuthorOperator.AuthorType;

/**
 * given a author related plan and construct a {@code AuthorPlan}
 * 
 * @author kangrong、whw
 *
 */
public class AuthorPlan extends PhysicalPlan {
	private final AuthorType authorType;
	private String userName;
	private String roleName;
	private String password;
	/*
	 * 刘昆修改的添加new password
	 */
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

	public boolean processNonQuery(QueryProcessExecutor config) throws ProcessorException{
		try {
			boolean flag = true;
			Set<Integer> permissions;
			switch (authorType) {
			/*
			 * 刘昆修改添加一个物理修改密码的操作
			 * 
			 */
			case UPDATE_USER:
				return config.updateUser(userName, newPassword);
			case CREATE_USER:
				return config.createUser(userName, password);
			case CREATE_ROLE:
				return config.createRole(roleName);
			case DROP_USER:
				return config.deleteUser(userName);
			case DROP_ROLE:
				return config.deleteRole(roleName);
			case GRANT_ROLE:
				permissions = pmsToInt(authorizationList);
				for (int i : permissions) {
					if (!config.addPmsToRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_USER:
				permissions = pmsToInt(authorizationList);
				for (int i : permissions) {
					if (!config.addPmsToUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case GRANT_ROLE_TO_USER:
				return config.grantRoleToUser(roleName, userName);
			case REVOKE_USER:
				permissions = pmsToInt(authorizationList);
				for (int i : permissions) {
					if (!config.removePmsFromUser(userName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE:
				permissions = pmsToInt(authorizationList);
				for (int i : permissions) {
					if (!config.removePmsFromRole(roleName, nodeName.getFullPath(), i))
						flag = false;
				}
				return flag;
			case REVOKE_ROLE_FROM_USER:
				return config.revokeRoleFromUser(roleName, userName);
			default:
				break;

			}
		} catch (AuthException e) {
			throw new ProcessorException(e.getMessage());
		}
		return false;
	}

	Set<Integer> pmsToInt(String[] authorizationList) {
		Set<Integer> result = new HashSet<Integer>();
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
	public List<Path> getInvolvedSeriesPaths() {
		List<Path> ret = new ArrayList<Path>();
		if (nodeName != null)
			ret.add(nodeName);
		return ret;
	}
}
