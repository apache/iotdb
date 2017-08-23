package cn.edu.thu.tsfiledb.qp.physical.sys;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import cn.edu.thu.tsfiledb.qp.logical.Operator.OperatorType;
import cn.edu.thu.tsfiledb.qp.logical.sys.AuthorOperator.AuthorType;
import cn.edu.thu.tsfiledb.qp.physical.PhysicalPlan;
import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;

/**
 * @author kangrong
 * @author qiaojialin
 */
public class AuthorPlan extends PhysicalPlan {
	private final AuthorType authorType;
	private String userName;
	private String roleName;
	private String password;
	private String newPassword;
	private Set<Integer> permissions;
	private Path nodeName;

	public AuthorPlan(AuthorType authorType, String userName, String roleName, String password, String newPassword,
			String[] authorizationList, Path nodeName) {
		super(false, OperatorType.AUTHOR);
		this.authorType = authorType;
		this.userName = userName;
		this.roleName = roleName;
		this.password = password;
		this.newPassword = newPassword;
		this.permissions = strToPermissions(authorizationList);
		this.nodeName = nodeName;

	}

	public AuthorType getAuthorType() {
		return authorType;
	}

	public String getUserName() {
		return userName;
	}

	public String getRoleName() {
		return roleName;
	}

	public String getPassword() {
		return password;
	}

	public String getNewPassword() {
		return newPassword;
	}

	public Set<Integer> getPermissions() {
		return permissions;
	}

	public Path getNodeName() {
		return nodeName;
	}

	private Set<Integer> strToPermissions(String[] authorizationList) {
		Set<Integer> result = new HashSet<>();
		if (authorizationList == null)
			return result;
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
	public String toString() {
		return "userName: "+ userName +
				"\nroleName: " + roleName +
				"\npassword: " + password +
				"\nnewPassword: " + newPassword +
				"\npermissions: " + permissions +
				"\nnodeName: " + nodeName +
				"\nauthorType: " + authorType;
	}

	@Override
	public List<Path> getPaths() {
		List<Path> ret = new ArrayList<>();
		if (nodeName != null)
			ret.add(nodeName);
		return ret;
	}
}
