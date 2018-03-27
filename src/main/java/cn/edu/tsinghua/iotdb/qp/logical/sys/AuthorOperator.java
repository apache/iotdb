package cn.edu.tsinghua.iotdb.qp.logical.sys;

import cn.edu.tsinghua.iotdb.qp.logical.Operator;
import cn.edu.tsinghua.iotdb.qp.logical.RootOperator;
import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.StringContainer;

/**
 * this class maintains information in Author statement, including CREATE, DROP, GRANT and REVOKE
 * 
 * @author kangrong
 *
 */
public class AuthorOperator extends RootOperator {

    private final AuthorType authorType;
    private String userName;
    private String roleName;
    private String password;
    private String newPassword;
    private String[] privilegeList;
    private Path nodeName;

    public AuthorOperator(int tokenIntType, AuthorType type) {
        super(tokenIntType);
        authorType = type;
        operatorType = OperatorType.AUTHOR;
    }

    public AuthorOperator(int tokenIntType, OperatorType type) {
        super(tokenIntType);
        authorType = null;
        operatorType = type;
    }

    public AuthorType getAuthorType() {
        return authorType;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getRoleName() {
        return roleName;
    }

    public void setRoleName(String roleName) {
        this.roleName = roleName;
    }

    public String getPassWord() {
        return password;
    }

    public void setPassWord(String password) {
        this.password = password;
    }

    public String getNewPassword() {
		return newPassword;
	}

	public void setNewPassword(String newPassword) {
		this.newPassword = newPassword;
	}

	public String[] getPrivilegeList() {
        return privilegeList;
    }

    public void setPrivilegeList(String[] authorizationList) {
        this.privilegeList = authorizationList;
    }
    
    public Path getNodeName() {
        return nodeName;
    }

    public void setNodeNameList(Path nodePath) {
        this.nodeName = nodePath;
    }

    public enum AuthorType {
        CREATE_USER, CREATE_ROLE, DROP_USER, DROP_ROLE, GRANT_ROLE, GRANT_USER, GRANT_ROLE_TO_USER, REVOKE_USER, REVOKE_ROLE, REVOKE_ROLE_FROM_USER,UPDATE_USER,
        LIST_USER, LIST_ROLE, LIST_USER_PRIVILEGE, LIST_ROLE_PRIVILEGE, LIST_USER_ROLES, LIST_ROLE_USERS
    }
}
