package cn.edu.thu.tsfiledb.qp.logical.sys;

import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.utils.StringContainer;
import cn.edu.thu.tsfiledb.qp.logical.RootOperator;

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

    public void setNodeNameList(String[] nodeNameList) {
        StringContainer sc = new StringContainer(SystemConstant.PATH_SEPARATOR);
        sc.addTail(nodeNameList);
        this.nodeName = new Path(sc);
    }

    public enum AuthorType {
        CREATE_USER, CREATE_ROLE, DROP_USER, DROP_ROLE, GRANT_ROLE, GRANT_USER, GRANT_ROLE_TO_USER, REVOKE_USER, REVOKE_ROLE, REVOKE_ROLE_FROM_USER,UPDATE_USER
    }
}
