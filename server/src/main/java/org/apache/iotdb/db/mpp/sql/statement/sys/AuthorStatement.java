package org.apache.iotdb.db.mpp.sql.statement.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.mpp.sql.constant.StatementType;
import org.apache.iotdb.db.mpp.sql.statement.Statement;
import org.apache.iotdb.db.mpp.sql.statement.StatementVisitor;

public class AuthorStatement extends Statement {

  private final AuthorStatement.AuthorType authorType;
  private String userName;
  private String roleName;
  private String password;
  private String newPassword;
  private String[] privilegeList;
  private PartialPath nodeName;

  /**
   * AuthorOperator Constructor with AuthorType.
   *
   * @param type author type
   */
  public AuthorStatement(AuthorStatement.AuthorType type) {
    super();
    authorType = type;
    statementType = StatementType.AUTHOR;
  }

  /**
   * AuthorOperator Constructor with OperatorType.
   *
   * @param type statement type
   */
  public AuthorStatement(StatementType type) {
    super();
    authorType = null;
    statementType = type;
  }

  public AuthorStatement.AuthorType getAuthorType() {
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

  public PartialPath getNodeName() {
    return nodeName;
  }

  public void setNodeNameList(PartialPath nodePath) {
    this.nodeName = nodePath;
  }

  public enum AuthorType {
    CREATE_USER,
    CREATE_ROLE,
    DROP_USER,
    DROP_ROLE,
    GRANT_ROLE,
    GRANT_USER,
    GRANT_ROLE_TO_USER,
    REVOKE_USER,
    REVOKE_ROLE,
    REVOKE_ROLE_FROM_USER,
    UPDATE_USER,
    LIST_USER,
    LIST_ROLE,
    LIST_USER_PRIVILEGE,
    LIST_ROLE_PRIVILEGE,
    LIST_USER_ROLES,
    LIST_ROLE_USERS;

    /**
     * deserialize short number.
     *
     * @param i short number
     * @return NamespaceType
     */
    public static AuthorStatement.AuthorType deserialize(short i) {
      switch (i) {
        case 0:
          return CREATE_USER;
        case 1:
          return CREATE_ROLE;
        case 2:
          return DROP_USER;
        case 3:
          return DROP_ROLE;
        case 4:
          return GRANT_ROLE;
        case 5:
          return GRANT_USER;
        case 6:
          return GRANT_ROLE_TO_USER;
        case 7:
          return REVOKE_USER;
        case 8:
          return REVOKE_ROLE;
        case 9:
          return REVOKE_ROLE_FROM_USER;
        case 10:
          return UPDATE_USER;
        case 11:
          return LIST_USER;
        case 12:
          return LIST_ROLE;
        case 13:
          return LIST_USER_PRIVILEGE;
        case 14:
          return LIST_ROLE_PRIVILEGE;
        case 15:
          return LIST_USER_ROLES;
        case 16:
          return LIST_ROLE_USERS;
        default:
          return null;
      }
    }

    /**
     * serialize.
     *
     * @return short number
     */
    public short serialize() {
      switch (this) {
        case CREATE_USER:
          return 0;
        case CREATE_ROLE:
          return 1;
        case DROP_USER:
          return 2;
        case DROP_ROLE:
          return 3;
        case GRANT_ROLE:
          return 4;
        case GRANT_USER:
          return 5;
        case GRANT_ROLE_TO_USER:
          return 6;
        case REVOKE_USER:
          return 7;
        case REVOKE_ROLE:
          return 8;
        case REVOKE_ROLE_FROM_USER:
          return 9;
        case UPDATE_USER:
          return 10;
        case LIST_USER:
          return 11;
        case LIST_ROLE:
          return 12;
        case LIST_USER_PRIVILEGE:
          return 13;
        case LIST_ROLE_PRIVILEGE:
          return 14;
        case LIST_USER_ROLES:
          return 15;
        case LIST_ROLE_USERS:
          return 16;
        default:
          return -1;
      }
    }
  }

  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    switch (this.authorType) {
      case CREATE_USER:
        return visitor.visitCreateUser(this, context);
      case CREATE_ROLE:
        return visitor.visitCreateRole(this, context);
      case DROP_USER:
        return visitor.visitDropUser(this, context);
      case DROP_ROLE:
        return visitor.visitDropRole(this, context);
      case GRANT_ROLE:
        return visitor.visitGrantRole(this, context);
      case GRANT_USER:
        return visitor.visitGrantUser(this, context);
      case GRANT_ROLE_TO_USER:
        return visitor.visitGrantRoleToUser(this, context);
      case REVOKE_USER:
        return visitor.visitRevokeUser(this, context);
      case REVOKE_ROLE:
        return visitor.visitRevokeRole(this, context);
      case REVOKE_ROLE_FROM_USER:
        return visitor.visitRevokeRoleFromUser(this, context);
      case UPDATE_USER:
        return visitor.visitAlterUser(this, context);
      case LIST_USER:
        return visitor.visitListUser(this, context);
      case LIST_ROLE:
        return visitor.visitListRole(this, context);
      case LIST_USER_PRIVILEGE:
        return visitor.visitListUserPrivileges(this, context);
      case LIST_ROLE_PRIVILEGE:
        return visitor.visitListRolePrivileges(this, context);
      case LIST_USER_ROLES:
        return visitor.visitListAllUserOfRole(this, context);
      case LIST_ROLE_USERS:
        return visitor.visitListAllRoleOfUser(this, context);
      default:
        return null;
    }
  }
}
