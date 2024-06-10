package org.apache.iotdb.confignode.consensus.request.auth;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.util.Objects;
import java.util.Set;

public abstract class AuthorPlan extends ConfigPhysicalPlan {

  private boolean isTreeModel;
  private String userName;
  private String roleName;

  private String password;
  private boolean grantOpt;

  public AuthorPlan(final ConfigPhysicalPlanType type, boolean isTreeModel) {
    super(type);
    this.isTreeModel = isTreeModel;
  }

  public String getUserName() {
    return this.userName;
  }

  public String getRoleName() {
    return roleName;
  }

  public boolean getGrantOpt() {
    return this.grantOpt;
  }

  public String getPassword() {
    return this.password;
  }

  public void setGrantOpt(boolean grantOpt) {
    this.grantOpt = grantOpt;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public void setRoleName(String roleName) {
    this.roleName = roleName;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public abstract Set<Integer> getPermissions();

  public ConfigPhysicalPlanType getAuthorType() {
    return super.getType();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AuthorPlan that = (AuthorPlan) o;
    return Objects.equals(isTreeModel, that.isTreeModel)
        && Objects.equals(userName, that.userName)
        && Objects.equals(roleName, that.roleName)
        && Objects.equals(password, that.password)
        && Objects.equals(grantOpt, that.grantOpt);
  }

  @Override
  public int hashCode() {
    return Objects.hash(isTreeModel, userName, roleName, password, grantOpt);
  }
}
