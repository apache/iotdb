package org.apache.iotdb.confignode.consensus.request.auth;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import java.util.Set;

public abstract class AuthorPlan extends ConfigPhysicalPlan {

  boolean isTreeModel;

  public AuthorPlan(final ConfigPhysicalPlanType type, boolean isTreeModel) {
    super(type);
    this.isTreeModel = isTreeModel;
  }

  public abstract String getUserName();

  public abstract String getRoleName();

  public abstract Set<Integer> getPermissions();

  public abstract boolean getGrantOpt();

  public ConfigPhysicalPlanType getAuthorType() {
    return super.getType();
  }
}
