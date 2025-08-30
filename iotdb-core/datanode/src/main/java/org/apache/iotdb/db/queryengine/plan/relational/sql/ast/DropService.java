package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropService extends Statement {
  private final String serviceName;

  public DropService(NodeLocation location, String serviceName) {
    super(requireNonNull(location, "location is null"));
    this.serviceName = requireNonNull(serviceName, "serviceName is null");
  }

  public String getServiceName() {
    return serviceName;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropService(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DropService o = (DropService) obj;
    return Objects.equals(serviceName, o.serviceName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("serviceName", serviceName).toString();
  }
}
