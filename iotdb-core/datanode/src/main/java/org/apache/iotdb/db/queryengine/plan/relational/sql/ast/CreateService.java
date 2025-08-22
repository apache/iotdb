package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class CreateService extends Statement {
  private final String serviceName;
  private final String className;
  @Nullable private final String uriString;

  public CreateService(
      NodeLocation location, String serviceName, String className, String uriString) {
    super(requireNonNull(location, "location is null"));

    this.serviceName = serviceName;
    this.className = className;
    this.uriString = uriString;
  }

  public String getServiceName() {
    return serviceName;
  }

  public String getClassName() {
    return className;
  }

  @Nullable
  public String getUriString() {
    return uriString;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateService(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, className, uriString);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    CreateService o = (CreateService) obj;
    return Objects.equals(serviceName, o.serviceName)
        && Objects.equals(className, o.className)
        && Objects.equals(uriString, o.uriString);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("serviceName", serviceName)
        .add("className", className)
        .add("uriString", uriString)
        .toString();
  }
}
