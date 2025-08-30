package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ShowServices extends Statement {
  @Nullable private final String serviceName;

  public ShowServices(NodeLocation location) {
    super(requireNonNull(location, "location is null"));
    serviceName = null;
  }

  public ShowServices(NodeLocation location, String serviceName) {
    super(requireNonNull(location, "location is null"));
    this.serviceName = serviceName;
  }

  @Nullable
  public String getServiceName() {
    return serviceName;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowServices(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(serviceName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    ShowServices o = (ShowServices) obj;
    return Objects.equals(serviceName, o.serviceName);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("serviceName", serviceName).toString();
  }
}
