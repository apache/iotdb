package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class WindowReference extends Node implements Window {
  private final Identifier name;

  public WindowReference(NodeLocation location, Identifier name) {
    super(location);
    this.name = requireNonNull(name, "name is null");
  }

  public Identifier getName() {
    return name;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitWindowReference(this, context);
  }

  @Override
  public List<Node> getChildren() {
    return ImmutableList.of(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    WindowReference o = (WindowReference) obj;
    return Objects.equals(name, o.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  public String toString() {
    return toStringHelper(this).add("name", name).toString();
  }

  @Override
  public boolean shallowEquals(Node other) {
    return sameClass(this, other);
  }
}
