package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class ShowModels extends Statement {

  private String modelId = null;

  public ShowModels() {
    super(null);
  }

  @Override
  public List<? extends Node> getChildren() {
    return ImmutableList.of();
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public String getModelId() {
    return modelId;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitShowModels(this, context);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ShowModels)) {
      return false;
    }
    if (modelId == null) {
      return ((ShowModels) obj).getModelId() == null;
    }
    return modelId.equals(((ShowModels) obj).getModelId());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
