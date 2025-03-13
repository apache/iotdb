package org.apache.iotdb.db.queryengine.plan.statement.metadata.model;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.List;
import java.util.Map;

public class CreateTrainingStatement extends Statement implements IConfigStatement {

  String modelId;
  String modelType;

  boolean useAllData = false;
  Map<String, String> parameters;
  String existingModelId = null;

  List<PartialPath> targetPathPatterns;

  public CreateTrainingStatement(String modelId, String modelType) {
    this.modelId = modelId;
    this.modelType = modelType;
  }

  public void setTargetPathPatterns(List<PartialPath> targetPathPatterns) {
    this.targetPathPatterns = targetPathPatterns;
  }

  public boolean isUseAllData() {
    return useAllData;
  }

  public Map<String, String> getParameters() {
    return parameters;
  }

  public String getExistingModelId() {
    return existingModelId;
  }

  public List<PartialPath> getTargetPathPatterns() {
    return targetPathPatterns;
  }

  public String getModelId() {
    return modelId;
  }

  public String getModelType() {
    return modelType;
  }

  public void setExistingModelId(String existingModelId) {
    this.existingModelId = existingModelId;
  }

  public void setModelId(String modelId) {
    this.modelId = modelId;
  }

  public void setModelType(String modelType) {
    this.modelType = modelType;
  }

  public void setParameters(Map<String, String> parameters) {
    this.parameters = parameters;
  }

  public void setUseAllData(boolean useAllData) {
    this.useAllData = useAllData;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return null;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return targetPathPatterns;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTraining(this, context);
  }
}
