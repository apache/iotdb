package org.apache.iotdb.db.metadata.plan.schemaregion.impl.write;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ICreateLogicalViewPlan;
import org.apache.iotdb.db.metadata.view.viewExpression.ViewExpression;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreateLogicalViewPlanImpl implements ICreateLogicalViewPlan {

  private PartialPath targetPath;
  private ViewExpression sourceExpression;

  public CreateLogicalViewPlanImpl(PartialPath targetPath, ViewExpression sourceExpression) {
    this.targetPath = targetPath;
    this.sourceExpression = sourceExpression;
  }

  @Override
  public int getViewSize() {
    return 1;
  }

  @Override
  public Map<PartialPath, ViewExpression> getViewPathToSourceExpressionMap() {
    Map<PartialPath, ViewExpression> result = new HashMap<>();
    result.put(this.targetPath, this.sourceExpression);
    return result;
  }

  @Override
  public List<PartialPath> getViewPathList() {
    return Collections.singletonList(this.targetPath);
  }

  @Override
  public void setViewPathToSourceExpressionMap(
      Map<PartialPath, ViewExpression> viewPathToSourceExpressionMap) {
    for (Map.Entry<PartialPath, ViewExpression> entry : viewPathToSourceExpressionMap.entrySet()) {
      this.targetPath = entry.getKey();
      this.sourceExpression = entry.getValue();
      break;
    }
  }

  public PartialPath getTargetPath() {
    return this.targetPath;
  }

  public ViewExpression getSourceExpression() {
    return this.sourceExpression;
  }
}
