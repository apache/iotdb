package org.apache.iotdb.db.qp.physical.crud;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;

public class AlignByTimePlan extends QueryPlan {

  private List<Path> deduplicatedPaths = new ArrayList<>();
  private List<TSDataType> deduplicatedDataTypes = new ArrayList<>();
  private IExpression expression = null;

  public AlignByTimePlan() {
    super();
  }

  public AlignByTimePlan(boolean isQuery, Operator.OperatorType operatorType) {
    super(isQuery, operatorType);
  }


  public IExpression getExpression() {
    return expression;
  }

  public void setExpression(IExpression expression) {
    this.expression = expression;
  }

  public List<Path> getDeduplicatedPaths() {
    return deduplicatedPaths;
  }

  public void addDeduplicatedPaths(Path path) {
    this.deduplicatedPaths.add(path);
  }

  public List<TSDataType> getDeduplicatedDataTypes() {
    return deduplicatedDataTypes;
  }

  public void addDeduplicatedDataTypes(TSDataType dataType) {
    this.deduplicatedDataTypes.add(dataType);
  }

  public void setDeduplicatedPaths(
      List<Path> deduplicatedPaths) {
    this.deduplicatedPaths = deduplicatedPaths;
  }

  public void setDeduplicatedDataTypes(
      List<TSDataType> deduplicatedDataTypes) {
    this.deduplicatedDataTypes = deduplicatedDataTypes;
  }
}
