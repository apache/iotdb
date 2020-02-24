package org.apache.iotdb.db.qp.physical.crud;

import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.query.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

public class GroupByFillPlan extends GroupByPlan {

  private Map<TSDataType, IFill> fillTypes;

  public GroupByFillPlan() {
    super();
    setOperatorType(Operator.OperatorType.GROUP_BY_FILL);
  }

  public Map<TSDataType, IFill> getFillType() {
    return fillTypes;
  }

  public void setFillType(Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }
}
