package org.apache.iotdb.db.qp.logical.crud;

import org.apache.iotdb.db.query.executor.fill.IFill;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.Map;

public class FillQueryOperator extends QueryOperator {

  public FillQueryOperator() {
    super();
  }

  private Map<TSDataType, IFill> fillTypes;

  public Map<TSDataType, IFill> getFillTypes() {
    return fillTypes;
  }

  public void setFillTypes(Map<TSDataType, IFill> fillTypes) {
    this.fillTypes = fillTypes;
  }
}
