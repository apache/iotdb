package org.apache.iotdb.db.qp.logical.crud;

import java.time.ZoneId;

public class SelectLastOperator extends SelectOperator {

  /** init with tokenIntType, default operatorType is <code>OperatorType.SELECT</code>. */
  public SelectLastOperator(ZoneId zoneId) {
    super(zoneId);
  }

  public boolean isLastQuery() {
    return true;
  }
}
